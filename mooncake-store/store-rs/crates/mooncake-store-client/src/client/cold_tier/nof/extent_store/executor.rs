//! Mooncake-managed ExtentStore executor for NVMe-oF targets.

use std::collections::BTreeMap;
use std::ffi::{c_char, c_int, c_void, CString};
use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::Arc;

use parking_lot::Mutex;

use mooncake_store_core::{Result, StoreError};

use crate::client::{
    decode_cold_object_manifest, encode_cold_object_manifest, payload_checksum, ColdObjectManifest,
    ColdPayloadMetadata, RecoveredColdObject,
};

use super::super::backing::NofBacking;
use super::super::managed::{
    NofManagedAllocationRequest, NofManagedAllocator, NofManagedLimits, NofManagedLocator,
    NofManagedRead, NofManagedReadRequest, NofManagedRecovery, NofManagedWrite,
    NofManagedWriteRequest,
};
use super::super::physical::{NofHealth, NofStorageHealth};

const RECORD_MAGIC: &[u8; 8] = b"NOFEXT01";
const LOCATOR_MAGIC: &[u8; 8] = b"NOFLOC01";
const RECORD_HEADER_LEN: usize = 64;
const DEFAULT_ALIGNMENT: u64 = 4096;
const DEFAULT_MAX_BATCH_ITEMS: usize = 1024;

#[derive(Clone, Debug)]
pub struct ExtentStoreExecutorConfig {
    pub start_offset: u64,
    pub device_bytes: u64,
    pub alignment: u64,
}

impl ExtentStoreExecutorConfig {
    pub fn new(start_offset: u64, device_bytes: u64) -> Self {
        Self {
            start_offset,
            device_bytes,
            alignment: DEFAULT_ALIGNMENT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExtentLocator {
    offset: u64,
    record_len: u64,
    payload_len: u64,
}

impl ExtentLocator {
    fn to_managed(self) -> Result<NofManagedLocator> {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(LOCATOR_MAGIC);
        bytes.extend_from_slice(&self.offset.to_le_bytes());
        bytes.extend_from_slice(&self.record_len.to_le_bytes());
        bytes.extend_from_slice(&self.payload_len.to_le_bytes());
        NofManagedLocator::new(bytes)
    }

    fn from_managed(locator: &NofManagedLocator) -> Result<Self> {
        let bytes = locator.as_bytes();
        if bytes.len() != 32 || &bytes[..8] != LOCATOR_MAGIC {
            return Err(StoreError::InvalidState(
                "invalid managed ExtentStore locator".to_string(),
            ));
        }
        Ok(Self {
            offset: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            record_len: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            payload_len: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        })
    }
}

#[derive(Clone, Copy)]
struct FreeRange {
    offset: u64,
    len: u64,
}

struct AllocatorState {
    free: Vec<FreeRange>,
    reserved: BTreeMap<Vec<u8>, ReservedRecord>,
}

#[derive(Clone)]
struct ReservedRecord {
    locator: ExtentLocator,
    manifest: Option<ColdObjectManifest>,
}

struct RecordHeader {
    payload_len: u64,
    record_len: u64,
    checksum: Option<u64>,
    manifest_len: usize,
}

pub struct ExtentStoreExecutor {
    device: Arc<SpdkNofBlockDevice>,
    config: ExtentStoreExecutorConfig,
    limits: NofManagedLimits,
    state: Mutex<AllocatorState>,
}

impl ExtentStoreExecutor {
    pub fn new(device: Arc<SpdkNofBlockDevice>, config: ExtentStoreExecutorConfig) -> Result<Self> {
        let config = validate_config(config, device.capacity_bytes())?;
        let free_len = config.device_bytes - config.start_offset;
        Ok(Self {
            device,
            limits: NofManagedLimits {
                max_batch_items: DEFAULT_MAX_BATCH_ITEMS,
                max_batch_bytes: free_len,
            }
            .validate()?,
            state: Mutex::new(AllocatorState {
                free: vec![FreeRange {
                    offset: config.start_offset,
                    len: free_len,
                }],
                reserved: BTreeMap::new(),
            }),
            config,
        })
    }

    fn record_len(&self, payload_len: u64, manifest_len: usize) -> Result<u64> {
        let raw = (RECORD_HEADER_LEN as u64)
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(manifest_len as u64))
            .ok_or_else(|| {
                StoreError::InvalidState("managed ExtentStore value is too large".to_string())
            })?;
        align_up_checked(raw, self.config.alignment)
    }

    fn validate_locator(&self, locator: ExtentLocator, expected_len: u64) -> Result<ExtentLocator> {
        if locator.payload_len != expected_len {
            return Err(StoreError::InvalidState(format!(
                "managed ExtentStore locator length {} does not match expected {}",
                locator.payload_len, expected_len
            )));
        }
        let end = locator
            .offset
            .checked_add(locator.record_len)
            .ok_or_else(|| {
                StoreError::InvalidState("managed ExtentStore locator overflows".to_string())
            })?;
        if locator.offset < self.config.start_offset || end > self.config.device_bytes {
            return Err(StoreError::InvalidState(
                "managed ExtentStore locator is outside configured device range".to_string(),
            ));
        }
        if !locator.offset.is_multiple_of(self.config.alignment)
            || !locator.record_len.is_multiple_of(self.config.alignment)
        {
            return Err(StoreError::InvalidState(
                "managed ExtentStore locator is not aligned".to_string(),
            ));
        }
        Ok(locator)
    }

    fn write_one(&self, request: &NofManagedWriteRequest<'_>) -> Result<()> {
        let locator = self.validate_locator(
            ExtentLocator::from_managed(&request.locator)?,
            request.value.len() as u64,
        )?;
        let mut record = vec![0u8; locator.record_len as usize];
        let manifest = self
            .state
            .lock()
            .reserved
            .get(request.locator.as_bytes())
            .map(|record| {
                if record.locator != locator {
                    return Err(StoreError::InvalidState(
                        "managed ExtentStore reserved locator does not match request".to_string(),
                    ));
                }
                Ok(record.manifest.clone())
            })
            .transpose()?
            .flatten();
        let manifest_bytes = manifest
            .as_ref()
            .map(encode_cold_object_manifest)
            .transpose()?
            .unwrap_or_default();
        encode_header(
            &mut record[..RECORD_HEADER_LEN],
            RecordHeader {
                payload_len: locator.payload_len,
                record_len: locator.record_len,
                checksum: request.checksum,
                manifest_len: manifest_bytes.len(),
            },
        )?;
        let payload_offset = payload_offset(manifest_bytes.len())?;
        record[RECORD_HEADER_LEN..payload_offset].copy_from_slice(&manifest_bytes);
        record[payload_offset..payload_offset + request.value.len()].copy_from_slice(request.value);
        self.device.write_all(locator.offset, &record)
    }

    fn read_one(&self, request: &NofManagedReadRequest) -> Result<Option<Vec<u8>>> {
        let locator = self.validate_locator(
            ExtentLocator::from_managed(&request.locator)?,
            request.length,
        )?;
        let mut record = vec![0u8; locator.record_len as usize];
        match self.device.read_exact(locator.offset, &mut record) {
            Ok(()) => {}
            Err(StoreError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        }
        let header = decode_header(&record[..RECORD_HEADER_LEN])?;
        let payload_len = header.payload_len;
        if payload_len != request.length {
            return Err(StoreError::InvalidState(format!(
                "managed ExtentStore record length {payload_len} does not match route length {}",
                request.length
            )));
        }
        let payload_offset = payload_offset(header.manifest_len)?;
        let value = record[payload_offset..payload_offset + payload_len as usize].to_vec();
        let expected = request.checksum.or(header.checksum);
        if expected.is_some_and(|checksum| checksum != payload_checksum(&value)) {
            return Err(StoreError::InvalidState(
                "managed ExtentStore checksum mismatch".to_string(),
            ));
        }
        Ok(Some(value))
    }
}

impl NofManagedAllocator for ExtentStoreExecutor {
    fn recover(&self, records: &[NofManagedReadRequest]) -> Result<()> {
        let mut used = Vec::with_capacity(records.len());
        for record in records {
            used.push(
                self.validate_locator(
                    ExtentLocator::from_managed(&record.locator)?,
                    record.length,
                )?,
            );
        }
        used.sort_by_key(|locator| locator.offset);
        let mut cursor = self.config.start_offset;
        let mut free = Vec::new();
        for locator in used {
            if locator.offset < cursor {
                return Err(StoreError::InvalidState(
                    "overlapping managed ExtentStore recovered locators".to_string(),
                ));
            }
            if locator.offset > cursor {
                free.push(FreeRange {
                    offset: cursor,
                    len: locator.offset - cursor,
                });
            }
            cursor = locator.offset + locator.record_len;
        }
        if cursor < self.config.device_bytes {
            free.push(FreeRange {
                offset: cursor,
                len: self.config.device_bytes - cursor,
            });
        }
        let mut state = self.state.lock();
        state.free = free;
        state.reserved.clear();
        Ok(())
    }

    fn reserve_batch(
        &self,
        requests: &[NofManagedAllocationRequest],
    ) -> Vec<Result<NofManagedLocator>> {
        let mut state = self.state.lock();
        requests
            .iter()
            .map(|request| {
                let manifest_len = request
                    .route_identity
                    .as_ref()
                    .map(|identity| {
                        encode_cold_object_manifest(&identity.manifest(
                            "0".repeat(64),
                            request.length,
                            request.checksum,
                        ))
                        .map(|bytes| bytes.len())
                    })
                    .transpose()?
                    .unwrap_or(0);
                let record_len = self.record_len(request.length, manifest_len)?;
                let offset = allocate(&mut state.free, record_len).ok_or_else(|| {
                    StoreError::Transport(
                        "managed ExtentStore target has no free extent".to_string(),
                    )
                })?;
                let locator = ExtentLocator {
                    offset,
                    record_len,
                    payload_len: request.length,
                };
                let managed = locator.to_managed()?;
                let manifest = request.route_identity.as_ref().map(|identity| {
                    identity.manifest(managed.to_hex(), request.length, request.checksum)
                });
                state.reserved.insert(
                    managed.as_bytes().to_vec(),
                    ReservedRecord { locator, manifest },
                );
                Ok(managed)
            })
            .collect()
    }

    fn release_batch(&self, requests: &[NofManagedReadRequest]) -> Vec<Result<()>> {
        let mut state = self.state.lock();
        requests
            .iter()
            .map(|request| {
                let locator = self.validate_locator(
                    ExtentLocator::from_managed(&request.locator)?,
                    request.length,
                )?;
                state.reserved.remove(request.locator.as_bytes());
                if range_is_free(&state.free, locator.offset, locator.record_len) {
                    return Ok(());
                }
                if range_overlaps_free(&state.free, locator.offset, locator.record_len) {
                    return Err(StoreError::InvalidState(
                        "managed ExtentStore release overlaps an existing free range".to_string(),
                    ));
                }
                release(&mut state.free, locator.offset, locator.record_len);
                Ok(())
            })
            .collect()
    }
}

impl NofManagedWrite for ExtentStoreExecutor {
    fn put_batch(&self, requests: &[NofManagedWriteRequest<'_>]) -> Vec<Result<()>> {
        requests
            .iter()
            .map(|request| self.write_one(request))
            .collect()
    }
    fn flush(&self) -> Result<()> {
        self.device.flush()
    }
}

impl NofManagedRead for ExtentStoreExecutor {
    fn get_batch(&self, requests: &[NofManagedReadRequest]) -> Vec<Result<Option<Vec<u8>>>> {
        requests
            .iter()
            .map(|request| self.read_one(request))
            .collect()
    }
}

impl NofManagedRecovery for ExtentStoreExecutor {
    fn scan_recovered_objects(&self, target_id: &str) -> Result<Vec<RecoveredColdObject>> {
        let mut recovered = Vec::new();
        let mut offset = self.config.start_offset;
        let probe_len = usize::try_from(self.config.alignment).map_err(|_| {
            StoreError::InvalidState("managed ExtentStore alignment is too large".to_string())
        })?;
        while offset + RECORD_HEADER_LEN as u64 <= self.config.device_bytes {
            let mut header_block = vec![0u8; probe_len];
            match self.device.read_exact(offset, &mut header_block) {
                Ok(()) => {}
                Err(StoreError::NotFound(_)) => {
                    offset += self.config.alignment;
                    continue;
                }
                Err(error) => return Err(error),
            }
            let header = match decode_header(&header_block[..RECORD_HEADER_LEN]) {
                Ok(header) => header,
                Err(StoreError::NotFound(_)) => {
                    offset += self.config.alignment;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if header.record_len == 0
                || header.record_len % self.config.alignment != 0
                || offset + header.record_len > self.config.device_bytes
            {
                offset += self.config.alignment;
                continue;
            }
            if header.manifest_len == 0 {
                offset += header.record_len;
                continue;
            }
            let record_len = usize::try_from(header.record_len).map_err(|_| {
                StoreError::InvalidState("managed ExtentStore record is too large".to_string())
            })?;
            let manifest_end = payload_offset(header.manifest_len)?;
            if manifest_end > record_len {
                offset += self.config.alignment;
                continue;
            }
            let mut record = vec![0u8; record_len];
            self.device.read_exact(offset, &mut record)?;
            let manifest = decode_cold_object_manifest(&record[RECORD_HEADER_LEN..manifest_end])?;
            if manifest.cold_tier_id == target_id {
                recovered.push(RecoveredColdObject {
                    metadata: ColdPayloadMetadata {
                        length: manifest.length,
                        checksum: manifest.checksum,
                    },
                    path: PathBuf::from(format!("nof://{target_id}/{}", manifest.object_locator)),
                    manifest,
                });
            }
            offset += header.record_len;
        }
        Ok(recovered)
    }
}

impl NofHealth for ExtentStoreExecutor {
    fn health(&self) -> Result<NofStorageHealth> {
        self.device.health()?;
        let state = self.state.lock();
        Ok(NofStorageHealth {
            capacity_bytes: Some(self.config.device_bytes - self.config.start_offset),
            available_bytes: Some(state.free.iter().map(|range| range.len).sum()),
        })
    }
}

impl NofBacking for ExtentStoreExecutor {
    fn health_capability(&self) -> Option<&dyn NofHealth> {
        Some(self)
    }
    fn managed_allocator(&self) -> Option<&dyn NofManagedAllocator> {
        Some(self)
    }
    fn managed_read(&self) -> Option<&dyn NofManagedRead> {
        Some(self)
    }
    fn managed_recovery(&self) -> Option<&dyn NofManagedRecovery> {
        Some(self)
    }
    fn managed_write(&self) -> Option<&dyn NofManagedWrite> {
        Some(self)
    }
    fn managed_limits(&self) -> Option<NofManagedLimits> {
        Some(self.limits)
    }
}

fn validate_config(
    mut config: ExtentStoreExecutorConfig,
    capacity: u64,
) -> Result<ExtentStoreExecutorConfig> {
    if config.alignment == 0 || !config.alignment.is_power_of_two() {
        return Err(StoreError::InvalidState(
            "managed ExtentStore alignment must be a non-zero power of two".to_string(),
        ));
    }
    config.start_offset = align_up_checked(config.start_offset, config.alignment)?;
    if config.device_bytes == 0 || config.device_bytes > capacity {
        config.device_bytes = capacity;
    }
    if config.start_offset >= config.device_bytes {
        return Err(StoreError::InvalidState(
            "managed ExtentStore configured range is empty".to_string(),
        ));
    }
    Ok(config)
}

fn align_up_checked(value: u64, alignment: u64) -> Result<u64> {
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| {
            StoreError::InvalidState("managed ExtentStore alignment overflow".to_string())
        })
}

fn allocate(free: &mut Vec<FreeRange>, len: u64) -> Option<u64> {
    let index = free.iter().position(|range| range.len >= len)?;
    let offset = free[index].offset;
    free[index].offset += len;
    free[index].len -= len;
    if free[index].len == 0 {
        free.remove(index);
    }
    Some(offset)
}

fn release(free: &mut Vec<FreeRange>, offset: u64, len: u64) {
    free.push(FreeRange { offset, len });
    free.sort_by_key(|range| range.offset);
    let mut merged: Vec<FreeRange> = Vec::with_capacity(free.len());
    for range in free.drain(..) {
        if let Some(last) = merged.last_mut() {
            if last.offset + last.len >= range.offset {
                let end = (last.offset + last.len).max(range.offset + range.len);
                last.len = end - last.offset;
                continue;
            }
        }
        merged.push(range);
    }
    *free = merged;
}

fn range_is_free(free: &[FreeRange], offset: u64, len: u64) -> bool {
    let Some(end) = offset.checked_add(len) else {
        return false;
    };
    free.iter().any(|range| {
        let range_end = range.offset.saturating_add(range.len);
        range.offset <= offset && end <= range_end
    })
}

fn range_overlaps_free(free: &[FreeRange], offset: u64, len: u64) -> bool {
    let Some(end) = offset.checked_add(len) else {
        return true;
    };
    free.iter().any(|range| {
        let range_end = range.offset.saturating_add(range.len);
        offset < range_end && range.offset < end
    })
}

fn payload_offset(manifest_len: usize) -> Result<usize> {
    RECORD_HEADER_LEN.checked_add(manifest_len).ok_or_else(|| {
        StoreError::InvalidState("managed ExtentStore manifest is too large".to_string())
    })
}

fn encode_header(dst: &mut [u8], header: RecordHeader) -> Result<()> {
    dst.fill(0);
    dst[..8].copy_from_slice(RECORD_MAGIC);
    dst[8..16].copy_from_slice(&header.payload_len.to_le_bytes());
    dst[32..40].copy_from_slice(&header.record_len.to_le_bytes());
    let manifest_len = u32::try_from(header.manifest_len).map_err(|_| {
        StoreError::InvalidState(format!(
            "managed ExtentStore manifest is too large: {}",
            header.manifest_len
        ))
    })?;
    dst[40..44].copy_from_slice(&manifest_len.to_le_bytes());
    if let Some(checksum) = header.checksum {
        dst[16..24].copy_from_slice(&checksum.to_le_bytes());
        dst[24] = 1;
    }
    Ok(())
}

fn decode_header(src: &[u8]) -> Result<RecordHeader> {
    if src.len() < RECORD_HEADER_LEN || &src[..8] != RECORD_MAGIC {
        return Err(StoreError::NotFound(
            "managed ExtentStore record is missing".to_string(),
        ));
    }
    Ok(RecordHeader {
        payload_len: u64::from_le_bytes(src[8..16].try_into().unwrap()),
        record_len: u64::from_le_bytes(src[32..40].try_into().unwrap()),
        checksum: (src[24] == 1).then(|| u64::from_le_bytes(src[16..24].try_into().unwrap())),
        manifest_len: u32::from_le_bytes(src[40..44].try_into().unwrap()) as usize,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpdkNofTransport {
    Tcp,
    Rdma,
}

impl SpdkNofTransport {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Rdma => "RDMA",
        }
    }
}

#[derive(Clone, Debug)]
pub struct SpdkNofBlockDeviceConfig {
    pub transport: SpdkNofTransport,
    pub traddr: String,
    pub port: String,
    pub subnqn: String,
    pub nsid: u32,
    pub hostnqn: Option<String>,
    pub no_huge: bool,
    pub submit_chunk_bytes: u64,
}

impl SpdkNofBlockDeviceConfig {
    pub fn tcp(traddr: &str, port: &str, subnqn: &str, nsid: u32) -> Self {
        Self::new(SpdkNofTransport::Tcp, traddr, port, subnqn, nsid)
    }

    pub fn rdma(traddr: &str, port: &str, subnqn: &str, nsid: u32) -> Self {
        Self::new(SpdkNofTransport::Rdma, traddr, port, subnqn, nsid)
    }

    fn new(transport: SpdkNofTransport, traddr: &str, port: &str, subnqn: &str, nsid: u32) -> Self {
        Self {
            transport,
            traddr: traddr.to_string(),
            port: port.to_string(),
            subnqn: subnqn.to_string(),
            nsid,
            hostnqn: None,
            no_huge: true,
            submit_chunk_bytes: 4 * 1024 * 1024,
        }
    }
}

pub struct SpdkNofBlockDevice {
    raw: NonNull<c_void>,
    capacity_bytes: u64,
    sector_size: u32,
    submit_chunk_bytes: u64,
}

unsafe impl Send for SpdkNofBlockDevice {}
unsafe impl Sync for SpdkNofBlockDevice {}

impl SpdkNofBlockDevice {
    pub fn connect(config: SpdkNofBlockDeviceConfig) -> Result<Self> {
        let transport = cstring("transport", config.transport.as_str())?;
        let traddr = cstring("traddr", &config.traddr)?;
        let port = cstring("port", &config.port)?;
        let subnqn = cstring("subnqn", &config.subnqn)?;
        let hostnqn = config
            .hostnqn
            .as_deref()
            .map(|value| cstring("hostnqn", value))
            .transpose()?;
        let mut capacity = 0u64;
        let mut sector_size = 0u32;
        let mut error = vec![0 as c_char; 256];
        let raw = unsafe {
            mc_nof_connect(
                transport.as_ptr(),
                traddr.as_ptr(),
                port.as_ptr(),
                subnqn.as_ptr(),
                hostnqn
                    .as_ref()
                    .map_or(std::ptr::null(), |value| value.as_ptr()),
                config.nsid,
                i32::from(config.no_huge),
                &mut capacity,
                &mut sector_size,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        let raw = NonNull::new(raw).ok_or_else(|| StoreError::Transport(read_error(&error)))?;
        if capacity == 0 || sector_size == 0 {
            unsafe { mc_nof_close(raw.as_ptr()) };
            return Err(StoreError::Transport(
                "SPDK NoF target reported an empty geometry".to_string(),
            ));
        }
        Ok(Self {
            raw,
            capacity_bytes: capacity,
            sector_size,
            submit_chunk_bytes: config.submit_chunk_bytes.max(sector_size as u64),
        })
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }
    pub fn sector_size(&self) -> u32 {
        self.sector_size
    }

    fn health(&self) -> Result<()> {
        let mut error = vec![0 as c_char; 256];
        let rc = unsafe { mc_nof_health(self.raw.as_ptr(), error.as_mut_ptr(), error.len()) };
        if rc == 0 {
            Ok(())
        } else {
            Err(StoreError::Transport(read_error(&error)))
        }
    }

    fn write_all(&self, offset: u64, src: &[u8]) -> Result<()> {
        self.for_each_chunk(offset, src.len() as u64, |chunk_offset, chunk_len| {
            let start = (chunk_offset - offset) as usize;
            self.write_chunk(chunk_offset, &src[start..start + chunk_len as usize])
        })
    }

    fn read_exact(&self, offset: u64, dst: &mut [u8]) -> Result<()> {
        let base = offset;
        self.for_each_chunk(offset, dst.len() as u64, |chunk_offset, chunk_len| {
            let start = (chunk_offset - base) as usize;
            self.read_chunk(chunk_offset, &mut dst[start..start + chunk_len as usize])
        })
    }

    fn flush(&self) -> Result<()> {
        let mut error = vec![0 as c_char; 256];
        let rc = unsafe { mc_nof_flush(self.raw.as_ptr(), error.as_mut_ptr(), error.len()) };
        if rc == 0 {
            Ok(())
        } else {
            Err(StoreError::Transport(read_error(&error)))
        }
    }

    fn for_each_chunk(
        &self,
        offset: u64,
        len: u64,
        mut f: impl FnMut(u64, u64) -> Result<()>,
    ) -> Result<()> {
        if !offset.is_multiple_of(self.sector_size as u64)
            || !len.is_multiple_of(self.sector_size as u64)
        {
            return Err(StoreError::InvalidState(
                "SPDK NoF I/O must be sector aligned".to_string(),
            ));
        }
        let mut remaining = len;
        let mut cursor = offset;
        while remaining > 0 {
            let chunk = remaining.min(self.submit_chunk_bytes);
            f(cursor, chunk)?;
            cursor += chunk;
            remaining -= chunk;
        }
        Ok(())
    }

    fn write_chunk(&self, offset: u64, src: &[u8]) -> Result<()> {
        let mut error = vec![0 as c_char; 256];
        let rc = unsafe {
            mc_nof_write(
                self.raw.as_ptr(),
                offset,
                src.as_ptr() as *const c_void,
                src.len() as u64,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(StoreError::Transport(read_error(&error)))
        }
    }

    fn read_chunk(&self, offset: u64, dst: &mut [u8]) -> Result<()> {
        let mut error = vec![0 as c_char; 256];
        let rc = unsafe {
            mc_nof_read(
                self.raw.as_ptr(),
                offset,
                dst.as_mut_ptr() as *mut c_void,
                dst.len() as u64,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(StoreError::Transport(read_error(&error)))
        }
    }
}

impl Drop for SpdkNofBlockDevice {
    fn drop(&mut self) {
        unsafe { mc_nof_close(self.raw.as_ptr()) };
    }
}

fn cstring(field: &str, value: &str) -> Result<CString> {
    CString::new(value).map_err(|_| StoreError::InvalidState(format!("SPDK {field} contains NUL")))
}

fn read_error(buffer: &[c_char]) -> String {
    let bytes = buffer
        .iter()
        .map(|byte| *byte as u8)
        .take_while(|byte| *byte != 0)
        .collect::<Vec<_>>();
    if bytes.is_empty() {
        "SPDK NoF operation failed".to_string()
    } else {
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

extern "C" {
    fn mc_nof_connect(
        transport: *const c_char,
        traddr: *const c_char,
        trsvcid: *const c_char,
        subnqn: *const c_char,
        hostnqn: *const c_char,
        nsid: u32,
        no_huge: c_int,
        capacity_bytes: *mut u64,
        sector_size: *mut u32,
        err: *mut c_char,
        err_len: usize,
    ) -> *mut c_void;
    fn mc_nof_close(device: *mut c_void);
    fn mc_nof_health(device: *mut c_void, err: *mut c_char, err_len: usize) -> c_int;
    fn mc_nof_write(
        device: *mut c_void,
        offset: u64,
        src: *const c_void,
        len: u64,
        err: *mut c_char,
        err_len: usize,
    ) -> c_int;
    fn mc_nof_read(
        device: *mut c_void,
        offset: u64,
        dst: *mut c_void,
        len: u64,
        err: *mut c_char,
        err_len: usize,
    ) -> c_int;
    fn mc_nof_flush(device: *mut c_void, err: *mut c_char, err_len: usize) -> c_int;
}
