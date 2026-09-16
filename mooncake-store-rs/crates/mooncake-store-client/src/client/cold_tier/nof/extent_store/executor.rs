//! Mooncake-managed ExtentStore executor for NVMe-oF targets.

use std::ffi::{c_char, c_int, c_void, CString};
use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::Arc;

use parking_lot::Mutex;

use mooncake_store_core::{Result, StoreError};

use crate::client::extent_store_engine::ExtentStoreBufferPool;
use crate::client::extent_store_format::{
    align_up_checked, scan_extent_store_headers, ExtentStoreLocator, ExtentStoreRecordHeader,
    EXTENT_STORE_ALIGNMENT, EXTENT_STORE_HEADER_LEN, EXTENT_STORE_RECORD_KIND_SINGLE,
};
use crate::client::{
    decode_cold_object_manifest, encode_cold_object_manifest, payload_checksum,
    ColdPayloadMetadata, FreeSpanSet, RecoveredColdObject,
};

use super::super::backing::NofBacking;
use super::super::managed::{
    NofManagedAllocationRequest, NofManagedAllocator, NofManagedLimits, NofManagedLocator,
    NofManagedRead, NofManagedReadRequest, NofManagedRecovery, NofManagedWrite,
    NofManagedWriteRequest,
};
use super::super::physical::{NofHealth, NofStorageHealth};

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
            alignment: EXTENT_STORE_ALIGNMENT,
        }
    }
}

struct AllocatorState {
    free: FreeSpanSet,
}

pub struct ExtentStoreExecutor {
    device: Arc<SpdkNofBlockDevice>,
    config: ExtentStoreExecutorConfig,
    limits: NofManagedLimits,
    state: Mutex<AllocatorState>,
    buffer_pool: Arc<ExtentStoreBufferPool>,
}

impl ExtentStoreExecutor {
    pub fn new(device: Arc<SpdkNofBlockDevice>, config: ExtentStoreExecutorConfig) -> Result<Self> {
        let config = validate_config(config, device.capacity_bytes(), device.sector_size())?;
        let free_len = config.device_bytes - config.start_offset;
        let buffer_pool_bytes = usize::try_from(free_len).unwrap_or(usize::MAX);
        Ok(Self {
            device,
            limits: NofManagedLimits {
                max_batch_items: DEFAULT_MAX_BATCH_ITEMS,
                max_batch_bytes: free_len,
            }
            .validate()?,
            state: Mutex::new(AllocatorState {
                free: FreeSpanSet::from_range(config.start_offset, free_len),
            }),
            buffer_pool: ExtentStoreBufferPool::new_untracked(buffer_pool_bytes),
            config,
        })
    }

    fn record_header(
        &self,
        payload_len: u64,
        manifest_len: usize,
        checksum: u64,
    ) -> Result<ExtentStoreRecordHeader> {
        ExtentStoreRecordHeader::single(manifest_len, payload_len, checksum, self.config.alignment)
    }

    fn validate_locator(
        &self,
        locator: ExtentStoreLocator,
        expected_len: u64,
    ) -> Result<ExtentStoreLocator> {
        if locator.segment_id != 0 || locator.value_len != expected_len {
            return Err(StoreError::InvalidState(format!(
                "managed ExtentStore locator length {} does not match expected {}",
                locator.value_len, expected_len
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
            locator_from_managed(&request.locator)?,
            request.value.len() as u64,
        )?;
        let identity = request.route_identity.as_ref().ok_or_else(|| {
            StoreError::InvalidState(
                "managed ExtentStore write requires route identity".to_string(),
            )
        })?;
        let manifest = identity.manifest(
            request.locator.to_hex(),
            request.value.len() as u64,
            request.checksum,
        );
        let manifest_bytes = encode_cold_object_manifest(&manifest)?;
        let header = self.record_header(
            request.value.len() as u64,
            manifest_bytes.len(),
            request
                .checksum
                .unwrap_or_else(|| payload_checksum(request.value)),
        )?;
        if header.value_offset != locator.value_offset || header.record_len != locator.record_len {
            return Err(StoreError::InvalidState(
                "managed ExtentStore record layout does not match its reservation".to_string(),
            ));
        }
        let record_len = usize::try_from(locator.record_len).map_err(|_| {
            StoreError::InvalidState("managed ExtentStore record is too large".to_string())
        })?;
        let mut record = self.buffer_pool.lease(record_len)?;
        record.fill(0);
        header.encode_into(&mut record[..EXTENT_STORE_HEADER_LEN]);
        let manifest_end = EXTENT_STORE_HEADER_LEN + manifest_bytes.len();
        record[EXTENT_STORE_HEADER_LEN..manifest_end].copy_from_slice(&manifest_bytes);
        let payload_offset = locator.value_offset as usize;
        record[payload_offset..payload_offset + request.value.len()].copy_from_slice(request.value);
        self.device.write_all(locator.offset, &record)
    }

    fn read_one(&self, request: &NofManagedReadRequest) -> Result<Option<Vec<u8>>> {
        let locator =
            self.validate_locator(locator_from_managed(&request.locator)?, request.length)?;
        let record_len = usize::try_from(locator.record_len).map_err(|_| {
            StoreError::InvalidState("managed ExtentStore record is too large".to_string())
        })?;
        let mut record = self.buffer_pool.lease(record_len)?;
        match self.device.read_exact(locator.offset, &mut record) {
            Ok(()) => {}
            Err(StoreError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        }
        let header = ExtentStoreRecordHeader::decode(&record[..EXTENT_STORE_HEADER_LEN])?
            .ok_or_else(|| {
                StoreError::NotFound("managed ExtentStore record is missing".to_string())
            })?;
        if header.record_kind != EXTENT_STORE_RECORD_KIND_SINGLE
            || header.value_len != request.length
            || header.value_offset != locator.value_offset
            || header.record_len != locator.record_len
        {
            return Err(StoreError::InvalidState(format!(
                "managed ExtentStore record length {} does not match route length {}",
                header.value_len, request.length
            )));
        }
        let payload_offset = header.value_offset as usize;
        let value = record[payload_offset..payload_offset + header.value_len as usize].to_vec();
        let expected = request.checksum.unwrap_or(header.checksum);
        if expected != payload_checksum(&value) {
            return Err(StoreError::InvalidState(
                "managed ExtentStore checksum mismatch".to_string(),
            ));
        }
        Ok(Some(value))
    }

    fn release_one(&self, request: &NofManagedReadRequest) -> Result<()> {
        let locator =
            self.validate_locator(locator_from_managed(&request.locator)?, request.length)?;
        let mut state = self.state.lock();
        if state.free.contains(locator.offset, locator.record_len) {
            return Ok(());
        }
        if state.free.overlaps(locator.offset, locator.record_len) {
            return Err(StoreError::InvalidState(
                "managed ExtentStore release overlaps an existing free range".to_string(),
            ));
        }
        let tombstone_len = usize::try_from(self.config.alignment).map_err(|_| {
            StoreError::InvalidState("managed ExtentStore alignment is too large".to_string())
        })?;
        let mut tombstone = self.buffer_pool.lease(tombstone_len)?;
        tombstone.fill(0);
        self.device.write_all(locator.offset, &tombstone)?;
        self.device.flush()?;
        state.free.insert(locator.offset, locator.record_len);
        Ok(())
    }
}

impl NofManagedAllocator for ExtentStoreExecutor {
    fn recover(&self, records: &[NofManagedReadRequest]) -> Result<()> {
        let mut used = Vec::with_capacity(records.len());
        for record in records {
            used.push(
                self.validate_locator(locator_from_managed(&record.locator)?, record.length)?,
            );
        }
        used.sort_by_key(|locator| locator.offset);
        let mut cursor = self.config.start_offset;
        let mut free = FreeSpanSet::default();
        for locator in used {
            if locator.offset < cursor {
                return Err(StoreError::InvalidState(
                    "overlapping managed ExtentStore recovered locators".to_string(),
                ));
            }
            if locator.offset > cursor {
                free.insert(cursor, locator.offset - cursor);
            }
            cursor = locator.offset + locator.record_len;
        }
        if cursor < self.config.device_bytes {
            free.insert(cursor, self.config.device_bytes - cursor);
        }
        let mut state = self.state.lock();
        state.free = free;
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
                let identity = request.route_identity.as_ref().ok_or_else(|| {
                    StoreError::InvalidState(
                        "managed ExtentStore reservation requires route identity".to_string(),
                    )
                })?;
                let manifest_len = encode_cold_object_manifest(&identity.manifest(
                    "0".repeat(ExtentStoreLocator::BINARY_LEN * 2),
                    request.length,
                    request.checksum,
                ))?
                .len();
                let header = self.record_header(
                    request.length,
                    manifest_len,
                    request.checksum.unwrap_or(0),
                )?;
                let offset = state
                    .free
                    .take_first_fit(header.record_len)
                    .ok_or_else(|| {
                        StoreError::Transport(
                            "managed ExtentStore target has no free extent".to_string(),
                        )
                    })?;
                let locator = ExtentStoreLocator {
                    segment_id: 0,
                    offset,
                    record_len: header.record_len,
                    value_offset: header.value_offset,
                    value_len: request.length,
                    generation: 0,
                };
                let managed = locator_to_managed(locator)?;
                Ok(managed)
            })
            .collect()
    }

    fn release_batch(&self, requests: &[NofManagedReadRequest]) -> Vec<Result<()>> {
        requests
            .iter()
            .map(|request| self.release_one(request))
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
        let cache_len = self.device.submit_chunk_bytes.max(self.config.alignment)
            / self.config.alignment
            * self.config.alignment;
        let cache_len = cache_len.min(self.config.device_bytes - self.config.start_offset);
        let cache_len = usize::try_from(cache_len).map_err(|_| {
            StoreError::InvalidState("managed ExtentStore scan buffer is too large".to_string())
        })?;
        let mut header_cache = vec![0u8; cache_len];
        let mut cache_offset = u64::MAX;
        let mut cache_valid = 0usize;

        scan_extent_store_headers(
            self.config.start_offset,
            self.config.device_bytes,
            Some(self.config.alignment),
            |offset| {
                let cache_end = cache_offset.saturating_add(cache_valid as u64);
                if offset < cache_offset
                    || offset.saturating_add(EXTENT_STORE_HEADER_LEN as u64) > cache_end
                {
                    cache_offset = offset;
                    cache_valid = cache_len.min(
                        usize::try_from(self.config.device_bytes - offset).unwrap_or(usize::MAX),
                    );
                    match self
                        .device
                        .read_exact(offset, &mut header_cache[..cache_valid])
                    {
                        Ok(()) => {}
                        Err(StoreError::NotFound(_)) => return Ok(None),
                        Err(error) => return Err(error),
                    }
                }
                let start = usize::try_from(offset - cache_offset).expect("cached header offset");
                Ok(Some(
                    header_cache[start..start + EXTENT_STORE_HEADER_LEN]
                        .try_into()
                        .expect("extent store header slice length"),
                ))
            },
            |offset, header| {
                if header.record_kind != EXTENT_STORE_RECORD_KIND_SINGLE || header.key_len == 0 {
                    return Ok(());
                }
                let record_len = usize::try_from(header.record_len).map_err(|_| {
                    StoreError::InvalidState("managed ExtentStore record is too large".to_string())
                })?;
                let manifest_len = usize::try_from(header.key_len).map_err(|_| {
                    StoreError::InvalidState(
                        "managed ExtentStore manifest is too large".to_string(),
                    )
                })?;
                let manifest_end = EXTENT_STORE_HEADER_LEN + manifest_len;
                if manifest_end > header.value_offset as usize || manifest_end > record_len {
                    return Ok(());
                }
                let mut record = self.buffer_pool.lease(record_len)?;
                self.device.read_exact(offset, &mut record)?;
                let manifest =
                    decode_cold_object_manifest(&record[EXTENT_STORE_HEADER_LEN..manifest_end])?;
                if manifest.cold_tier_id == target_id {
                    recovered.push(RecoveredColdObject {
                        metadata: ColdPayloadMetadata {
                            length: manifest.length,
                            checksum: manifest.checksum,
                        },
                        path: PathBuf::from(format!(
                            "nof://{target_id}/{}",
                            manifest.object_locator
                        )),
                        manifest,
                    });
                }
                Ok(())
            },
        )?;
        Ok(recovered)
    }
}

impl NofHealth for ExtentStoreExecutor {
    fn health(&self) -> Result<NofStorageHealth> {
        self.device.health()?;
        let state = self.state.lock();
        Ok(NofStorageHealth {
            capacity_bytes: Some(self.config.device_bytes - self.config.start_offset),
            available_bytes: Some(state.free.total_bytes()),
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
    sector_size: u32,
) -> Result<ExtentStoreExecutorConfig> {
    if config.alignment == 0 || !config.alignment.is_power_of_two() {
        return Err(StoreError::InvalidState(
            "managed ExtentStore alignment must be a non-zero power of two".to_string(),
        ));
    }
    if !config.alignment.is_multiple_of(sector_size as u64) {
        return Err(StoreError::InvalidState(format!(
            "managed ExtentStore alignment {} is not a multiple of sector size {sector_size}",
            config.alignment
        )));
    }
    config.start_offset = align_up_checked(config.start_offset, config.alignment)?;
    if config.device_bytes == 0 || config.device_bytes > capacity {
        config.device_bytes = capacity;
    }
    config.device_bytes -= config.device_bytes % config.alignment;
    if config.start_offset >= config.device_bytes {
        return Err(StoreError::InvalidState(
            "managed ExtentStore configured range is empty".to_string(),
        ));
    }
    Ok(config)
}

fn locator_to_managed(locator: ExtentStoreLocator) -> Result<NofManagedLocator> {
    NofManagedLocator::new(locator.encode_binary())
}

fn locator_from_managed(locator: &NofManagedLocator) -> Result<ExtentStoreLocator> {
    ExtentStoreLocator::decode_binary(locator.as_bytes())
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
