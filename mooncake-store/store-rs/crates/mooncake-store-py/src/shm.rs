use std::collections::BTreeMap;
use std::ffi::CString;
use std::io::{Read, Write};
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

use mooncake_store_core::{HugePageConfig, Result, StoreError};
use parking_lot::Mutex;

const SHM_ALLOCATOR_NAME: &str = "mooncake_store_rs_shm";
const SHM_REGISTER_MAGIC: u32 = 0x4d435348;
const HOT_CACHE_FD_MAGIC: u32 = 0x4d434846;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DummyClientId {
    pub high: u64,
    pub low: u64,
}

impl DummyClientId {
    pub fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let counter = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id() as u64;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos() as u64;
        Self {
            high: now ^ (pid << 32),
            low: counter,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ShmRegisterRequest {
    pub magic: u32,
    pub _reserved: u32,
    pub client_id_hi: u64,
    pub client_id_lo: u64,
    pub region_id: u64,
    pub size: u64,
}

impl ShmRegisterRequest {
    pub fn new(client_id: DummyClientId, region_id: u64, size: usize) -> Self {
        Self {
            magic: SHM_REGISTER_MAGIC,
            _reserved: 0,
            client_id_hi: client_id.high,
            client_id_lo: client_id.low,
            region_id,
            size: size as u64,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HotCacheFdRequest {
    pub magic: u32,
    pub _reserved: u32,
    pub client_id_hi: u64,
    pub client_id_lo: u64,
}

impl HotCacheFdRequest {
    pub fn new(client_id: DummyClientId) -> Self {
        Self {
            magic: HOT_CACHE_FD_MAGIC,
            _reserved: 0,
            client_id_hi: client_id.high,
            client_id_lo: client_id.low,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HotCacheFdResponse {
    pub magic: u32,
    pub status: i32,
    pub size: u64,
}

#[derive(Debug)]
struct SharedRegion {
    region_id: u64,
    base: usize,
    requested_len: usize,
    mapped_len: usize,
    fd: OwnedFd,
}

#[derive(Debug)]
pub struct SharedRegionRegistration {
    pub region_id: u64,
    pub requested_len: usize,
    pub registered_len: usize,
    pub fd: OwnedFd,
}

#[derive(Clone, Copy, Debug)]
pub struct ResolvedSharedRegion {
    pub region_id: u64,
    pub base: usize,
    pub offset: usize,
    pub len: usize,
}

#[derive(Debug)]
pub struct OwnedMappedRegion {
    base_addr: usize,
    len: usize,
    _fd: OwnedFd,
}

impl OwnedMappedRegion {
    pub fn base(&self) -> usize {
        self.base_addr
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn slice(&self, offset: usize, len: usize) -> Result<&[u8]> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| StoreError::Allocator("mapped region range overflow".to_string()))?;
        if end > self.len {
            return Err(StoreError::Allocator(format!(
                "mapped region slice exceeds range: offset={offset} len={len} region_len={}",
                self.len
            )));
        }
        let ptr = (self.base_addr + offset) as *const u8;
        Ok(unsafe { slice::from_raw_parts(ptr, len) })
    }

    #[allow(clippy::mut_from_ref)]
    pub fn slice_mut(&self, offset: usize, len: usize) -> Result<&mut [u8]> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| StoreError::Allocator("mapped region range overflow".to_string()))?;
        if end > self.len {
            return Err(StoreError::Allocator(format!(
                "mapped region slice exceeds range: offset={offset} len={len} region_len={}",
                self.len
            )));
        }
        let ptr = (self.base_addr + offset) as *mut u8;
        Ok(unsafe { slice::from_raw_parts_mut(ptr, len) })
    }
}

impl Drop for OwnedMappedRegion {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base_addr as *mut libc::c_void, self.len);
        }
    }
}

static SHARED_REGISTRY: LazyLock<Mutex<BTreeMap<usize, SharedRegion>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
static NEXT_REGION_ID: AtomicU64 = AtomicU64::new(1);

pub fn allocate_shared_region(size: usize) -> Result<usize> {
    allocate_shared_region_with_options(size, None, None)
}

pub fn allocate_shared_region_with_options(
    size: usize,
    hugepage_enabled: Option<bool>,
    hugepage_size_bytes: Option<usize>,
) -> Result<usize> {
    if size == 0 {
        return Err(StoreError::Allocator(
            "shared region size must be greater than zero".to_string(),
        ));
    }
    let hugepage = HugePageConfig::resolve(hugepage_enabled, hugepage_size_bytes)?;
    let mapped_len = hugepage.map(|hp| hp.align_up(size)).unwrap_or(size);
    let fd = memfd_create(SHM_ALLOCATOR_NAME, hugepage)?;
    ftruncate(fd.as_raw_fd(), mapped_len)?;
    let mapping = mmap_shared(fd.as_raw_fd(), mapped_len)?;
    let base = mapping as usize;
    let region = SharedRegion {
        region_id: NEXT_REGION_ID.fetch_add(1, Ordering::Relaxed),
        base,
        requested_len: size,
        mapped_len,
        fd,
    };
    SHARED_REGISTRY.lock().insert(base, region);
    Ok(base)
}

pub fn free_shared_region(ptr: usize) -> Result<()> {
    let Some(region) = SHARED_REGISTRY.lock().remove(&ptr) else {
        return Err(StoreError::NotFound(format!(
            "shared region {ptr:#x} not found"
        )));
    };
    unsafe {
        libc::munmap(region.base as *mut libc::c_void, region.mapped_len);
    }
    Ok(())
}

pub fn shared_region_for_registration(ptr: usize, size: usize) -> Result<SharedRegionRegistration> {
    let registry = SHARED_REGISTRY.lock();
    let Some(region) = registry.get(&ptr) else {
        return Err(StoreError::NotFound(format!(
            "shared region base {ptr:#x} not found"
        )));
    };
    if region.requested_len != size {
        return Err(StoreError::Allocator(format!(
            "shared region size mismatch: requested={size} actual={}",
            region.requested_len
        )));
    }
    Ok(SharedRegionRegistration {
        region_id: region.region_id,
        requested_len: region.requested_len,
        registered_len: region.mapped_len,
        fd: dup_fd(region.fd.as_raw_fd())?,
    })
}

pub fn resolve_shared_region(ptr: usize, size: usize) -> Result<ResolvedSharedRegion> {
    let end = ptr
        .checked_add(size)
        .ok_or_else(|| StoreError::Allocator("shared region range overflow".to_string()))?;
    let registry = SHARED_REGISTRY.lock();
    let Some((_, region)) = registry.range(..=ptr).next_back() else {
        return Err(StoreError::NotFound(format!(
            "shared region containing {ptr:#x} not found"
        )));
    };
    let region_end = region.base.saturating_add(region.mapped_len);
    if end > region_end {
        return Err(StoreError::Allocator(format!(
            "buffer range {ptr:#x}..{end:#x} exceeds shared region {:x}..{:x}",
            region.base, region_end
        )));
    }
    Ok(ResolvedSharedRegion {
        region_id: region.region_id,
        base: region.base,
        offset: ptr - region.base,
        len: size,
    })
}

fn sanitize_scope_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => ch,
            '-' => '-',
            _ => '_',
        })
        .collect::<String>()
}

fn abbreviate_scope_component(value: &str, max_chars: usize) -> String {
    sanitize_scope_component(value)
        .chars()
        .take(max_chars)
        .collect::<String>()
}

fn stable_socket_hash(server_addr: &str, worker_scope: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in server_addr
        .bytes()
        .chain(std::iter::once(0xff))
        .chain(worker_scope.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn scoped_ipc_socket_path(prefix: &str, server_addr: &str, worker_scope: &str) -> PathBuf {
    let addr = abbreviate_scope_component(server_addr, 16);
    let scope = abbreviate_scope_component(worker_scope, 24);
    let hash = stable_socket_hash(server_addr, worker_scope);
    std::env::temp_dir().join(format!("{prefix}-{addr}-{scope}-{hash:016x}.sock"))
}

pub fn dummy_ipc_socket_path(server_addr: &str, worker_scope: &str) -> PathBuf {
    scoped_ipc_socket_path("mc-d", server_addr, worker_scope)
}

pub fn hot_cache_ipc_socket_path(server_addr: &str, worker_scope: &str) -> PathBuf {
    scoped_ipc_socket_path("mc-dh", server_addr, worker_scope)
}

pub fn send_shm_register_request(
    socket_path: &Path,
    request: &ShmRegisterRequest,
    fd: &OwnedFd,
) -> Result<()> {
    let stream = UnixStream::connect(socket_path).map_err(|error| {
        StoreError::Transport(format!(
            "failed to connect shm socket {}: {error}",
            socket_path.display()
        ))
    })?;
    send_fd(stream.as_raw_fd(), fd.as_raw_fd(), as_bytes(request))?;
    let mut status = [0u8; 4];
    (&stream).read_exact(&mut status).map_err(|error| {
        StoreError::Transport(format!(
            "failed to read shm registration reply {}: {error}",
            socket_path.display()
        ))
    })?;
    let code = i32::from_ne_bytes(status);
    if code != 0 {
        return Err(StoreError::Transport(format!(
            "shm registration rejected with status {code}"
        )));
    }
    Ok(())
}

pub fn send_shm_register_reply(stream: &UnixStream, status: i32) -> Result<()> {
    let mut stream = stream;
    stream.write_all(&status.to_ne_bytes()).map_err(|error| {
        StoreError::Transport(format!("failed to ack shm registration: {error}"))
    })?;
    Ok(())
}

pub fn recv_shm_register_request(
    listener: &UnixListener,
) -> Result<(UnixStream, ShmRegisterRequest, OwnedFd)> {
    let (stream, _) = listener.accept().map_err(|error| {
        StoreError::Transport(format!("failed to accept shm registration: {error}"))
    })?;
    let (request, fd) = recv_fd::<ShmRegisterRequest>(stream.as_raw_fd())?;
    if request.magic != SHM_REGISTER_MAGIC {
        let _ = send_shm_register_reply(&stream, -1);
        return Err(StoreError::Transport(format!(
            "invalid shm registration magic: {}",
            request.magic
        )));
    }
    Ok((stream, request, fd))
}

pub fn recv_hot_cache_fd_request(
    listener: &UnixListener,
) -> Result<(UnixStream, HotCacheFdRequest)> {
    let (stream, _) = listener.accept().map_err(|error| {
        StoreError::Transport(format!("failed to accept hot cache fd request: {error}"))
    })?;
    let mut request: HotCacheFdRequest = unsafe { zeroed() };
    (&stream)
        .read_exact(as_bytes_mut(&mut request))
        .map_err(|error| {
            StoreError::Transport(format!("failed to read hot cache fd request: {error}"))
        })?;
    if request.magic != HOT_CACHE_FD_MAGIC {
        return Err(StoreError::Transport(format!(
            "invalid hot cache fd magic: {}",
            request.magic
        )));
    }
    Ok((stream, request))
}

pub fn send_hot_cache_fd_response(stream: &UnixStream, fd: &OwnedFd, size: usize) -> Result<()> {
    let response = HotCacheFdResponse {
        magic: HOT_CACHE_FD_MAGIC,
        status: 0,
        size: size as u64,
    };
    send_fd(stream.as_raw_fd(), fd.as_raw_fd(), as_bytes(&response))
}

pub fn request_hot_cache_region(
    socket_path: &Path,
    client_id: DummyClientId,
) -> Result<OwnedMappedRegion> {
    let stream = UnixStream::connect(socket_path).map_err(|error| {
        StoreError::Transport(format!(
            "failed to connect hot cache socket {}: {error}",
            socket_path.display()
        ))
    })?;
    let request = HotCacheFdRequest::new(client_id);
    (&stream).write_all(as_bytes(&request)).map_err(|error| {
        StoreError::Transport(format!(
            "failed to write hot cache fd request {}: {error}",
            socket_path.display()
        ))
    })?;
    let (response, fd) = recv_fd::<HotCacheFdResponse>(stream.as_raw_fd())?;
    if response.magic != HOT_CACHE_FD_MAGIC || response.status != 0 || response.size == 0 {
        return Err(StoreError::Transport(format!(
            "hot cache fd request failed: status={} size={}",
            response.status, response.size
        )));
    }
    map_registered_region(fd, response.size as usize)
}

pub fn bind_shm_listener(socket_path: &Path) -> Result<UnixListener> {
    if socket_path.exists() {
        std::fs::remove_file(socket_path).map_err(|error| {
            StoreError::Transport(format!(
                "failed to remove stale shm socket {}: {error}",
                socket_path.display()
            ))
        })?;
    }
    let listener = UnixListener::bind(socket_path).map_err(|error| {
        StoreError::Transport(format!(
            "failed to bind shm socket {}: {error}",
            socket_path.display()
        ))
    })?;
    Ok(listener)
}

pub fn map_registered_region(fd: OwnedFd, size: usize) -> Result<OwnedMappedRegion> {
    let ptr = mmap_shared(fd.as_raw_fd(), size)?;
    Ok(OwnedMappedRegion {
        base_addr: ptr as usize,
        len: size,
        _fd: fd,
    })
}

fn memfd_create(name: &str, hugepage: Option<HugePageConfig>) -> Result<OwnedFd> {
    let name =
        CString::new(name).map_err(|_| StoreError::Allocator("invalid memfd name".to_string()))?;
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (name, hugepage);
        return Err(StoreError::Unsupported(
            "memfd-backed shared memory is only supported on Linux".to_string(),
        ));
    }

    #[cfg(target_os = "linux")]
    {
        let flags = libc::MFD_CLOEXEC | hugepage.map(hugepage_memfd_flags).unwrap_or(0);
        let fd = unsafe { libc::memfd_create(name.as_ptr(), flags) };
        if fd < 0 {
            return Err(StoreError::Allocator(format!(
                "memfd_create failed{}: {}",
                hugepage
                    .map(|cfg| format!(" for hugepage {}", cfg.label()))
                    .unwrap_or_default(),
                std::io::Error::last_os_error()
            )));
        }
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

fn ftruncate(fd: RawFd, size: usize) -> Result<()> {
    let rc = unsafe { libc::ftruncate(fd, size as libc::off_t) };
    if rc != 0 {
        return Err(StoreError::Allocator(format!(
            "ftruncate failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn mmap_shared(fd: RawFd, size: usize) -> Result<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | map_populate_flag(),
            fd,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(StoreError::Allocator(format!(
            "mmap failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(ptr.cast::<u8>())
}

#[cfg(target_os = "linux")]
fn map_populate_flag() -> i32 {
    libc::MAP_POPULATE
}

#[cfg(not(target_os = "linux"))]
fn map_populate_flag() -> i32 {
    0
}

#[cfg(target_os = "linux")]
fn hugepage_memfd_flags(hugepage: HugePageConfig) -> u32 {
    libc::MFD_HUGETLB
        | match hugepage.bytes() {
            size if size == 2 * 1024 * 1024 => libc::MFD_HUGE_2MB,
            size if size == 1024 * 1024 * 1024 => libc::MFD_HUGE_1GB,
            _ => 0,
        }
}

fn dup_fd(fd: RawFd) -> Result<OwnedFd> {
    let new_fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if new_fd < 0 {
        return Err(StoreError::Allocator(format!(
            "dup fd failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(new_fd) })
}

fn as_bytes<T>(value: &T) -> &[u8] {
    unsafe { slice::from_raw_parts((value as *const T).cast::<u8>(), size_of::<T>()) }
}

fn as_bytes_mut<T>(value: &mut T) -> &mut [u8] {
    unsafe { slice::from_raw_parts_mut((value as *mut T).cast::<u8>(), size_of::<T>()) }
}

fn send_fd(sock: RawFd, fd: RawFd, payload: &[u8]) -> Result<()> {
    let mut control = [0u8; 64];
    let mut iov = libc::iovec {
        iov_base: payload.as_ptr().cast_mut().cast(),
        iov_len: payload.len(),
    };
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = control.len() as _;
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(StoreError::Transport(
                "failed to allocate control message".to_string(),
            ));
        }
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as _) as _;
        ptr::write(libc::CMSG_DATA(cmsg).cast::<RawFd>(), fd);
        msg.msg_controllen = (*cmsg).cmsg_len as _;
    }
    let sent = unsafe { libc::sendmsg(sock, &msg, 0) };
    if sent < 0 || sent as usize != payload.len() {
        return Err(StoreError::Transport(format!(
            "sendmsg failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn recv_fd<T>(sock: RawFd) -> Result<(T, OwnedFd)> {
    let mut payload: T = unsafe { zeroed() };
    let mut control = [0u8; 64];
    let mut iov = libc::iovec {
        iov_base: (&mut payload as *mut T).cast(),
        iov_len: size_of::<T>(),
    };
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = control.len() as _;
    let received = unsafe { libc::recvmsg(sock, &mut msg, 0) };
    if received < 0 || received as usize != size_of::<T>() {
        return Err(StoreError::Transport(format!(
            "recvmsg failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if cmsg.is_null() {
        return Err(StoreError::Transport(
            "recvmsg missing control message".to_string(),
        ));
    }
    let fd = unsafe { ptr::read(libc::CMSG_DATA(cmsg).cast::<RawFd>()) };
    if fd < 0 {
        return Err(StoreError::Transport(
            "recvmsg returned invalid file descriptor".to_string(),
        ));
    }
    Ok((payload, unsafe { OwnedFd::from_raw_fd(fd) }))
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;
    use std::thread;
    use std::time::Duration;

    use parking_lot::Mutex;

    use super::*;

    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn hugepages_available(bytes: usize) -> bool {
        let path = if bytes == 2 * 1024 * 1024 {
            "/sys/kernel/mm/hugepages/hugepages-2048kB/free_hugepages"
        } else if bytes == 1024 * 1024 * 1024 {
            "/sys/kernel/mm/hugepages/hugepages-1048576kB/free_hugepages"
        } else {
            return false;
        };
        std::fs::read_to_string(path)
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .is_some_and(|count| count > 0)
    }

    #[test]
    fn shared_regions_resolve_subranges() {
        let _guard = test_lock().lock();
        let ptr = allocate_shared_region(4096).expect("shared alloc should succeed");
        let resolved = resolve_shared_region(ptr + 128, 512).expect("subrange should resolve");
        assert_eq!(resolved.offset, 128);
        assert_eq!(resolved.len, 512);
        free_shared_region(ptr).expect("shared free should succeed");
    }

    #[test]
    fn shared_regions_can_use_hugepage_backing_when_available() {
        let _guard = test_lock().lock();
        let hugepage = HugePageConfig::new(2 * 1024 * 1024).expect("2MB hugepage should resolve");
        if !hugepages_available(hugepage.bytes()) {
            return;
        }
        let ptr = allocate_shared_region_with_options(4096, Some(true), Some(hugepage.bytes()))
            .expect("hugepage shm alloc should succeed");
        let registration =
            shared_region_for_registration(ptr, 4096).expect("registration should resolve");
        assert_eq!(registration.requested_len, 4096);
        assert_eq!(registration.registered_len, hugepage.bytes());
        free_shared_region(ptr).expect("hugepage shm free should succeed");
    }

    #[test]
    fn shared_region_registration_round_trip_maps_and_slices() {
        let _guard = test_lock().lock();
        let ptr = allocate_shared_region(4096).expect("shared alloc should succeed");
        let registration =
            shared_region_for_registration(ptr, 4096).expect("registration should resolve");
        let socket_path = unique_socket_path("round-trip");
        let listener = bind_shm_listener(&socket_path).expect("listener bind should succeed");
        let client_id = DummyClientId::new();
        let request = ShmRegisterRequest::new(
            client_id,
            registration.region_id,
            registration.requested_len,
        );
        let worker = thread::spawn(move || {
            let (stream, request, fd) =
                recv_shm_register_request(&listener).expect("listener should decode request");
            send_shm_register_reply(&stream, 0).expect("listener should ack request");
            Ok::<_, StoreError>((request, fd))
        });

        send_shm_register_request(&socket_path, &request, &registration.fd)
            .expect("registration request should be accepted");
        let (received, fd) = worker
            .join()
            .expect("listener worker should join")
            .expect("listener should return request");
        let mapped = map_registered_region(fd, registration.registered_len)
            .expect("received file descriptor should map");
        mapped
            .slice_mut(32, 4)
            .expect("mutable slice should stay in range")
            .copy_from_slice(b"pong");
        assert_eq!(
            mapped
                .slice(32, 4)
                .expect("read slice should stay in range"),
            b"pong"
        );
        assert_eq!(mapped.len(), registration.registered_len);
        assert_eq!(received.client_id_hi, client_id.high);
        assert_eq!(received.client_id_lo, client_id.low);
        assert_eq!(received.region_id, registration.region_id);
        assert_eq!(received.size as usize, registration.requested_len);
        assert!(matches!(
            mapped.slice(mapped.len(), 1),
            Err(StoreError::Allocator(_))
        ));
        assert!(matches!(
            mapped.slice_mut(mapped.len(), 1),
            Err(StoreError::Allocator(_))
        ));

        free_shared_region(ptr).expect("shared region cleanup should succeed");
        let _ = std::fs::remove_file(socket_path);
    }

    #[test]
    fn shared_region_helpers_report_kernel_and_range_errors() {
        let _guard = test_lock().lock();
        let sanitized = dummy_ipc_socket_path("tcp://127.0.0.1:7000?slot=1", "scope/a");
        let rendered = sanitized.to_string_lossy();
        assert!(rendered.contains("tcp___127_0_0_1"));
        assert!(rendered.contains("scope_a"));
        assert!(rendered.len() < 108);
        let hot_cache = hot_cache_ipc_socket_path(
            "127.0.0.1:35455",
            "mc/store-rs/e2e/local-hot-cache/1776947883484/dummy",
        );
        assert!(hot_cache.to_string_lossy().len() < 108);
        assert!(matches!(
            allocate_shared_region(0),
            Err(StoreError::Allocator(_))
        ));
        assert!(matches!(
            memfd_create("bad\0name", None),
            Err(StoreError::Allocator(_))
        ));
        assert!(matches!(dup_fd(-1), Err(StoreError::Allocator(_))));
        assert!(matches!(ftruncate(-1, 1), Err(StoreError::Allocator(_))));
        assert!(matches!(
            mmap_shared(-1, 4096),
            Err(StoreError::Allocator(_))
        ));

        let two_mb = HugePageConfig::new(2 * 1024 * 1024).expect("2MB hugepage should resolve");
        let one_gb = HugePageConfig::new(1024 * 1024 * 1024).expect("1GB hugepage should resolve");
        assert_ne!(hugepage_memfd_flags(two_mb) & libc::MFD_HUGETLB, 0);
        assert_ne!(hugepage_memfd_flags(one_gb) & libc::MFD_HUGETLB, 0);

        let ptr = allocate_shared_region(1024).expect("shared alloc should succeed");
        assert!(matches!(
            shared_region_for_registration(ptr, 2048),
            Err(StoreError::Allocator(_))
        ));
        assert!(matches!(
            shared_region_for_registration(ptr + 8, 1024),
            Err(StoreError::NotFound(_))
        ));
        assert!(matches!(
            resolve_shared_region(ptr + 900, 200),
            Err(StoreError::Allocator(_))
        ));
        assert!(matches!(
            resolve_shared_region(usize::MAX, 1),
            Err(StoreError::Allocator(_))
        ));
        free_shared_region(ptr).expect("shared region cleanup should succeed");
        assert!(matches!(
            free_shared_region(ptr),
            Err(StoreError::NotFound(_))
        ));
        assert!(matches!(
            resolve_shared_region(ptr, 1),
            Err(StoreError::NotFound(_))
        ));
    }

    #[test]
    fn shm_registration_rejects_bad_magic() {
        let _guard = test_lock().lock();
        let ptr = allocate_shared_region(1024).expect("shared alloc should succeed");
        let registration =
            shared_region_for_registration(ptr, 1024).expect("registration should resolve");
        let socket_path = unique_socket_path("bad-magic");
        let listener = bind_shm_listener(&socket_path).expect("listener bind should succeed");
        let mut request =
            ShmRegisterRequest::new(DummyClientId::new(), registration.region_id, 1024);
        request.magic = 0;
        let worker = thread::spawn(move || recv_shm_register_request(&listener));

        let client_error = send_shm_register_request(&socket_path, &request, &registration.fd)
            .expect_err("invalid magic should be rejected by the listener");
        assert!(matches!(client_error, StoreError::Transport(_)));
        let server_error = worker
            .join()
            .expect("listener worker should join")
            .expect_err("server should reject invalid magic");
        assert!(matches!(server_error, StoreError::Transport(_)));

        free_shared_region(ptr).expect("shared region cleanup should succeed");
        let _ = std::fs::remove_file(socket_path);
    }

    #[test]
    fn shm_registration_reply_waits_for_server_installation() {
        let _guard = test_lock().lock();
        let ptr = allocate_shared_region(1024).expect("shared alloc should succeed");
        let registration =
            shared_region_for_registration(ptr, 1024).expect("registration should resolve");
        let socket_path = unique_socket_path("delayed-ack");
        let listener = bind_shm_listener(&socket_path).expect("listener bind should succeed");
        let request = ShmRegisterRequest::new(DummyClientId::new(), registration.region_id, 1024);
        let worker = thread::spawn(move || {
            let (stream, _request, _fd) =
                recv_shm_register_request(&listener).expect("listener should decode request");
            thread::sleep(Duration::from_millis(100));
            send_shm_register_reply(&stream, 0).expect("listener should ack request");
        });

        let started = std::time::Instant::now();
        send_shm_register_request(&socket_path, &request, &registration.fd)
            .expect("registration request should be accepted");
        let elapsed = started.elapsed();
        worker.join().expect("listener worker should join");
        assert!(
            elapsed >= Duration::from_millis(75),
            "client should wait for server-side registration before returning, observed {:?}",
            elapsed
        );

        free_shared_region(ptr).expect("shared region cleanup should succeed");
        let _ = std::fs::remove_file(socket_path);
    }

    fn unique_socket_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "mooncake-store-rs-{label}-{}.sock",
            DummyClientId::new().low
        ))
    }

    fn test_lock() -> &'static Mutex<()> {
        TEST_LOCK.get_or_init(|| Mutex::new(()))
    }
}
