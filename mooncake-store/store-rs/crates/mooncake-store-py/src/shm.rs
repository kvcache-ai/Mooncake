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

use mooncake_store_core::{Result, StoreError};
use parking_lot::Mutex;

const SHM_ALLOCATOR_NAME: &str = "mooncake_store_rs_shm";
const SHM_REGISTER_MAGIC: u32 = 0x4d435348;

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

#[derive(Debug)]
struct SharedRegion {
    region_id: u64,
    base: usize,
    len: usize,
    fd: OwnedFd,
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
    if size == 0 {
        return Err(StoreError::Allocator(
            "shared region size must be greater than zero".to_string(),
        ));
    }
    let fd = memfd_create(SHM_ALLOCATOR_NAME)?;
    ftruncate(fd.as_raw_fd(), size)?;
    let mapping = mmap_shared(fd.as_raw_fd(), size)?;
    let base = mapping as usize;
    let region = SharedRegion {
        region_id: NEXT_REGION_ID.fetch_add(1, Ordering::Relaxed),
        base,
        len: size,
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
        libc::munmap(region.base as *mut libc::c_void, region.len);
    }
    Ok(())
}

pub fn shared_region_for_registration(ptr: usize, size: usize) -> Result<(u64, usize, OwnedFd)> {
    let registry = SHARED_REGISTRY.lock();
    let Some(region) = registry.get(&ptr) else {
        return Err(StoreError::NotFound(format!(
            "shared region base {ptr:#x} not found"
        )));
    };
    if region.len != size {
        return Err(StoreError::Allocator(format!(
            "shared region size mismatch: requested={size} actual={}",
            region.len
        )));
    }
    Ok((region.region_id, region.len, dup_fd(region.fd.as_raw_fd())?))
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
    let region_end = region.base.saturating_add(region.len);
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

pub fn dummy_ipc_socket_path(server_addr: &str) -> PathBuf {
    let sanitized = server_addr
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => ch,
            _ => '_',
        })
        .collect::<String>();
    std::env::temp_dir().join(format!("mooncake-store-rs-dummy-{sanitized}.sock"))
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

pub fn recv_shm_register_request(listener: &UnixListener) -> Result<(ShmRegisterRequest, OwnedFd)> {
    let (stream, _) = listener.accept().map_err(|error| {
        StoreError::Transport(format!("failed to accept shm registration: {error}"))
    })?;
    let (request, fd) = recv_fd::<ShmRegisterRequest>(stream.as_raw_fd())?;
    if request.magic != SHM_REGISTER_MAGIC {
        let _ = (&stream).write_all(&(-1i32).to_ne_bytes());
        return Err(StoreError::Transport(format!(
            "invalid shm registration magic: {}",
            request.magic
        )));
    }
    (&stream)
        .write_all(&(0i32).to_ne_bytes())
        .map_err(|error| StoreError::Transport(format!("failed to ack shm registration: {error}")))?;
    Ok((request, fd))
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

fn memfd_create(name: &str) -> Result<OwnedFd> {
    let name = CString::new(name)
        .map_err(|_| StoreError::Allocator("invalid memfd name".to_string()))?;
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(StoreError::Allocator(format!(
            "memfd_create failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
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
            libc::MAP_SHARED,
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
    msg.msg_controllen = control.len();
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
        msg.msg_controllen = (*cmsg).cmsg_len;
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
    msg.msg_controllen = control.len();
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
    use super::*;

    #[test]
    fn shared_regions_resolve_subranges() {
        let ptr = allocate_shared_region(4096).expect("shared alloc should succeed");
        let resolved = resolve_shared_region(ptr + 128, 512).expect("subrange should resolve");
        assert_eq!(resolved.offset, 128);
        assert_eq!(resolved.len, 512);
        free_shared_region(ptr).expect("shared free should succeed");
    }
}
