#[derive(Clone, Debug, Default)]
pub struct ProcessSnapshot {
    pub cpu_seconds_total: f64,
    pub resident_memory_bytes: u64,
    pub open_fds: Option<u64>,
}

pub(crate) fn snapshot_process() -> ProcessSnapshot {
    ProcessSnapshot {
        cpu_seconds_total: cpu_seconds_total(),
        resident_memory_bytes: resident_memory_bytes(),
        open_fds: open_fds(),
    }
}

#[cfg(unix)]
fn cpu_seconds_total() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return 0.0;
    }
    let usage = unsafe { usage.assume_init() };
    timeval_seconds(usage.ru_utime) + timeval_seconds(usage.ru_stime)
}

#[cfg(not(unix))]
fn cpu_seconds_total() -> f64 {
    0.0
}

#[cfg(unix)]
fn timeval_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
}

#[cfg(target_os = "linux")]
fn resident_memory_bytes() -> u64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    let Some(resident_pages) = statm
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return 0;
    };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return 0;
    }
    resident_pages.saturating_mul(page_size as u64)
}

#[cfg(not(target_os = "linux"))]
fn resident_memory_bytes() -> u64 {
    0
}

#[cfg(target_os = "linux")]
fn open_fds() -> Option<u64> {
    std::fs::read_dir("/proc/self/fd")
        .ok()
        .map(|entries| entries.count() as u64)
}

#[cfg(not(target_os = "linux"))]
fn open_fds() -> Option<u64> {
    None
}
