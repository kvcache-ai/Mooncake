---
name: debug-mooncake-crash
description: Systematic debugging guide for Mooncake crashes and failures. Use when encountering segfaults, RDMA errors, memory registration failures, transport failures, etcd connection issues, or any runtime crash in Mooncake components. Also use for debugging hangs in distributed operations.
---

# Debugging Mooncake Crashes

## Triage: Identify the Crash Category

### 1. RDMA / Transport Errors

**Symptoms:** "No matched device found", "ibv_reg_mr failed", transport timeout

**Diagnostic steps:**
```bash
# Check RDMA devices
ibv_devinfo 2>/dev/null || echo "No RDMA devices found"

# Check if using TCP fallback
grep -r "MC_FORCE_TCP" /proc/$(pgrep -f mooncake)/environ 2>/dev/null

# Check network interfaces
ip addr show | grep -E "ib|rdma|roce"

# Test basic connectivity
ibv_rc_pingpong -d <device> -g 0
```

**Common fixes:**
- Set `MC_FORCE_TCP=true` for environments without RDMA
- Check `MC_GID_INDEX` — wrong GID index causes silent failures
- Verify hugepages: `cat /proc/meminfo | grep Huge`

### 2. Memory Registration Failures

**Symptoms:** "Failed to register memory", OOM, mmap failures

```bash
# Check memory limits
ulimit -l  # Should be unlimited for RDMA
cat /proc/meminfo | grep -E "MemFree|HugePages"

# Check if hugepages are available
python scripts/check_hicache_hugepage_requirements.py
```

### 3. Metadata Server / etcd Issues

**Symptoms:** "Error from etcd client", connection refused, timeout

```bash
# Check etcd is running
etcdctl --endpoints=http://127.0.0.1:2379 endpoint health

# Check etcd data
etcdctl --endpoints=http://127.0.0.1:2379 get "" --prefix --keys-only | head -20
```

### 4. Segfaults in C++ Code

```bash
# Run with address sanitizer
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address"
make -j$(nproc)

# Get backtrace
gdb -batch -ex "run" -ex "bt full" --args ./program
```

### 5. Distributed Hangs

**Symptoms:** Process hangs during put/get/transfer operations

```bash
# Check if process is stuck in a syscall
strace -p <pid> -e trace=network -f 2>&1 | head -50

# Check for deadlocks
gdb -batch -ex "thread apply all bt" -p <pid>
```

## General Debug Environment Variables

```bash
export MC_LOG_LEVEL=DEBUG          # Verbose logging
export MC_FORCE_TCP=true           # Force TCP (no RDMA needed)
export MC_VERBOSE_TRANSPORT=1      # Transport-level traces
export NCCL_DEBUG=INFO             # For PG backend debugging
```

## Filing Bug Reports

When the crash is reproducible, file a GitHub issue with:
1. Mooncake version (`pip show mooncake-transfer-engine`)
2. Hardware (GPU, NIC, driver versions)
3. Full error output with `MC_LOG_LEVEL=DEBUG`
4. Minimal reproduction script
