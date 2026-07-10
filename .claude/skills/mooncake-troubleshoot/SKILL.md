---
name: mooncake-troubleshoot
description: Automatically diagnose Mooncake deployment and runtime issues. Checks services (mooncake_master, metadata server), RDMA devices, environment variables, connectivity, memory limits, and analyzes logs for common error patterns. Use when Mooncake deployment fails, services won't start, connections fail, or you encounter runtime errors like "Error from etcd client", "No matched device found", "Failed to register memory", "NO_AVAILABLE_HANDLE", or any RDMA/networking issues. Also use when user asks to troubleshoot, debug, diagnose, or fix Mooncake problems.
---

# Mooncake Deployment Troubleshooting

You are a Mooncake deployment troubleshooting specialist. Your job is to systematically diagnose issues and provide actionable solutions based on the comprehensive troubleshooting knowledge from Mooncake documentation.

## Diagnostic Strategy

Run checks systematically, reporting findings as you go. Start with simple checks (services, connectivity) before diving into complex issues (RDMA, memory registration).

### 1. Service Status Check

Check if critical services are running:

```bash
# Check mooncake_master
ps aux | grep mooncake_master | grep -v grep

# Check port usage
netstat -tuln | grep -E '(50051|8080|2379|9003)'

# If using etcd
ps aux | grep etcd | grep -v grep
```

**Common issues:**
- `bind address already in use` → Port conflict, use different port with `--rpc_port`
- Master not running → Check startup logs for errors

### 2. Metadata Server Connectivity

The metadata server is critical for node discovery and coordination.

```bash
# Test etcd connectivity
curl -s http://127.0.0.1:2379/version

# Or test custom metadata server
curl -s $MC_METADATA_SERVER

# Check for proxy interference
echo "http_proxy: $http_proxy"
echo "https_proxy: $https_proxy"
```

**Common issues:**
- `Error from etcd client` → Metadata server unreachable
  - **Fix:** Ensure etcd is bound to `0.0.0.0` not `127.0.0.1`:
    ```bash
    etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://<your_ip>:2379
    ```
  - **Fix:** Disable HTTP proxy:
    ```bash
    unset http_proxy https_proxy
    ```

### 3. Environment Variables Check

Verify critical environment variables are set correctly:

```bash
# Display all MC_* variables
env | grep ^MC_

# Key variables to check:
echo "MC_METADATA_SERVER: $MC_METADATA_SERVER"
echo "MC_FORCE_TCP: $MC_FORCE_TCP"
echo "MC_LOG_LEVEL: $MC_LOG_LEVEL"
echo "MC_YLT_LOG_LEVEL: $MC_YLT_LOG_LEVEL"
echo "MC_MS_AUTO_DISC: $MC_MS_AUTO_DISC"
echo "MC_MS_FILTERS: $MC_MS_FILTERS"
echo "MC_GID_INDEX: $MC_GID_INDEX"
echo "MC_MTU: $MC_MTU"
echo "MC_IB_PORT: $MC_IB_PORT"
echo "MC_ENABLE_DEST_DEVICE_AFFINITY: $MC_ENABLE_DEST_DEVICE_AFFINITY"
```

**Key variables:**
- `MC_METADATA_SERVER` - Metadata server URL (required)
- `MC_FORCE_TCP=true` - Force TCP for testing without RDMA
- `MC_LOG_LEVEL=0` - Enable verbose logging (0=INFO, 1=WARNING, 2=ERROR)
- `MC_YLT_LOG_LEVEL=debug` - yalantinglibs log level
- `MC_MS_AUTO_DISC=1` - Enable topology auto-discovery (default)
- `MC_MS_FILTERS` - Filter specific RDMA devices (e.g., "mlx5_1,mlx5_2")
- `MC_GID_INDEX` - RDMA GID index (set if GID is all zeros)
- `MC_MTU` - RDMA MTU size
- `MC_ENABLE_DEST_DEVICE_AFFINITY=1` - Reduce QP creation (fix "Failed to create QP")

### 4. RDMA Device Check

Only run if RDMA is being used (skip if `MC_FORCE_TCP=true`):

```bash
# List RDMA devices
ibv_devices

# Check device details and status
ibv_devinfo

# Check for ACTIVE ports
ibv_devinfo | grep -A 10 "state:"

# Check GID addresses (should NOT be all zeros)
ibv_devinfo | grep -A 20 "GID"

# Check peer memory modules
lsmod | grep peer_mem
lsmod | grep nvidia_peer_mem

# Check QP count (if "Failed to create QP" error)
rdma resource show qp
```

**Common issues:**
- `No matched device found` → RDMA device name in config doesn't exist
  - **Fix:** Use `ibv_devices` to get correct device names
- `Device XXX port not active` → RDMA port not in ACTIVE state
  - **Fix:** Check cable connections, verify with `ibv_devinfo | grep state`
  - **Fix:** Try different port with `MC_IB_PORT` environment variable
- GID all zeros → Wrong GID index
  - **Fix:** Set `MC_GID_INDEX=1` (or 2, 3 depending on network)
- `Failed to create QP: Cannot allocate memory` → Too many QPs created
  - **Fix:** Set `MC_ENABLE_DEST_DEVICE_AFFINITY=1`

### 5. Memory and Resource Limits

Check system limits that affect RDMA memory registration:

```bash
# Check ulimits
ulimit -a

# Focus on max locked memory
ulimit -l

# Check RDMA device memory limits
ibv_devinfo -v | grep max_mr_size

# Check dmesg for memory errors
dmesg -T | tail -50 | grep -i "out of mr size"
```

**Common issues:**
- `Failed to register memory: Input/output error` → Memory registration limit exceeded
  - **Diagnostic:** Check `max_mr_size` with `ibv_devinfo -v`
  - **Fix:** Reduce memory allocation or split into smaller chunks
- Cannot allocate memory → ulimit restriction
  - **Fix:** Set unlimited locked memory:
    ```bash
    ulimit -l unlimited
    ```
  - **Permanent fix:** Add to `/etc/security/limits.conf`:
    ```
    * soft memlock unlimited
    * hard memlock unlimited
    ```

### 6. Network Connectivity

Test connectivity between nodes:

```bash
# Test basic RDMA connectivity
ib_write_bw -d <device_name> -R

# On peer node:
ib_write_bw -d <device_name> -R <server_ip>

# Test GPU Direct RDMA (if CUDA enabled)
ib_write_bw -d <device_name> -R -x gdr

# On peer node:
ib_write_bw -d <device_name> -R -x gdr <server_ip>

# Test DNS resolution
nslookup <connectable_name>
ping <connectable_name>
```

**Common issues:**
- `connection refused` → Incorrect `connectable_name` or `rpc_port`
  - **Fix:** Ensure `connectable_name` is NOT loopback (127.0.0.1/localhost)
  - **Fix:** Use actual LAN/WAN IP or valid hostname
- `Failed to exchange handshake` → RDMA connection setup failure
  - **Fix:** Verify MTU matches: set `MC_MTU` environment variable
  - **Fix:** Verify GID is valid (not all zeros)
  - **Fix:** Test with `ib_send_bw` between nodes first

### 7. Log Analysis

Search logs for common error patterns and their meanings:

**Metadata/Connectivity Errors:**
- `Error from etcd client` → Cannot connect to metadata server
- `ERR_METADATA` → Metadata server communication failed
- `ERR_DNS` → Invalid `local_server_name` (not valid DNS/IP)

**RDMA Errors:**
- `No matched device found` → RDMA device name doesn't exist
- `Device XXX port not active` → RDMA port not in ACTIVE state
- `Failed to exchange handshake description` → RDMA handshake failed
- `Failed to modify QP to RTR, check mtu, gid, peer lid, peer qp num` → MTU/GID mismatch
- `Failed to register memory` → Memory registration limit exceeded
- `Failed to create QP` → Too many QPs, enable `MC_ENABLE_DEST_DEVICE_AFFINITY=1`
- `Worker: Process failed for slice` → Network instability
- `work request flushed error` → Cascading error (find first error)

**Store Errors:**
- `NO_AVAILABLE_HANDLE` (-200) → Memory pool exhausted
  - **Fix:** Increase `global_segment_size` in setup
  - **Fix:** Check eviction is working (look for eviction logs)
- `LEASE_EXPIRED` (-707) → Lease expired during transfer
  - **Fix:** Increase `default_kv_lease_ttl` in master startup
- `OBJECT_NOT_FOUND` (-704) → Object doesn't exist
- `SEGMENT_NOT_FOUND` (-101) → No available segments
- `Failed to get description of XXX` → Segment name mismatch
  - **Fix:** Ensure segment name matches `local_hostname` from peer

**Port/Service Errors:**
- `bind address already in use` → Port conflict
  - **Fix:** Use different port: `--rpc_port=50052`

### 8. Configuration Validation

Verify configuration is correct:

```bash
# Check connectable_name is not loopback
hostname -I

# Verify master startup flags
ps aux | grep mooncake_master

# Check if using correct protocol
env | grep MC_FORCE_TCP
```

**Critical checks:**
- `connectable_name` must be non-loopback IP or valid hostname
- MTU and GID configurations must match network environment
- RDMA device names must exist on the machine
- Ports must not be in use by other services

## Error Code Quick Reference

### Transfer Engine Error Codes

| Code | Name | Meaning | Fix |
|------|------|---------|-----|
| 0 | Success | Normal execution | - |
| -12 | ERR_ADDRESS_NOT_REGISTERED | Memory not registered | Register memory before use |
| -14 | ERR_DEVICE_NOT_FOUND | RDMA device not found | Check device name with `ibv_devices` |
| -16 | ERR_DNS | Invalid local_server_name | Use valid IP/hostname |
| -19 | ERR_REJECT_HANDSHAKE | Peer rejected handshake | Check peer logs for reason |
| -20 | ERR_METADATA | Metadata server unreachable | Check etcd/HTTP server |

### Store Error Codes

| Code | Name | Meaning | Fix |
|------|------|---------|-----|
| 0 | Success | Operation successful | - |
| -200 | NO_AVAILABLE_HANDLE | Memory pool exhausted | Increase segment size |
| -707 | LEASE_EXPIRED | Lease expired | Increase lease TTL |
| -704 | OBJECT_NOT_FOUND | Object doesn't exist | Check object key |
| -101 | SEGMENT_NOT_FOUND | No available segments | Check segment registration |
| -900 | RPC_FAIL | RPC failed | Check network/master |
| -1000 | ETCD_OPERATION_ERROR | etcd operation failed | Check etcd status |

## Output Format

Provide a structured diagnostic report:

```
🔍 MOONCAKE DEPLOYMENT DIAGNOSTICS
==================================

✅ PASSED CHECKS:
- Service status: mooncake_master running on port 50051
- Metadata server: etcd accessible at http://127.0.0.1:2379
- Environment: MC_METADATA_SERVER set correctly
- [other passing checks]

❌ FAILED CHECKS:
- RDMA device: mlx5_0 port not ACTIVE (state: PORT_DOWN)
- Memory limits: max locked memory is 64KB (too low)
- [other failures with specific error messages]

⚠️  WARNINGS:
- GID index not set, may cause connection issues
- HTTP proxy variables set, may interfere with metadata server
- [other potential issues]

🔧 RECOMMENDED FIXES:

1. Fix RDMA port status:
   - Check physical cable connections
   - Verify driver configuration
   - Command: ibv_devinfo | grep -A 10 "state:"

2. Increase memory limits:
   ulimit -l unlimited
   # Or permanently in /etc/security/limits.conf:
   * soft memlock unlimited
   * hard memlock unlimited

3. Set GID index:
   export MC_GID_INDEX=1

4. Disable HTTP proxy:
   unset http_proxy https_proxy

📋 SUMMARY:
[Brief 2-3 sentence conclusion about deployment health and next steps]
```

## Troubleshooting Workflow

1. **Start simple**: Check services and basic connectivity first
2. **Read logs carefully**: First error is usually root cause (subsequent errors cascade)
3. **Test incrementally**: Use `MC_FORCE_TCP=true` to isolate RDMA issues
4. **Verify basics**: Check connectable_name, ports, env vars before deep diving
5. **Use diagnostic tools**: ibv_devices, ibv_devinfo, ib_write_bw, curl
6. **Reference documentation**: Check error codes and troubleshooting guide

## Quick Fix Commands

**Start metadata server properly:**
```bash
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://<your_ip>:2379
```

**Enable verbose logging:**
```bash
export MC_LOG_LEVEL=0
export MC_YLT_LOG_LEVEL=debug
```

**Force TCP mode for testing:**
```bash
export MC_FORCE_TCP=true
```

**Fix memory limits:**
```bash
ulimit -l unlimited
```

**Fix too many QPs:**
```bash
export MC_ENABLE_DEST_DEVICE_AFFINITY=1
```

**Fix GID issues:**
```bash
export MC_GID_INDEX=1  # or 2, 3 depending on network
```

**Use different port:**
```bash
mooncake_master --rpc_port=50052
```

Now execute the diagnostic checks systematically and provide the structured report.
