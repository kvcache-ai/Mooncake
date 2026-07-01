# Behavior Fix Summary

## Overview
This change removes the breaking behavior introduced in the latest revision that rejected mixed-device batches with `InvalidArgument`. The code now maintains backward compatibility with the pre-#2569 behavior.

## Changes Made

### 1. Removed Mixed-Device Batch Rejection
**Files Modified:**
- `mooncake-transfer-engine/tent/src/transport/mnnvl/mnnvl_transport.cpp`
- `mooncake-transfer-engine/tent/src/transport/nvlink/nvlink_transport.cpp`

**What Changed:**
- Removed the `InvalidArgument` check that rejected batches with requests from different GPU devices
- Updated comments to clarify that mixed-GPU batches now use the first GPU's stream and rely on CUDA P2P for cross-device access
- This restores the original behavior where mixed-device batches run on the first GPU's stream (same as pre-#2569 and post-#2569 code)

**Before:**
```cpp
} else if (device_id >= 0 && device_id != batch_device_id) {
    return Status::InvalidArgument(
        "Multi-GPU batch not supported: requests from different GPU "
        "devices in the same batch" LOC_MARK);
}
```

**After:**
```cpp
// Capture the first GPU device encountered for stream creation.
// Mixed-GPU batches use the first GPU's stream and rely on CUDA P2P
// for cross-device access (same behavior as pre-#2569 code).
// A future refactor could group requests by device and dispatch to
// per-device streams, but that requires SubBatch structure changes.
```

### 2. Restored cudaGetDevice() for CPU-Only Batches
**Files Modified:**
- `mooncake-transfer-engine/tent/src/transport/mnnvl/mnnvl_transport.cpp`
- `mooncake-transfer-engine/tent/src/transport/nvlink/nvlink_transport.cpp`

**What Changed:**
- CPU-only batches (batch_device_id < 0) now call `cudaGetDevice(&stream_device)` to determine which device to create the stream on
- This restores the original behavior instead of hardcoding device 0

**Before:**
```cpp
int stream_device = (batch_device_id >= 0) ? batch_device_id : 0;
```

**After:**
```cpp
int stream_device = batch_device_id;
if (stream_device < 0) {
    // CPU-only batch: use current CUDA device
    cudaGetDevice(&stream_device);
}
```

## Rationale

1. **Backward Compatibility**: Existing callers may depend on mixed-device batches working (even if suboptimally). Breaking this behavior requires migration across all call sites.

2. **Graceful Degradation**: Running mixed-GPU batches on the first GPU's stream with CUDA P2P is not optimal but doesn't break functionality. The proper fix (per-device stream grouping) requires SubBatch structural changes and can be done as a follow-up.

3. **Multi-GPU Correctness**: On multi-GPU nodes, CPU-only batches should respect the current CUDA device context rather than always defaulting to device 0.

## Future Work

A proper per-device stream grouping refactor would require:
1. SubBatch structure changes to support multiple stream pairs
2. Grouping requests by device before dispatch
3. Separate completion tracking per device

This can be implemented in a follow-up PR without breaking existing callers.
