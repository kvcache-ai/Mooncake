// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// ===================================================================
// GDS Device Operations — multi-vendor abstraction layer
// ===================================================================
// Follows the pattern of gpu_staging_utils.h:
//   - Header-only (all functions inline)
//   - Namespace isolation: mooncake::gds_device_ops
//   - #ifdef vendor dispatch: USE_GDS_NVIDIA / USE_GDS_HYGON / ...
//   - Fallback stubs: return sentinel when no vendor is active
//   - No static/global state: driver singleton managed by gds_context.cpp
//
// GdsDeviceFileHandle is void* — compatible with NVIDIA CUfileHandle_t
// and all currently known vendor GDS APIs (Hygon DCU, Ascend, Moore
// Threads all use pointer-type handles). If a future vendor uses an
// int fd that cannot be boxed into void*, this type will need to be
// extended to a union or struct{void*; int; uint8_t tag;}.
//
// IMPORTANT: all #include directives for vendor SDK headers
// (cufile.h, cuda_runtime.h, etc.) MUST appear BEFORE the
// mooncake::gds_device_ops namespace so that CUDA symbols
// resolve in :: (global) scope correctly.

#include <sys/stat.h>
#include <cstddef>

namespace mooncake {
namespace gds_device_ops {

// Replacement for CUfileError_t. Only the .err field is used by callers.
// err == 0 means success (CU_FILE_SUCCESS), non-zero means failure.
struct GdsDeviceError {
    int err = -1;
    static constexpr int SUCCESS = 0;

    bool IsOk() const { return err == SUCCESS; }
    bool IsErr() const { return !IsOk(); }
};

// Replacement for CUfileHandle_t (typedef void*).
// Binary-compatible: cu_file_handle_ is already void* in GdsContext.
using GdsDeviceFileHandle = void*;

// ── API function declarations ──
//
// 1.  ProbeDeviceNode         — check GDS device node existence
// 2.  DriverOpen              — open GDS driver (caller manages singleton)
// 3.  FileHandleRegister      — register POSIX fd as GDS handle
// 4.  FileHandleDeregister    — deregister; nullptr must be no-op
// 5.  BufRegister             — register GPU buffer for GDS DMA
// 6.  BufDeregister           — deregister; caller guarantees only
//                               registered pointers are passed
// 7.  Write                   — GPU→SSD DMA write
// 8.  Read                    — SSD→GPU DMA read
// 9.  Malloc                  — GPU memory alloc (probe only); nullptr=fail
// 10. Free                    — GPU memory free; nullptr no-op
// 11. Memset                  — GPU memory fill
// 12. DeviceSynchronize       — sync current GPU device
// 13. GetDevice               — get current GPU device ID; -1 on failure
//
// BufDeregister semantics: callers guarantee only successfully
// registered pointers are passed (tracked via need_dereg / gpu_buf_ok
// flags). Each vendor must at least handle nullptr as no-op.
//
// DriverOpen singleton: std::call_once in gds_context.cpp manages
// process-level singleton. DriverOpen() itself does NOT self-singleton.
//
// SetDevice context: functions like Malloc, Memset, Write, Read assume
// the caller has already set the correct device via
// gpu_staging::SetDevice().

}  // namespace gds_device_ops
}  // namespace mooncake

// ═══════════════════════════════════════════════════════════════════
// NVIDIA cuFile (USE_GDS_NVIDIA)
// ═══════════════════════════════════════════════════════════════════
#ifdef USE_GDS_NVIDIA

#include <cufile.h>
#include <cuda_runtime.h>
#include <unistd.h>

// Re-open namespace after vendor headers (headers at global scope so
// that ::cuda* and ::cuFile* symbols resolve correctly).

namespace mooncake {
namespace gds_device_ops {

inline bool ProbeDeviceNode() {
    struct stat st;
    return (::stat("/dev/nvidia-fs", &st) == 0 && S_ISCHR(st.st_mode)) ||
           (::stat("/dev/nvidia-fs0", &st) == 0 && S_ISCHR(st.st_mode));
}

inline GdsDeviceError DriverOpen() {
    CUfileError_t r = cuFileDriverOpen();
    return GdsDeviceError{static_cast<int>(r.err)};
}

inline GdsDeviceError FileHandleRegister(GdsDeviceFileHandle* out, int fd) {
    CUfileDescr_t desc{};
    desc.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
    desc.handle.fd = fd;
    CUfileError_t r = cuFileHandleRegister(
        reinterpret_cast<CUfileHandle_t*>(out), &desc);
    return GdsDeviceError{static_cast<int>(r.err)};
}

inline void FileHandleDeregister(GdsDeviceFileHandle handle) {
    if (handle) cuFileHandleDeregister(static_cast<CUfileHandle_t>(handle));
}

inline GdsDeviceError BufRegister(void* ptr, size_t size) {
    CUfileError_t r = cuFileBufRegister(ptr, size, 0);
    return GdsDeviceError{static_cast<int>(r.err)};
}

inline void BufDeregister(void* ptr) {
    if (ptr) cuFileBufDeregister(ptr);
}

inline ssize_t Write(GdsDeviceFileHandle fh, void* buf,
                     size_t size, off_t file_offset) {
    return cuFileWrite(static_cast<CUfileHandle_t>(fh), buf, size,
                       file_offset, 0);
}

inline ssize_t Read(GdsDeviceFileHandle fh, void* buf,
                    size_t size, off_t file_offset) {
    return cuFileRead(static_cast<CUfileHandle_t>(fh), buf, size,
                      file_offset, 0);
}

inline void* Malloc(size_t size) {
    void* ptr = nullptr;
    return (cudaMalloc(&ptr, size) == cudaSuccess) ? ptr : nullptr;
}

inline void Free(void* ptr) {
    if (ptr) cudaFree(ptr);
}

inline void Memset(void* ptr, int value, size_t size) {
    cudaMemset(ptr, value, size);
}

inline void DeviceSynchronize() { cudaDeviceSynchronize(); }

inline void CopyDeviceToDevice(void* dst, const void* src, size_t size) {
    cudaMemcpy(dst, src, size, cudaMemcpyDeviceToDevice);
}

inline int GetDevice() {
    int dev = -1;
    cudaGetDevice(&dev);
    return dev;
}

}  // namespace gds_device_ops
}  // namespace mooncake

#endif  // USE_GDS_NVIDIA

// ═══════════════════════════════════════════════════════════════════
// Hygon DCU GDS (USE_GDS_HYGON)
// ═══════════════════════════════════════════════════════════════════
// GPU runtime: Hygon CUDA-compatible runtime (USE_HYGON) or <hip/hip_runtime.h>
// GDS API: pending Hygon GDS SDK. Expected API shape similar to cuFile.
//
// Implementation guide:
//   ProbeDeviceNode() → check /dev/hygon-gds or similar
//   DriverOpen()      → Hygon GDS driver init (no self-singleton)
//   FileHandleRegister / Deregister → Hygon GDS file handle ops
//   BufRegister / BufDeregister     → Hygon GPU buffer ops
//   Write / Read     → Hygon GPU↔SSD DMA
//   Malloc / Free / Memset / DeviceSynchronize / GetDevice
//                    → Hygon CUDA-compatible runtime or HIP API
//   SetDevice context → caller sets via gpu_staging::SetDevice

#ifdef USE_GDS_HYGON
// todo: include Hygon GDS and GPU runtime headers

namespace mooncake {
namespace gds_device_ops {

inline bool ProbeDeviceNode() {
    // todo: check Hygon GDS device node (e.g. /dev/hygon-gds)
    return false;
}

inline GdsDeviceError DriverOpen() {
    // todo: Hygon GDS driver init
    return GdsDeviceError{-1};
}

inline GdsDeviceError FileHandleRegister(GdsDeviceFileHandle*, int) {
    // todo: Hygon GDS file handle register
    return GdsDeviceError{-1};
}

inline void FileHandleDeregister(GdsDeviceFileHandle) {
    // todo: Hygon GDS file handle deregister
}

inline GdsDeviceError BufRegister(void*, size_t) {
    // todo: Hygon GPU buffer register
    return GdsDeviceError{-1};
}

inline void BufDeregister(void*) {
    // todo: Hygon GPU buffer deregister
}

inline ssize_t Write(GdsDeviceFileHandle, void*, size_t, off_t) {
    // todo: Hygon GPU→SSD DMA write
    return -1;
}

inline ssize_t Read(GdsDeviceFileHandle, void*, size_t, off_t) {
    // todo: Hygon SSD→GPU DMA read
    return -1;
}

inline void* Malloc(size_t) {
    // todo: Hygon GPU memory alloc (HIP or CUDA-compatible runtime)
    return nullptr;
}

inline void Free(void*) {
    // todo: Hygon GPU memory free
}

inline void Memset(void*, int, size_t) {
    // todo: Hygon GPU memory fill
}

inline void DeviceSynchronize() {
    // todo: Hygon GPU sync
}

inline int GetDevice() {
    // todo: Hygon get current device
    return -1;
}

inline void CopyDeviceToDevice(void*, const void*, size_t) {
    // todo: Hygon GPU D2D copy
}

}  // namespace gds_device_ops
}  // namespace mooncake

#endif  // USE_GDS_HYGON

// ═══════════════════════════════════════════════════════════════════
// Ascend GDS (USE_GDS_ASCEND)
// ═══════════════════════════════════════════════════════════════════
// GPU runtime: <acl/acl.h> (AscendCL)
// GDS API: pending Ascend DSA SDK.
//
// Implementation guide:
//   ProbeDeviceNode() → check Ascend DSA device node
//   DriverOpen()      → Ascend DSA init
//   FileHandleRegister / Deregister / BufRegister / BufDeregister
//                    → Ascend DSA API
//   Write / Read     → Ascend GPU↔SSD DMA
//   Malloc / Free / Memset / DeviceSynchronize / GetDevice
//                    → AscendCL API (aclrtMalloc etc.)
//   SetDevice context → caller sets via gpu_staging::SetDevice

#ifdef USE_GDS_ASCEND
// todo: include AscendCL and DSA headers

namespace mooncake {
namespace gds_device_ops {

inline bool ProbeDeviceNode() {
    // todo: check Ascend DSA device node
    return false;
}

inline GdsDeviceError DriverOpen() {
    // todo: Ascend DSA init
    return GdsDeviceError{-1};
}

inline GdsDeviceError FileHandleRegister(GdsDeviceFileHandle*, int) {
    // todo: Ascend DSA file handle register
    return GdsDeviceError{-1};
}

inline void FileHandleDeregister(GdsDeviceFileHandle) {
    // todo: Ascend DSA file handle deregister
}

inline GdsDeviceError BufRegister(void*, size_t) {
    // todo: Ascend buffer register
    return GdsDeviceError{-1};
}

inline void BufDeregister(void*) {
    // todo: Ascend buffer deregister
}

inline ssize_t Write(GdsDeviceFileHandle, void*, size_t, off_t) {
    // todo: Ascend GPU→SSD DMA write
    return -1;
}

inline ssize_t Read(GdsDeviceFileHandle, void*, size_t, off_t) {
    // todo: Ascend SSD→GPU DMA read
    return -1;
}

inline void* Malloc(size_t) {
    // todo: AscendCL aclrtMalloc
    return nullptr;
}

inline void Free(void*) {
    // todo: AscendCL aclrtFree
}

inline void Memset(void*, int, size_t) {
    // todo: AscendCL memory fill
}

inline void DeviceSynchronize() {
    // todo: AscendCL aclrtSynchronizeDevice
}

inline int GetDevice() {
    // todo: AscendCL aclrtGetDevice
    return -1;
}

inline void CopyDeviceToDevice(void*, const void*, size_t) {
    // todo: AscendCL aclrtMemcpy (D2D)
}

}  // namespace gds_device_ops
}  // namespace mooncake

#endif  // USE_GDS_ASCEND

// ═══════════════════════════════════════════════════════════════════
// Moore Threads GDS (USE_GDS_MOORE_THREADS)
// ═══════════════════════════════════════════════════════════════════
// GPU runtime: MUSA runtime (musa CUDA-compatible)
// GDS API: pending Moore Threads GDS SDK.
//
// Implementation guide: same 13-function pattern as NVIDIA/Hygon,
// replacing cuFile* / cuda* calls with MUSA equivalents.

#ifdef USE_GDS_MOORE_THREADS
// todo: include MUSA runtime and GDS headers

namespace mooncake {
namespace gds_device_ops {

inline bool ProbeDeviceNode() {
    // todo: check Moore Threads GDS device node
    return false;
}

inline GdsDeviceError DriverOpen() {
    // todo: Moore Threads GDS driver init
    return GdsDeviceError{-1};
}

inline GdsDeviceError FileHandleRegister(GdsDeviceFileHandle*, int) {
    // todo: Moore Threads GDS file handle register
    return GdsDeviceError{-1};
}

inline void FileHandleDeregister(GdsDeviceFileHandle) {
    // todo: Moore Threads GDS file handle deregister
}

inline GdsDeviceError BufRegister(void*, size_t) {
    // todo: Moore Threads buffer register
    return GdsDeviceError{-1};
}

inline void BufDeregister(void*) {
    // todo: Moore Threads buffer deregister
}

inline ssize_t Write(GdsDeviceFileHandle, void*, size_t, off_t) {
    // todo: Moore Threads GPU→SSD DMA write
    return -1;
}

inline ssize_t Read(GdsDeviceFileHandle, void*, size_t, off_t) {
    // todo: Moore Threads SSD→GPU DMA read
    return -1;
}

inline void* Malloc(size_t) {
    // todo: MUSA musaMalloc
    return nullptr;
}

inline void Free(void*) {
    // todo: MUSA musaFree
}

inline void Memset(void*, int, size_t) {
    // todo: MUSA musaMemset
}

inline void DeviceSynchronize() {
    // todo: MUSA musaDeviceSynchronize
}

inline int GetDevice() {
    // todo: MUSA musaGetDevice
    return -1;
}

inline void CopyDeviceToDevice(void*, const void*, size_t) {
    // todo: MUSA musaMemcpy (D2D)
}

}  // namespace gds_device_ops
}  // namespace mooncake

#endif  // USE_GDS_MOORE_THREADS

// ═══════════════════════════════════════════════════════════════════
// Fallback: no GDS vendor activated
// ═══════════════════════════════════════════════════════════════════
// All functions return sentinel. Compiles but provides no GDS capability.
// GdsContext::ProbeGdsAvailable() → false, enabled_ remains false.
#if !defined(USE_GDS_NVIDIA) && !defined(USE_GDS_HYGON) && \
    !defined(USE_GDS_ASCEND) && !defined(USE_GDS_MOORE_THREADS)

#include <unistd.h>

namespace mooncake {
namespace gds_device_ops {

inline bool ProbeDeviceNode() { return false; }
inline GdsDeviceError DriverOpen() { return GdsDeviceError{-1}; }
inline GdsDeviceError FileHandleRegister(GdsDeviceFileHandle*, int) {
    return GdsDeviceError{-1};
}
inline void FileHandleDeregister(GdsDeviceFileHandle) {}
inline GdsDeviceError BufRegister(void*, size_t) { return GdsDeviceError{-1}; }
inline void BufDeregister(void*) {}
inline ssize_t Write(GdsDeviceFileHandle, void*, size_t, off_t) { return -1; }
inline ssize_t Read(GdsDeviceFileHandle, void*, size_t, off_t) { return -1; }
inline void* Malloc(size_t) { return nullptr; }
inline void Free(void*) {}
inline void Memset(void*, int, size_t) {}
inline void DeviceSynchronize() {}
inline int GetDevice() { return -1; }
inline void CopyDeviceToDevice(void*, const void*, size_t) {}

}  // namespace gds_device_ops
}  // namespace mooncake

#endif  // no GDS vendor
