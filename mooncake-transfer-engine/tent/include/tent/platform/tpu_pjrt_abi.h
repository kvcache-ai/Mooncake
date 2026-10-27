// Copyright 2026 KVCache.AI
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

// C ABI contract between TENT and the TPU/PJRT adapter shared library.
//
// The adapter is built separately against the PJRT runtime and exports these
// symbols with C linkage. TENT resolves them at runtime via dlopen()/dlsym()
// (see TpuPjrtShim); it never links the adapter or the PJRT runtime directly.
//
// Pointer classification note: the "device pointer" passed across this ABI is
// the stable token the serving-engine integration registers with TENT for a TPU
// buffer (see registerLocalMemory with a "tpu:N" location). The adapter owns
// the mapping from that token to the underlying PJRT buffer; mc_tpu_pjrt_* copy
// and classification calls resolve the token through the adapter's own
// registry.
//
// INTERIOR POINTERS (load-bearing): TENT stages transfers through host DRAM in
// chunks (see ProxyManager, chunk_size defaults to 4 MiB), and hands the
// adapter `token + chunk_offset` for every chunk after the first. Every
// entrypoint below therefore takes an address that may point into the MIDDLE of
// a registered buffer, not only at its base. An adapter whose registry only
// matches base addresses will report `is_device_ptr(token + off) == 0`, TENT
// will classify TPU HBM as host memory, and the staging copy silently degrades
// into a memcpy from a non-data address -- corrupting every transfer larger
// than one chunk without raising an error. Adapters MUST resolve an address to
// the registered buffer whose range [base, base + size) contains it.
//
// The token is NOT required to be host-dereferenceable, and on real PJRT/TPU it
// is not: PJRT_Buffer_UnsafePointer returns an internal handle that reads as
// garbage rather than buffer contents. Never dereference it.

#ifndef TENT_PLATFORM_TPU_PJRT_ABI_H_
#define TENT_PLATFORM_TPU_PJRT_ABI_H_

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Initialize the adapter (creates/attaches the PJRT client). Returns 0 on
// success, non-zero on failure. Idempotent; safe to call more than once.
int mc_tpu_pjrt_init(void);

// Returns 1 if `addr` falls inside any TPU device buffer known to the adapter
// (base address or interior), else 0.
int mc_tpu_pjrt_is_device_ptr(const void *addr);

// Returns the device ordinal of the buffer containing `addr` (base address or
// interior), or -1 if `addr` is not inside a known TPU device buffer.
int mc_tpu_pjrt_device_index(const void *addr);

// Synchronous device->host copy. `device_src` may be an interior address; the
// whole range [device_src, device_src + len) must lie within a single
// registered buffer, otherwise the adapter must fail rather than copy short.
// Returns 0 on success, non-zero on failure.
int mc_tpu_pjrt_copy_d2h(void *host_dst, const void *device_src, size_t len);

// Synchronous host->device copy. `device_dst` may be an interior address; the
// whole range [device_dst, device_dst + len) must lie within a single
// registered buffer, otherwise the adapter must fail rather than copy short.
// Returns 0 on success, non-zero on failure.
int mc_tpu_pjrt_copy_h2d(void *device_dst, const void *host_src, size_t len);

// Number of visible TPU devices.
int mc_tpu_pjrt_device_count(void);

// NUMA node closest to device `index`, or -1 if unknown.
int mc_tpu_pjrt_device_numa(int index);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // TENT_PLATFORM_TPU_PJRT_ABI_H_
