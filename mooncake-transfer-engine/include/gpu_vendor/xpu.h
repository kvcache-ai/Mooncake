// Copyright 2024 KVCache.AI
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

#include <level_zero/ze_api.h>

#include <cstddef>
#include <cstdint>
#include <string>

// ---------- GPU prefix for Mooncake location strings ----------
const static std::string GPU_PREFIX = "xpu:";

// ---------- Error types ----------
typedef int cudaError_t;
#define cudaSuccess 0
// Returned by cudaStreamQuery() while a stream still has work in flight.
// Value matches CUDA's cudaErrorNotReady so callers comparing against it keep
// working.
#define cudaErrorNotReady 600

// ---------- Memory type enum ----------
enum cudaMemoryType {
    cudaMemoryTypeUnregistered = 0,
    cudaMemoryTypeHost = 1,
    cudaMemoryTypeDevice = 2,
    cudaMemoryTypeManaged = 3,
};

// ---------- Pointer attributes ----------
struct cudaPointerAttributes {
    enum cudaMemoryType type;
    int device;
    void *devicePointer;
    void *hostPointer;
};

// ---------- Copy direction enum ----------
enum cudaMemcpyKind {
    cudaMemcpyHostToHost = 0,
    cudaMemcpyHostToDevice = 1,
    cudaMemcpyDeviceToHost = 2,
    cudaMemcpyDeviceToDevice = 3,
    cudaMemcpyDefault = 4,
};

// ---------- Stream / Event opaque types ----------
// cudaStream_t is an opaque handle to an internal stream object (an in-order
// asynchronous Level Zero immediate command list plus the device it was created
// on). A null stream means "default stream" and behaves synchronously.
typedef void *cudaStream_t;
typedef void *cudaEvent_t;

// ---------- Stream creation flags ----------
// Values match CUDA's. Both are accepted; Level Zero has no notion of a
// blocking-vs-non-blocking relationship with a legacy default stream, and the
// null stream here is synchronous, so the distinction does not apply.
#define cudaStreamDefault 0x00
#define cudaStreamNonBlocking 0x01

// ---------- XPU runtime context (singleton) ----------
// Internal state managed in xpu.cpp; these functions provide
// the cuda-alike API surface that the rest of Mooncake uses.

// Device management
cudaError_t cudaSetDevice(int device);
cudaError_t cudaGetDevice(int *device);
cudaError_t cudaGetDeviceCount(int *count);

// Memory management
cudaError_t cudaMalloc(void **devPtr, size_t size);
cudaError_t cudaFree(void *devPtr);
cudaError_t cudaMemcpy(void *dst, const void *src, size_t count,
                       enum cudaMemcpyKind kind);
// Enqueues the copy on `stream` and returns without waiting for it. Use
// cudaStreamSynchronize() (or cudaStreamQuery()) before reading `dst` or
// freeing `src`. A null stream performs a synchronous copy.
//
// Difference from CUDA: CUDA's legacy default stream implicitly synchronises
// with other blocking streams. Here the default stream and each explicit stream
// are separate Level Zero command lists with no ordering between them, so work
// on the default stream is NOT ordered against work on an explicit stream.
// Sequences that rely on that implicit coupling must synchronise explicitly.
cudaError_t cudaMemcpyAsync(void *dst, const void *src, size_t count,
                            enum cudaMemcpyKind kind, cudaStream_t stream);

// Stream management. A stream is an in-order queue: work submitted to the same
// stream executes in submission order, so dependent copies need no explicit
// synchronisation between them.
//
// Not implemented: the cudaEvent_t family. Level Zero events need an event pool
// with its own lifetime, and nothing in the XPU build uses them yet; a caller
// that needs them gets a link error rather than silently wrong behaviour.
cudaError_t cudaStreamCreate(cudaStream_t *stream);
cudaError_t cudaStreamCreateWithFlags(cudaStream_t *stream, unsigned int flags);
cudaError_t cudaStreamDestroy(cudaStream_t stream);
// Blocks until every command previously submitted to `stream` has completed.
cudaError_t cudaStreamSynchronize(cudaStream_t stream);
// Non-blocking poll: cudaSuccess when the stream is idle, cudaErrorNotReady
// when work is still outstanding.
cudaError_t cudaStreamQuery(cudaStream_t stream);
// Blocks until all work on the current device -- the default stream and every
// live stream created on it -- has completed.
cudaError_t cudaDeviceSynchronize(void);

// Host pinned memory
cudaError_t cudaHostAlloc(void **pHost, size_t size, unsigned int flags);
cudaError_t cudaFreeHost(void *ptr);
#define cudaHostAllocMapped 0x02

// Pointer queries
cudaError_t cudaPointerGetAttributes(struct cudaPointerAttributes *attributes,
                                     const void *ptr);
const char *cudaGetErrorString(cudaError_t error);

// Device PCI bus ID (for topology discovery)
cudaError_t cudaDeviceGetPCIBusId(char *pciBusId, int len, int device);

// ---------- XPU-specific helpers for RDMA DMA-BUF ----------
// These are NOT cuda-alike; they are called explicitly under #ifdef USE_XPU.
namespace mooncake {
namespace xpu {

// Initialise Level Zero (idempotent). Returns 0 on success.
int ensureInitialized();

// Get the Level Zero context handle (created once on init).
ze_context_handle_t getContext();

// Get the Level Zero device handle for the given ordinal.
ze_device_handle_t getDevice(int ordinal);

// Query the base address and size of the allocation containing `ptr`.
// Needed because a tensor may sit at an offset inside a larger allocation
// (PyTorch's caching allocator packs several tensors per block).
// Returns 0 on success, -1 on failure.
int getAllocBase(const void *ptr, void **base, size_t *size);

// Export a DMA-BUF file descriptor for a device allocation.
// Returns >=0 fd on success, -1 on failure. The fd is OWNED BY THE CALLER,
// which must close() it when done. `size` is ignored: the exported dma_buf
// always covers the whole allocation.
int exportDmaBufFd(void *devPtr, size_t size);

// Query whether `ptr` is XPU device memory.
bool isDeviceMemory(const void *ptr);

}  // namespace xpu
}  // namespace mooncake
