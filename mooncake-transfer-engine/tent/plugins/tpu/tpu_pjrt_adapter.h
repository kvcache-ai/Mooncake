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

// Control API of Mooncake's built-in TPU adapter, libmooncake_tpu_pjrt.so.
//
// The adapter library serves two callers, which must load the SAME file so
// they share one registry (dlopen() of an already-loaded file returns it):
//   * TENT's TpuPjrtShim loads it via MC_TPU_PJRT_LIB and calls the
//     mc_tpu_pjrt_* data-path ABI (tent/platform/tpu_pjrt_abi.h).
//   * mooncake.tpu (Python, via ctypes) calls the functions below to bind a
//     buffer provider and register framework buffers BEFORE the
//     TransferEngine is created, because TENT reads the TPU device topology
//     from the adapter once, at engine construction.

#ifndef TENT_PLUGINS_TPU_TPU_PJRT_ADAPTER_H_
#define TENT_PLUGINS_TPU_TPU_PJRT_ADAPTER_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Loads the TPU buffer provider library at `path` (tpu_buffer_provider.h) and
// binds its function table. Calling it again with the same path is a no-op; a
// different path fails while a provider is bound. Returns 0 on success.
int mc_tpu_adapter_load_provider(const char *path);

// Registers provider handle `buffer`. The adapter takes ownership of the handle
// even on failure, and releases it once it is unregistered and no copy uses
// it. On success writes the buffer's token (the address to register with the
// TransferEngine), size in bytes and device index, and returns 0.
int mc_tpu_adapter_register(uint64_t buffer, uint64_t *token_out,
                            uint64_t *size_out, int *device_out);

// Unregisters the buffer whose token is exactly `token`. Unregister it from the
// TransferEngine first. Returns 0 on success, non-zero for an unknown token.
int mc_tpu_adapter_unregister(uint64_t token);

// Releases a provider handle that was never passed to mc_tpu_adapter_register
// (e.g. when registering a batch fails part-way). Returns 0 on success.
int mc_tpu_adapter_release_buffer(uint64_t buffer);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // TENT_PLUGINS_TPU_TPU_PJRT_ADAPTER_H_
