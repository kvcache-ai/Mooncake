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

// Control API for Mooncake's TPU adapter (libmooncake_tpu_pjrt.so).
//
// This library maintains a shared address registry used by two callers:
//
// 1. Python (mooncake.tpu):
//    Calls this control API to bind the buffer provider and register device
//    buffers before creating the TransferEngine.
//
// 2. TENT C++ Engine (TpuPjrtShim):
//    Reads TPU device topology at startup and executes transfers using the
//    data-path ABI (see tent/platform/tpu_pjrt_abi.h).
//
// Both callers must load the same shared library file so they share one
// in-process registry instance.

#ifndef TENT_PLUGINS_TPU_TPU_PJRT_ADAPTER_H_
#define TENT_PLUGINS_TPU_TPU_PJRT_ADAPTER_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Loads the TPU buffer provider shared library at `path` and binds its
// function table (see tent/platform/tpu_buffer_provider.h).
//
// Calling with a different path while a provider is already bound returns
// an error.
//
// Returns 0 on success, non-zero on failure.
int mc_tpu_adapter_load_provider(const char *path);

// Registers a device buffer handle from the provider.
//
// The adapter takes ownership of `buffer` immediately, releasing it when
// unregistered and all active transfers finish.
//
// On success:
// - `synthetic_addr_out`: synthetic address to register with TransferEngine
// - `size_out`: buffer size in bytes
// - `device_out`: TPU device ordinal (e.g. 0 for "tpu:0")
//
// Returns 0 on success, non-zero on failure.
int mc_tpu_adapter_register(uint64_t buffer, uint64_t *synthetic_addr_out,
                            uint64_t *size_out, int *device_out);

// Unregisters a buffer by its synthetic address.
//
// Caller should unregister the address from TransferEngine before calling this.
//
// Returns 0 on success, non-zero if the address is unknown.
int mc_tpu_adapter_unregister(uint64_t synthetic_addr);

// Releases an unregistered buffer handle.
//
// Useful for cleanup if batch registration aborts before passing `buffer` to
// mc_tpu_adapter_register.
//
// Returns 0 on success, non-zero on failure.
int mc_tpu_adapter_release_buffer(uint64_t buffer);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // TENT_PLUGINS_TPU_TPU_PJRT_ADAPTER_H_
