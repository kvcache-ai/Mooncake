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

#ifndef TENT_DEVICE_PLUGIN_H
#define TENT_DEVICE_PLUGIN_H

#include <stddef.h>
#include <stdint.h>

#define LOCATION_LEN 32

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

typedef struct location_t {
    void* start;
    size_t length;
    char location[LOCATION_LEN];
} location_t;

typedef struct device_plugin_t {
    const char* class_name;

    void* (*create_plugin)();
    int (*destroy_plugin)(void* handle);
    int (*alloc)(void* ctx, void** pptr, size_t size, const char* location);
    int (*free)(void* ctx, void* ptr, size_t size);
    int (*memcpy_sync)(void* ctx, void* dst, void* src, size_t length);
    int (*query_location)(void* ctx, void* addr, size_t size, location_t* buf,
                          size_t buf_count);
    int (*get_device_count)(void* ctx);
    int (*get_device_pci_bus_id)(void* ctx, int device_index, char* pci_bus_id,
                                 size_t len);
} device_plugin_t;

#define TENT_DEVICE_PLUGIN_REGISTER_SYMBOL "tent_register_device_plugin"

typedef int (*tent_register_device_plugin_fn)(device_plugin_t* out);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // TENT_DEVICE_PLUGIN_H