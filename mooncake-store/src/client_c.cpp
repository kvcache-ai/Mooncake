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

#include "client_c.h"
#include "client.h"
#include "utils.h"
#include <cstdlib>
#include <cstring>

using namespace mooncake;

// 管理 client 对象的生命周期
void* create_obj(std::shared_ptr<Client> client) {
    return new std::shared_ptr<void>(client);
}

void* get_raw(void* handle) {
    return reinterpret_cast<std::shared_ptr<void>*>(handle)->get();
}

void destroy_obj(void* handle) {
    delete reinterpret_cast<std::shared_ptr<void>*>(handle);
}

client_t mooncake_client_create(const char* local_hostname,
                                const char* metadata_connstring,
                                const char* protocol, const char* rdma_devices,
                                const char* master_server_entry) {
    std::optional<std::string> device_name =
        (rdma_devices && strcmp(rdma_devices, "") != 0)
            ? std::make_optional<std::string>(rdma_devices)
            : std::nullopt;
    std::optional<std::shared_ptr<Client>> native =
        Client::Create(local_hostname, metadata_connstring, protocol,
                       device_name, master_server_entry);
    if (native) {
        return create_obj(native.value());
    } else {
        return nullptr;
    }
}

ErrorCode_t mooncake_client_register_local_memory(client_t client, void* addr,
                                                  size_t length,
                                                  const char* location,
                                                  bool remote_accessible,
                                                  bool update_metadata) {
    Client* native_client = (Client*)get_raw(client);
    if (native_client == nullptr) {
        return MOONCAKE_ERROR_INVALID_PARAMS;
    }
    if (length == 0 || addr == nullptr) {
        return MOONCAKE_ERROR_OK;
    }
    auto result = native_client->RegisterLocalMemory(
        addr, length, location, remote_accessible, update_metadata);
    if (result) {
        return MOONCAKE_ERROR_OK;
    } else {
        LOG(ERROR) << "Failed to register local memory: "
                   << toString(result.error());
        return static_cast<ErrorCode_t>(result.error());
    }
}

ErrorCode_t mooncake_client_unregister_local_memory(client_t client, void* addr,
                                                    bool update_metadata) {
    Client* native_client = (Client*)get_raw(client);
    if (native_client == nullptr) {
        return MOONCAKE_ERROR_INVALID_PARAMS;
    }
    if (addr == nullptr) {
        return MOONCAKE_ERROR_OK;
    }
    auto result = native_client->unregisterLocalMemory(addr, update_metadata);
    if (result) {
        return MOONCAKE_ERROR_OK;
    } else {
        LOG(ERROR) << "Failed to unregister local memory: "
                   << toString(result.error());
        return static_cast<ErrorCode_t>(result.error());
    }
}

ErrorCode_t mooncake_client_mount_segment(client_t client, void* segment_ptr,
                                          size_t size) {
    Client* native_client = (Client*)get_raw(client);
    if (native_client == nullptr) {
        return MOONCAKE_ERROR_INVALID_PARAMS;
    }
    if (segment_ptr == nullptr || size == 0) {
        return MOONCAKE_ERROR_INVALID_PARAMS;
    }
    auto result = native_client->MountSegment(segment_ptr, size);
    if (result) {
        return MOONCAKE_ERROR_OK;
    } else {
        LOG(ERROR) << "Failed to mount segment: " << toString(result.error());
        return static_cast<ErrorCode_t>(result.error());
    }
}

ErrorCode_t mooncake_client_unmount_segment(client_t client, void* segment_ptr,
                                            size_t size) {
    Client* native_client = (Client*)get_raw(client);
    if (native_client == nullptr) {
        return MOONCAKE_ERROR_INVALID_PARAMS;
    }
    if (segment_ptr == nullptr || size == 0) {
        return MOONCAKE_ERROR_INVALID_PARAMS;
    }
    auto result = native_client->UnmountSegment(segment_ptr, size);
    if (result) {
        return MOONCAKE_ERROR_OK;
    } else {
        LOG(ERROR) << "Failed to unmount segment: " << toString(result.error());
        return static_cast<ErrorCode_t>(result.error());
    }
}

ErrorCode_t mooncake_client_get(client_t client, const char* key,
                                Slice_t* slices, size_t slices_count) {
    Client* native_client = (Client*)get_raw(client);
    std::vector<Slice> slices_vector;
    for (size_t i = 0; i < slices_count; i++) {
        Slice slice;
        slice.ptr = slices[i].ptr;
        slice.size = slices[i].size;
        slices_vector.push_back(slice);
    }
    auto result = native_client->Get(key, slices_vector);
    if (result) {
        return MOONCAKE_ERROR_OK;
    } else {
        LOG(ERROR) << "Failed to get: " << toString(result.error());
        return static_cast<ErrorCode_t>(result.error());
    }
}

ErrorCode_t mooncake_client_put(client_t client, const char* key,
                                Slice_t* slices, size_t slices_count,
                                const ReplicateConfig_t config) {
    Client* native_client = (Client*)get_raw(client);
    std::vector<Slice> slices_vector;
    for (size_t i = 0; i < slices_count; i++) {
        Slice slice;
        slice.ptr = slices[i].ptr;
        slice.size = slices[i].size;
        slices_vector.push_back(slice);
    }
    ReplicateConfig cpp_config;
    cpp_config.replica_num = config.replica_num;
    cpp_config.preferred_segment =
        config.preferred_segment ? config.preferred_segment : "";
    auto result = native_client->Put(key, slices_vector, cpp_config);
    if (result) {
        return MOONCAKE_ERROR_OK;
    } else {
        LOG(ERROR) << "Failed to put: " << toString(result.error());
        return static_cast<ErrorCode_t>(result.error());
    }
}

ErrorCode_t mooncake_client_isexist(client_t client, const char* key) {
    Client* native_client = (Client*)get_raw(client);
    auto result = native_client->IsExist(key);
    if (result) {
        if (result.value() == true) {
            return MOONCAKE_ERROR_OK;
        } else {
            return MOONCAKE_ERROR_OBJECT_NOT_FOUND;
        }
    } else {
        LOG(ERROR) << "Failed to query: " << toString(result.error());
        return static_cast<ErrorCode_t>(result.error());
    }
}

ErrorCode_t mooncake_client_remove(client_t client, const char* key) {
    Client* native_client = (Client*)get_raw(client);
    auto result = native_client->Remove(key);
    if (result) {
        return MOONCAKE_ERROR_OK;
    } else {
        LOG(ERROR) << "Failed to remove: " << toString(result.error());
        return static_cast<ErrorCode_t>(result.error());
    }
}

long mooncake_client_remove_byregex(client_t client, const char* regex) {
    Client* native_client = (Client*)get_raw(client);
    if (regex == nullptr) {
        return MOONCAKE_ERROR_INVALID_PARAMS;
    }
    auto result = native_client->RemoveByRegex(regex);
    if (result) {
        return result.value();
    } else {
        LOG(ERROR) << "Failed to remove by regex: " << toString(result.error());
        return -1;
    }
}

long mooncake_client_remove_all(client_t client) {
    Client* native_client = (Client*)get_raw(client);
    auto result = native_client->RemoveAll();
    if (result) {
        return result.value();
    } else {
        LOG(ERROR) << "Failed to remove all: " << toString(result.error());
        return -1;
    }
}

ErrorCode_span_t mooncake_client_batch_get(client_t client, BatchItem_t* items,
                                           size_t items_count) {
    ErrorCode_span_t error_span;
    error_span.results_count = items_count;
    error_span.results =
        (ErrorCode_t*)malloc(items_count * sizeof(ErrorCode_t));
    // init all results to MOONCAKE_ERROR_INTERNAL_ERROR
    memset(error_span.results, 0xFF, items_count * sizeof(ErrorCode_t));

    Client* native_client = (Client*)get_raw(client);
    if (items == nullptr || items_count == 0) {
        return error_span;
    }

    // Prepare keys and slices for C++ BatchGet
    std::vector<std::string> object_keys;
    std::unordered_map<std::string, std::vector<Slice>> slices_map;
    object_keys.reserve(items_count);

    for (size_t i = 0; i < items_count; ++i) {
        std::string key = items[i].key ? items[i].key : "";
        object_keys.push_back(key);

        // Convert C slices to C++ slices
        std::vector<Slice> cpp_slices;
        cpp_slices.reserve(items[i].slices_span->slices_count);
        for (size_t j = 0; j < items[i].slices_span->slices_count; ++j) {
            Slice slice;
            slice.ptr = items[i].slices_span->slices[j].ptr;
            slice.size = items[i].slices_span->slices[j].size;
            cpp_slices.push_back(slice);
        }
        slices_map[key] = std::move(cpp_slices);
    }

    // Call C++ BatchGet
    auto cpp_results = native_client->BatchGet(object_keys, slices_map);

    // Copy results back to C results array
    for (size_t i = 0; i < cpp_results.size(); ++i) {
        const auto& result = cpp_results[i];

        if (result) {
            error_span.results[i] = MOONCAKE_ERROR_OK;
        } else {
            error_span.results[i] = static_cast<ErrorCode_t>(result.error());
            LOG(ERROR) << "BatchGet failed for key " << object_keys[i] << ": "
                       << toString(result.error());
        }
    }

    return error_span;
}

ErrorCode_span_t mooncake_client_batch_put(client_t client, BatchItem_t* items,
                                           size_t items_count,
                                           const ReplicateConfig_t config) {
    ErrorCode_span_t error_span;
    error_span.results_count = items_count;
    error_span.results =
        (ErrorCode_t*)malloc(items_count * sizeof(ErrorCode_t));
    // init all results to MOONCAKE_ERROR_INTERNAL_ERROR
    memset(error_span.results, 0xFF, items_count * sizeof(ErrorCode_t));

    Client* native_client = (Client*)get_raw(client);
    if (items == nullptr || items_count == 0) {
        return error_span;
    }

    // Prepare keys and slices for C++ BatchPut
    std::vector<std::string> object_keys;
    std::vector<std::vector<Slice>> batched_slices;
    object_keys.reserve(items_count);
    batched_slices.reserve(items_count);

    for (size_t i = 0; i < items_count; ++i) {
        std::string key = items[i].key ? items[i].key : "";
        object_keys.push_back(key);

        // Convert C slices to C++ slices
        std::vector<Slice> cpp_slices;
        cpp_slices.reserve(items[i].slices_span->slices_count);
        for (size_t j = 0; j < items[i].slices_span->slices_count; ++j) {
            Slice slice;
            slice.ptr = items[i].slices_span->slices[j].ptr;
            slice.size = items[i].slices_span->slices[j].size;
            cpp_slices.push_back(slice);
        }
        batched_slices.push_back(std::move(cpp_slices));
    }

    // Convert C config to C++ config
    ReplicateConfig cpp_config;
    cpp_config.replica_num = config.replica_num;
    cpp_config.preferred_segment =
        config.preferred_segment ? config.preferred_segment : "";

    // Call C++ BatchPut
    auto cpp_results =
        native_client->BatchPut(object_keys, batched_slices, cpp_config);

    // Copy results back to C results array
    for (size_t i = 0; i < cpp_results.size(); ++i) {
        const auto& result = cpp_results[i];

        if (result) {
            error_span.results[i] = MOONCAKE_ERROR_OK;
        } else {
            error_span.results[i] = static_cast<ErrorCode_t>(result.error());
            LOG(ERROR) << "BatchPut failed for key " << object_keys[i] << ": "
                       << toString(result.error());
        }
    }

    return error_span;
}

void mooncake_client_destroy(client_t client) { destroy_obj(client); }

uint64_t mooncake_max_slice_size() { return kMaxSliceSize; }

void* mooncake_allocate_segment_memory(size_t size) {
    if (size == 0) {
        return nullptr;
    }
    return allocate_buffer_allocator_memory(size);
}
