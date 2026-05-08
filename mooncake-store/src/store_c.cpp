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

#include "store_c.h"

#include <cstring>
#include <cinttypes>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <vector>

#include "real_client.h"
#include "client_service.h"
#include "replica.h"
#include "types.h"

namespace {

// Thin wrapper to keep the shared_ptr alive across the C API boundary.
// RealClient::create() returns shared_ptr and registers a weak_ptr in
// ResourceTracker, so we must not let the shared_ptr die prematurely.
struct StoreHandle {
    std::shared_ptr<mooncake::RealClient> client;
};

inline const char *c_str_or(const char *s, const char *fallback) {
    return s ? s : fallback;
}

mooncake::ReplicateConfig to_replicate_config(
    const mooncake_replicate_config_t *c_config) {
    mooncake::ReplicateConfig config;
    if (!c_config) return config;

    config.replica_num = c_config->replica_num;
    config.with_soft_pin = c_config->with_soft_pin != 0;
    config.with_hard_pin = c_config->with_hard_pin != 0;
    if (c_config->preferred_segments &&
        c_config->preferred_segments_count > 0) {
        for (size_t i = 0; i < c_config->preferred_segments_count; ++i) {
            if (c_config->preferred_segments[i]) {
                config.preferred_segments.emplace_back(
                    c_config->preferred_segments[i]);
            }
        }
    }
    return config;
}

StoreHandle *as_handle(mooncake_store_t store) {
    return static_cast<StoreHandle *>(store);
}

mooncake::RealClient *as_client(mooncake_store_t store) {
    return as_handle(store)->client.get();
}

}  // namespace

extern "C" {

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

mooncake_store_t mooncake_store_create() {
    try {
        auto client = mooncake::RealClient::create();
        if (!client) return nullptr;
        auto *handle = new (std::nothrow) StoreHandle{std::move(client)};
        if (!handle) return nullptr;
        return static_cast<mooncake_store_t>(handle);
    } catch (...) {
        return nullptr;
    }
}

void mooncake_store_destroy(mooncake_store_t store) {
    if (!store) return;
    auto *handle = as_handle(store);
    try {
        if (handle->client) {
            handle->client->tearDownAll();
        }
    } catch (...) {
    }
    delete handle;
}

int mooncake_store_setup(mooncake_store_t store, const char *local_hostname,
                         const char *metadata_server,
                         uint64_t global_segment_size,
                         uint64_t local_buffer_size, const char *protocol,
                         const char *device_name,
                         const char *master_server_addr) {
    if (!store) return -1;
    try {
        return as_client(store)->setup_real(
            c_str_or(local_hostname, ""), c_str_or(metadata_server, ""),
            global_segment_size, local_buffer_size, c_str_or(protocol, "tcp"),
            c_str_or(device_name, ""),
            c_str_or(master_server_addr, "127.0.0.1:50051"));
    } catch (...) {
        return -1;
    }
}

int mooncake_store_init_all(mooncake_store_t store, const char *protocol,
                            const char *device_name,
                            uint64_t mount_segment_size) {
    if (!store) return -1;
    try {
        return as_client(store)->initAll(c_str_or(protocol, "tcp"),
                                         c_str_or(device_name, ""),
                                         mount_segment_size);
    } catch (...) {
        return -1;
    }
}

int mooncake_store_health_check(mooncake_store_t store) {
    if (!store) return -1;
    try {
        return as_client(store)->health_check();
    } catch (...) {
        return -1;
    }
}

// ---------------------------------------------------------------------------
// Put operations
// ---------------------------------------------------------------------------

int mooncake_store_put(mooncake_store_t store, const char *key,
                       const void *value, size_t size,
                       const mooncake_replicate_config_t *config) {
    if (!store || !key) return -1;
    if (!value && size > 0) return -1;
    try {
        auto rc = to_replicate_config(config);
        std::span<const char> data(static_cast<const char *>(value), size);
        return as_client(store)->put(key, data, rc);
    } catch (...) {
        return -1;
    }
}

int mooncake_store_put_from(mooncake_store_t store, const char *key,
                            void *buffer, size_t size,
                            const mooncake_replicate_config_t *config) {
    if (!store || !key) return -1;
    if (!buffer && size > 0) return -1;
    try {
        auto rc = to_replicate_config(config);
        return as_client(store)->put_from(key, buffer, size, rc);
    } catch (...) {
        return -1;
    }
}

int mooncake_store_batch_put_from(mooncake_store_t store, const char **keys,
                                  void **buffers, const size_t *sizes,
                                  size_t count,
                                  const mooncake_replicate_config_t *config,
                                  int *results_out) {
    if (!store || !keys || !buffers || !sizes || !results_out) return -1;
    for (size_t i = 0; i < count; ++i) {
        if (!keys[i] || (!buffers[i] && sizes[i] > 0)) return -1;
    }
    try {
        std::vector<std::string> key_vec;
        key_vec.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            key_vec.emplace_back(keys[i]);
        }
        std::vector<void *> buf_vec(buffers, buffers + count);
        std::vector<size_t> size_vec(sizes, sizes + count);

        auto rc = to_replicate_config(config);
        auto results =
            as_client(store)->batch_put_from(key_vec, buf_vec, size_vec, rc);

        for (size_t i = 0; i < count; ++i) {
            results_out[i] = (i < results.size()) ? results[i] : -1;
        }
        return 0;
    } catch (...) {
        return -1;
    }
}

// ---------------------------------------------------------------------------
// Get operations
// ---------------------------------------------------------------------------

int64_t mooncake_store_get_into(mooncake_store_t store, const char *key,
                                void *buffer, size_t size) {
    if (!store || !key) return -1;
    if (!buffer && size > 0) return -1;
    try {
        return as_client(store)->get_into(key, buffer, size);
    } catch (...) {
        return -1;
    }
}

int mooncake_store_batch_get_into(mooncake_store_t store, const char **keys,
                                  void **buffers, const size_t *sizes,
                                  size_t count, int64_t *results_out) {
    if (!store || !keys || !buffers || !sizes || !results_out) return -1;
    for (size_t i = 0; i < count; ++i) {
        if (!keys[i] || (!buffers[i] && sizes[i] > 0)) return -1;
    }
    try {
        std::vector<std::string> key_vec;
        key_vec.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            key_vec.emplace_back(keys[i]);
        }
        std::vector<void *> buf_vec(buffers, buffers + count);
        std::vector<size_t> size_vec(sizes, sizes + count);

        auto results =
            as_client(store)->batch_get_into(key_vec, buf_vec, size_vec);

        for (size_t i = 0; i < count; ++i) {
            results_out[i] = (i < results.size()) ? results[i] : -1;
        }
        return 0;
    } catch (...) {
        return -1;
    }
}

// ---------------------------------------------------------------------------
// Existence / size / hostname
// ---------------------------------------------------------------------------

int mooncake_store_is_exist(mooncake_store_t store, const char *key) {
    if (!store || !key) return -1;
    try {
        return as_client(store)->isExist(key);
    } catch (...) {
        return -1;
    }
}

int mooncake_store_batch_is_exist(mooncake_store_t store, const char **keys,
                                  size_t count, int *results_out) {
    if (!store || !keys || !results_out) return -1;
    for (size_t i = 0; i < count; ++i) {
        if (!keys[i]) return -1;
    }
    try {
        std::vector<std::string> key_vec;
        key_vec.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            key_vec.emplace_back(keys[i]);
        }

        auto results = as_client(store)->batchIsExist(key_vec);

        for (size_t i = 0; i < count; ++i) {
            results_out[i] = (i < results.size()) ? results[i] : -1;
        }
        return 0;
    } catch (...) {
        return -1;
    }
}

int64_t mooncake_store_get_size(mooncake_store_t store, const char *key) {
    if (!store || !key) return -1;
    try {
        return as_client(store)->getSize(key);
    } catch (...) {
        return -1;
    }
}

int mooncake_store_get_hostname(mooncake_store_t store, char *buf_out,
                                size_t buf_len) {
    if (!store || !buf_out || buf_len == 0) return -1;
    try {
        std::string hostname = as_client(store)->get_hostname();
        if (hostname.size() >= buf_len) return -1;
        std::memcpy(buf_out, hostname.c_str(), hostname.size() + 1);
        return 0;
    } catch (...) {
        return -1;
    }
}

// ---------------------------------------------------------------------------
// Remove operations
// ---------------------------------------------------------------------------

int mooncake_store_remove(mooncake_store_t store, const char *key, int force) {
    if (!store || !key) return -1;
    try {
        return as_client(store)->remove(key, force != 0);
    } catch (...) {
        return -1;
    }
}

int64_t mooncake_store_remove_by_regex(mooncake_store_t store,
                                       const char *pattern, int force) {
    if (!store || !pattern) return -1;
    try {
        return static_cast<int64_t>(
            as_client(store)->removeByRegex(pattern, force != 0));
    } catch (...) {
        return -1;
    }
}

int64_t mooncake_store_remove_all(mooncake_store_t store, int force) {
    if (!store) return -1;
    try {
        return static_cast<int64_t>(as_client(store)->removeAll(force != 0));
    } catch (...) {
        return -1;
    }
}

// ---------------------------------------------------------------------------
// Buffer registration
// ---------------------------------------------------------------------------

int mooncake_store_register_buffer(mooncake_store_t store, void *buffer,
                                   size_t size) {
    if (!store || !buffer || size == 0) return -1;
    try {
        return as_client(store)->register_buffer(buffer, size);
    } catch (...) {
        return -1;
    }
}

int mooncake_store_unregister_buffer(mooncake_store_t store, void *buffer) {
    if (!store) return -1;
    try {
        return as_client(store)->unregister_buffer(buffer);
    } catch (...) {
        return -1;
    }
}

// ---------------------------------------------------------------------------
// Cache statistics
// ---------------------------------------------------------------------------

int mooncake_store_calc_cache_stats(mooncake_store_t store, char *buf_out,
                                    size_t buf_len) {
    if (!store || !buf_out || buf_len == 0) return -1;
    try {
        auto *handle = static_cast<StoreHandle *>(store);
        if (!handle->client || !handle->client->client_) return -1;

        auto stats_result = handle->client->client_->CalcCacheStats();
        if (!stats_result.has_value()) return -1;

        using CS = mooncake::MasterMetricManager::CacheHitStat;
        auto &stats = stats_result.value();

        auto get = [&](CS key) -> double {
            auto it = stats.find(key);
            return it != stats.end() ? it->second : 0.0;
        };

        char tmp[1024];
        int len = snprintf(
            tmp, sizeof(tmp),
            "{\"memory_hits\":%.0f,\"ssd_hits\":%.0f,"
            "\"memory_total\":%.0f,\"ssd_total\":%.0f,"
            "\"memory_hit_rate\":%.4f,\"ssd_hit_rate\":%.4f,"
            "\"overall_hit_rate\":%.4f,\"valid_get_rate\":%.4f}",
            get(CS::MEMORY_HITS), get(CS::SSD_HITS), get(CS::MEMORY_TOTAL),
            get(CS::SSD_TOTAL), get(CS::MEMORY_HIT_RATE),
            get(CS::SSD_HIT_RATE), get(CS::OVERALL_HIT_RATE),
            get(CS::VALID_GET_RATE));

        if (len < 0 || static_cast<size_t>(len) >= sizeof(tmp)) return -1;
        size_t copy_len =
            static_cast<size_t>(len) < buf_len - 1 ? len : buf_len - 1;
        memcpy(buf_out, tmp, copy_len);
        buf_out[copy_len] = '\0';
        return len;
    } catch (...) {
        return -1;
    }
}

int mooncake_store_get_client_stats(mooncake_store_t store, char *buf_out,
                                    size_t buf_len) {
    if (!store || !buf_out || buf_len == 0) return -1;
    try {
        auto *handle = static_cast<StoreHandle *>(store);
        if (!handle->client || !handle->client->client_) return -1;

        auto stats = handle->client->client_->GetClientStats();

        char tmp[512];
        int len = snprintf(
            tmp, sizeof(tmp),
            "{\"get_from_memory_count\":%" PRId64
            ",\"get_from_disk_count\":%" PRId64
            ",\"get_from_memory_bytes\":%" PRId64
            ",\"get_from_disk_bytes\":%" PRId64
            ",\"put_to_memory_count\":%" PRId64
            ",\"put_to_disk_count\":%" PRId64
            ",\"put_to_memory_bytes\":%" PRId64
            ",\"put_to_disk_bytes\":%" PRId64 "}",
            stats.get_from_memory_count, stats.get_from_disk_count,
            stats.get_from_memory_bytes, stats.get_from_disk_bytes,
            stats.put_to_memory_count, stats.put_to_disk_count,
            stats.put_to_memory_bytes, stats.put_to_disk_bytes);

        if (len < 0) return -1;
        if (static_cast<size_t>(len) >= sizeof(tmp)) return -1;  // truncated
        size_t copy_len =
            static_cast<size_t>(len) < buf_len - 1 ? len : buf_len - 1;
        memcpy(buf_out, tmp, copy_len);
        buf_out[copy_len] = '\0';
        return len;
    } catch (...) {
        return -1;
    }
}

}  // extern "C"
