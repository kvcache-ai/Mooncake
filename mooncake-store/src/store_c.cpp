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

#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <vector>

#include "real_client.h"
#include "replica.h"
#include "types.h"

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "libetcd_wrapper.h"
#endif

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

// ---------------------------------------------------------------------------
// TLS helpers (internal)
// ---------------------------------------------------------------------------

#ifdef STORE_USE_ETCD
namespace {

// Tracks whether set_etcd_tls_config_internal was called with explicit
// (non-null) parameters.  When false, mooncake_store_setup() automatically
// triggers an env-var fallback so MC_ETCD_CA_FILE / MC_ETCD_CERT_FILE /
// MC_ETCD_KEY_FILE are honoured without an explicit C API call.
bool etcd_tls_explicitly_set = false;

void set_etcd_tls_config_internal(const char *ca_file, const char *cert_file,
                                  const char *key_file) {
    bool use_fallback =
        (ca_file == nullptr && cert_file == nullptr && key_file == nullptr);
    std::string ca = ca_file ? ca_file : "";
    std::string cert = cert_file ? cert_file : "";
    std::string key = key_file ? key_file : "";

    if (!use_fallback) {
        etcd_tls_explicitly_set = true;
    }

    if (use_fallback) {
        const char *env_ca = std::getenv("MC_ETCD_CA_FILE");
        if (env_ca) ca = env_ca;
        const char *env_cert = std::getenv("MC_ETCD_CERT_FILE");
        if (env_cert) cert = env_cert;
        const char *env_key = std::getenv("MC_ETCD_KEY_FILE");
        if (env_key) key = env_key;
    }

    if (ca.empty() && cert.empty() && key.empty()) {
        // No TLS configured
        return;
    }

    // Configure TLS for both etcd clients
    SetGlobalTLSConfig(const_cast<char *>(ca.c_str()),
                       const_cast<char *>(cert.c_str()),
                       const_cast<char *>(key.c_str()));
    mooncake::EtcdHelper::SetTLSConfig(ca, cert, key);

    LOG(INFO) << "etcd TLS configured via C API / env vars (ca=" << ca << ")";
}

}  // anonymous namespace
#endif  // STORE_USE_ETCD

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

void mooncake_store_set_etcd_tls_config(mooncake_store_t store,
                                        const char *ca_file,
                                        const char *cert_file,
                                        const char *key_file) {
    (void)store;  // Store handle is not needed (TLS is global)
#ifdef STORE_USE_ETCD
    set_etcd_tls_config_internal(ca_file, cert_file, key_file);
#endif
}

int mooncake_store_setup(mooncake_store_t store, const char *local_hostname,
                         const char *metadata_server,
                         uint64_t global_segment_size,
                         uint64_t local_buffer_size, const char *protocol,
                         const char *device_name,
                         const char *master_server_addr) {
    if (!store) return -1;
#ifdef STORE_USE_ETCD
    // Auto-load MC_ETCD_CA_FILE / MC_ETCD_CERT_FILE / MC_ETCD_KEY_FILE from
    // environment if set_etcd_tls_config_internal was never called with
    // explicit parameters.
    if (!etcd_tls_explicitly_set) {
        set_etcd_tls_config_internal(nullptr, nullptr, nullptr);
    }
#endif
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

}  // extern "C"
