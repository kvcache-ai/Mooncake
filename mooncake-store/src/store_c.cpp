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
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "real_client.h"
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

std::optional<mooncake::ReplicateConfig> to_replicate_config(
    const mooncake_replicate_config_t *c_config) {
    mooncake::ReplicateConfig config;
    if (!c_config) return config;

    const int action = c_config->soft_pin_action;
    // Reject before casting: the enum's uint8_t underlying type would
    // otherwise wrap out-of-range values into valid actions.
    if (action < static_cast<int>(mooncake::SoftPinAction::PRESERVE) ||
        action > static_cast<int>(mooncake::SoftPinAction::DISABLE)) {
        return std::nullopt;
    }
    config.replica_num = c_config->replica_num;
    config.soft_pin_action = static_cast<mooncake::SoftPinAction>(action);
    if (c_config->has_soft_pin_ttl) {
        config.soft_pin_ttl_ms = c_config->soft_pin_ttl_ms;
    }
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

mooncake::ExistOptions to_exist_options(
    const mooncake_exist_options_t *c_options) {
    mooncake::ExistOptions options;
    if (!c_options) return options;
    options.prefetch_to_memory = c_options->prefetch_to_memory != 0;
    return options;
}

using mooncake::ErrorCode;

template <typename Fn>
int with_migration_store(mooncake_store_t store, Fn &&fn) {
    if (!store) {
        return toInt(ErrorCode::INVALID_PARAMS);
    }
    try {
        return toInt(fn(*as_client(store)));
    } catch (...) {
        return toInt(ErrorCode::INTERNAL_ERROR);
    }
}

template <typename Output, typename Fn>
int with_migration_output(mooncake_store_t store, Output *out, Fn &&fn) {
    if (!out) {
        return toInt(ErrorCode::INVALID_PARAMS);
    }
    *out = {};
    return with_migration_store(store, std::forward<Fn>(fn));
}

bool read_segments(const char *const *names, size_t count,
                   std::vector<std::string> &out) {
    if (count && !names) {
        return false;
    }
    out.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        if (!names[i] || !*names[i]) {
            return false;
        }
        out.emplace_back(names[i]);
    }
    return true;
}

mooncake_store_uuid_t c_uuid(const mooncake::UUID &id) {
    return {id.first, id.second};
}

ErrorCode write_id(const tl::expected<mooncake::UUID, ErrorCode> &result,
                   mooncake_store_uuid_t *out) {
    if (!result) {
        return result.error();
    }
    *out = c_uuid(*result);
    return ErrorCode::OK;
}

const char *copy_c_string(char *&cursor, const std::string &value) {
    auto *start = cursor;
    const auto size = value.size() + 1;
    std::memcpy(start, value.c_str(), size);
    cursor += size;
    return start;
}

// Single malloc block: optional page header, job array, segment pointers,
// then NUL-terminated strings. Only the leading object is freed by the caller.
ErrorCode allocate_drain_results(
    std::span<const mooncake::QueryJobResponse> jobs, const std::string &token,
    mooncake_store_drain_job_info_t **job_out,
    mooncake_store_drain_job_page_t **page_out) {
    constexpr size_t job_alignment = alignof(mooncake_store_drain_job_info_t);
    constexpr size_t page_size = sizeof(mooncake_store_drain_job_page_t);
    constexpr size_t aligned_page_size =
        (page_size + job_alignment - 1) / job_alignment * job_alignment;
    static_assert(alignof(mooncake_store_drain_job_info_t) >=
                  alignof(const char *));
    size_t pointer_count = 0;
    size_t string_bytes = token.size() + 1;
    for (const auto &job : jobs) {
        pointer_count += job.segments.size();
        string_bytes += job.message.size() + 1;
        for (const auto &segment : job.segments) {
            string_bytes += segment.size() + 1;
        }
    }
    const size_t header_size = page_out ? aligned_page_size : 0;
    auto *block = static_cast<char *>(std::malloc(
        header_size + jobs.size() * sizeof(mooncake_store_drain_job_info_t) +
        pointer_count * sizeof(const char *) + string_bytes));
    if (!block) {
        return ErrorCode::INTERNAL_ERROR;
    }
    auto *infos = reinterpret_cast<mooncake_store_drain_job_info_t *>(
        block + header_size);
    auto *segments = reinterpret_cast<const char **>(infos + jobs.size());
    auto *strings = reinterpret_cast<char *>(segments + pointer_count);
    for (size_t i = 0; i < jobs.size(); ++i) {
        const auto &job = jobs[i];
        infos[i] = {.id = c_uuid(job.id),
                    .status = static_cast<int32_t>(job.status),
                    .created_at_ms_epoch = job.created_at_ms_epoch,
                    .last_updated_at_ms_epoch = job.last_updated_at_ms_epoch,
                    .segments = job.segments.empty() ? nullptr : segments,
                    .segment_count = job.segments.size(),
                    .succeeded_units = job.succeeded_units,
                    .failed_units = job.failed_units,
                    .blocked_units = job.blocked_units,
                    .active_units = job.active_units,
                    .migrated_bytes = job.migrated_bytes,
                    .message = copy_c_string(strings, job.message)};
        for (const auto &segment : job.segments) {
            *segments++ = copy_c_string(strings, segment);
        }
    }
    if (page_out) {
        auto *page = reinterpret_cast<mooncake_store_drain_job_page_t *>(block);
        *page = {.jobs = jobs.empty() ? nullptr : infos,
                 .count = jobs.size(),
                 .next_page_token = copy_c_string(strings, token)};
        *page_out = page;
    } else {
        *job_out = infos;
    }
    return ErrorCode::OK;
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
        if (!rc) return -1;
        std::span<const char> data(static_cast<const char *>(value), size);
        return as_client(store)->put(key, data, *rc);
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
        if (!rc) return -1;
        return as_client(store)->put_from(key, buffer, size, *rc);
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
        if (!rc) return -1;
        auto results =
            as_client(store)->batch_put_from(key_vec, buf_vec, size_vec, *rc);

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
// Multi-buffer batch operations
// ---------------------------------------------------------------------------

namespace {

// Nest the flat buffers and sizes per key by `counts`. Empty when a key or
// a non-empty slice is null.
struct NestedBuffers {
    std::vector<std::string> keys;
    std::vector<std::vector<void *>> buffers;
    std::vector<std::vector<size_t>> sizes;
};

std::optional<NestedBuffers> nest_buffers(const char **keys, void **buffers,
                                          const size_t *sizes,
                                          const size_t *counts, size_t count) {
    NestedBuffers out;
    out.keys.reserve(count);
    out.buffers.reserve(count);
    out.sizes.reserve(count);
    size_t at = 0;
    for (size_t i = 0; i < count; ++i) {
        if (!keys[i]) return std::nullopt;
        for (size_t j = at; j < at + counts[i]; ++j) {
            if (!buffers[j] && sizes[j] > 0) return std::nullopt;
        }
        out.keys.emplace_back(keys[i]);
        out.buffers.emplace_back(buffers + at, buffers + at + counts[i]);
        out.sizes.emplace_back(sizes + at, sizes + at + counts[i]);
        at += counts[i];
    }
    return out;
}

}  // namespace

int mooncake_store_batch_put_from_multi_buffers(
    mooncake_store_t store, const char **keys, void **buffers,
    const size_t *sizes, const size_t *counts, size_t count,
    const mooncake_replicate_config_t *config, int *results_out) {
    if (!store || !keys || !buffers || !sizes || !counts || !results_out)
        return -1;
    try {
        auto nested = nest_buffers(keys, buffers, sizes, counts, count);
        if (!nested) return -1;
        auto rc = to_replicate_config(config);
        if (!rc) return -1;
        auto results = as_client(store)->batch_put_from_multi_buffers(
            nested->keys, nested->buffers, nested->sizes, *rc);

        for (size_t i = 0; i < count; ++i) {
            results_out[i] = (i < results.size()) ? results[i] : -1;
        }
        return 0;
    } catch (...) {
        return -1;
    }
}

int mooncake_store_batch_get_into_multi_buffers(
    mooncake_store_t store, const char **keys, void **buffers,
    const size_t *sizes, const size_t *counts, size_t count,
    int prefer_alloc_in_same_node, int64_t *results_out) {
    if (!store || !keys || !buffers || !sizes || !counts || !results_out)
        return -1;
    try {
        auto nested = nest_buffers(keys, buffers, sizes, counts, count);
        if (!nested) return -1;
        auto results = as_client(store)->batch_get_into_multi_buffers(
            nested->keys, nested->buffers, nested->sizes,
            prefer_alloc_in_same_node != 0);

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

int mooncake_store_is_exist_with_options(
    mooncake_store_t store, const char *key,
    const mooncake_exist_options_t *options) {
    if (!store || !key) return -1;
    try {
        return as_client(store)->isExist(std::string(key),
                                         to_exist_options(options));
    } catch (...) {
        return -1;
    }
}

int mooncake_store_batch_is_exist_with_options(
    mooncake_store_t store, const char **keys, size_t count, int *results_out,
    const mooncake_exist_options_t *options) {
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

        auto results =
            as_client(store)->batchIsExist(key_vec, to_exist_options(options));

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

int mooncake_store_create_copy_task(mooncake_store_t store, const char *key,
                                    const char *const *targets,
                                    size_t target_count,
                                    mooncake_store_uuid_t *task_id_out) {
    return with_migration_output(store, task_id_out, [&](auto &client) {
        std::vector<std::string> target_segments;
        if (!key || target_count == 0 ||
            !read_segments(targets, target_count, target_segments)) {
            return ErrorCode::INVALID_PARAMS;
        }
        return write_id(client.create_copy_task(key, target_segments),
                        task_id_out);
    });
}

int mooncake_store_create_move_task(mooncake_store_t store, const char *key,
                                    const char *source, const char *target,
                                    mooncake_store_uuid_t *task_id_out) {
    return with_migration_output(store, task_id_out, [&](auto &client) {
        if (!key || !source || !*source || !target || !*target) {
            return ErrorCode::INVALID_PARAMS;
        }
        return write_id(client.create_move_task(key, source, target),
                        task_id_out);
    });
}

int mooncake_store_query_task(mooncake_store_t store,
                              mooncake_store_uuid_t task_id,
                              mooncake_store_task_info_t **result_out) {
    return with_migration_output(store, result_out, [&](auto &client) {
        auto result = client.query_task({task_id.high, task_id.low});
        if (!result) {
            return result.error();
        }
        auto *info = static_cast<mooncake_store_task_info_t *>(std::malloc(
            sizeof(mooncake_store_task_info_t) + result->message.size() + 1));
        if (!info) {
            return ErrorCode::INTERNAL_ERROR;
        }
        auto *message = reinterpret_cast<char *>(info + 1);
        *info = {.id = c_uuid(result->id),
                 .type = static_cast<int32_t>(result->type),
                 .status = static_cast<int32_t>(result->status),
                 .created_at_ms_epoch = result->created_at_ms_epoch,
                 .last_updated_at_ms_epoch = result->last_updated_at_ms_epoch,
                 .assigned_client = c_uuid(result->assigned_client),
                 .message = copy_c_string(message, result->message)};
        *result_out = info;
        return ErrorCode::OK;
    });
}

int mooncake_store_create_drain_job(
    mooncake_store_t store, const mooncake_store_drain_job_request_t *request,
    mooncake_store_uuid_t *job_id_out) {
    return with_migration_output(store, job_id_out, [&](auto &client) {
        mooncake::CreateDrainJobRequest native;
        if (!request || request->segment_count == 0 ||
            request->max_concurrency == 0 ||
            !read_segments(request->segments, request->segment_count,
                           native.segments) ||
            !read_segments(request->target_segments, request->target_count,
                           native.target_segments)) {
            return ErrorCode::INVALID_PARAMS;
        }
        native.max_concurrency = request->max_concurrency;
        return write_id(client.create_drain_job(native), job_id_out);
    });
}

int mooncake_store_query_drain_job(
    mooncake_store_t store, mooncake_store_uuid_t job_id,
    mooncake_store_drain_job_info_t **result_out) {
    return with_migration_output(store, result_out, [&](auto &client) {
        auto result = client.query_drain_job({job_id.high, job_id.low});
        if (!result) {
            return result.error();
        }
        return allocate_drain_results({&*result, 1}, "", result_out, nullptr);
    });
}

int mooncake_store_cancel_drain_job(mooncake_store_t store,
                                    mooncake_store_uuid_t job_id) {
    return with_migration_store(store, [&](auto &client) {
        auto result = client.cancel_drain_job({job_id.high, job_id.low});
        return result ? ErrorCode::OK : result.error();
    });
}

int mooncake_store_list_drain_jobs(
    mooncake_store_t store, const mooncake_store_drain_job_filter_t *filter,
    mooncake_store_drain_job_page_t **result_out) {
    return with_migration_output(store, result_out, [&](auto &client) {
        mooncake::DrainJobListRequest request;
        if (filter) {
            if (filter->has_status) {
                if (filter->status < MOONCAKE_STORE_DRAIN_CREATED ||
                    filter->status > MOONCAKE_STORE_DRAIN_CANCELED) {
                    return ErrorCode::INVALID_PARAMS;
                }
                request.status =
                    static_cast<mooncake::JobStatus>(filter->status);
            }
            if (filter->segment) {
                request.segment = filter->segment;
            }
            request.page_token = c_str_or(filter->page_token, "");
            if (filter->limit != 0) {
                request.limit = filter->limit;
            }
        }
        auto result = client.list_drain_jobs(request);
        if (!result) {
            return result.error();
        }
        return allocate_drain_results(result->jobs, result->next_page_token,
                                      nullptr, result_out);
    });
}

}  // extern "C"
