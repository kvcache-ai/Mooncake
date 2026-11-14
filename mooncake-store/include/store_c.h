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

#ifndef MOONCAKE_STORE_C_H
#define MOONCAKE_STORE_C_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *mooncake_store_t;

/*
 * Soft-pin intent for mooncake_replicate_config_t::soft_pin_action.
 * A soft pin guards an object from eviction until a deadline fixed at
 * write time; reads never extend it. Zero-initialized configs default
 * to MOONCAKE_SOFT_PIN_PRESERVE without a TTL.
 */
enum mooncake_soft_pin_action {
    MOONCAKE_SOFT_PIN_PRESERVE = 0, /* keep an existing unexpired deadline */
    MOONCAKE_SOFT_PIN_ENABLE = 1,   /* commit a new soft pin on publish */
    MOONCAKE_SOFT_PIN_DISABLE = 2   /* remove an existing soft pin */
};

struct mooncake_replicate_config {
    size_t replica_num;
    int with_hard_pin;
    const char **preferred_segments;
    size_t preferred_segments_count;
    /* One of enum mooncake_soft_pin_action; put functions return -1
     * for any other value. */
    int soft_pin_action;
    /* TTL override in milliseconds; meaningful only with
     * MOONCAKE_SOFT_PIN_ENABLE. When unset, the master default TTL
     * applies. The master rejects TTLs on other actions and values
     * above its configured maximum. */
    int has_soft_pin_ttl;
    uint64_t soft_pin_ttl_ms;
};
typedef struct mooncake_replicate_config mooncake_replicate_config_t;

/*
 * All memory pointed to by the "char *" parameters will not be used
 * after the C function returns.
 * This means that the caller can free the memory pointed to by "char *"
 * parameters, after the call is completed.
 * All the C functions here follow this convention.
 */

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

mooncake_store_t mooncake_store_create();

void mooncake_store_destroy(mooncake_store_t store);

int mooncake_store_setup(mooncake_store_t store, const char *local_hostname,
                         const char *metadata_server,
                         uint64_t global_segment_size,
                         uint64_t local_buffer_size, const char *protocol,
                         const char *device_name,
                         const char *master_server_addr);

int mooncake_store_init_all(mooncake_store_t store, const char *protocol,
                            const char *device_name,
                            uint64_t mount_segment_size);

int mooncake_store_health_check(mooncake_store_t store);

// ---------------------------------------------------------------------------
// Put operations
// ---------------------------------------------------------------------------

int mooncake_store_put(mooncake_store_t store, const char *key,
                       const void *value, size_t size,
                       const mooncake_replicate_config_t *config);

int mooncake_store_put_from(mooncake_store_t store, const char *key,
                            void *buffer, size_t size,
                            const mooncake_replicate_config_t *config);

int mooncake_store_batch_put_from(mooncake_store_t store, const char **keys,
                                  void **buffers, const size_t *sizes,
                                  size_t count,
                                  const mooncake_replicate_config_t *config,
                                  int *results_out);

// ---------------------------------------------------------------------------
// Get operations
// ---------------------------------------------------------------------------

int64_t mooncake_store_get_into(mooncake_store_t store, const char *key,
                                void *buffer, size_t size);

int mooncake_store_batch_get_into(mooncake_store_t store, const char **keys,
                                  void **buffers, const size_t *sizes,
                                  size_t count, int64_t *results_out);

// ---------------------------------------------------------------------------
// Multi-buffer batch operations
// ---------------------------------------------------------------------------
//
// An object spread over several registered buffers (a KV block's per-layer
// slices, say): its value is their bytes in order. `buffers` and `sizes` are
// flat over all keys, key i owning the `counts[i]` entries after key i-1's,
// so there are sum(counts) of each for `count` keys. Every buffer must
// resolve to Store-managed registered memory, as for the single-buffer
// batch functions. Results are per key: 0 or an error code for a put, the
// bytes read (the sum of the key's sizes) or a negative error code for a
// get.

int mooncake_store_batch_put_from_multi_buffers(
    mooncake_store_t store, const char **keys, void **buffers,
    const size_t *sizes, const size_t *counts, size_t count,
    const mooncake_replicate_config_t *config, int *results_out);

// prefer_alloc_in_same_node: when non-zero, the slices of a key that live in
// one segment go out as one transfer.
int mooncake_store_batch_get_into_multi_buffers(
    mooncake_store_t store, const char **keys, void **buffers,
    const size_t *sizes, const size_t *counts, size_t count,
    int prefer_alloc_in_same_node, int64_t *results_out);

// ---------------------------------------------------------------------------
// Existence / size / hostname
// ---------------------------------------------------------------------------

int mooncake_store_is_exist(mooncake_store_t store, const char *key);

int mooncake_store_batch_is_exist(mooncake_store_t store, const char **keys,
                                  size_t count, int *results_out);

// Options for the *_with_options existence checks. All-zero means the
// legacy behavior.
struct mooncake_exist_options {
    // Best-effort SSD->DRAM promotion for SSD-only keys. Requires the client
    // to be configured with enable_ssd_prefetch; otherwise ignored.
    int prefetch_to_memory;
};
typedef struct mooncake_exist_options mooncake_exist_options_t;

int mooncake_store_is_exist_with_options(
    mooncake_store_t store, const char *key,
    const mooncake_exist_options_t *options);

int mooncake_store_batch_is_exist_with_options(
    mooncake_store_t store, const char **keys, size_t count, int *results_out,
    const mooncake_exist_options_t *options);

int64_t mooncake_store_get_size(mooncake_store_t store, const char *key);

int mooncake_store_get_hostname(mooncake_store_t store, char *buf_out,
                                size_t buf_len);

// ---------------------------------------------------------------------------
// Remove operations
// ---------------------------------------------------------------------------

int mooncake_store_remove(mooncake_store_t store, const char *key, int force);

int64_t mooncake_store_remove_by_regex(mooncake_store_t store,
                                       const char *pattern, int force);

int64_t mooncake_store_remove_all(mooncake_store_t store, int force);

// ---------------------------------------------------------------------------
// Buffer registration (for zero-copy operations)
// ---------------------------------------------------------------------------

int mooncake_store_register_buffer(mooncake_store_t store, void *buffer,
                                   size_t size);

int mooncake_store_unregister_buffer(mooncake_store_t store, void *buffer);

// ---------------------------------------------------------------------------
// Replica migration tasks and DrainJobs
// ---------------------------------------------------------------------------

typedef struct mooncake_store_uuid {
    uint64_t high;
    uint64_t low;
} mooncake_store_uuid_t;

enum mooncake_store_task_type {
    MOONCAKE_STORE_TASK_REPLICA_COPY = 0,
    MOONCAKE_STORE_TASK_REPLICA_MOVE = 1
};

enum mooncake_store_task_status {
    MOONCAKE_STORE_TASK_PENDING = 0,
    MOONCAKE_STORE_TASK_PROCESSING = 1,
    MOONCAKE_STORE_TASK_FAILED = 2,
    MOONCAKE_STORE_TASK_SUCCESS = 3,
    MOONCAKE_STORE_TASK_CANCELED = 4
};

typedef struct mooncake_store_task_info {
    mooncake_store_uuid_t id;
    int32_t type;   /* enum mooncake_store_task_type */
    int32_t status; /* enum mooncake_store_task_status */
    int64_t created_at_ms_epoch;
    int64_t last_updated_at_ms_epoch;
    mooncake_store_uuid_t assigned_client;
    const char *message;
} mooncake_store_task_info_t;

enum mooncake_store_drain_job_status {
    MOONCAKE_STORE_DRAIN_CREATED = 0,
    MOONCAKE_STORE_DRAIN_PLANNING = 1,
    MOONCAKE_STORE_DRAIN_RUNNING = 2,
    MOONCAKE_STORE_DRAIN_SUCCEEDED = 3,
    MOONCAKE_STORE_DRAIN_FAILED = 4,
    MOONCAKE_STORE_DRAIN_CANCELED = 5
};

typedef struct mooncake_store_drain_job_request {
    const char *const *segments;
    size_t segment_count;
    const char *const *target_segments; /* optional when target_count == 0 */
    size_t target_count;
    uint32_t max_concurrency; /* positive, typically 4 */
} mooncake_store_drain_job_request_t;

typedef struct mooncake_store_drain_job_info {
    mooncake_store_uuid_t id;
    int32_t status; /* enum mooncake_store_drain_job_status */
    int64_t created_at_ms_epoch;
    int64_t last_updated_at_ms_epoch;
    const char *const *segments;
    size_t segment_count;
    uint64_t succeeded_units;
    uint64_t failed_units;
    uint64_t blocked_units;
    uint64_t active_units;
    uint64_t migrated_bytes;
    const char *message;
} mooncake_store_drain_job_info_t;

typedef struct mooncake_store_drain_job_filter {
    int32_t has_status;     /* zero to disable the status filter */
    int32_t status;         /* enum mooncake_store_drain_job_status */
    const char *segment;    /* optional exact source segment name */
    const char *page_token; /* NULL or empty on the first call */
    uint32_t limit;         /* 0 for default (100), otherwise 1..1000 */
} mooncake_store_drain_job_filter_t;

typedef struct mooncake_store_drain_job_page {
    const mooncake_store_drain_job_info_t *jobs;
    size_t count;
    const char *next_page_token; /* empty when there are no more results */
} mooncake_store_drain_job_page_t;

/*
 * These operations return 0 or a negative Store error code. Invalid input
 * returns -600, an unknown task ID returns -1400, and an unknown job ID
 * returns -1402.
 *
 * All input strings must be NUL-terminated. The library borrows inputs only
 * for the duration of the call. The caller supplies storage for IDs and output
 * pointers. A failed create leaves its output ID zeroed.
 *
 * Successful query and list calls return one malloc-allocated result. The
 * caller owns it and releases it with free(result), including after destroying
 * the store. All nested pointers are borrowed from that result and valid until
 * it is freed. Free only the result pointer. On failure the result is NULL.
 */
int mooncake_store_create_copy_task(mooncake_store_t store, const char *key,
                                    const char *const *targets,
                                    size_t target_count,
                                    mooncake_store_uuid_t *task_id_out);

int mooncake_store_create_move_task(mooncake_store_t store, const char *key,
                                    const char *source, const char *target,
                                    mooncake_store_uuid_t *task_id_out);

int mooncake_store_query_task(mooncake_store_t store,
                              mooncake_store_uuid_t task_id,
                              mooncake_store_task_info_t **result_out);

/* DrainJobs apply to mounted memory segments across the cluster. An empty
 * target list lets the master choose destinations. */
int mooncake_store_create_drain_job(
    mooncake_store_t store, const mooncake_store_drain_job_request_t *request,
    mooncake_store_uuid_t *job_id_out);

int mooncake_store_query_drain_job(
    mooncake_store_t store, mooncake_store_uuid_t job_id,
    mooncake_store_drain_job_info_t **result_out);

/* Cancel a nonterminal job with no active moves. Otherwise returns -1010. */
int mooncake_store_cancel_drain_job(mooncake_store_t store,
                                    mooncake_store_uuid_t job_id);

/* Pass NULL for default filters. Reuse next_page_token with the same filters
 * to read the next page. Each page reads live state in UUID order. */
int mooncake_store_list_drain_jobs(
    mooncake_store_t store, const mooncake_store_drain_job_filter_t *filter,
    mooncake_store_drain_job_page_t **result_out);

#ifdef __cplusplus
}
#endif

#endif  // MOONCAKE_STORE_C_H
