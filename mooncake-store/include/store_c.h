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

struct mooncake_replicate_config {
    size_t replica_num;
    int with_soft_pin;
    int with_hard_pin;
    const char **preferred_segments;
    size_t preferred_segments_count;
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
// Existence / size / hostname
// ---------------------------------------------------------------------------

int mooncake_store_is_exist(mooncake_store_t store, const char *key);

int mooncake_store_batch_is_exist(mooncake_store_t store, const char **keys,
                                  size_t count, int *results_out);

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
// Cache statistics (DRAM vs SSD split)
// ---------------------------------------------------------------------------

/**
 * Calculate cache hit statistics split by DRAM and SSD.
 * Returns a JSON string with keys: memory_hits, ssd_hits, memory_total,
 * ssd_total, memory_hit_rate, ssd_hit_rate, overall_hit_rate, valid_get_rate.
 *
 * NOTE: ssd_hits and ssd_hit_rate are approximate. The master-side SSD
 * counters track metadata lookups (first replica type in GetReplicaList),
 * not actual SSD reads. For accurate per-tier Get observability, use the
 * client-side Prometheus metrics: get_from_memory_count,
 * get_from_disk_count, get_from_memory_bytes, get_from_disk_bytes.
 *
 * @param store The store handle.
 * @param buf_out Output buffer to write the JSON string.
 * @param buf_len Size of the output buffer.
 * @return Length of the JSON string on success (may exceed buf_len if
 *         truncated), or -1 on error.
 */
int mooncake_store_calc_cache_stats(mooncake_store_t store, char *buf_out,
                                    size_t buf_len);

/**
 * Get client-side transfer statistics by storage tier.
 * Returns a JSON string with per-tier Get and Put counts and bytes.
 * These are local counters (no RPC to master).
 *
 * @param store The store handle.
 * @param buf_out Output buffer to write the JSON string.
 * @param buf_len Size of the output buffer.
 * @return Length of the JSON string on success, or -1 on error.
 */
int mooncake_store_get_client_stats(mooncake_store_t store, char *buf_out,
                                    size_t buf_len);

#ifdef __cplusplus
}
#endif

#endif  // MOONCAKE_STORE_C_H
