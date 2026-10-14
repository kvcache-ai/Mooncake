// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#ifndef MOONCAKE_CONDUCTOR_C_H
#define MOONCAKE_CONDUCTOR_C_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOONCAKE_CONDUCTOR_ABI_VERSION 1

enum mooncake_conductor_error {
    MOONCAKE_CONDUCTOR_OK = 0,
    MOONCAKE_CONDUCTOR_INTERNAL_ERROR = -1,
    MOONCAKE_CONDUCTOR_INVALID_PARAMS = -600,
    MOONCAKE_CONDUCTOR_RPC_FAIL = -900,
    MOONCAKE_CONDUCTOR_RPC_TIMEOUT = -901,
    MOONCAKE_CONDUCTOR_UNAVAILABLE = -2000,
    MOONCAKE_CONDUCTOR_SERVICE_NOT_FOUND = -2001
};

typedef struct mooncake_conductor_client* mooncake_conductor_client_t;
typedef struct mooncake_conductor_query_result mooncake_conductor_query_result;
typedef struct mooncake_conductor_services_result
    mooncake_conductor_services_result;
typedef struct mooncake_conductor_global_result
    mooncake_conductor_global_result;
typedef struct mooncake_conductor_registration_result
    mooncake_conductor_registration_result;
typedef struct mooncake_conductor_unregistration_result
    mooncake_conductor_unregistration_result;

// Strings are length-delimited (no required NUL terminator). NULL is valid
// only when size is zero. Inputs are borrowed for the synchronous call only.
typedef struct {
    const char* data;
    size_t size;
} mooncake_conductor_string;

typedef struct {
    mooncake_conductor_string strategy, algorithm, python_hash_seed,
        index_projection;
} mooncake_conductor_hash_config;

typedef struct {
    mooncake_conductor_hash_config config;
    mooncake_conductor_string root_digest;
} mooncake_conductor_hash_profile;

typedef struct {
    mooncake_conductor_string address;
    // Positive millisecond durations, at most INT64_MAX / 1000000.
    int64_t connect_timeout_ms, request_timeout_ms;
} mooncake_conductor_client_config;

typedef struct {
    mooncake_conductor_string instance_id, endpoint, replay_endpoint,
        publisher_type;
    mooncake_conductor_string model_name, lora_name, tenant_id;
    int64_t block_size;
    int32_t dp_rank;
    int32_t has_cache_group;
    int64_t cache_group;
    mooncake_conductor_hash_config hash_profile;
} mooncake_conductor_service_config;

typedef struct {
    mooncake_conductor_service_config config;
    mooncake_conductor_string root_digest;
} mooncake_conductor_registered_service;

typedef struct {
    mooncake_conductor_string model_name, lora_name, tenant_id;
    int64_t block_size;
    mooncake_conductor_string cache_salt, instance_filter;
} mooncake_conductor_query_request;

typedef struct {
    int64_t device, cpu_local, cpu_share, disk;
} mooncake_conductor_tier_match;

typedef struct {
    int64_t rank, tokens;
} mooncake_conductor_dp_match;

typedef struct {
    int64_t rank;
    mooncake_conductor_tier_match match;
} mooncake_conductor_rank_match;

typedef struct {
    mooncake_conductor_string instance_id;
    int64_t longest_matched;
    mooncake_conductor_tier_match match;
    const mooncake_conductor_dp_match* dp;
    size_t dp_count;
    const mooncake_conductor_rank_match* rank_matches;
    size_t rank_count;
} mooncake_conductor_cache_hit;

typedef struct {
    mooncake_conductor_string instance_id;
    const int64_t* ranks;
    size_t rank_count;
} mooncake_conductor_instance_ranks;

typedef struct {
    mooncake_conductor_string model_name, lora_name, tenant_id;
    int64_t block_size;
    uint64_t prefix_count;
    mooncake_conductor_hash_profile hash_profile;
    const mooncake_conductor_instance_ranks* instances;
    size_t instance_count;
} mooncake_conductor_context_view;

// All fallible functions return a numeric error code. Out handles are NULL on
// failure. Response view pointers belong to their result, remain valid until
// free, and survive client close. Free(NULL) is safe. Do not double-free.
// The caller must serialize all operations on the same client, including
// health/setup/close/destruction. Independent clients can run concurrently.
uint32_t mooncake_conductor_abi_version(void);
int32_t mooncake_conductor_create(mooncake_conductor_client_t* out);
int32_t mooncake_conductor_destroy(mooncake_conductor_client_t client);
int32_t mooncake_conductor_close(mooncake_conductor_client_t client);
int32_t mooncake_conductor_setup(
    mooncake_conductor_client_t client,
    const mooncake_conductor_client_config* config);
// status: 0 healthy, 1 uninitialized, 2 unreachable. On unreachable the return
// code preserves the failed probe's error. This does not measure event health.
int32_t mooncake_conductor_health_check(mooncake_conductor_client_t client,
                                        int32_t* status);
int32_t mooncake_conductor_register(
    mooncake_conductor_client_t client,
    const mooncake_conductor_service_config* config,
    mooncake_conductor_registration_result** out);
int32_t mooncake_conductor_unregister(
    mooncake_conductor_client_t client, mooncake_conductor_string instance_id,
    mooncake_conductor_string tenant_id, int32_t dp_rank,
    mooncake_conductor_unregistration_result** out);
int32_t mooncake_conductor_query(
    mooncake_conductor_client_t client,
    const mooncake_conductor_query_request* request, const int32_t* token_ids,
    size_t token_count, mooncake_conductor_query_result** out);
int32_t mooncake_conductor_list_services(
    mooncake_conductor_client_t client,
    mooncake_conductor_services_result** out);
int32_t mooncake_conductor_get_global_view(
    mooncake_conductor_client_t client, mooncake_conductor_global_result** out);

int32_t mooncake_conductor_query_view(
    const mooncake_conductor_query_result* result,
    const mooncake_conductor_cache_hit** hits, size_t* count);
int32_t mooncake_conductor_services_view(
    const mooncake_conductor_services_result* result,
    const mooncake_conductor_registered_service** services, size_t* count);
int32_t mooncake_conductor_global_view(
    const mooncake_conductor_global_result* result,
    const mooncake_conductor_context_view** contexts, size_t* count);
int32_t mooncake_conductor_registration_view(
    const mooncake_conductor_registration_result* result,
    mooncake_conductor_string* instance_id, int32_t* is_new);
int32_t mooncake_conductor_unregistration_view(
    const mooncake_conductor_unregistration_result* result,
    mooncake_conductor_string* removed_key);

void mooncake_conductor_query_free(mooncake_conductor_query_result* result);
void mooncake_conductor_services_free(
    mooncake_conductor_services_result* result);
void mooncake_conductor_global_free(mooncake_conductor_global_result* result);
void mooncake_conductor_registration_free(
    mooncake_conductor_registration_result* result);
void mooncake_conductor_unregistration_free(
    mooncake_conductor_unregistration_result* result);

#ifdef __cplusplus
}
#endif
#endif
