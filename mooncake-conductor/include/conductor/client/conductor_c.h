// C ABI over ConductorClient, consumed by the Rust bindings in
// mooncake-conductor/rust. Mirrors the Python _conductor module: the same
// operations, defaults, and numeric return codes.

#ifndef MOONCAKE_CONDUCTOR_C_H
#define MOONCAKE_CONDUCTOR_C_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *mooncake_conductor_t;

/*
 * Return codes of the int-returning functions. Values equal
 * mooncake::conductor::ErrorCode (conductor/client/types.h).
 */
enum mooncake_conductor_error {
    MOONCAKE_CONDUCTOR_OK = 0,
    MOONCAKE_CONDUCTOR_INTERNAL_ERROR = -1,
    MOONCAKE_CONDUCTOR_INVALID_PARAMS = -600,
    MOONCAKE_CONDUCTOR_RPC_FAIL = -900,
    MOONCAKE_CONDUCTOR_RPC_TIMEOUT = -901,
    MOONCAKE_CONDUCTOR_UNAVAILABLE = -2000,
    MOONCAKE_CONDUCTOR_SERVICE_NOT_FOUND = -2001
};

/* Return values of mooncake_conductor_health_check (same codes as store). */
enum mooncake_conductor_health {
    MOONCAKE_CONDUCTOR_HEALTH_OK = 0,
    MOONCAKE_CONDUCTOR_HEALTH_NOT_INITIALIZED = 1,
    MOONCAKE_CONDUCTOR_HEALTH_UNREACHABLE = 2
};

/* KV-event publisher kinds; names match common::PublisherKindName. */
enum mooncake_conductor_publisher_kind {
    MOONCAKE_CONDUCTOR_PUBLISHER_VLLM = 0,     /* "vLLM" (default) */
    MOONCAKE_CONDUCTOR_PUBLISHER_MOONCAKE = 1, /* "Mooncake" */
    MOONCAKE_CONDUCTOR_PUBLISHER_SGLANG = 2    /* "SGLang" */
};

/*
 * Memory conventions:
 * - Input pointers ("const char *", token_ids, input structs) are not used
 *   after the call returns, so the caller may free them immediately.
 * - A NULL "const char *" field in an input struct means "", and an empty
 *   string keeps the server-side default (for example tenant_id falls back
 *   to "default", an empty cache_salt is unset).
 * - Output structs are caller-owned storage that the call fills with
 *   C-allocated memory. On success release it with the matching *_free;
 *   on failure the struct is left zeroed, so calling *_free is optional
 *   and harmless.
 *
 * Calls on one handle are serialized by an internal mutex, so a handle may
 * be shared across threads; mooncake_conductor_destroy must not race with
 * any other call on the same handle.
 */

// ---------------------------------------------------------------------------
// Input structs
// ---------------------------------------------------------------------------

/* Prefix-cache query; fields match the Python client's query(). */
struct mooncake_conductor_query_request {
    const char *model_name; /* required */
    const char *lora_name;
    int64_t block_size; /* required, > 0 */
    const char *tenant_id;
    const int32_t *token_ids; /* may be NULL only when token_count is 0 */
    size_t token_count;
    const char *cache_salt;      /* NULL or "" = unset */
    const char *instance_filter; /* NULL or "" = unset */
};
typedef struct mooncake_conductor_query_request
    mooncake_conductor_query_request_t;

/* Hash recipe; the server derives root_digest from it at registration. */
struct mooncake_conductor_hash_profile_config {
    const char *strategy;
    const char *algorithm;
    const char *python_hash_seed;
    const char *index_projection;
};
typedef struct mooncake_conductor_hash_profile_config
    mooncake_conductor_hash_profile_config_t;

/*
 * Service registration; fields match the Python client's register() dict.
 * Required (validated server-side): instance_id, endpoint, model_name,
 * block_size, hash_profile.
 */
struct mooncake_conductor_service_config {
    const char *instance_id;
    const char *endpoint;
    const char *replay_endpoint;
    /* One of enum mooncake_conductor_publisher_kind;
     * mooncake_conductor_register returns MOONCAKE_CONDUCTOR_INVALID_PARAMS for
     * any other value. */
    int publisher_kind;
    const char *model_name;
    const char *lora_name;
    const char *tenant_id;
    int64_t block_size;
    int dp_rank;
    /* cache_group is sent only when has_cache_group is nonzero. */
    int has_cache_group;
    int64_t cache_group;
    mooncake_conductor_hash_profile_config_t hash_profile;
};
typedef struct mooncake_conductor_service_config
    mooncake_conductor_service_config_t;

// ---------------------------------------------------------------------------
// Output structs (C-allocated, released by the matching *_free)
// ---------------------------------------------------------------------------

struct mooncake_conductor_dp_hit {
    int64_t rank;
    int64_t tokens;
};
typedef struct mooncake_conductor_dp_hit mooncake_conductor_dp_hit_t;

struct mooncake_conductor_rank_match {
    int64_t rank;
    int64_t device;
    int64_t cpu_local;
    int64_t cpu_share;
    int64_t disk;
};
typedef struct mooncake_conductor_rank_match mooncake_conductor_rank_match_t;

/* Per-instance hit; arrays are sorted by rank. */
struct mooncake_conductor_cache_hit {
    char *instance_id;
    int64_t longest_matched;
    mooncake_conductor_dp_hit_t *dp;
    size_t dp_count;
    mooncake_conductor_rank_match_t *rank_matches;
    size_t rank_match_count;
    int64_t device;
    int64_t cpu_local;
    int64_t cpu_share;
    int64_t disk;
};
typedef struct mooncake_conductor_cache_hit mooncake_conductor_cache_hit_t;

/* Query result; hits are sorted by instance_id. */
struct mooncake_conductor_query_result {
    mooncake_conductor_cache_hit_t *hits;
    size_t hit_count;
};
typedef struct mooncake_conductor_query_result
    mooncake_conductor_query_result_t;

/* Resolved hash profile, including the server-derived root_digest. */
struct mooncake_conductor_hash_profile {
    char *strategy;
    char *algorithm;
    char *python_hash_seed;
    char *root_digest;
    char *index_projection;
};
typedef struct mooncake_conductor_hash_profile
    mooncake_conductor_hash_profile_t;

/* DP ranks of one instance within a context, sorted ascending. */
struct mooncake_conductor_instance_ranks {
    char *instance_id;
    int64_t *ranks;
    size_t rank_count;
};
typedef struct mooncake_conductor_instance_ranks
    mooncake_conductor_instance_ranks_t;

/* One indexed context; instances are sorted by instance_id. */
struct mooncake_conductor_context_view {
    char *model_name;
    char *lora_name;
    int64_t block_size;
    char *tenant_id;
    uint64_t prefix_count;
    mooncake_conductor_hash_profile_t hash_profile;
    mooncake_conductor_instance_ranks_t *instances;
    size_t instance_count;
};
typedef struct mooncake_conductor_context_view
    mooncake_conductor_context_view_t;

struct mooncake_conductor_global_view {
    mooncake_conductor_context_view_t *contexts;
    size_t context_count;
};
typedef struct mooncake_conductor_global_view mooncake_conductor_global_view_t;

/* A registered service as reported by the conductor. */
struct mooncake_conductor_service_info {
    char *endpoint;
    char *replay_endpoint;
    int publisher_kind; /* enum mooncake_conductor_publisher_kind */
    char *model_name;
    char *lora_name;
    char *tenant_id;
    char *instance_id;
    int64_t block_size;
    int dp_rank;
    int has_cache_group;
    int64_t cache_group;
    mooncake_conductor_hash_profile_t hash_profile;
};
typedef struct mooncake_conductor_service_info
    mooncake_conductor_service_info_t;

struct mooncake_conductor_service_list {
    mooncake_conductor_service_info_t *services;
    size_t service_count;
};
typedef struct mooncake_conductor_service_list
    mooncake_conductor_service_list_t;

/*
 * Unless noted otherwise, the int-returning functions below return one of
 * enum mooncake_conductor_error, and a NULL handle or NULL struct pointer
 * yields MOONCAKE_CONDUCTOR_INVALID_PARAMS.
 */

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/* Returns NULL on allocation failure. */
mooncake_conductor_t mooncake_conductor_create(void);

/* Closes the connection and releases the handle; NULL is a no-op. */
void mooncake_conductor_destroy(mooncake_conductor_t conductor);

/* Connects to "host:port". Setup may be called again after close. */
int mooncake_conductor_setup(mooncake_conductor_t conductor,
                             const char *conductor_addr,
                             int64_t connect_timeout_ms,
                             int64_t request_timeout_ms);

/* Idempotent; NULL is a no-op. */
void mooncake_conductor_close(mooncake_conductor_t conductor);

/*
 * Returns enum mooncake_conductor_health: NOT_INITIALIZED until a setup has
 * succeeded (also for a NULL handle), then OK if a ListServices round trip
 * succeeds and UNREACHABLE otherwise.
 */
int mooncake_conductor_health_check(mooncake_conductor_t conductor);

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

int mooncake_conductor_query(mooncake_conductor_t conductor,
                             const mooncake_conductor_query_request_t *request,
                             mooncake_conductor_query_result_t *result);

void mooncake_conductor_query_result_free(
    mooncake_conductor_query_result_t *result);

/*
 * When is_new is non-NULL it receives 1 for a new registration and 0 when
 * the same service was already registered.
 */
int mooncake_conductor_register(
    mooncake_conductor_t conductor,
    const mooncake_conductor_service_config_t *config, int *is_new);

/* NULL or "" tenant_id means "default". */
int mooncake_conductor_unregister(mooncake_conductor_t conductor,
                                  const char *instance_id,
                                  const char *tenant_id, int dp_rank);

int mooncake_conductor_get_global_view(mooncake_conductor_t conductor,
                                       mooncake_conductor_global_view_t *view);

void mooncake_conductor_global_view_free(
    mooncake_conductor_global_view_t *view);

int mooncake_conductor_list_services(mooncake_conductor_t conductor,
                                     mooncake_conductor_service_list_t *list);

void mooncake_conductor_service_list_free(
    mooncake_conductor_service_list_t *list);

#ifdef __cplusplus
}
#endif

#endif  // MOONCAKE_CONDUCTOR_C_H
