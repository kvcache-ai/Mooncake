#include "conductor/client/conductor_c.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <new>
#include <string>
#include <vector>

#include "conductor/client/conductor_client.h"

namespace mc = mooncake::conductor;

static_assert(MOONCAKE_CONDUCTOR_OK == static_cast<int>(mc::ErrorCode::OK));
static_assert(MOONCAKE_CONDUCTOR_INTERNAL_ERROR ==
              static_cast<int>(mc::ErrorCode::INTERNAL_ERROR));
static_assert(MOONCAKE_CONDUCTOR_INVALID_PARAMS ==
              static_cast<int>(mc::ErrorCode::INVALID_PARAMS));
static_assert(MOONCAKE_CONDUCTOR_RPC_FAIL ==
              static_cast<int>(mc::ErrorCode::RPC_FAIL));
static_assert(MOONCAKE_CONDUCTOR_RPC_TIMEOUT ==
              static_cast<int>(mc::ErrorCode::RPC_TIMEOUT));
static_assert(MOONCAKE_CONDUCTOR_UNAVAILABLE ==
              static_cast<int>(mc::ErrorCode::CONDUCTOR_UNAVAILABLE));
static_assert(MOONCAKE_CONDUCTOR_SERVICE_NOT_FOUND ==
              static_cast<int>(mc::ErrorCode::SERVICE_NOT_FOUND));

namespace {

// setup_called mirrors PyConductorClient: it is set by the first successful
// Setup and never cleared, so health_check after close reports UNREACHABLE
// rather than NOT_INITIALIZED.
struct ConductorHandle {
    mc::ConductorClient client;
    std::atomic<bool> setup_called{false};
};

ConductorHandle *as_handle(mooncake_conductor_t conductor) {
    return static_cast<ConductorHandle *>(conductor);
}

inline std::string to_string_or_empty(const char *s) {
    return s ? std::string(s) : std::string();
}

inline int to_rc(mc::ErrorCode code) { return static_cast<int>(code); }

// Output memory comes from malloc/calloc so the *_free functions can release
// it with free(). Allocation failures throw std::bad_alloc; each entry point
// catches it, frees the partially filled output (calloc'ed elements and counts
// set right after each allocation keep that safe), and returns INTERNAL_ERROR.
char *dup_string(const std::string &s) {
    auto *out = static_cast<char *>(std::malloc(s.size() + 1));
    if (!out) throw std::bad_alloc();
    std::memcpy(out, s.data(), s.size());
    out[s.size()] = '\0';
    return out;
}

template <typename T>
T *alloc_array(size_t count) {
    if (count == 0) return nullptr;
    auto *out = static_cast<T *>(std::calloc(count, sizeof(T)));
    if (!out) throw std::bad_alloc();
    return out;
}

bool from_c_publisher_kind(int kind, mc::common::PublisherKind *out) {
    switch (kind) {
        case MOONCAKE_CONDUCTOR_PUBLISHER_VLLM:
            *out = mc::common::PublisherKind::kVllm;
            return true;
        case MOONCAKE_CONDUCTOR_PUBLISHER_MOONCAKE:
            *out = mc::common::PublisherKind::kMooncake;
            return true;
        case MOONCAKE_CONDUCTOR_PUBLISHER_SGLANG:
            *out = mc::common::PublisherKind::kSglang;
            return true;
    }
    return false;
}

int to_c_publisher_kind(mc::common::PublisherKind kind) {
    switch (kind) {
        case mc::common::PublisherKind::kVllm:
            return MOONCAKE_CONDUCTOR_PUBLISHER_VLLM;
        case mc::common::PublisherKind::kMooncake:
            return MOONCAKE_CONDUCTOR_PUBLISHER_MOONCAKE;
        case mc::common::PublisherKind::kSglang:
            return MOONCAKE_CONDUCTOR_PUBLISHER_SGLANG;
    }
    return -1;
}

void fill_hash_profile(const mc::common::ResolvedHashProfile &src,
                       mooncake_conductor_hash_profile_t *dst) {
    dst->strategy = dup_string(src.strategy);
    dst->algorithm = dup_string(src.algorithm);
    dst->python_hash_seed = dup_string(src.python_hash_seed);
    dst->root_digest = dup_string(src.root_digest);
    dst->index_projection = dup_string(src.index_projection);
}

void free_hash_profile(mooncake_conductor_hash_profile_t *profile) {
    std::free(profile->strategy);
    std::free(profile->algorithm);
    std::free(profile->python_hash_seed);
    std::free(profile->root_digest);
    std::free(profile->index_projection);
}

void fill_query_result(const mc::QueryResult &src,
                       mooncake_conductor_query_result_t *dst) {
    dst->hits =
        alloc_array<mooncake_conductor_cache_hit_t>(src.instances.size());
    dst->hit_count = src.instances.size();
    size_t i = 0;
    for (const auto &[instance_id, hit] : src.instances) {
        auto &out = dst->hits[i++];
        out.instance_id = dup_string(instance_id);
        out.longest_matched = hit.longest_match_tokens;
        out.device = hit.device;
        out.cpu_local = hit.cpu_local;
        out.cpu_share = hit.cpu_share;
        out.disk = hit.disk;
        out.dp = alloc_array<mooncake_conductor_dp_hit_t>(hit.dp.size());
        out.dp_count = hit.dp.size();
        size_t j = 0;
        for (const auto &[rank, tokens] : hit.dp) {
            out.dp[j++] = {rank, tokens};
        }
        out.rank_matches = alloc_array<mooncake_conductor_rank_match_t>(
            hit.rank_matches.size());
        out.rank_match_count = hit.rank_matches.size();
        j = 0;
        for (const auto &[rank, match] : hit.rank_matches) {
            out.rank_matches[j++] = {rank, match.device, match.cpu_local,
                                     match.cpu_share, match.disk};
        }
    }
}

void fill_global_view(const mc::prefixindex::GlobalView &src,
                      mooncake_conductor_global_view_t *dst) {
    dst->contexts =
        alloc_array<mooncake_conductor_context_view_t>(src.contexts.size());
    dst->context_count = src.contexts.size();
    for (size_t i = 0; i < src.contexts.size(); ++i) {
        const auto &view = src.contexts[i];
        auto &out = dst->contexts[i];
        out.model_name = dup_string(view.context.model_name);
        out.lora_name = dup_string(view.context.lora_name);
        out.block_size = view.context.block_size;
        out.tenant_id = dup_string(view.context.tenant_id);
        out.prefix_count = static_cast<uint64_t>(view.prefix_count);
        fill_hash_profile(view.profile, &out.hash_profile);
        out.instances = alloc_array<mooncake_conductor_instance_ranks_t>(
            view.instance_ranks.size());
        out.instance_count = view.instance_ranks.size();
        size_t j = 0;
        for (const auto &[instance_id, ranks] : view.instance_ranks) {
            auto &instance = out.instances[j++];
            instance.instance_id = dup_string(instance_id);
            instance.ranks = alloc_array<int64_t>(ranks.size());
            instance.rank_count = ranks.size();
            std::copy(ranks.begin(), ranks.end(), instance.ranks);
        }
    }
}

void fill_service_list(const std::vector<mc::common::ServiceConfig> &src,
                       mooncake_conductor_service_list_t *dst) {
    dst->services = alloc_array<mooncake_conductor_service_info_t>(src.size());
    dst->service_count = src.size();
    for (size_t i = 0; i < src.size(); ++i) {
        const auto &svc = src[i];
        auto &out = dst->services[i];
        out.endpoint = dup_string(svc.endpoint);
        out.replay_endpoint = dup_string(svc.replay_endpoint);
        out.publisher_kind = to_c_publisher_kind(svc.publisher_kind);
        out.model_name = dup_string(svc.model_name);
        out.lora_name = dup_string(svc.lora_name);
        out.tenant_id = dup_string(svc.tenant_id);
        out.instance_id = dup_string(svc.instance_id);
        out.block_size = svc.block_size;
        out.dp_rank = svc.dp_rank;
        out.has_cache_group = svc.cache_group.has_value() ? 1 : 0;
        out.cache_group = svc.cache_group.value_or(0);
        fill_hash_profile(svc.hash_profile, &out.hash_profile);
    }
}

}  // namespace

extern "C" {

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

mooncake_conductor_t mooncake_conductor_create() {
    try {
        return static_cast<mooncake_conductor_t>(new (std::nothrow)
                                                     ConductorHandle());
    } catch (...) {
        return nullptr;
    }
}

void mooncake_conductor_destroy(mooncake_conductor_t conductor) {
    if (!conductor) return;
    auto *handle = as_handle(conductor);
    try {
        handle->client.Close();
    } catch (...) {
    }
    delete handle;
}

int mooncake_conductor_setup(mooncake_conductor_t conductor,
                             const char *conductor_addr,
                             int64_t connect_timeout_ms,
                             int64_t request_timeout_ms) {
    if (!conductor || !conductor_addr) {
        return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    }
    auto *handle = as_handle(conductor);
    try {
        auto result = handle->client.Setup(
            {conductor_addr, connect_timeout_ms, request_timeout_ms});
        if (!result) return to_rc(result.error());
        handle->setup_called.store(true);
        return MOONCAKE_CONDUCTOR_OK;
    } catch (...) {
        return MOONCAKE_CONDUCTOR_INTERNAL_ERROR;
    }
}

void mooncake_conductor_close(mooncake_conductor_t conductor) {
    if (!conductor) return;
    try {
        as_handle(conductor)->client.Close();
    } catch (...) {
    }
}

int mooncake_conductor_health_check(mooncake_conductor_t conductor) {
    if (!conductor) return MOONCAKE_CONDUCTOR_HEALTH_NOT_INITIALIZED;
    auto *handle = as_handle(conductor);
    if (!handle->setup_called.load()) {
        return MOONCAKE_CONDUCTOR_HEALTH_NOT_INITIALIZED;
    }
    try {
        return handle->client.HealthCheck()
                   ? MOONCAKE_CONDUCTOR_HEALTH_OK
                   : MOONCAKE_CONDUCTOR_HEALTH_UNREACHABLE;
    } catch (...) {
        return MOONCAKE_CONDUCTOR_HEALTH_UNREACHABLE;
    }
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

int mooncake_conductor_query(mooncake_conductor_t conductor,
                             const mooncake_conductor_query_request_t *request,
                             mooncake_conductor_query_result_t *result) {
    if (!result) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    *result = {};
    if (!conductor || !request ||
        (!request->token_ids && request->token_count > 0)) {
        return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    }
    try {
        mc::QueryRequest req;
        req.context.tenant_id = to_string_or_empty(request->tenant_id);
        req.context.model_name = to_string_or_empty(request->model_name);
        req.context.lora_name = to_string_or_empty(request->lora_name);
        req.context.block_size = request->block_size;
        if (request->token_count > 0) {
            req.token_ids.assign(request->token_ids,
                                 request->token_ids + request->token_count);
        }
        // Empty strings stay unset, matching the Python binding.
        if (request->cache_salt && request->cache_salt[0] != '\0') {
            req.cache_salt = request->cache_salt;
        }
        if (request->instance_filter && request->instance_filter[0] != '\0') {
            req.instance_filter = request->instance_filter;
        }
        auto rpc = as_handle(conductor)->client.Query(req);
        if (!rpc) return to_rc(rpc.error());
        fill_query_result(*rpc, result);
        return MOONCAKE_CONDUCTOR_OK;
    } catch (...) {
        mooncake_conductor_query_result_free(result);
        return MOONCAKE_CONDUCTOR_INTERNAL_ERROR;
    }
}

void mooncake_conductor_query_result_free(
    mooncake_conductor_query_result_t *result) {
    if (!result) return;
    for (size_t i = 0; i < result->hit_count; ++i) {
        std::free(result->hits[i].instance_id);
        std::free(result->hits[i].dp);
        std::free(result->hits[i].rank_matches);
    }
    std::free(result->hits);
    *result = {};
}

int mooncake_conductor_register(
    mooncake_conductor_t conductor,
    const mooncake_conductor_service_config_t *config, int *is_new) {
    if (is_new) *is_new = 0;
    if (!conductor || !config) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    try {
        mc::common::ServiceConfig svc;
        if (!from_c_publisher_kind(config->publisher_kind,
                                   &svc.publisher_kind)) {
            return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
        }
        svc.instance_id = to_string_or_empty(config->instance_id);
        svc.endpoint = to_string_or_empty(config->endpoint);
        svc.replay_endpoint = to_string_or_empty(config->replay_endpoint);
        svc.model_name = to_string_or_empty(config->model_name);
        svc.lora_name = to_string_or_empty(config->lora_name);
        svc.tenant_id = to_string_or_empty(config->tenant_id);
        svc.block_size = config->block_size;
        svc.dp_rank = config->dp_rank;
        if (config->has_cache_group) svc.cache_group = config->cache_group;
        // root_digest is derived server-side from the recipe.
        const auto &hp = config->hash_profile;
        svc.hash_profile.strategy = to_string_or_empty(hp.strategy);
        svc.hash_profile.algorithm = to_string_or_empty(hp.algorithm);
        svc.hash_profile.python_hash_seed =
            to_string_or_empty(hp.python_hash_seed);
        svc.hash_profile.index_projection =
            to_string_or_empty(hp.index_projection);
        auto rpc = as_handle(conductor)->client.Register(svc);
        if (!rpc) return to_rc(rpc.error());
        if (is_new) *is_new = rpc->is_new ? 1 : 0;
        return MOONCAKE_CONDUCTOR_OK;
    } catch (...) {
        return MOONCAKE_CONDUCTOR_INTERNAL_ERROR;
    }
}

int mooncake_conductor_unregister(mooncake_conductor_t conductor,
                                  const char *instance_id,
                                  const char *tenant_id, int dp_rank) {
    if (!conductor) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    try {
        auto rpc = as_handle(conductor)->client.Unregister(
            to_string_or_empty(instance_id), to_string_or_empty(tenant_id),
            dp_rank);
        return rpc ? MOONCAKE_CONDUCTOR_OK : to_rc(rpc.error());
    } catch (...) {
        return MOONCAKE_CONDUCTOR_INTERNAL_ERROR;
    }
}

int mooncake_conductor_get_global_view(mooncake_conductor_t conductor,
                                       mooncake_conductor_global_view_t *view) {
    if (!view) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    *view = {};
    if (!conductor) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    try {
        auto rpc = as_handle(conductor)->client.GetGlobalView();
        if (!rpc) return to_rc(rpc.error());
        fill_global_view(*rpc, view);
        return MOONCAKE_CONDUCTOR_OK;
    } catch (...) {
        mooncake_conductor_global_view_free(view);
        return MOONCAKE_CONDUCTOR_INTERNAL_ERROR;
    }
}

void mooncake_conductor_global_view_free(
    mooncake_conductor_global_view_t *view) {
    if (!view) return;
    for (size_t i = 0; i < view->context_count; ++i) {
        auto &context = view->contexts[i];
        std::free(context.model_name);
        std::free(context.lora_name);
        std::free(context.tenant_id);
        free_hash_profile(&context.hash_profile);
        for (size_t j = 0; j < context.instance_count; ++j) {
            std::free(context.instances[j].instance_id);
            std::free(context.instances[j].ranks);
        }
        std::free(context.instances);
    }
    std::free(view->contexts);
    *view = {};
}

int mooncake_conductor_list_services(mooncake_conductor_t conductor,
                                     mooncake_conductor_service_list_t *list) {
    if (!list) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    *list = {};
    if (!conductor) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    try {
        auto rpc = as_handle(conductor)->client.ListServices();
        if (!rpc) return to_rc(rpc.error());
        fill_service_list(*rpc, list);
        return MOONCAKE_CONDUCTOR_OK;
    } catch (...) {
        mooncake_conductor_service_list_free(list);
        return MOONCAKE_CONDUCTOR_INTERNAL_ERROR;
    }
}

void mooncake_conductor_service_list_free(
    mooncake_conductor_service_list_t *list) {
    if (!list) return;
    for (size_t i = 0; i < list->service_count; ++i) {
        auto &svc = list->services[i];
        std::free(svc.endpoint);
        std::free(svc.replay_endpoint);
        std::free(svc.model_name);
        std::free(svc.lora_name);
        std::free(svc.tenant_id);
        std::free(svc.instance_id);
        free_hash_profile(&svc.hash_profile);
    }
    std::free(list->services);
    *list = {};
}

}  // extern "C"
