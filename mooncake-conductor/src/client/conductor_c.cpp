// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "conductor/client/conductor_c.h"

#include <memory>
#include <limits>
#include <stdexcept>
#include <utility>

#include "conductor/client/conductor_client.h"

namespace mc = mooncake::conductor;

struct mooncake_conductor_client {
    mc::ConductorClient client;
    bool initialized = false;
};

namespace {
constexpr int32_t Code(mc::ErrorCode code) {
    return static_cast<int32_t>(code);
}
static_assert(Code(mc::ErrorCode::INVALID_PARAMS) ==
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
static_assert(Code(mc::ErrorCode::INTERNAL_ERROR) ==
              MOONCAKE_CONDUCTOR_INTERNAL_ERROR);
static_assert(Code(mc::ErrorCode::RPC_FAIL) == MOONCAKE_CONDUCTOR_RPC_FAIL);
static_assert(Code(mc::ErrorCode::RPC_TIMEOUT) ==
              MOONCAKE_CONDUCTOR_RPC_TIMEOUT);
static_assert(Code(mc::ErrorCode::CONDUCTOR_UNAVAILABLE) ==
              MOONCAKE_CONDUCTOR_UNAVAILABLE);
static_assert(Code(mc::ErrorCode::SERVICE_NOT_FOUND) ==
              MOONCAKE_CONDUCTOR_SERVICE_NOT_FOUND);

template <typename Fn>
int32_t Guard(Fn&& fn) noexcept {
    try {
        return fn();
    } catch (const std::invalid_argument&) {
        return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    } catch (...) {
        return MOONCAKE_CONDUCTOR_INTERNAL_ERROR;
    }
}

std::string Copy(mooncake_conductor_string s) {
    if ((!s.data && s.size) || s.size > std::string().max_size())
        throw std::invalid_argument("invalid string view");
    return s.size ? std::string(s.data, s.size) : std::string();
}
mooncake_conductor_string View(const std::string& s) {
    return {s.data(), s.size()};
}
mooncake_conductor_string View(std::string_view s) {
    return {s.data(), s.size()};
}

mooncake_conductor_hash_config ProfileView(
    const mc::common::ResolvedHashProfile& p) {
    return {View(p.strategy), View(p.algorithm), View(p.python_hash_seed),
            View(p.index_projection)};
}
mooncake_conductor_service_config ServiceView(
    const mc::common::ServiceConfig& s) {
    return {View(s.instance_id),
            View(s.endpoint),
            View(s.replay_endpoint),
            View(mc::common::PublisherKindName(s.publisher_kind)),
            View(s.model_name),
            View(s.lora_name),
            View(s.tenant_id),
            s.block_size,
            s.dp_rank,
            s.cache_group.has_value(),
            s.cache_group.value_or(0),
            ProfileView(s.hash_profile)};
}
mc::common::ServiceConfig Service(const mooncake_conductor_service_config& c) {
    mc::common::ServiceConfig s;
    s.instance_id = Copy(c.instance_id);
    s.endpoint = Copy(c.endpoint);
    s.replay_endpoint = Copy(c.replay_endpoint);
    const auto kind = Copy(c.publisher_type);
    if (!kind.empty()) {
        auto parsed = mc::common::ParsePublisherKind(kind);
        if (!parsed) throw std::invalid_argument("invalid publisher type");
        s.publisher_kind = *parsed;
    }
    s.model_name = Copy(c.model_name);
    s.lora_name = Copy(c.lora_name);
    s.tenant_id = Copy(c.tenant_id);
    s.block_size = c.block_size;
    s.dp_rank = c.dp_rank;
    if (c.has_cache_group != 0 && c.has_cache_group != 1)
        throw std::invalid_argument("invalid cache group presence");
    if (c.has_cache_group) s.cache_group = c.cache_group;
    s.hash_profile = {Copy(c.hash_profile.strategy),
                      Copy(c.hash_profile.algorithm),
                      Copy(c.hash_profile.python_hash_seed), "",
                      Copy(c.hash_profile.index_projection)};
    return s;
}
template <typename Match>
mooncake_conductor_tier_match Tiers(const Match& m) {
    return {m.device, m.cpu_local, m.cpu_share, m.disk};
}
}  // namespace

struct mooncake_conductor_query_result {
    mc::QueryResult source;
    std::vector<mooncake_conductor_cache_hit> hits;
    std::vector<std::vector<mooncake_conductor_dp_match>> dp;
    std::vector<std::vector<mooncake_conductor_rank_match>> ranks;

    explicit mooncake_conductor_query_result(mc::QueryResult value)
        : source(std::move(value)) {
        dp.resize(source.instances.size());
        ranks.resize(source.instances.size());
        hits.reserve(source.instances.size());
        size_t i = 0;
        for (const auto& [id, hit] : source.instances) {
            for (const auto& [rank, tokens] : hit.dp)
                dp[i].push_back({rank, tokens});
            for (const auto& [rank, match] : hit.rank_matches)
                ranks[i].push_back({rank, Tiers(match)});
            hits.push_back({View(id), hit.longest_match_tokens, Tiers(hit),
                            dp[i].data(), dp[i].size(), ranks[i].data(),
                            ranks[i].size()});
            ++i;
        }
    }
};
struct mooncake_conductor_services_result {
    std::vector<mc::common::ServiceConfig> source;
    std::vector<mooncake_conductor_registered_service> services;
    explicit mooncake_conductor_services_result(
        std::vector<mc::common::ServiceConfig> value)
        : source(std::move(value)) {
        services.reserve(source.size());
        for (const auto& s : source)
            services.push_back(
                {ServiceView(s), View(s.hash_profile.root_digest)});
    }
};
struct mooncake_conductor_global_result {
    mc::prefixindex::GlobalView source;
    std::vector<mooncake_conductor_context_view> contexts;
    std::vector<std::vector<mooncake_conductor_instance_ranks>> instances;
    std::vector<std::vector<std::vector<int64_t>>> ranks;
    explicit mooncake_conductor_global_result(mc::prefixindex::GlobalView value)
        : source(std::move(value)) {
        instances.resize(source.contexts.size());
        ranks.resize(source.contexts.size());
        contexts.reserve(source.contexts.size());
        for (size_t i = 0; i < source.contexts.size(); ++i) {
            const auto& v = source.contexts[i];
            ranks[i].resize(v.instance_ranks.size());
            size_t j = 0;
            for (const auto& [id, rr] : v.instance_ranks) {
                ranks[i][j].assign(rr.begin(), rr.end());
                instances[i].push_back(
                    {View(id), ranks[i][j].data(), ranks[i][j].size()});
                ++j;
            }
            contexts.push_back(
                {View(v.context.model_name),
                 View(v.context.lora_name),
                 View(v.context.tenant_id),
                 v.context.block_size,
                 v.prefix_count,
                 {ProfileView(v.profile), View(v.profile.root_digest)},
                 instances[i].data(),
                 instances[i].size()});
        }
    }
};
struct mooncake_conductor_registration_result {
    mc::RegisterResult source;
    explicit mooncake_conductor_registration_result(mc::RegisterResult value)
        : source(std::move(value)) {}
};
struct mooncake_conductor_unregistration_result {
    mc::UnregisterResult source;
    explicit mooncake_conductor_unregistration_result(
        mc::UnregisterResult value)
        : source(std::move(value)) {}
};

namespace {
template <typename Result, typename Fn>
int32_t Call(mooncake_conductor_client_t client, Result** out, Fn&& fn) {
    if (!out) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    *out = nullptr;
    if (!client) return MOONCAKE_CONDUCTOR_UNAVAILABLE;
    return Guard([&]() -> int32_t {
        auto value = fn(client->client);
        if (!value) return Code(value.error());
        auto result = std::make_unique<Result>(std::move(*value));
        *out = result.release();
        return MOONCAKE_CONDUCTOR_OK;
    });
}
template <typename Result, typename Item>
int32_t GetView(const Result* result, const std::vector<Item> Result::* member,
                const Item** data, size_t* count) {
    if (data) *data = nullptr;
    if (count) *count = 0;
    if (!result || !data || !count) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    const auto& items = result->*member;
    *data = items.data();
    *count = items.size();
    return MOONCAKE_CONDUCTOR_OK;
}
}  // namespace

extern "C" {
uint32_t mooncake_conductor_abi_version(void) {
    return MOONCAKE_CONDUCTOR_ABI_VERSION;
}
int32_t mooncake_conductor_create(mooncake_conductor_client_t* out) {
    if (!out) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    *out = nullptr;
    return Guard([&]() -> int32_t {
        *out = new mooncake_conductor_client;
        return MOONCAKE_CONDUCTOR_OK;
    });
}
int32_t mooncake_conductor_destroy(mooncake_conductor_client_t client) {
    return Guard([&]() -> int32_t {
        // Close can allocate through the underlying asynchronous runtime.
        // Keep ownership in RAII even if it fails.
        std::unique_ptr<mooncake_conductor_client> owner(client);
        if (client) {
            client->initialized = false;
            client->client.Close();
        }
        return MOONCAKE_CONDUCTOR_OK;
    });
}
int32_t mooncake_conductor_close(mooncake_conductor_client_t client) {
    if (!client) return MOONCAKE_CONDUCTOR_OK;
    return Guard([&]() -> int32_t {
        client->initialized = false;
        client->client.Close();
        return MOONCAKE_CONDUCTOR_OK;
    });
}
int32_t mooncake_conductor_setup(
    mooncake_conductor_client_t client,
    const mooncake_conductor_client_config* config) {
    if (!client) return MOONCAKE_CONDUCTOR_UNAVAILABLE;
    return Guard([&]() -> int32_t {
        client->initialized = false;
        client->client.Close();
        constexpr int64_t max_timeout_ms =
            std::numeric_limits<int64_t>::max() / 1000000;
        if (!config || config->connect_timeout_ms <= 0 ||
            config->request_timeout_ms <= 0 ||
            config->connect_timeout_ms > max_timeout_ms ||
            config->request_timeout_ms > max_timeout_ms)
            return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
        auto result = client->client.Setup({Copy(config->address),
                                            config->connect_timeout_ms,
                                            config->request_timeout_ms});
        client->initialized = result.has_value();
        return result ? MOONCAKE_CONDUCTOR_OK : Code(result.error());
    });
}
int32_t mooncake_conductor_health_check(mooncake_conductor_client_t client,
                                        int32_t* status) {
    if (!status) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    *status = 1;
    if (!client || !client->initialized) return MOONCAKE_CONDUCTOR_OK;
    *status = 2;
    return Guard([&]() -> int32_t {
        auto result = client->client.ListServices();
        if (!result) return Code(result.error());
        *status = 0;
        return MOONCAKE_CONDUCTOR_OK;
    });
}
int32_t mooncake_conductor_register(
    mooncake_conductor_client_t client,
    const mooncake_conductor_service_config* config,
    mooncake_conductor_registration_result** out) {
    return Call(client, out, [&](mc::ConductorClient& c) {
        if (!config) throw std::invalid_argument("null service");
        return c.Register(Service(*config));
    });
}
int32_t mooncake_conductor_unregister(
    mooncake_conductor_client_t client, mooncake_conductor_string instance_id,
    mooncake_conductor_string tenant_id, int32_t dp_rank,
    mooncake_conductor_unregistration_result** out) {
    return Call(client, out, [&](mc::ConductorClient& c) {
        return c.Unregister(Copy(instance_id), Copy(tenant_id), dp_rank);
    });
}
int32_t mooncake_conductor_query(
    mooncake_conductor_client_t client,
    const mooncake_conductor_query_request* request, const int32_t* token_ids,
    size_t token_count, mooncake_conductor_query_result** out) {
    return Call(client, out, [&](mc::ConductorClient& c) {
        if (!request || (!token_ids && token_count) ||
            token_count > std::vector<int32_t>().max_size())
            throw std::invalid_argument("invalid query input");
        mc::QueryRequest req;
        req.context = {Copy(request->tenant_id), Copy(request->model_name),
                       Copy(request->lora_name), request->block_size};
        if (token_count)
            req.token_ids.assign(token_ids, token_ids + token_count);
        auto salt = Copy(request->cache_salt);
        auto filter = Copy(request->instance_filter);
        if (!salt.empty()) req.cache_salt = std::move(salt);
        if (!filter.empty()) req.instance_filter = std::move(filter);
        return c.Query(req);
    });
}
int32_t mooncake_conductor_list_services(
    mooncake_conductor_client_t client,
    mooncake_conductor_services_result** out) {
    return Call(client, out,
                [](mc::ConductorClient& c) { return c.ListServices(); });
}
int32_t mooncake_conductor_get_global_view(
    mooncake_conductor_client_t client,
    mooncake_conductor_global_result** out) {
    return Call(client, out,
                [](mc::ConductorClient& c) { return c.GetGlobalView(); });
}
int32_t mooncake_conductor_query_view(
    const mooncake_conductor_query_result* result,
    const mooncake_conductor_cache_hit** hits, size_t* count) {
    return GetView(result, &mooncake_conductor_query_result::hits, hits, count);
}
int32_t mooncake_conductor_services_view(
    const mooncake_conductor_services_result* result,
    const mooncake_conductor_registered_service** services, size_t* count) {
    return GetView(result, &mooncake_conductor_services_result::services,
                   services, count);
}
int32_t mooncake_conductor_global_view(
    const mooncake_conductor_global_result* result,
    const mooncake_conductor_context_view** contexts, size_t* count) {
    return GetView(result, &mooncake_conductor_global_result::contexts,
                   contexts, count);
}
int32_t mooncake_conductor_registration_view(
    const mooncake_conductor_registration_result* result,
    mooncake_conductor_string* instance_id, int32_t* is_new) {
    if (instance_id) *instance_id = {nullptr, 0};
    if (is_new) *is_new = 0;
    if (!result || !instance_id || !is_new)
        return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    *instance_id = View(result->source.instance_id);
    *is_new = result->source.is_new;
    return MOONCAKE_CONDUCTOR_OK;
}
int32_t mooncake_conductor_unregistration_view(
    const mooncake_conductor_unregistration_result* result,
    mooncake_conductor_string* removed_key) {
    if (removed_key) *removed_key = {nullptr, 0};
    if (!result || !removed_key) return MOONCAKE_CONDUCTOR_INVALID_PARAMS;
    *removed_key = View(result->source.removed_key);
    return MOONCAKE_CONDUCTOR_OK;
}
void mooncake_conductor_query_free(mooncake_conductor_query_result* r) {
    delete r;
}
void mooncake_conductor_services_free(mooncake_conductor_services_result* r) {
    delete r;
}
void mooncake_conductor_global_free(mooncake_conductor_global_result* r) {
    delete r;
}
void mooncake_conductor_registration_free(
    mooncake_conductor_registration_result* r) {
    delete r;
}
void mooncake_conductor_unregistration_free(
    mooncake_conductor_unregistration_result* r) {
    delete r;
}
}  // extern "C"
