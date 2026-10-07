#include "conductor/kvevent/conductor_service.h"

#include <optional>

#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/hash_strategy.h"

namespace mooncake::conductor::kvevent {

prefixindex::ContextKey ContextFromService(
    const common::ServiceConfig& service) {
    return {.tenant_id = service.tenant_id,
            .model_name = service.model_name,
            .lora_name = service.lora_name,
            .block_size = service.block_size};
}

prefixindex::HashProfile ProfileFromService(
    const common::ServiceConfig& service) {
    return service.hash_profile;
}

prefixindex::EngineRegistration RegistrationFromService(
    const common::ServiceConfig& service) {
    return {.context = ContextFromService(service),
            .profile = ProfileFromService(service),
            .instance_id = service.instance_id,
            .dp_rank = service.dp_rank,
            .effective_block_size = service.block_size,
            .cache_group = service.cache_group};
}

std::string ValidateServiceConfig(const common::ServiceConfig& service) {
    if (service.endpoint.empty()) {
        return "endpoint is required";
    }
    if (service.model_name.empty()) {
        return "modelname is required";
    }
    if (service.tenant_id.empty()) {
        return "tenant_id must not be empty after normalization";
    }
    if (service.block_size <= 0) {
        return "block_size must be greater than zero";
    }
    if (service.dp_rank < 0) {
        return "dp_rank must be non-negative";
    }
    if (service.cache_group.has_value() && *service.cache_group != 0) {
        return "only cache group zero is supported";
    }
    if (service.publisher_kind == common::PublisherKind::kVllm ||
        service.publisher_kind == common::PublisherKind::kSglang) {
        if (service.instance_id.empty()) {
            return "instance_id is required for vLLM/SGLang";
        }
        return prefixindex::PrefixCacheTable::ValidateRegistration(
                   RegistrationFromService(service))
            .error;
    }
    if (service.publisher_kind == common::PublisherKind::kMooncake) {
        return prefixindex::ValidateHashProfile(ProfileFromService(service));
    }
    return "unsupported publisher kind";
}

tl::expected<QueryResult, ErrorCode> ConductorService::Query(
    const QueryRequest& request) {
    if (manager_.IsStopped()) {
        return tl::make_unexpected(ErrorCode::CONDUCTOR_UNAVAILABLE);
    }
    // RPC bypasses the HTTP parser, but must enforce the same value contract.
    if (request.context.model_name.empty() || request.context.block_size <= 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Only copy the context when normalization is needed; token_ids and the
    // usual explicit-tenant context remain borrowed from the request.
    std::optional<prefixindex::ContextKey> normalized_context;
    if (request.context.tenant_id.empty()) {
        normalized_context = request.context;
        normalized_context->tenant_id = "default";
    }
    QueryResult result;
    result.instances = manager_.indexer_.Query(
        normalized_context ? *normalized_context : request.context,
        request.token_ids, request.cache_salt, request.instance_filter);
    return result;
}

prefixindex::GlobalView ConductorService::GetGlobalView() {
    return manager_.indexer_.GetGlobalView();
}

std::vector<common::ServiceConfig> ConductorService::ListServices() {
    std::shared_lock lock(manager_.mu_);
    std::vector<common::ServiceConfig> out;
    out.reserve(manager_.active_configs_.size());
    for (const auto& [key, svc] : manager_.active_configs_) {
        out.push_back(svc);
    }
    return out;
}

tl::expected<RegisterResult, ErrorCode> ConductorService::Register(
    const common::ServiceConfig& config) {
    common::ServiceConfig resolved = config;
    // Single point of tenant_id normalization: the HTTP parsing layer
    // (ParseServiceConfigRequest) and the static config layer (config.cpp)
    // normalize on their own; this idempotently covers RPC calls that reach
    // this layer directly, keeping both channels semantically consistent.
    if (resolved.tenant_id.empty()) {
        resolved.tenant_id = "default";
    }
    // The HTTP channel resolves the recipe in its parsing layer (keeping the
    // detailed 400 report whose reason/field fields tests assert); the RPC
    // channel reaches this layer directly, so an empty root_digest is resolved
    // here as a fallback. ResolveHashProfile is a pure function, so both
    // channels produce the same result.
    if (resolved.hash_profile.root_digest.empty()) {
        common::HashProfileConfig source{config.hash_profile.strategy,
                                         config.hash_profile.algorithm,
                                         config.hash_profile.python_hash_seed,
                                         config.hash_profile.index_projection};
        std::string error_field;
        if (std::string error = prefixindex::ResolveHashProfile(
                source, &resolved.hash_profile, &error_field);
            !error.empty()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    if (const std::string error = ValidateServiceConfig(resolved);
        !error.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::unique_lock lock(manager_.mu_);
    auto [is_new, err] = manager_.SubscribeToService(resolved);
    if (!err.empty()) {
        // Mirrors the HTTP handler's error split: a ZMQ startup failure is an
        // internal error; anything else is a registration-parameter problem.
        if (err.starts_with("failed to start ZMQ client")) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (is_new) {
        manager_.services_.push_back(resolved);
    }
    return RegisterResult{resolved.instance_id, is_new};
}

tl::expected<UnregisterResult, ErrorCode> ConductorService::Unregister(
    const std::string& instance_id, const std::string& tenant_id, int dp_rank) {
    const std::string normalized_tenant =
        tenant_id.empty() ? "default" : tenant_id;
    const std::string key =
        MakeServiceKey(instance_id, normalized_tenant, dp_rank);
    auto [removed, error] = manager_.UnsubscribeFromService(
        instance_id, normalized_tenant, dp_rank);
    if (!removed) {
        return tl::make_unexpected(ErrorCode::SERVICE_NOT_FOUND);
    }
    if (!error.empty()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return UnregisterResult{key};
}

}  // namespace mooncake::conductor::kvevent
