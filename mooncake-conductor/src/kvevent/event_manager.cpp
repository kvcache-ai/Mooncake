#include "conductor/kvevent/event_manager.h"

#include <glog/logging.h>
#include <json/json.h>
#include <csignal>
#include <ylt/coro_http/coro_http_server.hpp>

#include <algorithm>
#include <chrono>
#include <limits>
#include <optional>
#include <set>
#include <thread>
#include <utility>

#include "conductor/prefixindex/hash_strategy.h"

namespace conductor {
namespace kvevent {

namespace {

using coro_http::coro_http_request;
using coro_http::coro_http_response;
using coro_http::status_type;

constexpr const char* kTextPlain = "text/plain; charset=utf-8";
constexpr const char* kApplicationJson = "application/json";

prefixindex::ContextKey ContextFromService(
    const common::ServiceConfig& service) {
    return {.tenant_id = service.tenant_id,
            .model_name = service.model_name,
            .lora_name = service.lora_name,
            .block_size = service.block_size};
}

prefixindex::HashProfile ProfileFromService(
    const common::ServiceConfig& service) {
    return {.strategy = service.hash_profile.strategy,
            .algorithm = service.hash_profile.algorithm,
            .root_digest = service.hash_profile.root_digest,
            .index_projection = service.hash_profile.index_projection};
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

bool JsonInt64(const Json::Value& value, int64_t* out) {
    if (value.type() == Json::intValue) {
        *out = value.asInt64();
        return true;
    }
    if (value.type() == Json::uintValue &&
        value.asUInt64() <=
            static_cast<Json::UInt64>(std::numeric_limits<int64_t>::max())) {
        *out = static_cast<int64_t>(value.asUInt64());
        return true;
    }
    return false;
}

// Writes an error response: text/plain body with trailing \n.
void HttpError(coro_http_response& resp, status_type status,
               const std::string& message) {
    resp.add_header("Content-Type", kTextPlain);
    resp.set_status_and_content(status, message + "\n");
}

void HttpJson(coro_http_response& resp, status_type status,
              const Json::Value& value) {
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    // JSON-encoded output terminates with a trailing '\n' (wire contract).
    resp.add_header("Content-Type", kApplicationJson);
    resp.set_status_and_content(status, Json::writeString(wb, value) + "\n");
}

void HttpJson(coro_http_response& resp, const Json::Value& value) {
    HttpJson(resp, status_type::ok, value);
}

void HttpJsonError(coro_http_response& resp, const char* reason,
                   const std::string& message, const char* field = nullptr,
                   std::optional<size_t> index = std::nullopt) {
    Json::Value error(Json::objectValue);
    error["error"] = message;
    error["reason"] = reason;
    if (field != nullptr) {
        error["field"] = field;
    }
    if (index.has_value()) {
        error["index"] = Json::Value::UInt64(*index);
    }
    HttpJson(resp, status_type::bad_request, error);
}

bool RejectUnknownFields(const Json::Value& body,
                         const std::set<std::string>& allowed,
                         coro_http_response& resp) {
    for (const std::string& name : body.getMemberNames()) {
        if (!allowed.contains(name)) {
            HttpJsonError(resp, "unknown_field",
                          "unsupported request field: " + name, name.c_str());
            return false;
        }
    }
    return true;
}

bool RequiredString(const Json::Value& body, const char* field,
                    coro_http_response& resp, std::string* out) {
    if (!body.isMember(field)) {
        HttpJsonError(resp, "missing", std::string(field) + " is required",
                      field);
        return false;
    }
    if (!body[field].isString()) {
        HttpJsonError(resp, "invalid_type",
                      std::string(field) + " must be a string", field);
        return false;
    }
    *out = body[field].asString();
    if (out->empty()) {
        HttpJsonError(resp, "invalid_value",
                      std::string(field) + " must not be empty", field);
        return false;
    }
    return true;
}

bool OptionalStringStrict(const Json::Value& body, const char* field,
                          const std::string& fallback, coro_http_response& resp,
                          std::string* out) {
    if (!body.isMember(field)) {
        *out = fallback;
        return true;
    }
    if (!body[field].isString()) {
        HttpJsonError(resp, "invalid_type",
                      std::string(field) + " must be a string", field);
        return false;
    }
    *out = body[field].asString();
    return true;
}

bool RequiredPositiveInt64(const Json::Value& body, const char* field,
                           coro_http_response& resp, int64_t* out) {
    if (!body.isMember(field)) {
        HttpJsonError(resp, "missing", std::string(field) + " is required",
                      field);
        return false;
    }
    if (!JsonInt64(body[field], out)) {
        HttpJsonError(resp, "invalid_type",
                      std::string(field) + " must be an integer", field);
        return false;
    }
    if (*out <= 0) {
        HttpJsonError(resp, "out_of_range",
                      std::string(field) + " must be greater than zero", field);
        return false;
    }
    return true;
}

bool ParseOptionalCacheGroup(const Json::Value& body, coro_http_response& resp,
                             std::optional<int64_t>* cache_group) {
    if (!body.isMember("cache_group") || body["cache_group"].isNull()) {
        cache_group->reset();
        return true;
    }
    int64_t value = 0;
    if (!JsonInt64(body["cache_group"], &value)) {
        HttpJsonError(resp, "invalid_type",
                      "cache_group must be an integer or null", "cache_group");
        return false;
    }
    if (value != 0) {
        HttpJsonError(resp, "unsupported", "only cache group zero is supported",
                      "cache_group");
        return false;
    }
    *cache_group = value;
    return true;
}

bool ParseHashProfileConfig(const Json::Value& body, coro_http_response& resp,
                            common::HashProfileConfig* profile) {
    if (!body.isMember("hash_profile")) {
        HttpJsonError(resp, "missing", "hash_profile is required",
                      "hash_profile");
        return false;
    }
    const Json::Value& value = body["hash_profile"];
    if (!value.isObject()) {
        HttpJsonError(resp, "invalid_type", "hash_profile must be an object",
                      "hash_profile");
        return false;
    }
    static const std::set<std::string> kAllowedProfileFields = {
        "algorithm", "index_projection", "root_digest", "strategy"};
    if (!RejectUnknownFields(value, kAllowedProfileFields, resp)) {
        return false;
    }
    return RequiredString(value, "strategy", resp, &profile->strategy) &&
           RequiredString(value, "algorithm", resp, &profile->algorithm) &&
           RequiredString(value, "root_digest", resp, &profile->root_digest) &&
           RequiredString(value, "index_projection", resp,
                          &profile->index_projection);
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
    if (service.type == common::kServiceTypeVLLM) {
        if (service.instance_id.empty()) {
            return "instance_id is required for vLLM";
        }
        return prefixindex::PrefixCacheTable::ValidateRegistration(
                   RegistrationFromService(service))
            .error;
    }
    if (service.type == common::kServiceTypeMooncake) {
        return prefixindex::ValidateHashProfile(ProfileFromService(service));
    }
    return "unsupported service type: " + service.type;
}

bool ParseJsonObject(coro_http_request& req, Json::Value* out,
                     std::string* errors, bool strict = false) {
    Json::CharReaderBuilder rb;
    if (strict) {
        rb["allowComments"] = false;
        rb["allowTrailingCommas"] = false;
        rb["failIfExtra"] = true;
    }
    const auto body = req.get_body();
    std::unique_ptr<Json::CharReader> reader(rb.newCharReader());
    if (!reader->parse(body.data(), body.data() + body.size(), out, errors)) {
        return false;
    }
    if (!out->isObject()) {
        *errors = "request body must be a JSON object";
        return false;
    }
    return true;
}

// Parses a request body as a JSON object. Returns false (and writes the
// 400 response) on malformed JSON.
bool ParseJsonBody(coro_http_request& req, coro_http_response& resp,
                   const char* what, Json::Value* out) {
    std::string errs;
    if (!ParseJsonObject(req, out, &errs)) {
        LOG(ERROR) << "Failed to decode " << what << " JSON err=" << errs;
        HttpError(resp, status_type::bad_request, "Invalid JSON");
        return false;
    }
    return true;
}

bool ParseQueryJsonBody(coro_http_request& req, coro_http_response& resp,
                        Json::Value* out) {
    std::string errs;
    if (!ParseJsonObject(req, out, &errs, true)) {
        LOG(ERROR) << "Failed to decode query JSON err=" << errs;
        HttpJsonError(resp, "invalid_json", "Invalid JSON object");
        return false;
    }
    return true;
}

bool ParseQueryTokenIds(const Json::Value& body, coro_http_response& resp,
                        std::vector<int32_t>* token_ids) {
    if (!body.isMember("token_ids")) {
        HttpJsonError(resp, "missing", "token_ids is required", "token_ids");
        return false;
    }

    const Json::Value& values = body["token_ids"];
    if (!values.isArray()) {
        HttpJsonError(resp, "not_array", "token_ids must be an array",
                      "token_ids");
        return false;
    }

    token_ids->clear();
    token_ids->reserve(values.size());
    for (Json::ArrayIndex index = 0; index < values.size(); ++index) {
        const Json::Value& value = values[index];
        int32_t token_id = 0;
        if (value.type() == Json::intValue) {
            const Json::Int64 signed_value = value.asInt64();
            if (signed_value < std::numeric_limits<int32_t>::min() ||
                signed_value > std::numeric_limits<int32_t>::max()) {
                HttpJsonError(resp, "out_of_range",
                              "token_ids element is outside the int32 range",
                              "token_ids", index);
                return false;
            }
            token_id = static_cast<int32_t>(signed_value);
        } else if (value.type() == Json::uintValue) {
            const Json::UInt64 unsigned_value = value.asUInt64();
            if (unsigned_value > static_cast<Json::UInt64>(
                                     std::numeric_limits<int32_t>::max())) {
                HttpJsonError(resp, "out_of_range",
                              "token_ids element is outside the int32 range",
                              "token_ids", index);
                return false;
            }
            token_id = static_cast<int32_t>(unsigned_value);
        } else {
            HttpJsonError(resp, "invalid_type",
                          "token_ids element must be a JSON integer",
                          "token_ids", index);
            return false;
        }
        token_ids->push_back(token_id);
    }
    return true;
}

struct QueryRequest {
    prefixindex::ContextKey context;
    std::vector<int32_t> token_ids;
    std::optional<std::string> cache_salt;
    std::optional<std::string> instance_filter;
};

bool ParseQueryRequest(const Json::Value& body, coro_http_response& resp,
                       QueryRequest* request) {
    static const std::set<std::string> kAllowedFields = {
        "block_size", "cache_salt", "instance_id", "lora_name",
        "model",      "tenant_id",  "token_ids"};
    if (!RejectUnknownFields(body, kAllowedFields, resp)) {
        return false;
    }

    if (!RequiredString(body, "model", resp, &request->context.model_name) ||
        !RequiredPositiveInt64(body, "block_size", resp,
                               &request->context.block_size) ||
        !ParseQueryTokenIds(body, resp, &request->token_ids) ||
        !OptionalStringStrict(body, "tenant_id", "default", resp,
                              &request->context.tenant_id) ||
        !OptionalStringStrict(body, "lora_name", "", resp,
                              &request->context.lora_name)) {
        return false;
    }
    if (request->context.tenant_id.empty()) {
        request->context.tenant_id = "default";
    }

    request->cache_salt.reset();
    if (body.isMember("cache_salt") && !body["cache_salt"].isNull()) {
        if (!body["cache_salt"].isString()) {
            HttpJsonError(resp, "invalid_type",
                          "cache_salt must be a string or null", "cache_salt");
            return false;
        }
        const std::string value = body["cache_salt"].asString();
        if (!value.empty()) {
            request->cache_salt = value;
        }
    }

    request->instance_filter.reset();
    if (body.isMember("instance_id")) {
        if (!body["instance_id"].isString()) {
            HttpJsonError(resp, "invalid_type", "instance_id must be a string",
                          "instance_id");
            return false;
        }
        request->instance_filter = body["instance_id"].asString();
    }
    return true;
}

bool ParseServiceConfigRequest(const Json::Value& body,
                               coro_http_response& resp,
                               common::ServiceConfig* service) {
    static const std::set<std::string> kAllowedFields = {
        "block_size",      "cache_group", "dp_rank",   "endpoint",
        "hash_profile",    "instance_id", "lora_name", "modelname",
        "replay_endpoint", "tenant_id",   "type"};
    if (!RejectUnknownFields(body, kAllowedFields, resp) ||
        !RequiredString(body, "endpoint", resp, &service->endpoint) ||
        !RequiredString(body, "type", resp, &service->type) ||
        !RequiredString(body, "modelname", resp, &service->model_name) ||
        !RequiredString(body, "instance_id", resp, &service->instance_id) ||
        !RequiredPositiveInt64(body, "block_size", resp,
                               &service->block_size) ||
        !OptionalStringStrict(body, "replay_endpoint", "", resp,
                              &service->replay_endpoint) ||
        !OptionalStringStrict(body, "lora_name", "", resp,
                              &service->lora_name) ||
        !OptionalStringStrict(body, "tenant_id", "default", resp,
                              &service->tenant_id) ||
        !ParseOptionalCacheGroup(body, resp, &service->cache_group) ||
        !ParseHashProfileConfig(body, resp, &service->hash_profile)) {
        return false;
    }
    if (service->tenant_id.empty()) {
        service->tenant_id = "default";
    }

    if (!body.isMember("dp_rank")) {
        HttpJsonError(resp, "missing", "dp_rank is required", "dp_rank");
        return false;
    }
    int64_t dp_rank = 0;
    if (!JsonInt64(body["dp_rank"], &dp_rank)) {
        HttpJsonError(resp, "invalid_type", "dp_rank must be an integer",
                      "dp_rank");
        return false;
    }
    if (dp_rank < 0 ||
        dp_rank > static_cast<int64_t>(std::numeric_limits<int>::max())) {
        HttpJsonError(resp, "out_of_range",
                      "dp_rank must be a non-negative int", "dp_rank");
        return false;
    }
    service->dp_rank = static_cast<int>(dp_rank);

    if (const std::string error = ValidateServiceConfig(*service);
        !error.empty()) {
        HttpJsonError(resp, "invalid_registration", error);
        return false;
    }
    return true;
}

Json::Value CacheHitResultToJson(const prefixindex::CacheHitResult& result) {
    Json::Value out(Json::objectValue);
    out["longest_matched"] = Json::Value::Int64(result.longest_match_tokens);
    Json::Value dp(Json::objectValue);
    for (const auto& [rank, tokens] : result.dp) {
        // map<int64,int64> is serialised with decimal-string keys (JSON wire
        // contract).
        dp[std::to_string(rank)] = Json::Value::Int64(tokens);
    }
    out["dp"] = dp;
    out["gpu"] = Json::Value::Int64(result.gpu);
    out["cpu"] = Json::Value::Int64(result.cpu);
    out["disk"] = Json::Value::Int64(result.disk);
    return out;
}

Json::Value ServiceConfigToJson(const common::ServiceConfig& svc) {
    // Field names are the exported struct-field names of
    // common.ServiceConfig (fixed JSON wire contract).
    Json::Value out(Json::objectValue);
    out["Endpoint"] = svc.endpoint;
    out["ReplayEndpoint"] = svc.replay_endpoint;
    out["Type"] = svc.type;
    out["ModelName"] = svc.model_name;
    out["LoraName"] = svc.lora_name;
    out["TenantID"] = svc.tenant_id;
    out["InstanceID"] = svc.instance_id;
    out["BlockSize"] = Json::Value::Int64(svc.block_size);
    out["DPRank"] = svc.dp_rank;
    if (svc.cache_group.has_value()) {
        out["CacheGroup"] = Json::Value::Int64(*svc.cache_group);
    } else {
        out["CacheGroup"] = Json::Value(Json::nullValue);
    }
    Json::Value profile(Json::objectValue);
    profile["strategy"] = svc.hash_profile.strategy;
    profile["algorithm"] = svc.hash_profile.algorithm;
    profile["root_digest"] = svc.hash_profile.root_digest;
    profile["index_projection"] = svc.hash_profile.index_projection;
    out["HashProfile"] = profile;
    return out;
}

}  // namespace

std::string MakeServiceKey(const std::string& instance_id,
                           const std::string& tenant_id, int dp_rank) {
    return instance_id + "|" + tenant_id + "|" + std::to_string(dp_rank);
}

EventManager::EventManager(std::vector<common::ServiceConfig> services,
                           int http_server_port)
    : services_(std::move(services)), http_server_port_(http_server_port) {}

EventManager::~EventManager() { Stop(); }

bool EventManager::IsStopped() {
    std::shared_lock lock(mu_);
    return stopped_;
}

void EventManager::Start() {
    LOG(INFO) << "Starting KV Event Manager...";

    std::vector<common::ServiceConfig> services_snapshot;
    {
        std::shared_lock lock(mu_);
        services_snapshot = services_;
    }

    // Subscribe to all services concurrently.
    // mu_ serialises the check-then-act inside SubscribeToService and
    // serialises with concurrent /register HTTP handlers so that
    // subscribers_, active_configs_, and services_ stay consistent.
    std::atomic<int> failure_count{0};
    std::vector<std::thread> workers;
    workers.reserve(services_snapshot.size());
    for (const auto& svc : services_snapshot) {
        workers.emplace_back([this, svc, &failure_count] {
            std::pair<bool, std::string> result;
            {
                std::unique_lock lock(mu_);
                result = SubscribeToService(svc);
            }
            if (!result.second.empty()) {
                LOG(ERROR) << "Failed to initiate subscription service_type="
                           << svc.type << " instance_id=" << svc.instance_id
                           << " endpoint=" << svc.endpoint
                           << " error=" << result.second;
                failure_count.fetch_add(1);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    const int failed = failure_count.load();
    LOG(INFO) << "Static KV Event Manager started. Subscriptions success="
              << (static_cast<int>(services_snapshot.size()) - failed)
              << " failed=" << failed;
}

void EventManager::Stop() {
    {
        std::unique_lock lock(mu_);
        if (stopped_) {
            return;
        }
        stopped_ = true;
    }

    LOG(INFO) << "Stopping Conductor KV Event Manager.....";

    // yalantinglibs closes the acceptor and current connections, stops the
    // thread pool, and joins the server thread before stop() returns.
    if (http_server_) {
        LOG(INFO) << "Shutting down HTTP server";
        http_server_->stop();
    }

    // Stop all ZMQ clients. Collect them under the lock but Stop()
    // outside it — same deadlock rule as UnsubscribeFromService.
    std::vector<std::pair<std::string, std::shared_ptr<zmq::ZMQClient>>>
        clients;
    {
        std::unique_lock lock(mu_);
        clients.assign(subscribers_.begin(), subscribers_.end());
    }
    for (auto& [key, client] : clients) {
        client->Stop();
        LOG(INFO) << "Stopped all subscription service_key=" << key;
    }
}

std::pair<bool, std::string> EventManager::SubscribeToService(
    const common::ServiceConfig& svc) {
    // The caller holds mu_ exclusively, so read stopped_ directly rather
    // than recursively acquiring the non-recursive shared mutex via
    // IsStopped().
    if (stopped_) {
        return {false, "manager stopped"};
    }

    if (const std::string error = ValidateServiceConfig(svc); !error.empty()) {
        return {false, "invalid service registration: " + error};
    }

    const std::string svc_key =
        MakeServiceKey(svc.instance_id, svc.tenant_id, svc.dp_rank);
    if (unregistering_.contains(svc_key)) {
        return {false, "service is being unregistered: " + svc_key};
    }
    if (auto existing = active_configs_.find(svc_key);
        existing != active_configs_.end()) {
        if (existing->second == svc) {
            return {false, ""};
        }
        return {false, "conflicting registration for service key: " + svc_key};
    }
    if (subscribers_.contains(svc_key)) {
        return {false,
                "inconsistent subscriber state for service key: " + svc_key};
    }

    // Use ReplayEndpoint directly, fallback to empty if not provided
    const std::string replay_endpoint = svc.replay_endpoint;

    auto handler = std::make_shared<KVEventHandler>(this, svc);

    // Configure ZMQ Client
    zmq::ZMQClientConfig zmq_config;
    zmq_config.cache_pool_key = svc_key;
    zmq_config.endpoint = svc.endpoint;
    zmq_config.replay_endpoint = replay_endpoint;
    zmq_config.model_name = svc.model_name;
    zmq_config.poll_timeout = std::chrono::milliseconds(100);
    zmq_config.replay_timeout = std::chrono::seconds(5);
    zmq_config.reconnect_delay = std::chrono::seconds(1);

    if (auto err = zmq::ValidateConfig(zmq_config); !err.empty()) {
        return {false, "invalid ZMQ config: " + err};
    }

    bool inserted_registration = false;
    if (svc.type == common::kServiceTypeVLLM) {
        const auto registration_result =
            indexer_.Register(RegistrationFromService(svc));
        if (!registration_result.error.empty()) {
            return {false, "failed to register prefix context: " +
                               registration_result.error};
        }
        inserted_registration = registration_result.inserted;
    } else {
        const std::string binding_error = indexer_.ValidateProfileBinding(
            ContextFromService(svc), ProfileFromService(svc));
        if (!binding_error.empty() &&
            binding_error != "ContextKey is not registered") {
            return {false,
                    "failed to bind shared-cache profile: " + binding_error};
        }
    }

    auto client = std::make_shared<zmq::ZMQClient>(zmq_config, handler);
    if (auto err = client->Start(); !err.empty()) {
        if (inserted_registration) {
            const std::string rollback_error = indexer_.Unregister(
                ContextFromService(svc), svc.instance_id, svc.dp_rank);
            if (!rollback_error.empty()) {
                LOG(ERROR) << "Registration rollback failed service_key="
                           << svc_key << " error=" << rollback_error;
            }
        }
        return {false, "failed to start ZMQ client: " + err};
    }

    subscribers_[svc_key] = client;
    active_configs_[svc_key] = svc;

    LOG(INFO) << "Successfully subscribed to service service_type=" << svc.type
              << " service_key=" << svc_key
              << " instance_id=" << svc.instance_id
              << " tenant_id=" << svc.tenant_id << " endpoint=" << svc.endpoint
              << " replay_endpoint=" << replay_endpoint;

    return {true, ""};
}

std::pair<bool, std::string> EventManager::UnsubscribeFromService(
    const std::string& instance_id, const std::string& tenant_id, int dp_rank) {
    const std::string svc_key = MakeServiceKey(instance_id, tenant_id, dp_rank);

    std::shared_ptr<zmq::ZMQClient> client;
    common::ServiceConfig service;
    {
        std::unique_lock lock(mu_);
        if (unregistering_.contains(svc_key)) {
            return {false, "service is already being unregistered: " + svc_key};
        }
        auto client_it = subscribers_.find(svc_key);
        auto config_it = active_configs_.find(svc_key);
        if (client_it == subscribers_.end() ||
            config_it == active_configs_.end()) {
            return {false, "service not found: " + svc_key};
        }
        client = client_it->second;
        service = config_it->second;
        // Keep the maps populated while Stop joins the event loop. This
        // reserves the key and prevents a replacement registration from being
        // removed by this in-flight unregister operation.
        unregistering_.insert(svc_key);
    }

    // Stop the ZMQ client OUTSIDE mu_ to avoid deadlock:
    // HandleEvent acquires mu_ (read). If the ZMQ event-loop thread is
    // currently inside HandleEvent (or about to enter it), holding mu_
    // while waiting for that thread to exit via client->Stop() -> join
    // would deadlock.
    client->Stop();

    std::string index_error;
    {
        std::unique_lock lock(mu_);
        if (service.type == common::kServiceTypeVLLM) {
            index_error =
                indexer_.Unregister(ContextFromService(service),
                                    service.instance_id, service.dp_rank);
        }
        subscribers_.erase(svc_key);
        active_configs_.erase(svc_key);
        if (auto service_it =
                std::find(services_.begin(), services_.end(), service);
            service_it != services_.end()) {
            services_.erase(service_it);
        }
        unregistering_.erase(svc_key);
    }

    LOG(INFO) << "Successfully unsubscribed from service service_key="
              << svc_key << " instance_id=" << instance_id
              << " tenant_id=" << tenant_id;
    return {true, index_error};
}

void EventManager::RegisterHttpHandlers() {
    using coro_http::GET;
    using coro_http::POST;
    auto* server = http_server_.get();

    // ---- /query ---------------------------------------------------------
    server->set_http_handler<POST>(
        "/query", [this](coro_http_request& req, coro_http_response& resp) {
            VLOG(1) << "receive req method=POST path=/query";

            Json::Value body;
            if (!ParseQueryJsonBody(req, resp, &body)) {
                return;
            }

            QueryRequest query;
            if (!ParseQueryRequest(body, resp, &query)) {
                return;
            }

            const auto results =
                indexer_.Query(query.context, query.token_ids, query.cache_salt,
                               query.instance_filter);
            Json::Value instances(Json::objectValue);
            for (const auto& [instance_id, result] : results) {
                instances[instance_id] = CacheHitResultToJson(result);
            }

            Json::Value response_result(Json::objectValue);
            response_result["instances"] = instances;
            HttpJson(resp, response_result);
        });
    server->set_http_handler<GET>("/query", [](coro_http_request&,
                                               coro_http_response& resp) {
        HttpError(resp, status_type::method_not_allowed, "Method not allowed");
    });

    // ---- /register ------------------------------------------------------
    server->set_http_handler<POST>(
        "/register", [this](coro_http_request& req, coro_http_response& resp) {
            Json::Value body;
            if (!ParseJsonBody(req, resp, "register", &body)) {
                return;
            }

            common::ServiceConfig svc;
            if (!ParseServiceConfigRequest(body, resp, &svc)) {
                return;
            }

            {
                std::unique_lock lock(mu_);
                auto [is_new, err] = SubscribeToService(svc);
                if (!err.empty()) {
                    lock.unlock();
                    LOG(ERROR) << "Dynamic register failed instance_id="
                               << svc.instance_id << " err=" << err;
                    if (err.starts_with("failed to start ZMQ client")) {
                        HttpError(resp, status_type::internal_server_error,
                                  "Failed to subscribe: " + err);
                    } else {
                        HttpJsonError(resp, "invalid_registration", err);
                    }
                    return;
                }
                if (is_new) {
                    services_.push_back(svc);
                }
            }

            Json::Value out(Json::objectValue);
            out["status"] = "registered successfully";
            out["instance_id"] = svc.instance_id;
            HttpJson(resp, out);
        });
    server->set_http_handler<GET>("/register", [](coro_http_request&,
                                                  coro_http_response& resp) {
        HttpError(resp, status_type::method_not_allowed, "Method not allowed");
    });

    // ---- /unregister ----------------------------------------------------
    server->set_http_handler<POST>(
        "/unregister",
        [this](coro_http_request& req, coro_http_response& resp) {
            Json::Value body;
            if (!ParseJsonBody(req, resp, "unregister", &body)) {
                return;
            }

            static const std::set<std::string> kAllowedFields = {
                "dp_rank", "instance_id", "tenant_id"};
            if (!RejectUnknownFields(body, kAllowedFields, resp)) {
                return;
            }
            std::string target_tenant;
            std::string instance_id;
            if (!RequiredString(body, "instance_id", resp, &instance_id) ||
                !OptionalStringStrict(body, "tenant_id", "default", resp,
                                      &target_tenant)) {
                return;
            }
            if (target_tenant.empty()) {
                target_tenant = "default";
            }
            if (!body.isMember("dp_rank")) {
                HttpJsonError(resp, "missing", "dp_rank is required",
                              "dp_rank");
                return;
            }
            int64_t parsed_rank = 0;
            if (!JsonInt64(body["dp_rank"], &parsed_rank) || parsed_rank < 0 ||
                parsed_rank >
                    static_cast<int64_t>(std::numeric_limits<int>::max())) {
                HttpJsonError(resp, "invalid_value",
                              "dp_rank must be a non-negative int", "dp_rank");
                return;
            }
            const int dp_rank = static_cast<int>(parsed_rank);
            const std::string target_key =
                MakeServiceKey(instance_id, target_tenant, dp_rank);

            const auto [removed_service, error] =
                UnsubscribeFromService(instance_id, target_tenant, dp_rank);
            if (!removed_service) {
                HttpError(
                    resp, status_type::not_found,
                    error.empty() ? "service not found: " + target_key : error);
                return;
            }
            if (!error.empty()) {
                HttpError(resp, status_type::internal_server_error,
                          "Failed to unregister prefix context: " + error);
                return;
            }

            Json::Value out(Json::objectValue);
            out["status"] = "unregistered successfully";
            Json::Value removed(Json::arrayValue);
            removed.append(target_key);
            out["removed_instances"] = removed;
            HttpJson(resp, out);
        });
    server->set_http_handler<GET>("/unregister", [](coro_http_request&,
                                                    coro_http_response& resp) {
        HttpError(resp, status_type::method_not_allowed, "Method not allowed");
    });

    // ---- /global_view ---------------------------------------------------
    server->set_http_handler<GET>(
        "/global_view", [this](coro_http_request&, coro_http_response& resp) {
            const auto global_view = indexer_.GetGlobalView();

            Json::Value out(Json::objectValue);
            out["context_count"] = global_view.context_count;
            Json::Value contexts(Json::arrayValue);
            for (const auto& view : global_view.contexts) {
                Json::Value c(Json::objectValue);
                c["model_name"] = view.context.model_name;
                c["lora_name"] = view.context.lora_name;
                c["block_size"] = Json::Value::Int64(view.context.block_size);
                c["tenant_id"] = view.context.tenant_id;
                c["prefix_count"] = Json::Value::UInt64(view.prefix_count);

                Json::Value profile(Json::objectValue);
                profile["strategy"] = view.profile.strategy;
                profile["algorithm"] = view.profile.algorithm;
                profile["root_digest"] = view.profile.root_digest;
                profile["index_projection"] = view.profile.index_projection;
                c["hash_profile"] = profile;

                Json::Value instances(Json::objectValue);
                for (const auto& [instance_id, ranks] : view.instance_ranks) {
                    Json::Value rank_values(Json::arrayValue);
                    for (const int64_t rank : ranks) {
                        rank_values.append(Json::Value::Int64(rank));
                    }
                    instances[instance_id] = rank_values;
                }
                c["instances"] = instances;
                contexts.append(c);
            }
            out["contexts"] = contexts;
            HttpJson(resp, out);
        });
    server->set_http_handler<POST>(
        "/global_view", [](coro_http_request&, coro_http_response& resp) {
            HttpError(resp, status_type::method_not_allowed,
                      "Method not allowed");
        });

    // ---- /services ------------------------------------------------------
    server->set_http_handler<GET>(
        "/services", [this](coro_http_request&, coro_http_response& resp) {
            VLOG(1) << "receive req method=GET path=/services";

            Json::Value services(Json::arrayValue);
            int count = 0;
            {
                std::shared_lock lock(mu_);
                for (const auto& [key, svc] : active_configs_) {
                    services.append(ServiceConfigToJson(svc));
                    ++count;
                }
            }

            Json::Value out(Json::objectValue);
            out["count"] = count;
            out["services"] = services;
            // This endpoint writes the JSON body with no trailing newline,
            // unlike the other endpoints (fixed wire contract).
            Json::StreamWriterBuilder wb;
            wb["indentation"] = "";
            resp.add_header("Content-Type", kApplicationJson);
            resp.set_status_and_content(status_type::ok,
                                        Json::writeString(wb, out));
        });
    server->set_http_handler<POST>("/services", [](coro_http_request&,
                                                   coro_http_response& resp) {
        HttpError(resp, status_type::method_not_allowed, "Method not allowed");
    });
}

bool EventManager::StartHTTPServer() {
    http_server_ = std::make_unique<coro_http::coro_http_server>(
        /*thread_num=*/4, static_cast<unsigned short>(http_server_port_));
    RegisterHttpHandlers();

    LOG(INFO) << "HTTP server listening port=" << http_server_port_;
    // async_start returns a future that resolves on failure or stop;
    // errors surface asynchronously and are logged rather than returned.
    auto future = http_server_->async_start();
    // Give a synchronous bind failure a brief chance to surface, so
    // callers see startup errors like "address in use".
    if (future.hasResult()) {
        const auto ec = std::move(future).get();
        if (ec) {
            LOG(ERROR) << "HTTP server failed err=" << ec.message();
            return false;
        }
    }
    return true;
}

}  // namespace kvevent
}  // namespace conductor
