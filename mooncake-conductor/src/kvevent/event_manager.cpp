#include "conductor/kvevent/event_manager.h"

#include <glog/logging.h>
#include <json/json.h>
#include <csignal>
#include <ylt/coro_http/coro_http_server.hpp>

#include <chrono>
#include <thread>
#include <utility>

namespace conductor {
namespace kvevent {

namespace {

using coro_http::coro_http_request;
using coro_http::coro_http_response;
using coro_http::status_type;

constexpr const char* kTextPlain = "text/plain; charset=utf-8";
constexpr const char* kApplicationJson = "application/json";

// Writes an error response: text/plain body with trailing \n.
void HttpError(coro_http_response& resp, status_type status,
               const std::string& message) {
    resp.add_header("Content-Type", kTextPlain);
    resp.set_status_and_content(status, message + "\n");
}

void HttpJson(coro_http_response& resp, const Json::Value& value) {
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    // JSON-encoded output terminates with a trailing '\n' (wire contract).
    resp.add_header("Content-Type", kApplicationJson);
    resp.set_status_and_content(status_type::ok,
                                Json::writeString(wb, value) + "\n");
}

// Parses a request body as a JSON object. Returns false (and writes the
// 400 response) on malformed JSON.
bool ParseJsonBody(coro_http_request& req, coro_http_response& resp,
                   const char* what, Json::Value* out) {
    Json::CharReaderBuilder rb;
    std::string errs;
    const auto body = req.get_body();
    std::unique_ptr<Json::CharReader> reader(rb.newCharReader());
    if (!reader->parse(body.data(), body.data() + body.size(), out, &errs) ||
        !out->isObject()) {
        LOG(ERROR) << "Failed to decode " << what << " JSON err=" << errs;
        HttpError(resp, status_type::bad_request, "Invalid JSON");
        return false;
    }
    return true;
}

// Optional-string semantics: an absent or non-string field falls back.
std::string OptionalString(const Json::Value& obj, const char* key,
                           const std::string& fallback) {
    if (obj.isMember(key) && obj[key].isString()) {
        return obj[key].asString();
    }
    return fallback;
}

// tenant_id: absent-or-empty falls back to the default ("" also falls back).
std::string TenantOrDefault(const Json::Value& obj) {
    const std::string t = OptionalString(obj, "tenant_id", "");
    return t.empty() ? "default" : t;
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
    out["DP"] = dp;
    out["GPU"] = Json::Value::Int64(result.gpu);
    out["CPU"] = Json::Value::Int64(result.cpu);
    out["DISK"] = Json::Value::Int64(result.disk);
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
    out["AdditionalSalt"] = svc.additional_salt;
    return out;
}

}  // namespace

std::string MakeServiceKey(const std::string& instance_id,
                           const std::string& tenant_id, int dp_rank) {
    return instance_id + "|" + tenant_id + "|" + std::to_string(dp_rank);
}

EventManager::EventManager(std::vector<common::ServiceConfig> services,
                           int http_server_port)
    : services_(std::move(services)), http_server_port_(http_server_port) {
    // TODO: create an independent indexer for each ModelContext
}

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

    // Use (instance_id, tenant_id) as composite key to support
    // multi-tenant replicas
    std::string svc_key =
        MakeServiceKey(svc.instance_id, svc.tenant_id, svc.dp_rank);
    if (svc.instance_id.empty()) {
        svc_key = MakeServiceKey(svc.endpoint, svc.tenant_id, svc.dp_rank);
    }

    if (subscribers_.count(svc_key) != 0) {
        return {false, ""};
    }

    // Validate endpoint
    if (svc.endpoint.empty()) {
        return {false, "endpoint is required"};
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

    auto client = std::make_shared<zmq::ZMQClient>(zmq_config, handler);
    if (auto err = client->Start(); !err.empty()) {
        return {false, "failed to start ZMQ client: " + err};
    }

    subscribers_[svc_key] = client;
    active_configs_[svc_key] = svc;

    // Add instance to tenant's instance map
    {
        std::unique_lock tenant_lock(tenant_mutex_);
        tenant_instance_map_[svc.tenant_id].insert(svc.instance_id);
    }

    LOG(INFO) << "Successfully subscribed to service service_type=" << svc.type
              << " service_key=" << svc_key
              << " instance_id=" << svc.instance_id
              << " tenant_id=" << svc.tenant_id << " endpoint=" << svc.endpoint
              << " replay_endpoint=" << replay_endpoint;

    return {true, ""};
}

void EventManager::UnsubscribeFromService(const std::string& instance_id,
                                          const std::string& tenant_id,
                                          int dp_rank) {
    const std::string svc_key = MakeServiceKey(instance_id, tenant_id, dp_rank);

    // Atomically remove from tracking maps under mu_ so that a
    // concurrent /register sees a consistent (empty) state.
    std::shared_ptr<zmq::ZMQClient> client;
    {
        std::unique_lock lock(mu_);
        auto it = subscribers_.find(svc_key);
        if (it != subscribers_.end()) {
            client = it->second;
            subscribers_.erase(it);
            active_configs_.erase(svc_key);
        }
    }

    if (client == nullptr) {
        return;
    }

    // Stop the ZMQ client OUTSIDE mu_ to avoid deadlock:
    // HandleEvent acquires mu_ (read). If the ZMQ event-loop thread is
    // currently inside HandleEvent (or about to enter it), holding mu_
    // while waiting for that thread to exit via client->Stop() -> join
    // would deadlock.
    client->Stop();

    // Remove engine_instance from tenant's instance set
    {
        std::unique_lock tenant_lock(tenant_mutex_);
        auto it = tenant_instance_map_.find(tenant_id);
        if (it != tenant_instance_map_.end()) {
            it->second.erase(instance_id);
        }
    }

    LOG(INFO) << "Successfully unsubscribed from service service_key="
              << svc_key << " instance_id=" << instance_id
              << " tenant_id=" << tenant_id;
}

void EventManager::RegisterHttpHandlers() {
    using coro_http::GET;
    using coro_http::POST;
    auto* server = http_server_.get();

    // ---- /query ---------------------------------------------------------
    server->set_http_handler<POST>("/query", [this](coro_http_request& req,
                                                    coro_http_response& resp) {
        VLOG(1) << "receive req method=POST path=/query";

        Json::Value body;
        if (!ParseJsonBody(req, resp, "query", &body)) {
            return;
        }

        const std::string tenant_id = TenantOrDefault(body);
        const std::string lora_name = OptionalString(body, "lora_name", "");
        const std::string cache_salt = OptionalString(body, "cache_salt", "");
        const std::string model = OptionalString(body, "model", "");
        const int64_t block_size =
            body.isMember("block_size") && body["block_size"].isNumeric()
                ? body["block_size"].asInt64()
                : 0;

        std::vector<int32_t> token_ids;
        if (body.isMember("token_ids") && body["token_ids"].isArray()) {
            for (const auto& t : body["token_ids"]) {
                token_ids.push_back(t.asInt());
            }
        }

        auto make_context = [&](const std::string& instance_id) {
            prefixindex::ModelContext ctx;
            ctx.tenant_id = tenant_id;
            ctx.model_name = model;
            ctx.lora_name = lora_name;
            ctx.block_size = block_size;
            ctx.additional_salt = cache_salt;
            ctx.instance_id = instance_id;
            return ctx;
        };

        // {tenant: {instance: CacheHitResult}}
        Json::Value response_result(Json::objectValue);
        auto add_result = [&](const std::string& instance_id) {
            const auto ctx = make_context(instance_id);
            const auto result = indexer_.CacheHitCompute(ctx, token_ids);
            if (!response_result.isMember(tenant_id)) {
                response_result[tenant_id] = Json::Value(Json::objectValue);
            }
            response_result[tenant_id][instance_id] =
                CacheHitResultToJson(result);
        };

        if (body.isMember("instance_id") && body["instance_id"].isString()) {
            const std::string instance_id = body["instance_id"].asString();
            LOG(INFO) << "search all engine instance for tenant. "
                      << "instance_id=" << instance_id;
            add_result(instance_id);
        } else {
            std::shared_lock tenant_lock(tenant_mutex_);
            auto it = tenant_instance_map_.find(tenant_id);
            if (it != tenant_instance_map_.end()) {
                for (const auto& instance_id : it->second) {
                    add_result(instance_id);
                }
            } else {
                LOG(WARNING) << "current tenant has no engine_instance. "
                             << "tenant_id=" << tenant_id;
            }
        }

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

            // Handle Optional fields' default values
            const std::string tenant_id = TenantOrDefault(body);
            const std::string lora_name = OptionalString(body, "lora_name", "");
            const std::string additional_salt =
                OptionalString(body, "additionalsalt", "");

            common::ServiceConfig svc;
            svc.endpoint = OptionalString(body, "endpoint", "");
            svc.replay_endpoint = OptionalString(body, "replay_endpoint", "");
            svc.type = OptionalString(body, "type", "");
            svc.model_name = OptionalString(body, "modelname", "");
            svc.lora_name = lora_name;
            svc.tenant_id = tenant_id;
            svc.instance_id = OptionalString(body, "instance_id", "");
            svc.block_size =
                body.isMember("block_size") && body["block_size"].isNumeric()
                    ? body["block_size"].asInt64()
                    : 0;
            svc.dp_rank =
                body.isMember("dp_rank") && body["dp_rank"].isNumeric()
                    ? body["dp_rank"].asInt()
                    : 0;
            svc.additional_salt = additional_salt;

            {
                std::unique_lock lock(mu_);
                auto [is_new, err] = SubscribeToService(svc);
                if (!err.empty()) {
                    lock.unlock();
                    LOG(ERROR) << "Dynamic register failed instance_id="
                               << svc.instance_id << " err=" << err;
                    HttpError(resp, status_type::internal_server_error,
                              "Failed to subscribe: " + err);
                    return;
                }
                if (is_new) {
                    services_.push_back(svc);
                    prefixindex::ModelContext model_context;
                    model_context.tenant_id = tenant_id;
                    model_context.model_name = svc.model_name;
                    model_context.lora_name = lora_name;
                    model_context.block_size = svc.block_size;
                    model_context.additional_salt = additional_salt;
                    model_context.instance_id = svc.instance_id;
                    indexer_.AddDpSize(model_context,
                                       static_cast<int64_t>(svc.dp_rank));
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

            // Build target service key from instance_id and tenant_id
            const std::string target_tenant = TenantOrDefault(body);
            const std::string instance_id =
                OptionalString(body, "instance_id", "");
            const int dp_rank =
                body.isMember("dp_rank") && body["dp_rank"].isNumeric()
                    ? body["dp_rank"].asInt()
                    : 0;
            const std::string target_key =
                MakeServiceKey(instance_id, target_tenant, dp_rank);

            // Direct lookup and removal. Hold mu_ only for the existence
            // check; UnsubscribeFromService handles map removal under its
            // own mu_ and calls client->Stop() outside the lock to avoid
            // deadlock with HandleEvent's read lock.
            bool exists;
            {
                std::unique_lock lock(mu_);
                exists = active_configs_.count(target_key) != 0;
            }

            if (!exists) {
                HttpError(resp, status_type::not_found,
                          "service not found: " + target_key);
                return;
            }

            UnsubscribeFromService(instance_id, target_tenant, dp_rank);

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
            for (const auto& ctx : global_view.model_contexts) {
                Json::Value c(Json::objectValue);
                c["model_name"] = ctx.model_name;
                c["lora_name"] = ctx.lora_name;
                c["block_size"] = Json::Value::Int64(ctx.block_size);
                c["additional_salt"] = ctx.additional_salt;
                c["tenant_id"] = ctx.tenant_id;
                c["instance_id"] = ctx.instance_id;
                contexts.append(c);
            }
            out["model_contexts"] = contexts;
            Json::Value hashmaps(Json::arrayValue);
            for (const auto& mapping : global_view.proxy_hash_map) {
                Json::Value m(Json::objectValue);
                for (const auto& [engine_hash, conductor_hash] : mapping) {
                    // map<uint64,uint64> is serialised with decimal-string
                    // keys and plain-number values (JSON wire contract;
                    // uint64 precision is covered by json_uint64_test.cpp).
                    m[std::to_string(engine_hash)] =
                        Json::Value::UInt64(conductor_hash);
                }
                hashmaps.append(m);
            }
            out["hashmap"] = hashmaps;
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
