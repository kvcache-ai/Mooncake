#include "master_http_server.h"

#include <sstream>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include "master_metric_manager.h"
#include "rpc_service.h"

namespace mooncake {

MasterHttpServer::MasterHttpServer(uint16_t port, size_t thread_num)
    : http_server_(thread_num, port) {
    RegisterHandlers();
}

MasterHttpServer::~MasterHttpServer() { Stop(); }

void MasterHttpServer::Start() {
    http_server_.async_start();
    LOG(INFO) << "HTTP server started on port " << http_server_.port();
}

void MasterHttpServer::Stop() { http_server_.stop(); }

void MasterHttpServer::SetService(WrappedMasterService* service) {
    service_.store(service, std::memory_order_release);
}

void MasterHttpServer::ClearService() {
    service_.store(nullptr, std::memory_order_release);
}

void MasterHttpServer::RegisterHandlers() {
    using namespace coro_http;

    // /health -- always returns 200 OK regardless of service_ state
    http_server_.set_http_handler<GET>(
        "/health", [this](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "application/json");
            auto* svc = service_.load(std::memory_order_acquire);
            const char* body = svc
                ? R"({"status":"ok","role":"leader"})"
                : R"({"status":"ok","role":"follower"})";
            resp.set_status_and_content(status_type::ok, body);
        });

    // /metrics -- gated on service_
    http_server_.set_http_handler<GET>(
        "/metrics", [this](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto* svc = service_.load(std::memory_order_acquire);
            if (!svc) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "STANDBY");
                return;
            }
            std::string metrics =
                MasterMetricManager::instance().serialize_metrics();
            resp.set_status_and_content(status_type::ok, std::move(metrics));
        });

    // /metrics/summary -- gated on service_
    http_server_.set_http_handler<GET>(
        "/metrics/summary",
        [this](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto* svc = service_.load(std::memory_order_acquire);
            if (!svc) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "STANDBY");
                return;
            }
            std::string summary =
                MasterMetricManager::instance().get_summary_string();
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    // /query_key -- gated on service_
    http_server_.set_http_handler<GET>(
        "/query_key",
        [this](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto* svc = service_.load(std::memory_order_acquire);
            if (!svc) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "STANDBY");
                return;
            }
            auto key = req.get_query_value("key");
            auto get_result = svc->GetReplicaList(std::string(key));
            if (get_result) {
                std::string ss = "";
                const std::vector<Replica::Descriptor>& replicas =
                    get_result.value().replicas;
                for (size_t i = 0; i < replicas.size(); i++) {
                    if (replicas[i].is_memory_replica()) {
                        auto& memory_descriptors =
                            replicas[i].get_memory_descriptor();
                        std::string tmp = "";
                        struct_json::to_json(
                            memory_descriptors.buffer_descriptor, tmp);
                        ss += tmp;
                        ss += "\n";
                    }
                }
                resp.set_status_and_content(status_type::ok, std::move(ss));
            } else {
                resp.set_status_and_content(status_type::not_found,
                                            toString(get_result.error()));
            }
        });

    // /get_all_keys -- gated on service_
    http_server_.set_http_handler<GET>(
        "/get_all_keys",
        [this](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto* svc = service_.load(std::memory_order_acquire);
            if (!svc) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "STANDBY");
                return;
            }
            auto result = svc->GetAllKeys();
            if (result) {
                std::string ss = "";
                auto keys = result.value();
                for (const auto& key : keys) {
                    ss += key;
                    ss += "\n";
                }
                resp.set_status_and_content(status_type::ok, std::move(ss));
            } else {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to get all keys");
            }
        });

    // /get_all_segments -- gated on service_
    http_server_.set_http_handler<GET>(
        "/get_all_segments",
        [this](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto* svc = service_.load(std::memory_order_acquire);
            if (!svc) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "STANDBY");
                return;
            }
            auto result = svc->GetAllSegments();
            if (result) {
                std::string ss = "";
                auto segments = result.value();
                for (const auto& segment_name : segments) {
                    ss += segment_name;
                    ss += "\n";
                }
                resp.set_status_and_content(status_type::ok, std::move(ss));
            } else {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to get all segments");
            }
        });

    // /query_segment -- gated on service_
    http_server_.set_http_handler<GET>(
        "/query_segment",
        [this](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto* svc = service_.load(std::memory_order_acquire);
            if (!svc) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "STANDBY");
                return;
            }
            auto segment = req.get_query_value("segment");
            auto result = svc->QuerySegments(std::string(segment));
            if (result) {
                std::string ss = "";
                auto [used, capacity] = result.value();
                ss += segment;
                ss += "\n";
                ss += "Used(bytes): ";
                ss += std::to_string(used);
                ss += "\nCapacity(bytes) : ";
                ss += std::to_string(capacity);
                ss += "\n";
                resp.set_status_and_content(status_type::ok, std::move(ss));
            } else {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to query segment");
            }
        });

    // /batch_query_keys -- gated on service_
    http_server_.set_http_handler<GET>(
        "/batch_query_keys",
        [this](coro_http_request& req, coro_http_response& resp) {
            auto* svc = service_.load(std::memory_order_acquire);
            if (!svc) {
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::service_unavailable,
                                            "STANDBY");
                return;
            }
            auto keys_view = req.get_query_value("keys");
            std::vector<std::string> keys;

            if (!keys_view.empty()) {
                std::string keys_str(keys_view);
                std::string key;
                std::istringstream iss(keys_str);
                while (std::getline(iss, key, ',')) {
                    keys.push_back(std::move(key));
                }
            }

            resp.add_header("Content-Type",
                            "application/json; charset=utf-8");

            if (keys.empty()) {
                resp.set_status_and_content(
                    status_type::bad_request,
                    "{\"success\":false,\"error\":\"No keys provided. Use "
                    "?keys=key1,key2,...\"}");
                return;
            }

            auto results = svc->BatchGetReplicaList(keys);
            const size_t n = std::min(keys.size(), results.size());

            std::string ss;
            ss.reserve(n * 512);

            ss += "{\"success\":true,\"data\":{";

            for (size_t i = 0; i < n; ++i) {
                if (i > 0) ss += ",";

                const auto& key = keys[i];
                const auto& r = results[i];

                ss += "\"";
                ss += key;
                ss += "\":";

                if (!r.has_value()) {
                    ss += "{\"ok\":false,\"error\":\"";
                    ss += toString(r.error());
                    ss += "\"}";
                    continue;
                }

                ss += "{\"ok\":true,\"values\":[";
                bool first = true;

                const auto& replicas = r.value().replicas;
                for (const auto& rep : replicas) {
                    if (!rep.is_memory_replica()) continue;

                    auto& mem_desc = rep.get_memory_descriptor();
                    std::string tmp;
                    struct_json::to_json(mem_desc.buffer_descriptor, tmp);
                    if (!first) ss += ",";
                    ss += tmp;
                    first = false;
                }
                ss += "]}";
            }

            ss += "}}";

            if (results.size() != keys.size()) {
                LOG(WARNING)
                    << "BatchGetReplicaList size mismatch: keys="
                    << keys.size() << " results=" << results.size();
            }

            resp.set_status_and_content(status_type::ok, std::move(ss));
        });
}

}  // namespace mooncake
