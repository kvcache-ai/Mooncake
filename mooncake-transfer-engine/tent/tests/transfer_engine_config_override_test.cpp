// Copyright 2026 KVCache.AI
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

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"

#ifndef _WIN32
#include <asio.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
#endif

namespace mooncake {
namespace tent {

namespace {

constexpr char kLoopbackHostname[] = "127.0.0.1";
constexpr char kInvalidHostname[] = "256.256.256.256";
constexpr char kSegmentName[] = "store-segment-A";
constexpr char kMetadataKeyPrefix[] = "mooncake/tent/";

class EnvVarGuard {
   public:
    EnvVarGuard(const char* name, const std::string& value) : name_(name) {
        const char* old = std::getenv(name);
        if (old) {
            old_value_ = old;
            had_value_ = true;
        }
        setenv(name, value.c_str(), 1);
    }

    ~EnvVarGuard() {
        if (had_value_) {
            setenv(name_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::string old_value_;
    bool had_value_ = false;
};

class TempConfigFile {
   public:
    explicit TempConfigFile(const std::string& content) {
        auto unique_name =
            "tent-config-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".json";
        path_ = std::filesystem::temp_directory_path() / unique_name;
        std::ofstream ofs(path_);
        ofs << content;
    }

    ~TempConfigFile() {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }

    std::string path() const { return path_.string(); }

   private:
    std::filesystem::path path_;
};

void configureTcpOnlyTransports(Config& config) {
    config.set("transports/tcp/enable", true);
    config.set("transports/shm/enable", false);
    config.set("transports/rdma/enable", false);
    config.set("transports/io_uring/enable", false);
    config.set("transports/nvlink/enable", false);
    config.set("transports/mnnvl/enable", false);
    config.set("transports/gds/enable", false);
    config.set("transports/ascend_direct/enable", false);
}

std::string buildHttpMetadataEndpoint(uint16_t port) {
    return std::string("http://") + kLoopbackHostname + ":" +
           std::to_string(port) + "/metadata";
}

std::string buildMetadataKey(const std::string& segment_name) {
    return std::string(kMetadataKeyPrefix) + segment_name;
}

#ifndef _WIN32
uint16_t reserveUnusedTcpPort() {
    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor(
        io_context,
        asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    return acceptor.local_endpoint().port();
}

class TestHttpMetadataServer {
   public:
    explicit TestHttpMetadataServer(uint16_t port)
        : port_(port),
          server_(std::make_unique<coro_http::coro_http_server>(1, port)) {
        initServer();
    }

    ~TestHttpMetadataServer() { stop(); }

    bool start() {
        if (started_) {
            return running_;
        }

        server_->async_start();
        started_ = true;
        for (int attempt = 0; attempt < 50; ++attempt) {
            if (isReachable()) {
                running_ = true;
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }

        stop();
        return false;
    }

    void stop() {
        if (!started_) {
            return;
        }
        server_->stop();
        started_ = false;
        running_ = false;
    }

    std::optional<std::string> getStoredValue(const std::string& key) const {
        std::lock_guard<std::mutex> lock(store_mutex_);
        auto it = store_.find(key);
        if (it == store_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

   private:
    void initServer() {
        using namespace coro_http;

        server_->set_http_handler<GET>(
            "/health", [](coro_http_request&, coro_http_response& resp) {
                resp.set_status_and_content(status_type::ok, "OK");
            });

        server_->set_http_handler<GET>(
            "/metadata",
            [this](coro_http_request& req, coro_http_response& resp) {
                auto key = req.get_decode_query_value("key");
                if (key.empty()) {
                    resp.set_status_and_content(status_type::bad_request,
                                                "Missing key parameter");
                    return;
                }

                std::lock_guard<std::mutex> lock(store_mutex_);
                auto it = store_.find(key);
                if (it == store_.end()) {
                    resp.set_status_and_content(status_type::not_found,
                                                "metadata not found");
                    return;
                }

                resp.add_header("Content-Type", "application/json");
                resp.set_status_and_content(status_type::ok, it->second);
            });

        server_->set_http_handler<PUT>(
            "/metadata",
            [this](coro_http_request& req, coro_http_response& resp) {
                auto key = req.get_decode_query_value("key");
                if (key.empty()) {
                    resp.set_status_and_content(status_type::bad_request,
                                                "Missing key parameter");
                    return;
                }

                std::lock_guard<std::mutex> lock(store_mutex_);
                store_[key] = std::string(req.get_body());
                resp.set_status_and_content(status_type::ok,
                                            "metadata updated");
            });

        server_->set_http_handler<coro_http::http_method::DEL>(
            "/metadata",
            [this](coro_http_request& req, coro_http_response& resp) {
                auto key = req.get_decode_query_value("key");
                if (key.empty()) {
                    resp.set_status_and_content(status_type::bad_request,
                                                "Missing key parameter");
                    return;
                }

                std::lock_guard<std::mutex> lock(store_mutex_);
                auto it = store_.find(key);
                if (it == store_.end()) {
                    resp.set_status_and_content(status_type::not_found,
                                                "metadata not found");
                    return;
                }

                store_.erase(it);
                resp.set_status_and_content(status_type::ok,
                                            "metadata deleted");
            });
    }

    bool isReachable() const {
        asio::io_context io_context;
        asio::ip::tcp::socket socket(io_context);
        std::error_code ec;
        socket.connect(
            asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), port_),
            ec);
        return !ec;
    }

   private:
    uint16_t port_;
    bool started_ = false;
    bool running_ = false;
    std::unique_ptr<coro_http::coro_http_server> server_;
    mutable std::mutex store_mutex_;
    std::unordered_map<std::string, std::string> store_;
};
#endif

TEST(TransferEngineConfigOverrideTest,
     ExplicitMetadataOverridesDriveSuccessfulHttpInitialization) {
#ifdef _WIN32
    GTEST_SKIP() << "Requires local HTTP metadata server support";
#else
    const auto live_port = reserveUnusedTcpPort();
    auto dead_port = reserveUnusedTcpPort();
    while (dead_port == live_port) {
        dead_port = reserveUnusedTcpPort();
    }
    TestHttpMetadataServer metadata_server(live_port);
    ASSERT_TRUE(metadata_server.start());

    const auto live_endpoint = buildHttpMetadataEndpoint(live_port);
    const auto dead_endpoint = buildHttpMetadataEndpoint(dead_port);
    TempConfigFile conf_file(
        "{\n"
        "  \"metadata_type\": \"p2p\",\n"
        "  \"metadata_servers\": \"" +
        dead_endpoint +
        "\",\n"
        "  \"rpc_server_hostname\": \"" +
        std::string(kLoopbackHostname) +
        "\",\n"
        "  \"rpc_server_port\": 15011,\n"
        "  \"log_level\": \"warning\",\n"
        "  \"merge_requests\": false\n"
        "}");
    EnvVarGuard guard("MC_TENT_CONF", conf_file.path());

    const auto metadata_key = buildMetadataKey(kSegmentName);
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "http");
    config->set("metadata_servers", live_endpoint);
    config->set("local_segment_name", kSegmentName);
    config->set("rpc_server_hostname", kLoopbackHostname);
    config->set("rpc_server_port", "0");
    configureTcpOnlyTransports(*config);

    {
        TransferEngineImpl engine(config);
        ASSERT_TRUE(engine.available());
        EXPECT_EQ(config->get("metadata_type", ""), "http");
        EXPECT_EQ(config->get("metadata_servers", ""), live_endpoint);
        EXPECT_EQ(config->get("local_segment_name", ""), kSegmentName);
        EXPECT_EQ(config->get("rpc_server_hostname", ""), kLoopbackHostname);
        EXPECT_EQ(config->get("rpc_server_port", ""), "0");
        EXPECT_EQ(engine.getSegmentName(), kSegmentName);
        EXPECT_EQ(engine.getRpcServerAddress(), kLoopbackHostname);
        EXPECT_NE(engine.getRpcServerPort(), 0);
        EXPECT_NE(engine.getRpcServerPort(), 15011);

        EXPECT_EQ(config->get("log_level", ""), "warning");
        EXPECT_FALSE(config->get("merge_requests", true));

        auto stored_metadata = metadata_server.getStoredValue(metadata_key);
        ASSERT_TRUE(stored_metadata.has_value());
        auto stored_desc = json::parse(*stored_metadata);
        EXPECT_EQ(stored_desc.at("name").get<std::string>(), kSegmentName);
    }

    EXPECT_FALSE(metadata_server.getStoredValue(metadata_key).has_value());
#endif
}

TEST(TransferEngineConfigOverrideTest,
     MissingExplicitKeysContinueUsingMcTentConfValuesThroughConstructor) {
    TempConfigFile conf_file(R"({
        "metadata_type": "p2p",
        "metadata_servers": "127.0.0.1:2379",
        "rpc_server_hostname": "256.256.256.256",
        "rpc_server_port": 15012
    })");
    EnvVarGuard guard("MC_TENT_CONF", conf_file.path());

    auto config = std::make_shared<Config>();
    config->set("local_segment_name", "store-segment-B");

    TransferEngineImpl engine(config);

    EXPECT_FALSE(engine.available());
    EXPECT_EQ(config->get("metadata_type", ""), "p2p");
    EXPECT_EQ(config->get("metadata_servers", ""), "127.0.0.1:2379");
    EXPECT_EQ(config->get("rpc_server_hostname", ""), kInvalidHostname);
    EXPECT_EQ(config->get("rpc_server_port", 0), 15012);
    EXPECT_EQ(config->get("local_segment_name", ""), "store-segment-B");
    EXPECT_EQ(engine.getRpcServerAddress(), kInvalidHostname);
    EXPECT_EQ(engine.getRpcServerPort(), 15012);
}

TEST(TransferEngineConfigOverrideTest,
     InvalidExplicitStringPortFailsInsteadOfSilentlyOverridingMcTentConfValue) {
    TempConfigFile conf_file(R"({
        "metadata_type": "p2p",
        "metadata_servers": "127.0.0.1:2379",
        "rpc_server_hostname": "127.0.0.1",
        "rpc_server_port": 15013
    })");
    EnvVarGuard guard("MC_TENT_CONF", conf_file.path());

    for (const auto& invalid_port :
         {std::string("70000"), std::string("abc")}) {
        SCOPED_TRACE(invalid_port);
        auto config = std::make_shared<Config>();
        config->set("rpc_server_hostname", kLoopbackHostname);
        config->set("rpc_server_port", invalid_port);

        TransferEngineImpl engine(config);

        EXPECT_FALSE(engine.available());
        EXPECT_EQ(config->get("rpc_server_port", ""), invalid_port);
        EXPECT_EQ(engine.getRpcServerPort(), 0);
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
