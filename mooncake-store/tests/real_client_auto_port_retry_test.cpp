#include "real_client.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "test_server_helpers.h"
#include "error.h"
#ifdef STORE_USE_ETCD
#include "ha/leadership/leader_coordinator_factory.h"
#endif

namespace mooncake::testing {
namespace {

static_assert(ERR_RPC_META_CONFLICT != ERR_CONTEXT_EINVAL,
              "RPC metadata conflicts must not alias RDMA context failures");

class FailingHttpMetadataServer {
   public:
    ~FailingHttpMetadataServer() {
        stopping_.store(true);
        if (worker_.joinable()) worker_.join();
        if (listener_ >= 0) close(listener_);
    }

    bool Start() {
        listener_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listener_ < 0) return false;

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (bind(listener_, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) != 0 ||
            listen(listener_, 16) != 0) {
            close(listener_);
            listener_ = -1;
            return false;
        }

        socklen_t address_size = sizeof(address);
        if (getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                        &address_size) != 0) {
            close(listener_);
            listener_ = -1;
            return false;
        }
        port_ = ntohs(address.sin_port);
        worker_ = std::thread([this] { Serve(); });
        return true;
    }

    std::string Url() const {
        return "http://127.0.0.1:" + std::to_string(port_) + "/metadata";
    }

    int PutRequests() const { return put_requests_.load(); }

   private:
    void Serve() {
        while (!stopping_.load()) {
            pollfd descriptor{listener_, POLLIN, 0};
            const int ready = poll(&descriptor, 1, 50);
            if (ready <= 0) continue;

            const int client = accept(listener_, nullptr, nullptr);
            if (client < 0) continue;

            std::string request;
            char buffer[4096];
            while (request.find("\r\n\r\n") == std::string::npos) {
                pollfd client_descriptor{client, POLLIN, 0};
                if (poll(&client_descriptor, 1, 1000) <= 0) break;

                const ssize_t received =
                    recv(client, buffer, sizeof(buffer), 0);
                if (received <= 0) break;
                request.append(buffer, received);
            }

            const auto header_end = request.find("\r\n\r\n");
            const auto first_space = request.find(' ');
            if (first_space != std::string::npos &&
                request.compare(0, first_space, "PUT") == 0) {
                put_requests_.fetch_add(1);
            }

            if (header_end != std::string::npos) {
                const std::string response =
                    "HTTP/1.1 500 Internal Server Error\r\n"
                    "Content-Length: 0\r\n"
                    "Connection: close\r\n\r\n";
                (void)send(client, response.data(), response.size(),
                           MSG_NOSIGNAL);
            }
            close(client);
        }
    }

    int listener_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
    std::atomic<int> put_requests_{0};
    std::thread worker_;
};

class RpcMetaConflictHttpMetadataServer {
   public:
    explicit RpcMetaConflictHttpMetadataServer(
        std::chrono::milliseconds conflict_delay = std::chrono::milliseconds{0})
        : conflict_delay_(conflict_delay) {}

    ~RpcMetaConflictHttpMetadataServer() {
        stopping_.store(true);
        if (worker_.joinable()) worker_.join();
        if (listener_ >= 0) close(listener_);
    }

    bool Start() {
        listener_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listener_ < 0) return false;

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (bind(listener_, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) != 0 ||
            listen(listener_, 16) != 0) {
            close(listener_);
            listener_ = -1;
            return false;
        }

        socklen_t address_size = sizeof(address);
        if (getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                        &address_size) != 0) {
            close(listener_);
            listener_ = -1;
            return false;
        }
        port_ = ntohs(address.sin_port);
        worker_ = std::thread([this] { Serve(); });
        return true;
    }

    std::string Url() const {
        return "http://127.0.0.1:" + std::to_string(port_) + "/metadata";
    }

    int ConflictPutRequests() const { return conflict_put_requests_.load(); }
    int DeleteRequests() const { return delete_requests_.load(); }

    size_t DistinctRpcMetaPutTargets() const {
        std::lock_guard lock(mutex_);
        return rpc_meta_put_targets_.size();
    }

   private:
    void Serve() {
        while (!stopping_.load()) {
            pollfd descriptor{listener_, POLLIN, 0};
            const int ready = poll(&descriptor, 1, 50);
            if (ready <= 0) continue;

            const int client = accept(listener_, nullptr, nullptr);
            if (client < 0) continue;

            std::string request;
            char buffer[4096];
            size_t request_size = 0;
            while (true) {
                const auto header_end = request.find("\r\n\r\n");
                if (header_end != std::string::npos) {
                    const auto content_length_pos =
                        request.find("Content-Length:");
                    if (content_length_pos != std::string::npos &&
                        content_length_pos < header_end) {
                        const auto value_start =
                            content_length_pos + std::strlen("Content-Length:");
                        const auto value_end =
                            request.find("\r\n", value_start);
                        request_size = std::stoul(request.substr(
                            value_start, value_end - value_start));
                    }
                    if (request.size() >= header_end + 4 + request_size) break;
                }

                pollfd client_descriptor{client, POLLIN, 0};
                if (poll(&client_descriptor, 1, 1000) <= 0) break;
                const ssize_t received =
                    recv(client, buffer, sizeof(buffer), 0);
                if (received <= 0) break;
                request.append(buffer, received);
            }

            const auto header_end = request.find("\r\n\r\n");
            std::string method;
            std::string target;
            const auto first_space = request.find(' ');
            const auto second_space = request.find(' ', first_space + 1);
            if (first_space != std::string::npos &&
                second_space != std::string::npos) {
                method = request.substr(0, first_space);
                target = request.substr(first_space + 1,
                                        second_space - first_space - 1);
            }

            int status = 200;
            std::string status_text = "OK";
            std::string body;
            {
                std::lock_guard lock(mutex_);
                if (method == "GET" &&
                    target.find("rpc_meta") != std::string::npos) {
                    if (conflict_target_.empty()) conflict_target_ = target;
                    if (target == conflict_target_ && stale_entry_exists_) {
                        body =
                            R"({"ip_or_host_name":"127.0.0.1","rpc_port":1,"metadata_version":1})";
                    } else {
                        status = 404;
                        status_text = "Not Found";
                    }
                } else if (method == "GET") {
                    status = 404;
                    status_text = "Not Found";
                } else if (method == "PUT") {
                    if (target.find("rpc_meta") != std::string::npos) {
                        rpc_meta_put_targets_.insert(target);
                    }
                    if (target == conflict_target_ && stale_entry_exists_) {
                        ++conflict_put_requests_;
                        status = 400;
                        status_text = "Bad Request";
                        body = "Duplicate rpc_meta key not allowed";
                    }
                } else if (method == "DELETE") {
                    ++delete_requests_;
                    if (target == conflict_target_) stale_entry_exists_ = false;
                }
            }

            if (header_end != std::string::npos) {
                if (status == 400) {
                    std::this_thread::sleep_for(conflict_delay_);
                }
                const std::string response =
                    "HTTP/1.1 " + std::to_string(status) + " " + status_text +
                    "\r\nContent-Length: " + std::to_string(body.size()) +
                    "\r\nConnection: close\r\n\r\n" + body;
                (void)send(client, response.data(), response.size(),
                           MSG_NOSIGNAL);
            }
            close(client);
        }
    }

    int listener_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
    std::atomic<int> conflict_put_requests_{0};
    std::atomic<int> delete_requests_{0};
    mutable std::mutex mutex_;
    std::string conflict_target_;
    bool stale_entry_exists_ = true;
    std::set<std::string> rpc_meta_put_targets_;
    std::thread worker_;
    std::chrono::milliseconds conflict_delay_;
};

class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        const char* previous = std::getenv(name);
        if (previous != nullptr) {
            had_previous_ = true;
            previous_ = previous;
        }
        setenv(name, value, 1);
    }

    ~ScopedEnvVar() {
        if (had_previous_) {
            setenv(name_.c_str(), previous_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::string previous_;
    bool had_previous_ = false;
};

TEST(RealClientAutoPortRetryTest,
     MetadataWriteFailureDoesNotRetryWithAnotherStorePort) {
    FailingHttpMetadataServer metadata_server;
    ASSERT_TRUE(metadata_server.Start());

    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));

    ScopedEnvVar retries("MC_STORE_CLIENT_SETUP_RETRIES", "4");

    auto client = RealClient::create();
    const int result =
        client->setup_real("127.0.0.1", metadata_server.Url(), 0, 0, "tcp", "",
                           master.master_address());

    EXPECT_NE(result, 0);
    EXPECT_EQ(metadata_server.PutRequests(), 1);
}

TEST(RealClientAutoPortRetryTest,
     StaleRpcMetaConflictRetriesWithDifferentStoreEndpoint) {
    RpcMetaConflictHttpMetadataServer metadata_server;
    ASSERT_TRUE(metadata_server.Start());
    ScopedEnvVar retries("MC_STORE_CLIENT_SETUP_RETRIES", "20");
    ScopedEnvVar min_port("MC_STORE_CLIENT_MIN_PORT", "12300");
    ScopedEnvVar max_port("MC_STORE_CLIENT_MAX_PORT", "14300");

    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));

    auto client = RealClient::create();
    const int result =
        client->setup_real("127.0.0.1", metadata_server.Url(), 0, 0, "tcp", "",
                           master.master_address());

    EXPECT_EQ(result, 0);
    EXPECT_GE(metadata_server.ConflictPutRequests(), 1);
    EXPECT_GE(metadata_server.DistinctRpcMetaPutTargets(), 2);
    EXPECT_EQ(metadata_server.DeleteRequests(), 0);
}

#ifdef STORE_USE_ETCD
TEST(RealClientAutoPortRetryTest,
     EtcdHaConflictRetryDoesNotWaitForLeaderWatchTimeout) {
    const char* endpoints = std::getenv("MOONCAKE_TEST_ETCD_ENDPOINTS");
    if (!endpoints || !*endpoints) {
        GTEST_SKIP() << "Set MOONCAKE_TEST_ETCD_ENDPOINTS to an isolated etcd";
    }
    const std::string cluster = "auto-port-retry-" + std::to_string(getpid());
    ScopedEnvVar cluster_id("MC_STORE_CLUSTER_ID", cluster.c_str());
    ScopedEnvVar retries("MC_STORE_CLIENT_SETUP_RETRIES", "20");

    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    auto coordinator =
        ha::CreateLeaderCoordinator({.type = ha::HABackendType::ETCD,
                                     .connstring = endpoints,
                                     .cluster_namespace = cluster,
                                     .leader_lease_ttl_sec = 120});
    ASSERT_TRUE(coordinator.has_value());
    auto acquired =
        coordinator.value()->TryAcquireLeadership(master.master_address());
    ASSERT_TRUE(acquired.has_value());
    ASSERT_EQ(acquired->status, ha::AcquireLeadershipStatus::ACQUIRED);
    ASSERT_TRUE(acquired->session.has_value());
    const auto session = *acquired->session;
    ASSERT_EQ(coordinator.value()->PublishServiceReady(session), ErrorCode::OK);

    // Hold the first failed write long enough for the old eager monitor to
    // enter its 30-second watch. The retry must not wait for that watch.
    RpcMetaConflictHttpMetadataServer metadata_server(
        std::chrono::milliseconds{300});
    ASSERT_TRUE(metadata_server.Start());
    auto client = RealClient::create();
    const auto start = std::chrono::steady_clock::now();
    const int result =
        client->setup_real("127.0.0.1", metadata_server.Url(), 0, 0, "tcp", "",
                           "etcd://" + std::string(endpoints));
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(result, 0);
    EXPECT_GE(metadata_server.ConflictPutRequests(), 1);
    EXPECT_GE(metadata_server.DistinctRpcMetaPutTargets(), 2);
    EXPECT_LT(
        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
        10000);

    // Releasing leadership wakes the successful client's watch on teardown.
    std::thread release([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
        EXPECT_EQ(coordinator.value()->ReleaseLeadership(session),
                  ErrorCode::OK);
    });
    client.reset();
    release.join();
}
#endif

}  // namespace
}  // namespace mooncake::testing
