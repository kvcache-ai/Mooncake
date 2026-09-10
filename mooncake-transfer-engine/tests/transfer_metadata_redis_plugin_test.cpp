// Copyright 2024 KVCache.AI
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

// Tests for RedisStoragePlugin's connection lifecycle. The plugin is private to
// transfer_metadata_plugin.cpp, so it is exercised through the public
// MetadataStoragePlugin::Create("redis://...") factory.
//
// These tests talk to an in-process fake that speaks just enough RESP to serve
// GET/SET/DEL/AUTH/SELECT. A real redis cannot be scripted to drop a connection
// at an exact moment, which is precisely the condition being tested here.

#include <arpa/inet.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "transfer_metadata_plugin.h"

namespace mooncake {
namespace {

// Binds 127.0.0.1 on an ephemeral port and returns the socket plus the port.
int BindLoopbackSocket(uint16_t &port, uint16_t requested_port = 0) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(requested_port);
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    socklen_t addr_len = sizeof(addr);
    if (getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &addr_len) < 0) {
        close(fd);
        return -1;
    }
    port = ntohs(addr.sin_port);
    return fd;
}

// Reserves an ephemeral port and releases it, so a test can point the plugin at
// an address where nothing is listening.
uint16_t ReserveUnusedPort() {
    uint16_t port = 0;
    int fd = BindLoopbackSocket(port);
    EXPECT_GE(fd, 0);
    if (fd >= 0) close(fd);
    return port;
}

// A minimal RESP server: parses inline command arrays and keeps values in a
// map. Only the subset of commands the plugin issues is implemented.
class FakeRedisServer {
   public:
    explicit FakeRedisServer(uint16_t requested_port = 0) {
        // gtest's ASSERT_* macros cannot be used in a constructor, and a
        // harness that fails to bind cannot produce meaningful results anyway.
        listen_fd_ = BindLoopbackSocket(port_, requested_port);
        CHECK_GE(listen_fd_, 0) << "failed to bind fake redis server";
        CHECK_EQ(listen(listen_fd_, 8), 0);
        worker_running_.store(true);
        worker_ = std::thread([this] { Loop(); });
    }

    ~FakeRedisServer() { Stop(); }

    uint16_t port() const { return port_; }

    std::string endpoint() const {
        return "redis://127.0.0.1:" + std::to_string(port_);
    }

    // Closes every established connection, emulating an idle timeout on the
    // server side or a middlebox reaping the socket. Blocks until the worker
    // thread has actually closed them, so the next plugin call is guaranteed to
    // hit a dead socket.
    void DropConnections() {
        drop_generation_.fetch_add(1);
        uint64_t target = drop_generation_.load();
        while (worker_running_.load() && drop_applied_.load() < target) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void Stop() {
        if (stopped_.exchange(true)) return;
        if (worker_.joinable()) worker_.join();
        if (listen_fd_ >= 0) {
            close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    std::vector<std::string> commands() {
        std::lock_guard<std::mutex> lock(mutex_);
        return commands_;
    }

    // Number of times a client completed a TCP connect.
    int accept_count() const { return accept_count_.load(); }

    void SetValue(const std::string &key, const std::string &value) {
        std::lock_guard<std::mutex> lock(mutex_);
        store_[key] = value;
    }

   private:
    void Loop() {
        std::vector<int> clients;
        std::unordered_map<int, std::string> buffers;

        while (!stopped_.load()) {
            uint64_t generation = drop_generation_.load();
            if (drop_applied_.load() < generation) {
                for (int fd : clients) {
                    close(fd);
                    buffers.erase(fd);
                }
                clients.clear();
                drop_applied_.store(generation);
            }

            std::vector<pollfd> fds;
            fds.push_back(pollfd{listen_fd_, POLLIN, 0});
            for (int fd : clients) fds.push_back(pollfd{fd, POLLIN, 0});

            int ready = poll(fds.data(), static_cast<nfds_t>(fds.size()), 20);
            if (ready < 0) {
                if (errno == EINTR) continue;
                break;
            }

            if (fds[0].revents & POLLIN) {
                int client = accept(listen_fd_, nullptr, nullptr);
                if (client >= 0) {
                    clients.push_back(client);
                    accept_count_.fetch_add(1);
                }
            }

            for (size_t i = 1; i < fds.size(); ++i) {
                if (!(fds[i].revents & (POLLIN | POLLHUP | POLLERR))) continue;
                int fd = fds[i].fd;
                if (!ServeClient(fd, buffers[fd])) {
                    close(fd);
                    buffers.erase(fd);
                    clients.erase(
                        std::remove(clients.begin(), clients.end(), fd),
                        clients.end());
                }
            }
        }

        for (int fd : clients) close(fd);
        worker_running_.store(false);
    }

    // Returns false when the client should be dropped (peer closed or error).
    bool ServeClient(int fd, std::string &buffer) {
        char chunk[4096];
        ssize_t bytes = recv(fd, chunk, sizeof(chunk), 0);
        if (bytes <= 0) return false;
        buffer.append(chunk, bytes);

        std::vector<std::string> argv;
        while (TryParseCommand(buffer, argv)) {
            std::string reply = Dispatch(argv);
            if (!SendAll(fd, reply)) return false;
            argv.clear();
        }
        return true;
    }

    static bool SendAll(int fd, const std::string &data) {
        size_t sent = 0;
        while (sent < data.size()) {
            ssize_t n = send(fd, data.data() + sent, data.size() - sent, 0);
            if (n <= 0) return false;
            sent += static_cast<size_t>(n);
        }
        return true;
    }

    // Consumes one complete RESP array from buffer. Leaves buffer untouched and
    // returns false when the command has not fully arrived yet.
    static bool TryParseCommand(std::string &buffer,
                                std::vector<std::string> &argv) {
        if (buffer.empty() || buffer[0] != '*') return false;

        size_t cursor = 0;
        auto read_line = [&](std::string &line) {
            size_t end = buffer.find("\r\n", cursor);
            if (end == std::string::npos) return false;
            line = buffer.substr(cursor, end - cursor);
            cursor = end + 2;
            return true;
        };

        std::string header;
        if (!read_line(header)) return false;
        int count = std::atoi(header.c_str() + 1);
        if (count <= 0) return false;

        std::vector<std::string> parsed;
        for (int i = 0; i < count; ++i) {
            std::string size_line;
            if (!read_line(size_line)) return false;
            if (size_line.empty() || size_line[0] != '$') return false;
            int length = std::atoi(size_line.c_str() + 1);
            if (length < 0) return false;
            if (buffer.size() < cursor + static_cast<size_t>(length) + 2)
                return false;
            parsed.emplace_back(buffer.substr(cursor, length));
            cursor += static_cast<size_t>(length) + 2;
        }

        buffer.erase(0, cursor);
        argv = std::move(parsed);
        return true;
    }

    std::string Dispatch(const std::vector<std::string> &argv) {
        std::string command = argv[0];
        for (auto &c : command) c = std::toupper(c);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            std::string logged = command;
            for (size_t i = 1; i < argv.size(); ++i) logged += " " + argv[i];
            commands_.push_back(logged);
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (command == "GET") {
            auto it = store_.find(argv[1]);
            if (it == store_.end()) return "$-1\r\n";
            return "$" + std::to_string(it->second.size()) + "\r\n" +
                   it->second + "\r\n";
        }
        if (command == "SET") {
            store_[argv[1]] = argv[2];
            return "+OK\r\n";
        }
        if (command == "DEL") {
            return store_.erase(argv[1]) ? ":1\r\n" : ":0\r\n";
        }
        // AUTH / SELECT and anything else the plugin may send.
        return "+OK\r\n";
    }

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::thread worker_;
    std::atomic<bool> stopped_{false};
    std::atomic<bool> worker_running_{false};
    std::atomic<uint64_t> drop_generation_{0};
    std::atomic<uint64_t> drop_applied_{0};
    std::atomic<int> accept_count_{0};

    std::mutex mutex_;
    std::unordered_map<std::string, std::string> store_;
    std::vector<std::string> commands_;
};

int CountCommands(const std::vector<std::string> &commands,
                  const std::string &prefix) {
    int count = 0;
    for (const auto &command : commands) {
        if (command.rfind(prefix, 0) == 0) ++count;
    }
    return count;
}

class RedisStoragePluginTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // hiredis does not mask SIGPIPE on Linux; writing to a connection the
        // server has closed would otherwise abort the test binary.
        signal(SIGPIPE, SIG_IGN);
        unsetenv("MC_REDIS_USERNAME");
        unsetenv("MC_REDIS_PASSWORD");
        unsetenv("MC_REDIS_DB_INDEX");
    }

    void TearDown() override {
        unsetenv("MC_REDIS_USERNAME");
        unsetenv("MC_REDIS_PASSWORD");
        unsetenv("MC_REDIS_DB_INDEX");
    }
};

TEST_F(RedisStoragePluginTest, SetGetRemoveRoundTrip) {
    FakeRedisServer server;
    auto plugin = MetadataStoragePlugin::Create(server.endpoint());
    ASSERT_NE(plugin, nullptr);

    Json::Value written;
    written["segment"] = "node-1";
    ASSERT_TRUE(plugin->set("mooncake/node-1", written));

    Json::Value read;
    ASSERT_TRUE(plugin->get("mooncake/node-1", read));
    EXPECT_EQ(read["segment"].asString(), "node-1");

    ASSERT_TRUE(plugin->remove("mooncake/node-1"));
    EXPECT_FALSE(plugin->get("mooncake/node-1", read));
}

// The regression this patch fixes: hiredis latches the error into the
// redisContext, so before the fix every call after a dropped connection failed
// for the remaining lifetime of the process.
TEST_F(RedisStoragePluginTest, SetSucceedsAfterServerDropsConnection) {
    FakeRedisServer server;
    auto plugin = MetadataStoragePlugin::Create(server.endpoint());
    ASSERT_NE(plugin, nullptr);

    Json::Value value;
    value["segment"] = "node-1";
    ASSERT_TRUE(plugin->set("mooncake/node-1", value));
    ASSERT_EQ(server.accept_count(), 1);

    server.DropConnections();

    value["segment"] = "node-1-again";
    EXPECT_TRUE(plugin->set("mooncake/node-1", value));
    EXPECT_EQ(server.accept_count(), 2) << "plugin did not reconnect";

    Json::Value read;
    ASSERT_TRUE(plugin->get("mooncake/node-1", read));
    EXPECT_EQ(read["segment"].asString(), "node-1-again");
}

TEST_F(RedisStoragePluginTest, GetSucceedsAfterServerDropsConnection) {
    FakeRedisServer server;
    server.SetValue("mooncake/node-2", "{\"segment\":\"node-2\"}");
    auto plugin = MetadataStoragePlugin::Create(server.endpoint());
    ASSERT_NE(plugin, nullptr);

    Json::Value read;
    ASSERT_TRUE(plugin->get("mooncake/node-2", read));

    server.DropConnections();

    read = Json::Value();
    EXPECT_TRUE(plugin->get("mooncake/node-2", read));
    EXPECT_EQ(read["segment"].asString(), "node-2");
    EXPECT_EQ(server.accept_count(), 2);
}

TEST_F(RedisStoragePluginTest, RemoveSucceedsAfterServerDropsConnection) {
    FakeRedisServer server;
    server.SetValue("mooncake/node-3", "{\"segment\":\"node-3\"}");
    auto plugin = MetadataStoragePlugin::Create(server.endpoint());
    ASSERT_NE(plugin, nullptr);

    Json::Value read;
    ASSERT_TRUE(plugin->get("mooncake/node-3", read));

    server.DropConnections();

    EXPECT_TRUE(plugin->remove("mooncake/node-3"));
    EXPECT_EQ(server.accept_count(), 2);
}

// Before the fix a redis that was down at construction time left client_ null
// forever, permanently disabling metadata publishing for the process.
TEST_F(RedisStoragePluginTest, RecoversWhenRedisIsDownAtConstruction) {
    uint16_t port = ReserveUnusedPort();
    ASSERT_NE(port, 0);

    auto plugin = MetadataStoragePlugin::Create("redis://127.0.0.1:" +
                                                std::to_string(port));
    ASSERT_NE(plugin, nullptr);

    Json::Value value;
    value["segment"] = "node-4";
    ASSERT_FALSE(plugin->set("mooncake/node-4", value));

    FakeRedisServer server(port);
    ASSERT_EQ(server.port(), port);

    EXPECT_TRUE(plugin->set("mooncake/node-4", value));

    Json::Value read;
    ASSERT_TRUE(plugin->get("mooncake/node-4", read));
    EXPECT_EQ(read["segment"].asString(), "node-4");
}

// AUTH/SELECT moved into the connect path, so they must be replayed on every
// reconnect. A reconnected-but-unauthenticated connection would fail on the
// server in production even though the socket is healthy.
TEST_F(RedisStoragePluginTest, AuthAndSelectAreReplayedOnReconnect) {
    setenv("MC_REDIS_USERNAME", "mooncake", 1);
    setenv("MC_REDIS_PASSWORD", "secret", 1);
    setenv("MC_REDIS_DB_INDEX", "3", 1);

    FakeRedisServer server;
    auto plugin = MetadataStoragePlugin::Create(server.endpoint());
    ASSERT_NE(plugin, nullptr);

    Json::Value value;
    value["segment"] = "node-5";
    ASSERT_TRUE(plugin->set("mooncake/node-5", value));

    auto commands = server.commands();
    EXPECT_EQ(CountCommands(commands, "AUTH mooncake secret"), 1);
    EXPECT_EQ(CountCommands(commands, "SELECT 3"), 1);

    server.DropConnections();
    ASSERT_TRUE(plugin->set("mooncake/node-5", value));

    commands = server.commands();
    EXPECT_EQ(CountCommands(commands, "AUTH mooncake secret"), 2);
    EXPECT_EQ(CountCommands(commands, "SELECT 3"), 2);
}

// A permanently dead redis must still fail (and fail fast) rather than hang or
// spin on reconnect attempts.
TEST_F(RedisStoragePluginTest, FailsWhenRedisStaysDown) {
    FakeRedisServer server;
    auto plugin = MetadataStoragePlugin::Create(server.endpoint());
    ASSERT_NE(plugin, nullptr);

    Json::Value value;
    value["segment"] = "node-6";
    ASSERT_TRUE(plugin->set("mooncake/node-6", value));

    server.Stop();

    EXPECT_FALSE(plugin->set("mooncake/node-6", value));

    Json::Value read;
    EXPECT_FALSE(plugin->get("mooncake/node-6", read));
    EXPECT_FALSE(plugin->remove("mooncake/node-6"));
}

}  // namespace
}  // namespace mooncake

int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
