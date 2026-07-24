#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cerrno>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "client_wrapper.h"
#include "e2e_utils.h"
#include "ha/oplog/p2p_oplog_types.h"
#include "ha/oplog/redis_oplog_store.h"
#include "process_handler.h"
#include "redis_master_view_helper.h"
#include "../redis_test_utils.h"
#include "types.h"

#include <hiredis/hiredis.h>

FLAG_master_path;
FLAG_out_dir;
DEFINE_string(clientctl_path, "./mooncake-store/tests/e2e/clientctl",
              "Path to the clientctl executable");
DEFINE_string(redis_endpoint, "127.0.0.1:6379",
              "Redis endpoint for Redis master failover test");
DEFINE_string(redis_username, "",
              "Redis ACL username for Redis master failover test");
DEFINE_string(redis_password, "",
              "Redis password for Redis master failover test");
DEFINE_string(redis_cluster_id, "redis_chaos_test",
              "Redis cluster ID for Redis master failover test");
DEFINE_int32(redis_master_view_ttl_sec, 5,
             "Redis master view TTL for Redis master failover test");
DEFINE_int32(redis_heartbeat_interval_sec, 2,
             "Redis heartbeat interval for Redis master failover test");

constexpr int kMasterPortBase = 51051;
constexpr int kMasterNum = 3;
constexpr int kClientPortBase = 52051;

namespace mooncake {
namespace testing {

namespace {

std::pair<std::string, int> ParseRedisEndpoint() {
    std::string host = "127.0.0.1";
    int port = 6379;
    auto colon_pos = FLAGS_redis_endpoint.rfind(':');
    if (colon_pos != std::string::npos) {
        host = FLAGS_redis_endpoint.substr(0, colon_pos);
        port = std::stoi(FLAGS_redis_endpoint.substr(colon_pos + 1));
    }
    return {host, port};
}

std::string RedisClusterIdWithSlash() {
    std::string cluster_id = FLAGS_redis_cluster_id;
    if (!cluster_id.empty() && cluster_id.back() != '/') {
        cluster_id += '/';
    }
    return cluster_id;
}

std::string MasterViewKey() {
    return "mooncake:{" + RedisClusterIdWithSlash() + "}master_view";
}

std::string MasterEpochKey() {
    return "mooncake:{" + RedisClusterIdWithSlash() + "}master_epoch";
}

bool DeleteRedisKey(const std::string& key) {
    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    if (!AuthenticateRedisContext(ctx, FLAGS_redis_username,
                                  FLAGS_redis_password)) {
        if (ctx) redisFree(ctx);
        return false;
    }

    redisReply* reply = static_cast<redisReply*>(
        redisCommand(ctx, "DEL %b", key.data(), key.size()));
    bool deleted =
        reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
    if (reply) freeReplyObject(reply);
    redisFree(ctx);
    return deleted;
}

void CleanupRedisKeys() {
    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    if (!AuthenticateRedisContext(ctx, FLAGS_redis_username,
                                  FLAGS_redis_password)) {
        if (ctx) redisFree(ctx);
        return;
    }

    std::vector<std::string> keys = {MasterViewKey(), MasterEpochKey()};
    std::string oplog_pattern =
        "mooncake:{" + FLAGS_redis_cluster_id + "}:oplog*";
    redisReply* keys_reply = static_cast<redisReply*>(redisCommand(
        ctx, "KEYS %b", oplog_pattern.data(), oplog_pattern.size()));
    if (keys_reply && keys_reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < keys_reply->elements; ++i) {
            redisReply* key = keys_reply->element[i];
            if (key && key->type == REDIS_REPLY_STRING) {
                keys.emplace_back(key->str, key->len);
            }
        }
    }
    if (keys_reply) freeReplyObject(keys_reply);

    for (const auto& key : keys) {
        redisReply* reply = static_cast<redisReply*>(
            redisCommand(ctx, "DEL %b", key.data(), key.size()));
        if (reply) freeReplyObject(reply);
    }
    redisFree(ctx);
}

int MasterIndexFromAddress(const std::string& master_address) {
    auto colon_pos = master_address.rfind(':');
    if (colon_pos == std::string::npos) {
        return -1;
    }
    int port = std::stoi(master_address.substr(colon_pos + 1));
    return port - kMasterPortBase;
}

class ClientCtlProcess {
   public:
    ClientCtlProcess(int index, const std::string& out_dir)
        : index_(index), out_dir_(out_dir) {}

    ~ClientCtlProcess() { Stop(); }

    bool Start() {
        if (mkdir(out_dir_.c_str(), 0755) != 0 && errno != EEXIST) {
            LOG(ERROR) << "failed to create output dir: " << out_dir_
                       << ", error=" << strerror(errno);
            return false;
        }
        int stdin_pipe[2] = {-1, -1};
        int stdout_pipe[2] = {-1, -1};
        if (pipe(stdin_pipe) != 0) {
            LOG(ERROR) << "failed to create clientctl pipes: "
                       << strerror(errno);
            return false;
        }
        if (pipe(stdout_pipe) != 0) {
            LOG(ERROR) << "failed to create clientctl pipes: "
                       << strerror(errno);
            close(stdin_pipe[0]);
            close(stdin_pipe[1]);
            return false;
        }

        pid_t pid = fork();
        if (pid == -1) {
            LOG(ERROR) << "failed to fork clientctl: " << strerror(errno);
            close(stdin_pipe[0]);
            close(stdin_pipe[1]);
            close(stdout_pipe[0]);
            close(stdout_pipe[1]);
            return false;
        }

        if (pid == 0) {
            struct sigaction default_action = {};
            default_action.sa_handler = SIG_DFL;
            sigemptyset(&default_action.sa_mask);
            if (sigaction(SIGPIPE, &default_action, nullptr) != 0) {
                _exit(1);
            }

            dup2(stdin_pipe[0], STDIN_FILENO);
            dup2(stdout_pipe[1], STDOUT_FILENO);

            std::string stderr_file =
                out_dir_ + "/clientctl_" + std::to_string(index_) + ".err";
            int stderr_fd =
                open(stderr_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (stderr_fd >= 0) {
                dup2(stderr_fd, STDERR_FILENO);
                close(stderr_fd);
            }

            close(stdin_pipe[0]);
            close(stdin_pipe[1]);
            close(stdout_pipe[0]);
            close(stdout_pipe[1]);

            std::vector<std::string> args = {
                FLAGS_clientctl_path,
                "--master_server_entry=redis://" + FLAGS_redis_endpoint,
                "--redis_cluster_id=" + FLAGS_redis_cluster_id,
                "--redis_username=" + FLAGS_redis_username,
                "--redis_password=" + FLAGS_redis_password,
                "--deployment_mode=P2P",
                "--p2p_local_transfer_mode=memcpy",
                "--engine_meta_url=P2PHANDSHAKE",
                "--protocol=tcp",
                "--device_name="};
            std::vector<char*> argv;
            argv.reserve(args.size() + 1);
            for (auto& arg : args) {
                argv.push_back(arg.data());
            }
            argv.push_back(nullptr);
            execv(FLAGS_clientctl_path.c_str(), argv.data());
            LOG(ERROR) << "clientctl exec failed: " << strerror(errno);
            _exit(1);
        }

        pid_ = pid;
        stdin_fd_ = stdin_pipe[1];
        stdout_fd_ = stdout_pipe[0];
        close(stdin_pipe[0]);
        close(stdout_pipe[1]);
        int flags = fcntl(stdout_fd_, F_GETFL, 0);
        fcntl(stdout_fd_, F_SETFL, flags | O_NONBLOCK);
        return true;
    }

    bool SendAndWait(const std::string& command,
                     const std::string& expected_output,
                     std::chrono::seconds timeout) {
        size_t start_pos = output_.size();
        std::string line = command + "\n";
        if (write(stdin_fd_, line.data(), line.size()) < 0) {
            LOG(ERROR) << "failed to write clientctl command: "
                       << strerror(errno);
            return false;
        }

        auto deadline = std::chrono::steady_clock::now() + timeout;
        char buffer[1024];
        while (std::chrono::steady_clock::now() < deadline) {
            ssize_t n = read(stdout_fd_, buffer, sizeof(buffer));
            if (n > 0) {
                output_.append(buffer, n);
                std::string_view new_output(output_.data() + start_pos,
                                            output_.size() - start_pos);
                if (new_output.find(expected_output) !=
                    std::string_view::npos) {
                    return true;
                }
                if (new_output.find("Failed to ") != std::string_view::npos ||
                    new_output.find("Unknown command") !=
                        std::string_view::npos) {
                    LOG(ERROR)
                        << "clientctl command failed, output=" << new_output;
                    return false;
                }
            } else if (n == 0) {
                LOG(ERROR) << "clientctl exited, output=" << output_;
                return false;
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                LOG(ERROR) << "failed to read clientctl output: "
                           << strerror(errno);
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::string_view new_output(output_.data() + start_pos,
                                    output_.size() - start_pos);
        LOG(ERROR) << "timed out waiting for clientctl output: "
                   << expected_output << ", output=" << new_output;
        return false;
    }

    void Stop() {
        if (stdin_fd_ >= 0) {
            std::string command = "terminate\n";
            (void)write(stdin_fd_, command.data(), command.size());
            close(stdin_fd_);
            stdin_fd_ = -1;
        }
        if (stdout_fd_ >= 0) {
            close(stdout_fd_);
            stdout_fd_ = -1;
        }
        if (pid_ > 0) {
            int status = 0;
            auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < deadline) {
                pid_t ret = waitpid(pid_, &status, WNOHANG);
                if (ret == pid_) {
                    pid_ = 0;
                    return;
                }
                if (ret < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    if (errno != ECHILD) {
                        LOG(ERROR) << "failed to wait for clientctl: "
                                   << strerror(errno);
                    }
                    pid_ = 0;
                    return;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            kill(pid_, SIGKILL);
            waitpid(pid_, &status, 0);
            pid_ = 0;
        }
    }

   private:
    int index_;
    std::string out_dir_;
    pid_t pid_{0};
    int stdin_fd_{-1};
    int stdout_fd_{-1};
    std::string output_;
};

}  // namespace

class RedisChaosTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("RedisChaosTest");
        FLAGS_logtostderr = 1;

        struct sigaction ignore_action = {};
        ignore_action.sa_handler = SIG_IGN;
        sigemptyset(&ignore_action.sa_mask);
        ASSERT_EQ(sigaction(SIGPIPE, &ignore_action, &old_sigpipe_action_), 0)
            << strerror(errno);
    }

    static void TearDownTestSuite() {
        EXPECT_EQ(sigaction(SIGPIPE, &old_sigpipe_action_, nullptr), 0)
            << strerror(errno);
        google::ShutdownGoogleLogging();
    }

    void SetUp() override {
        CleanupRedisKeys();
        master_view_helper_ = std::make_unique<RedisMasterViewHelper>(
            FLAGS_redis_cluster_id, FLAGS_redis_endpoint, FLAGS_redis_password,
            /*db_index=*/0, FLAGS_redis_master_view_ttl_sec,
            FLAGS_redis_heartbeat_interval_sec, FLAGS_redis_username);
        ASSERT_EQ(master_view_helper_->Connect(), ErrorCode::OK);

        MasterRunnerConfig config;
        config.election_backend = "redis";
        config.redis_endpoint = FLAGS_redis_endpoint;
        config.redis_username = FLAGS_redis_username;
        config.redis_password = FLAGS_redis_password;
        config.redis_master_view_ttl_sec = FLAGS_redis_master_view_ttl_sec;
        config.redis_heartbeat_interval_sec =
            FLAGS_redis_heartbeat_interval_sec;
        config.cluster_id = FLAGS_redis_cluster_id;
        config.rpc_address = "127.0.0.1";
        config.deployment_mode = "P2P";
        config.enable_oplog = true;
        config.oplog_store_type = "redis";
        config.oplog_data_dir = FLAGS_redis_endpoint;
        config.max_client_per_key = 0;

        for (int i = 0; i < kMasterNum; ++i) {
            masters_.emplace_back(std::make_unique<MasterProcessHandler>(
                FLAGS_master_path, config, kMasterPortBase + i, i,
                FLAGS_out_dir));
            ASSERT_TRUE(masters_.back()->start());
        }
    }

    void TearDown() override {
        masters_.clear();
        master_view_helper_.reset();
        CleanupRedisKeys();
    }

    bool WaitForLeader(int& leader_index, ViewVersionId& version,
                       std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::string master_address;
            ViewVersionId next_version = 0;
            auto err = master_view_helper_->GetMasterView(master_address,
                                                          next_version);
            if (err == ErrorCode::OK) {
                int index = MasterIndexFromAddress(master_address);
                if (index >= 0 && index < kMasterNum) {
                    leader_index = index;
                    version = next_version;
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    bool WaitForNewLeader(int old_leader_index, ViewVersionId old_version,
                          int& new_leader_index, ViewVersionId& new_version) {
        auto timeout =
            std::chrono::seconds(FLAGS_redis_master_view_ttl_sec * 8);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (WaitForLeader(new_leader_index, new_version,
                              std::chrono::seconds(1)) &&
                new_leader_index != old_leader_index &&
                new_version > old_version) {
                return true;
            }
        }
        return false;
    }

    bool WaitForNewerView(ViewVersionId old_version, int& leader_index,
                          ViewVersionId& version) {
        auto timeout =
            std::chrono::seconds(FLAGS_redis_master_view_ttl_sec * 8);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (WaitForLeader(leader_index, version, std::chrono::seconds(1)) &&
                version > old_version) {
                return true;
            }
        }
        return false;
    }

    void WaitForLeaderServiceReady() {
        std::this_thread::sleep_for(
            std::chrono::seconds(FLAGS_redis_master_view_ttl_sec + 1));
    }

    std::shared_ptr<ClientTestWrapper> CreateRedisClient(
        int index, std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            auto client = ClientTestWrapper::CreateClientWrapper(
                "127.0.0.1:" + std::to_string(kClientPortBase + index),
                "P2PHANDSHAKE", "tcp", "", "redis://" + FLAGS_redis_endpoint,
                /*local_buffer_size=*/1024 * 1024 * 128, FLAGS_redis_cluster_id,
                /*enable_http_server=*/false, /*deployment_mode=*/"P2P",
                /*p2p_local_transfer_mode=*/"memcpy",
                static_cast<uint16_t>(kClientPortBase + index),
                FLAGS_redis_username, FLAGS_redis_password);
            if (client.has_value()) {
                return client.value();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return nullptr;
    }

    bool WaitForClientPutGet(ClientTestWrapper& client,
                             const std::string& key_prefix,
                             const std::string& value,
                             std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        int attempt = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            std::string key = key_prefix + "_" + std::to_string(attempt++);
            if (client.Put(key, value) == ErrorCode::OK) {
                std::string got;
                if (client.Get(key, got) == ErrorCode::OK && got == value) {
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return false;
    }

    bool WaitForClientGet(ClientTestWrapper& client, const std::string& key,
                          const std::string& value,
                          std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::string got;
            if (client.Get(key, got) == ErrorCode::OK && got == value) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return false;
    }

    bool WaitForReplicaOpLog(const std::string& key,
                             std::chrono::seconds timeout) {
        RedisOpLogStore store(FLAGS_redis_cluster_id, FLAGS_redis_endpoint,
                              /*enable_write=*/false,
                              /*poll_interval_ms=*/100, FLAGS_redis_password,
                              FLAGS_redis_username);
        if (store.Init() != ErrorCode::OK) {
            return false;
        }

        uint64_t read_from_seq = 0;
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::vector<OpLogEntry> entries;
            if (store.ReadOpLogSince(read_from_seq, 100, entries) ==
                ErrorCode::OK) {
                for (const auto& entry : entries) {
                    if (entry.sequence_id > read_from_seq) {
                        read_from_seq = entry.sequence_id;
                    }
                    if (entry.op_type == OpType_ADD_REPLICA &&
                        entry.object_key == key) {
                        return true;
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    bool WaitForClientCtlCommand(ClientCtlProcess& client,
                                 const std::string& command,
                                 const std::string& expected_output,
                                 std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (client.SendAndWait(command, expected_output,
                                   std::chrono::seconds(5))) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return false;
    }

    std::vector<std::unique_ptr<MasterProcessHandler>> masters_;
    std::unique_ptr<RedisMasterViewHelper> master_view_helper_;
    inline static struct sigaction old_sigpipe_action_ = {};
};

TEST_F(RedisChaosTest, LeaderKilledFailover) {
    int leader_index = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader_index, version, std::chrono::seconds(10)));
    ASSERT_GE(leader_index, 0);
    ASSERT_LT(leader_index, kMasterNum);

    ASSERT_TRUE(masters_[leader_index]->kill());

    int new_leader_index = -1;
    ViewVersionId new_version = 0;
    ASSERT_TRUE(
        WaitForNewLeader(leader_index, version, new_leader_index, new_version));
    EXPECT_NE(new_leader_index, leader_index);
    EXPECT_GT(new_version, version);
}

TEST_F(RedisChaosTest, LeaderKeyDeletedTriggersReElection) {
    int leader_index = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader_index, version, std::chrono::seconds(10)));
    ASSERT_GE(leader_index, 0);
    ASSERT_LT(leader_index, kMasterNum);

    ASSERT_TRUE(DeleteRedisKey(MasterViewKey()));

    int next_leader_index = -1;
    ViewVersionId next_version = 0;
    ASSERT_TRUE(WaitForNewerView(version, next_leader_index, next_version));
    EXPECT_GE(next_leader_index, 0);
    EXPECT_LT(next_leader_index, kMasterNum);
    EXPECT_GT(next_version, version);
}

TEST_F(RedisChaosTest, ClientRedisDiscoverySurvivesLeaderFailover) {
    int leader_index = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader_index, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();

    ClientCtlProcess client(/*index=*/0, FLAGS_out_dir);
    ASSERT_TRUE(client.Start());
    ASSERT_TRUE(client.SendAndWait(
        "create c0 " + std::to_string(kClientPortBase),
        "Successfully created client: c0", std::chrono::seconds(30)));

    const std::string key = "redis_oplog_before_failover";
    const std::string value = "value_before_failover";
    ASSERT_TRUE(client.SendAndWait("put c0 " + key + " " + value,
                                   "Successfully put value for key: " + key,
                                   std::chrono::seconds(10)));
    ASSERT_TRUE(
        client.SendAndWait("expect_get c0 " + key + " " + value,
                           "Successfully expected value for key: " + key,
                           std::chrono::seconds(10)));
    ASSERT_TRUE(WaitForReplicaOpLog(key, std::chrono::seconds(10)));

    ASSERT_TRUE(masters_[leader_index]->kill());

    int new_leader_index = -1;
    ViewVersionId new_version = 0;
    ASSERT_TRUE(
        WaitForNewLeader(leader_index, version, new_leader_index, new_version));
    WaitForLeaderServiceReady();

    ASSERT_TRUE(
        client.SendAndWait("expect_get c0 " + key + " " + value,
                           "Successfully expected value for key: " + key,
                           std::chrono::seconds(60)));
    const std::string after_key = "redis_failover_after";
    const std::string after_value = "value2";
    ASSERT_TRUE(WaitForClientCtlCommand(
        client, "put c0 " + after_key + " " + after_value,
        "Successfully put value for key: " + after_key,
        std::chrono::seconds(60)));
    ASSERT_TRUE(
        client.SendAndWait("expect_get c0 " + after_key + " " + after_value,
                           "Successfully expected value for key: " + after_key,
                           std::chrono::seconds(30)));
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
