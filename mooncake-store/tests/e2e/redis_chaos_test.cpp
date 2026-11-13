#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/struct_json/json_reader.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "e2e_utils.h"
#include "p2p/ha/ha_status.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/ha/oplog/p2p_standby_snapshot_service.h"
#include "p2p/ha/oplog/redis_oplog_store.h"
#include "p2p/master/p2p_master_client.h"
#include "process_handler.h"
#include "p2p/ha/p2p_master_view.h"
#include "../p2p/redis_test_utils.h"
#include "types.h"

#include <hiredis/hiredis.h>

DEFINE_string(master_path, "./mooncake-store/src/mooncake_master",
              "Path to the P2P master executable");
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
DEFINE_int32(redis_master_view_ttl_sec, 4,
             "Redis master view TTL for Redis master failover test");
DEFINE_int32(redis_heartbeat_interval_sec, 1,
             "Redis heartbeat interval for Redis master failover test");

constexpr int kMasterPortBase = 51051;
constexpr int kMasterNum = 3;
constexpr int kClientPortBase = 52051;
constexpr int kSnapshotPortBase = 53051;

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

std::string MasterViewKey() {
    return "mooncake:{" + FLAGS_redis_cluster_id + "}:master_view";
}

std::string MasterEpochKey() {
    return "mooncake:{" + FLAGS_redis_cluster_id + "}:master_epoch";
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
    keys.push_back("mooncake:{" + FLAGS_redis_cluster_id +
                   "}:masters:heartbeat");
    keys.push_back("mooncake:{" + FLAGS_redis_cluster_id +
                   "}:masters:metadata");
    std::string oplog_pattern =
        "mooncake:" + FLAGS_redis_cluster_id + ":oplog*";
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

bool TcpPortAccepting(const std::string& master_address) {
    auto colon_pos = master_address.rfind(':');
    if (colon_pos == std::string::npos) {
        return false;
    }
    const std::string host = master_address.substr(0, colon_pos);
    const int port = std::stoi(master_address.substr(colon_pos + 1));
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        close(fd);
        return false;
    }
    bool accepting =
        connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    close(fd);
    return accepting;
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
        master_view_ = std::make_unique<P2PRedisMasterView>(
            FLAGS_redis_cluster_id, FLAGS_redis_endpoint, FLAGS_redis_password,
            /*db_index=*/0, FLAGS_redis_master_view_ttl_sec,
            FLAGS_redis_heartbeat_interval_sec, FLAGS_redis_username);
        ASSERT_EQ(master_view_->Connect(), ErrorCode::OK);

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
        config.enable_oplog = true;
        config.oplog_store_type = "redis";
        config.oplog_data_dir = FLAGS_redis_endpoint;
        config.max_client_per_key = 0;
        config.standby_snapshot_service_port_base = kSnapshotPortBase;
        ConfigureMasters(config);

        for (int i = 0; i < kMasterNum; ++i) {
            masters_.emplace_back(std::make_unique<MasterProcessHandler>(
                FLAGS_master_path, config, kMasterPortBase + i, i,
                FLAGS_out_dir));
            ASSERT_TRUE(masters_.back()->start());
        }
    }

    virtual void ConfigureMasters(MasterRunnerConfig& config) {}

    void TearDown() override {
        masters_.clear();
        master_view_.reset();
        CleanupRedisKeys();
    }

    bool WaitForLeader(int& leader_index, ViewVersionId& version,
                       std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::string master_address;
            ViewVersionId next_version = 0;
            auto err =
                master_view_->GetMasterView(master_address, next_version);
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
        // Wait for leader RPC readiness so writes publish replicas to the
        // master.
        auto deadline =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(FLAGS_redis_master_view_ttl_sec * 4);
        while (std::chrono::steady_clock::now() < deadline) {
            std::string master_address;
            ViewVersionId version = 0;
            if (master_view_->GetMasterView(master_address, version) ==
                    ErrorCode::OK &&
                MasterIndexFromAddress(master_address) >= 0 &&
                TcpPortAccepting(master_address)) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        // Fall back to a TTL-based wait if the probe times out.
        std::this_thread::sleep_for(
            std::chrono::seconds(FLAGS_redis_master_view_ttl_sec + 1));
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
                    if (entry.op_type == OpType_PUBLISH_ROUTE &&
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

    bool WaitForMasterLog(int index, const std::string& needle,
                          std::chrono::seconds timeout) {
        return WaitForMasterLogSince(index, needle, std::streampos(0), timeout);
    }

    std::streampos MasterLogEndOffset(int index) {
        const std::string path =
            FLAGS_out_dir + "/master_" + std::to_string(index) + ".err";
        std::ifstream input(path, std::ios::binary | std::ios::ate);
        if (!input) {
            return std::streampos(0);
        }
        return input.tellg();
    }

    bool WaitForMasterLogSince(int index, const std::string& needle,
                               std::streampos offset,
                               std::chrono::seconds timeout) {
        const std::string path =
            FLAGS_out_dir + "/master_" + std::to_string(index) + ".err";
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::ifstream input(path, std::ios::binary);
            if (!input) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                continue;
            }
            input.seekg(offset);
            std::string content((std::istreambuf_iterator<char>(input)),
                                std::istreambuf_iterator<char>());
            if (content.find(needle) != std::string::npos) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    bool WaitForMasterLogOrderSince(int index, const std::string& first,
                                    const std::string& second,
                                    std::streampos offset,
                                    std::chrono::seconds timeout) {
        const std::string path =
            FLAGS_out_dir + "/master_" + std::to_string(index) + ".err";
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::ifstream input(path, std::ios::binary);
            if (!input) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                continue;
            }
            input.seekg(offset);
            std::string content((std::istreambuf_iterator<char>(input)),
                                std::istreambuf_iterator<char>());
            const auto first_pos = content.find(first);
            const auto second_pos = content.find(second);
            if (second_pos != std::string::npos &&
                (first_pos == std::string::npos || second_pos < first_pos)) {
                return false;
            }
            if (first_pos != std::string::npos &&
                second_pos != std::string::npos) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    bool WaitForSnapshotObject(int source_index, const std::string& key,
                               std::chrono::seconds timeout) {
        const std::string endpoint =
            "127.0.0.1:" + std::to_string(kSnapshotPortBase + source_index);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            P2PStandbyMetadataStore metadata;
            P2PStandbySnapshotClient snapshot_client;
            uint64_t baseline_sequence_id = 0;
            if (snapshot_client.Bootstrap(endpoint, FLAGS_redis_cluster_id,
                                          &metadata, baseline_sequence_id,
                                          /*chunk_size=*/256) ==
                    ErrorCode::OK &&
                metadata.RouteExists(key)) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    bool WaitForSnapshotBaseline(int source_index, uint64_t expected_sequence,
                                 std::chrono::seconds timeout) {
        const std::string endpoint =
            "127.0.0.1:" + std::to_string(kSnapshotPortBase + source_index);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            P2PStandbyMetadataStore metadata;
            P2PStandbySnapshotClient snapshot_client;
            uint64_t baseline_sequence_id = 0;
            if (snapshot_client.Bootstrap(endpoint, FLAGS_redis_cluster_id,
                                          &metadata, baseline_sequence_id,
                                          /*chunk_size=*/256) ==
                    ErrorCode::OK &&
                baseline_sequence_id >= expected_sequence) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    std::vector<std::unique_ptr<MasterProcessHandler>> masters_;
    std::unique_ptr<P2PRedisMasterView> master_view_;
    inline static struct sigaction old_sigpipe_action_ = {};
};

class RedisGracefulShutdownTest : public RedisChaosTest {
   protected:
    void SetUp() override {
        // Longer than shutdown timeout so lease expiry cannot make it pass.
        FLAGS_redis_master_view_ttl_sec = 10;
        RedisChaosTest::SetUp();
    }

    void ConfigureMasters(MasterRunnerConfig& config) override {
        config.metrics_port_base = 54051;
        config.heartbeat_rpc_port_base = 55051;
    }

    bool WaitForSnapshotReadiness(int index, const std::string& role,
                                  bool ready, std::chrono::seconds timeout) {
        RedisMasterRegistry registry(FLAGS_redis_cluster_id,
                                     FLAGS_redis_endpoint, FLAGS_redis_username,
                                     FLAGS_redis_password, 0);
        const std::string endpoint =
            "127.0.0.1:" + std::to_string(kMasterPortBase + index);
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            coro_http::coro_http_client client;
            auto response =
                client.get("http://127.0.0.1:" + std::to_string(54051 + index) +
                           "/ha/status");
            if (response.status == 200) {
                HAStatusSnapshot status;
                struct_json::from_json(status, response.resp_body);
                if (status.role == role && status.snapshot_ready == ready &&
                    status.state ==
                        (role == "primary" ? "RUNNING" : "WATCHING")) {
                    std::vector<RedisMasterRegistryEntry> entries;
                    if (registry.DiscoverAlive(std::chrono::seconds(10),
                                               entries) == ErrorCode::OK) {
                        for (const auto& entry : entries) {
                            // Registry readiness counts standby snapshot
                            // sources; primary fallback availability is
                            // reported in HTTP.
                            if (entry.master_endpoint == endpoint &&
                                entry.role == role &&
                                entry.snapshot_ready ==
                                    (role == "standby" && ready)) {
                                return true;
                            }
                        }
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    std::string RegistryInstance(int index) {
        RedisMasterRegistry registry(FLAGS_redis_cluster_id,
                                     FLAGS_redis_endpoint, FLAGS_redis_username,
                                     FLAGS_redis_password, 0);
        std::vector<RedisMasterRegistryEntry> entries;
        EXPECT_EQ(registry.DiscoverAlive(std::chrono::seconds(10), entries),
                  ErrorCode::OK);
        const std::string endpoint =
            "127.0.0.1:" + std::to_string(kMasterPortBase + index);
        for (const auto& entry : entries) {
            if (entry.master_endpoint == endpoint) {
                return entry.instance_id;
            }
        }
        return {};
    }

    void ExpectRegistryRemoved(const std::string& instance_id) {
        auto [host, port] = ParseRedisEndpoint();
        auto ctx = std::unique_ptr<redisContext, decltype(&redisFree)>(
            redisConnect(host.c_str(), port), redisFree);
        ASSERT_TRUE(AuthenticateRedisContext(ctx.get(), FLAGS_redis_username,
                                             FLAGS_redis_password));
        const std::string prefix =
            "mooncake:{" + FLAGS_redis_cluster_id + "}:masters:";
        const std::string metadata_key = prefix + "metadata";
        const std::string heartbeat_key = prefix + "heartbeat";
        RedisReplyPtr metadata((redisReply*)redisCommand(
            ctx.get(), "HEXISTS %b %b", metadata_key.data(),
            metadata_key.size(), instance_id.data(), instance_id.size()));
        ASSERT_TRUE(metadata && metadata->type == REDIS_REPLY_INTEGER);
        EXPECT_EQ(metadata->integer, 0);
        RedisReplyPtr heartbeat((redisReply*)redisCommand(
            ctx.get(), "ZSCORE %b %b", heartbeat_key.data(),
            heartbeat_key.size(), instance_id.data(), instance_id.size()));
        ASSERT_TRUE(heartbeat);
        EXPECT_EQ(heartbeat->type, REDIS_REPLY_NIL);
        const auto epoch_key = MasterEpochKey();
        RedisReplyPtr epoch((redisReply*)redisCommand(
            ctx.get(), "EXISTS %b", epoch_key.data(), epoch_key.size()));
        ASSERT_TRUE(epoch && epoch->type == REDIS_REPLY_INTEGER);
        EXPECT_EQ(epoch->integer, 1);
    }

    void ExpectPromptSuccessor(int old_leader, ViewVersionId old_version,
                               std::chrono::steady_clock::time_point start) {
        const auto deadline = start + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            int next_leader = -1;
            ViewVersionId next_version = 0;
            if (WaitForLeader(next_leader, next_version,
                              std::chrono::seconds(1)) &&
                next_leader != old_leader && next_version > old_version) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "No successor within 5s after SIGTERM (lease TTL is 10s)";
    }

    gflags::FlagSaver saved_flags_;
};

TEST_F(RedisGracefulShutdownTest, PrimarySigtermReleasesLeaseAndRegistry) {
    int leader = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();
    ASSERT_TRUE(TcpPortAccepting("127.0.0.1:" +
                                 std::to_string(kMasterPortBase + leader)));
    ASSERT_TRUE(
        TcpPortAccepting("127.0.0.1:" + std::to_string(55051 + leader)));
    const auto instance_id = RegistryInstance(leader);
    ASSERT_FALSE(instance_id.empty());

    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(masters_[leader]->terminate(std::chrono::seconds(60)));
    ExpectPromptSuccessor(leader, version, start);
    ExpectRegistryRemoved(instance_id);
    EXPECT_FALSE(TcpPortAccepting("127.0.0.1:" +
                                  std::to_string(kMasterPortBase + leader)));
    EXPECT_FALSE(
        TcpPortAccepting("127.0.0.1:" + std::to_string(55051 + leader)));
    EXPECT_TRUE(
        WaitForMasterLog(leader, "released=1", std::chrono::seconds(1)));
    EXPECT_TRUE(WaitForMasterLog(leader, "HA shutdown completed",
                                 std::chrono::seconds(1)));
}

TEST_F(RedisGracefulShutdownTest,
       SigtermDrainsAcknowledgedRouteBeforeTakeover) {
    int leader = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();
    const auto client_id = generate_uuid();
    P2PMasterClient client(client_id);
    client.SetHeartbeatRpcPort(55051 + leader);
    ASSERT_EQ(
        client.Connect("127.0.0.1:" + std::to_string(kMasterPortBase + leader)),
        ErrorCode::OK);
    P2PSegment segment;
    segment.id = generate_uuid();
    segment.name = "sigterm-drain";
    segment.size = 4096;
    segment.memory_type = MemoryType::DRAM;
    ASSERT_TRUE(client
                    .RegisterClient({.client_id = client_id,
                                     .segments = {segment},
                                     .ip_address = "127.0.0.1",
                                     .rpc_port = kClientPortBase})
                    .has_value());
    ASSERT_TRUE(client
                    .Heartbeat({.client_id = client_id,
                                .service_state = P2PClientServiceState::ONLINE})
                    .has_value());

    auto [host, port] = ParseRedisEndpoint();
    auto ctx = std::unique_ptr<redisContext, decltype(&redisFree)>(
        redisConnect(host.c_str(), port), redisFree);
    ASSERT_TRUE(AuthenticateRedisContext(ctx.get(), FLAGS_redis_username,
                                         FLAGS_redis_password));
    const auto log_offset = MasterLogEndOffset(leader);
    // Redis 6 also supports PAUSE (all commands). Keep this below both the
    // 3s command/drain timeout and the 10s lease. RPC and cached HTTP status
    // remain live, so an acknowledgement with pending writes proves that the
    // request has returned before persistence, without client-side republish.
    RedisReplyPtr paused(
        (redisReply*)redisCommand(ctx.get(), "CLIENT PAUSE 2000"));
    ASSERT_TRUE(paused && paused->type == REDIS_REPLY_STATUS);
    const std::string key = "acknowledged_before_sigterm";
    ASSERT_TRUE(client
                    .PublishRoute({.key = key,
                                   .object_size = 128,
                                   .client_id = client_id,
                                   .segment_id = segment.id})
                    .has_value());
    coro_http::coro_http_client http;
    auto response = http.get(
        "http://127.0.0.1:" + std::to_string(54051 + leader) + "/ha/status");
    ASSERT_EQ(response.status, 200);
    HAStatusSnapshot status;
    struct_json::from_json(status, response.resp_body);
    ASSERT_GT(status.oplog_writer_pending_writes, 0u);

    ASSERT_TRUE(masters_[leader]->terminate(std::chrono::seconds(8)));
    EXPECT_TRUE(WaitForMasterLogOrderSince(
        leader, "P2P primary shutdown OpLog drain completed", "released=1",
        log_offset, std::chrono::seconds(1)));
    ASSERT_TRUE(WaitForReplicaOpLog(key, std::chrono::seconds(2)));
    int next_leader = -1;
    ViewVersionId next_version = 0;
    ASSERT_TRUE(WaitForNewLeader(leader, version, next_leader, next_version));
    WaitForLeaderServiceReady();
    client.SetHeartbeatRpcPort(55051 + next_leader);
    ASSERT_EQ(client.Connect("127.0.0.1:" +
                             std::to_string(kMasterPortBase + next_leader)),
              ErrorCode::OK);
    // Query the recovered metadata directly: no re-registration or publish.
    const auto owners = client.GetKeyClients(key);
    ASSERT_TRUE(owners.has_value());
    ASSERT_EQ(owners->size(), 1u);
    EXPECT_EQ(owners->front().client_id, client_id);
}

TEST_F(RedisGracefulShutdownTest, PrimaryStepDownReleasesLeaseBeforeTTL) {
    int leader = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();
    ASSERT_TRUE(TcpPortAccepting("127.0.0.1:" +
                                 std::to_string(kMasterPortBase + leader)));

    const auto log_offset = MasterLogEndOffset(leader);
    auto [host, port] = ParseRedisEndpoint();
    auto ctx = std::unique_ptr<redisContext, decltype(&redisFree)>(
        redisConnect(host.c_str(), port), redisFree);
    ASSERT_TRUE(AuthenticateRedisContext(ctx.get(), FLAGS_redis_username,
                                         FLAGS_redis_password));
    const auto key = MasterViewKey();
    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + std::chrono::seconds(5);
    RedisReplyPtr ttl((redisReply*)redisCommand(ctx.get(), "PTTL %b",
                                                key.data(), key.size()));
    ASSERT_TRUE(ttl && ttl->type == REDIS_REPLY_INTEGER);
    ASSERT_GT(ttl->integer, 5000) << "Lease expiry must not satisfy this test";

    coro_http::coro_http_client client;
    auto response =
        client.post("http://127.0.0.1:" + std::to_string(54051 + leader) +
                        "/ha/stepdown?cooldown_sec=0",
                    "", coro_http::req_content_type::octet_stream);
    ASSERT_EQ(response.status, 200) << response.resp_body;
    ASSERT_NE(response.resp_body.find("\"success\":true"),
              std::string_view::npos);

    bool newer_view = false;
    while (std::chrono::steady_clock::now() < deadline) {
        std::string next_address;
        ViewVersionId next_version = 0;
        // The same node may win again: this verifies release, not cooldown.
        if (master_view_->GetMasterView(next_address, next_version) ==
                ErrorCode::OK &&
            next_version > version &&
            std::chrono::steady_clock::now() < deadline) {
            newer_view = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(newer_view) << "No new leader term within 5s after stepdown";
    EXPECT_TRUE(WaitForMasterLogOrderSince(
        leader, "P2P primary stepdown OpLog drain completed", "released=1",
        log_offset, std::chrono::seconds(1)));
}

TEST_F(RedisGracefulShutdownTest, PrimaryStepDownPausesReelection) {
    int leader = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();
    for (int i = 0; i < kMasterNum; ++i) {
        ASSERT_TRUE(
            WaitForSnapshotReadiness(i, i == leader ? "primary" : "standby",
                                     true, std::chrono::seconds(3)))
            << i;
    }
    const auto log_offset = MasterLogEndOffset(leader);
    const auto start = std::chrono::steady_clock::now();
    const std::string url =
        "http://127.0.0.1:" + std::to_string(54051 + leader);
    coro_http::coro_http_client client;
    auto response =
        client.post(url + "/ha/stepdown?cooldown_sec=5&drain_timeout_ms=1234",
                    "", coro_http::req_content_type::octet_stream);
    ASSERT_EQ(response.status, 200) << response.resp_body;
    ASSERT_NE(response.resp_body.find("\"cooldown_sec\":5"),
              std::string_view::npos);
    ASSERT_NE(response.resp_body.find("\"drain_timeout_ms\":1234"),
              std::string_view::npos);
    ASSERT_TRUE(WaitForMasterLogSince(leader,
                                      "HA election paused after stepdown",
                                      log_offset, std::chrono::seconds(3)));
    ExpectPromptSuccessor(leader, version, start);
    EXPECT_TRUE(WaitForMasterLogSince(
        leader, "OpLog drain completed, drain_timeout_ms=1234", log_offset,
        std::chrono::seconds(1)));
    ASSERT_TRUE(WaitForSnapshotReadiness(leader, "standby", true,
                                         std::chrono::seconds(3)));

    // Standby diagnostics remain reachable while election is paused.
    coro_http::coro_http_client status_client;
    auto status = status_client.get(url + "/ha/status");
    EXPECT_EQ(status.status, 200) << status.resp_body;
    EXPECT_FALSE(TcpPortAccepting("127.0.0.1:" +
                                  std::to_string(kMasterPortBase + leader)));
    EXPECT_TRUE(WaitForMasterLogOrderSince(
        leader, "HA election pause completed", "Trying to elect self as leader",
        log_offset, std::chrono::seconds(7)));
    EXPECT_GE(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(5));
}

TEST_F(RedisGracefulShutdownTest, StepdownCooldownDoesNotBlockShutdown) {
    int leader = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();
    const auto log_offset = MasterLogEndOffset(leader);
    coro_http::coro_http_client client;
    auto response = client.post(
        "http://127.0.0.1:" + std::to_string(54051 + leader) + "/ha/stepdown",
        "", coro_http::req_content_type::octet_stream);
    ASSERT_EQ(response.status, 200) << response.resp_body;
    ASSERT_NE(response.resp_body.find("\"cooldown_sec\":30"),
              std::string_view::npos);
    ASSERT_TRUE(WaitForMasterLogSince(leader,
                                      "HA election paused after stepdown",
                                      log_offset, std::chrono::seconds(3)));
    ASSERT_TRUE(masters_[leader]->terminate(std::chrono::seconds(3)));
    EXPECT_TRUE(WaitForMasterLogSince(
        leader, "HA shutdown requested during election pause", log_offset,
        std::chrono::seconds(1)));
}

class RedisSnapshotDisabledTest : public RedisGracefulShutdownTest {
   protected:
    void ConfigureMasters(MasterRunnerConfig& config) override {
        RedisGracefulShutdownTest::ConfigureMasters(config);
        config.standby_snapshot_service_port_base = 0;
    }
};

TEST_F(RedisSnapshotDisabledTest, ReadinessStaysFalseAcrossStepdown) {
    int leader = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();
    for (int i = 0; i < kMasterNum; ++i) {
        ASSERT_TRUE(
            WaitForSnapshotReadiness(i, i == leader ? "primary" : "standby",
                                     false, std::chrono::seconds(3)))
            << i;
    }
    const auto start = std::chrono::steady_clock::now();
    coro_http::coro_http_client client;
    auto response =
        client.post("http://127.0.0.1:" + std::to_string(54051 + leader) +
                        "/ha/stepdown?cooldown_sec=30",
                    "", coro_http::req_content_type::octet_stream);
    ASSERT_EQ(response.status, 200) << response.resp_body;
    ExpectPromptSuccessor(leader, version, start);
    ASSERT_TRUE(WaitForSnapshotReadiness(leader, "standby", false,
                                         std::chrono::seconds(3)));
    int successor = -1;
    ViewVersionId next_version = 0;
    ASSERT_TRUE(
        WaitForLeader(successor, next_version, std::chrono::seconds(3)));
    ASSERT_NE(successor, leader);
    ASSERT_TRUE(WaitForSnapshotReadiness(successor, "primary", false,
                                         std::chrono::seconds(15)));
}

TEST_F(RedisGracefulShutdownTest, StandbySigtermPreservesLeader) {
    int leader = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader, version, std::chrono::seconds(10)));
    const int standby = (leader + 1) % kMasterNum;
    ASSERT_TRUE(WaitForMasterLog(standby, "Trying to elect self as leader",
                                 std::chrono::seconds(10)));
    const auto instance_id = RegistryInstance(standby);
    ASSERT_FALSE(instance_id.empty());

    ASSERT_TRUE(masters_[standby]->terminate(std::chrono::seconds(5)));
    ExpectRegistryRemoved(instance_id);
    int remaining_leader = -1;
    ViewVersionId remaining_version = 0;
    ASSERT_TRUE(WaitForLeader(remaining_leader, remaining_version,
                              std::chrono::seconds(1)));
    EXPECT_EQ(remaining_leader, leader);
    EXPECT_EQ(remaining_version, version);
}

TEST_F(RedisGracefulShutdownTest, SigtermBeforePromotionReleasesLease) {
    int leader = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader, version, std::chrono::seconds(10)));
    const auto instance_id = RegistryInstance(leader);
    ASSERT_FALSE(instance_id.empty());
    ASSERT_FALSE(TcpPortAccepting("127.0.0.1:" +
                                  std::to_string(kMasterPortBase + leader)));

    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(masters_[leader]->terminate(std::chrono::seconds(5)));
    ExpectRegistryRemoved(instance_id);
    ExpectPromptSuccessor(leader, version, start);
}

class RedisOpLogStopTest : public RedisGracefulShutdownTest {
   protected:
    void ConfigureMasters(MasterRunnerConfig& config) override {
        RedisGracefulShutdownTest::ConfigureMasters(config);
        // clientctl follows the elected RPC address; keep heartbeats on it.
        // The graceful-shutdown tests above cover dedicated heartbeat ports.
        config.heartbeat_rpc_port_base = 0;
    }

    bool StartWriterClient(ClientCtlProcess& client) {
        return client.Start() &&
               client.SendAndWait(
                   "create c_writes " + std::to_string(kClientPortBase + 30),
                   "Successfully created client: c_writes",
                   std::chrono::seconds(30));
    }

    std::jthread StartWrites(ClientCtlProcess& client, std::string prefix,
                             std::atomic<size_t>& last_written) {
        return std::jthread([&client, prefix = std::move(prefix),
                             &last_written](std::stop_token stop) {
            for (size_t n = 1; !stop.stop_requested(); ++n) {
                const auto key = prefix + std::to_string(n);
                // Role changes can fail a command; SendAndWait logs details.
                if (client.SendAndWait("put c_writes " + key + " value",
                                       "Successfully put value for key: " + key,
                                       std::chrono::seconds(2))) {
                    last_written.store(n);
                }
                // Also bound retries if clientctl exits or rejects immediately.
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }

    std::string WaitForCommittedWrite(const std::string& prefix,
                                      const std::atomic<size_t>& last_written,
                                      size_t after) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline) {
            const auto n = last_written.load();
            if (n > after) {
                const auto key = prefix + std::to_string(n);
                if (WaitForReplicaOpLog(key, std::chrono::seconds(2))) {
                    return key;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return {};
    }
};

TEST_F(RedisOpLogStopTest, SigtermDuringContinuousWrites) {
    int leader = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();
    const auto instance_id = RegistryInstance(leader);
    ASSERT_FALSE(instance_id.empty());

    ClientCtlProcess client(/*index=*/30, FLAGS_out_dir);
    ASSERT_TRUE(StartWriterClient(client));
    std::atomic<size_t> last_written{0};
    const std::string prefix = "sigterm_writes_";
    // Stop/join before client and last_written are destroyed, including ASSERT.
    auto writes = StartWrites(client, prefix, last_written);
    ASSERT_FALSE(WaitForCommittedWrite(prefix, last_written, 2).empty());

    const auto log_offset = MasterLogEndOffset(leader);
    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(masters_[leader]->terminate(std::chrono::seconds(5)));
    ExpectPromptSuccessor(leader, version, start);
    ASSERT_FALSE(HasFatalFailure());
    ExpectRegistryRemoved(instance_id);
    EXPECT_FALSE(TcpPortAccepting("127.0.0.1:" +
                                  std::to_string(kMasterPortBase + leader)));
    EXPECT_TRUE(WaitForMasterLogOrderSince(
        leader, "P2P primary shutdown OpLog drain completed", "released=1",
        log_offset, std::chrono::seconds(1)));
    EXPECT_TRUE(WaitForMasterLogSince(leader, "HA shutdown completed",
                                      log_offset, std::chrono::seconds(1)));
    WaitForLeaderServiceReady();
    // Local/degraded put success is insufficient: require a newer committed
    // PUBLISH_ROUTE after the successor is ready, using the same running
    // client.
    ASSERT_FALSE(
        WaitForCommittedWrite(prefix, last_written, last_written.load())
            .empty());
}

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

TEST_F(RedisChaosTest, RestartedPrimaryWaitsForStandbyBeforeElection) {
    int leader_index = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader_index, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();

    ASSERT_TRUE(masters_[leader_index]->kill());
    const auto restart_log_offset = MasterLogEndOffset(leader_index);
    ASSERT_TRUE(masters_[leader_index]->start());

    EXPECT_TRUE(WaitForMasterLogOrderSince(
        leader_index, "Standby state transition: SYNCING -> WATCHING",
        "Trying to elect self as leader", restart_log_offset,
        std::chrono::seconds(40)));

    int next_leader_index = -1;
    ViewVersionId next_version = 0;
    ASSERT_TRUE(WaitForNewerView(version, next_leader_index, next_version));
    EXPECT_GT(next_version, version);
}

TEST_F(RedisChaosTest, TrimmedStandbyAutoDiscoversPeerAndBootstraps) {
    int leader_index = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader_index, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();

    const int lagging_index = (leader_index + 1) % kMasterNum;
    const int source_index = (leader_index + 2) % kMasterNum;
    ASSERT_TRUE(WaitForMasterLog(source_index, "Redis Master registered",
                                 std::chrono::seconds(10)));
    ASSERT_TRUE(masters_[lagging_index]->kill());

    ClientCtlProcess client(/*index=*/20, FLAGS_out_dir);
    ASSERT_TRUE(client.Start());
    ASSERT_TRUE(client.SendAndWait(
        "create c_trim " + std::to_string(kClientPortBase + 20),
        "Successfully created client: c_trim", std::chrono::seconds(30)));
    const std::string key = "trim_rebootstrap_key";
    const std::string value = "trim_rebootstrap_value";
    ASSERT_TRUE(client.SendAndWait("put c_trim " + key + " " + value,
                                   "Successfully put value for key: " + key,
                                   std::chrono::seconds(10)));
    ASSERT_TRUE(WaitForReplicaOpLog(key, std::chrono::seconds(10)));

    RedisOpLogStore store(FLAGS_redis_cluster_id, FLAGS_redis_endpoint,
                          /*enable_write=*/false, /*poll_interval_ms=*/100,
                          FLAGS_redis_password, FLAGS_redis_username);
    ASSERT_EQ(store.Init(), ErrorCode::OK);
    uint64_t latest_sequence_id = 0;
    ASSERT_EQ(store.GetLatestSequenceId(latest_sequence_id), ErrorCode::OK);
    ASSERT_GT(latest_sequence_id, 0);
    ASSERT_TRUE(WaitForSnapshotBaseline(source_index, latest_sequence_id,
                                        std::chrono::seconds(10)));
    ASSERT_EQ(store.CleanupOpLogBefore(latest_sequence_id + 1), ErrorCode::OK);

    auto lagging_log_offset = MasterLogEndOffset(lagging_index);
    ASSERT_TRUE(masters_[lagging_index]->start());
    ASSERT_TRUE(WaitForMasterLogSince(
        lagging_index, "Standby snapshot: bootstrap completed",
        lagging_log_offset, std::chrono::seconds(20)));

    ASSERT_TRUE(masters_[source_index]->kill());
    ASSERT_TRUE(masters_[leader_index]->kill());
    int new_leader_index = -1;
    ViewVersionId new_version = 0;
    ASSERT_TRUE(
        WaitForNewLeader(leader_index, version, new_leader_index, new_version));
    ASSERT_EQ(new_leader_index, lagging_index);
    ASSERT_TRUE(WaitForClientCtlCommand(
        client, "expect_get c_trim " + key + " " + value,
        "Successfully expected value for key: " + key,
        std::chrono::seconds(60)));
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

TEST_F(RedisChaosTest, DemotedPrimaryCatchesUpFromOplogWithoutSnapshot) {
    int leader_index = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader_index, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();

    ClientCtlProcess client(/*index=*/21, FLAGS_out_dir);
    ASSERT_TRUE(client.Start());
    ASSERT_TRUE(client.SendAndWait(
        "create c_handoff " + std::to_string(kClientPortBase + 21),
        "Successfully created client: c_handoff", std::chrono::seconds(30)));

    const std::string before_key = "demotion_handoff_before";
    const std::string before_value = "before_value";
    ASSERT_TRUE(
        client.SendAndWait("put c_handoff " + before_key + " " + before_value,
                           "Successfully put value for key: " + before_key,
                           std::chrono::seconds(10)));
    ASSERT_TRUE(WaitForReplicaOpLog(before_key, std::chrono::seconds(10)));

    auto demoted_log_offset = MasterLogEndOffset(leader_index);
    ASSERT_TRUE(masters_[leader_index]->suspend());

    int new_leader_index = -1;
    ViewVersionId new_version = 0;
    ASSERT_TRUE(
        WaitForNewLeader(leader_index, version, new_leader_index, new_version));
    WaitForLeaderServiceReady();

    ClientCtlProcess after_client(/*index=*/22, FLAGS_out_dir);
    ASSERT_TRUE(after_client.Start());
    ASSERT_TRUE(after_client.SendAndWait(
        "create c_handoff_after " + std::to_string(kClientPortBase + 22),
        "Successfully created client: c_handoff_after",
        std::chrono::seconds(30)));

    const std::string after_key = "demotion_handoff_after";
    const std::string after_value = "after_value";
    ASSERT_TRUE(WaitForClientCtlCommand(
        after_client, "put c_handoff_after " + after_key + " " + after_value,
        "Successfully put value for key: " + after_key,
        std::chrono::seconds(60)));
    ASSERT_TRUE(WaitForReplicaOpLog(after_key, std::chrono::seconds(10)));

    ASSERT_TRUE(masters_[leader_index]->resume());
    ASSERT_TRUE(WaitForMasterLogSince(
        leader_index, "Captured P2P primary demotion handoff",
        demoted_log_offset, std::chrono::seconds(30)));
    ASSERT_TRUE(WaitForMasterLogSince(
        leader_index, "local_committed_sequence_id=", demoted_log_offset,
        std::chrono::seconds(30)));
    ASSERT_TRUE(WaitForMasterLogSince(
        leader_index, "Starting P2P standby from demoted primary handoff",
        demoted_log_offset, std::chrono::seconds(30)));
    ASSERT_TRUE(WaitForMasterLogSince(
        leader_index,
        "P2PHotStandbyService: continuing from initial metadata via OpLog "
        "without snapshot",
        demoted_log_offset, std::chrono::seconds(30)));
    ASSERT_TRUE(WaitForSnapshotObject(leader_index, before_key,
                                      std::chrono::seconds(30)));
    ASSERT_TRUE(WaitForSnapshotObject(leader_index, after_key,
                                      std::chrono::seconds(30)));
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
