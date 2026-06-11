// Copyright 2025 KVCache.AI
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

#include "coordinator.h"
#include "tent/common/utils/ip.h"

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <thread>
#include <unordered_map>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <gflags/gflags.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace mooncake {
namespace bench {

//==============================================================================
// YLT Reflection for RPC Types
//==============================================================================

YLT_REFL(NodeInfo, node_rank, rpc_address, hostname);
YLT_REFL(RegisterRequest, rpc_address, hostname);
YLT_REFL(RegisterResponse, assigned_rank, total_nodes);
YLT_REFL(WaitRequest, my_rank);
YLT_REFL(WaitResponse, ready, message, all_nodes);
YLT_REFL(ResultReport, node_rank, num_threads, block_size, batch_size,
         total_samples, total_duration_avg, transfer_duration_avg,
         transfer_duration_min, transfer_duration_max, transfer_duration_p99,
         transfer_duration_p999);
YLT_REFL(ResultAck, success);
YLT_REFL(PingRequest, node_rank);
YLT_REFL(PingResponse, alive);

//==============================================================================
// TestConfigKey & Hash
//==============================================================================

struct TestConfigKey {
    int32_t num_threads;
    int64_t block_size;
    int64_t batch_size;

    bool operator==(const TestConfigKey& o) const {
        return num_threads == o.num_threads && block_size == o.block_size &&
               batch_size == o.batch_size;
    }

    bool operator<(const TestConfigKey& o) const {
        if (num_threads != o.num_threads) return num_threads < o.num_threads;
        if (block_size != o.block_size) return block_size < o.block_size;
        return batch_size < o.batch_size;
    }
};

struct TestConfigKeyHash {
    size_t operator()(const TestConfigKey& key) const {
        size_t h1 = std::hash<int32_t>{}(key.num_threads);
        size_t h2 = std::hash<int64_t>{}(key.block_size);
        size_t h3 = std::hash<int64_t>{}(key.batch_size);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

//==============================================================================
// CoordinatorService (server-side logic)
//==============================================================================

class CoordinatorService {
   public:
    CoordinatorService(int32_t total_nodes) : total_nodes_(total_nodes) {
        LOG(INFO) << "CoordinatorService: total_nodes=" << total_nodes;
    }

    tl::expected<RegisterResponse, ErrorCode> Register(
        const RegisterRequest& req);
    tl::expected<WaitResponse, ErrorCode> Wait(const WaitRequest& req);
    tl::expected<ResultAck, ErrorCode> Report(const ResultReport& req);
    tl::expected<PingResponse, ErrorCode> Ping(const PingRequest& req);
    void PrintResults();
    bool AllNodesRegistered() const;

   private:
    struct ConfigData {
        std::vector<ResultReport> reports;
        int32_t num_nodes_reported = 0;
    };

    // Calculate bandwidth in GB/s
    // NOTE: total_duration_avg is the sum of execution times across all
    // threads, NOT the actual parallel execution time. To get the correct
    // parallel bandwidth, we divide by num_threads to get the actual wall-clock
    // time.
    static double CalculateBandwidth(const ResultReport& r) {
        size_t total_data = r.block_size * r.batch_size * r.total_samples;
        // total_duration_avg is accumulated across threads, so divide by
        // num_threads to get actual parallel execution time
        double duration_sec = r.total_duration_avg / 1e6;
        return (total_data / 1e9) / duration_sec;
    }

    int32_t total_nodes_;
    mutable std::mutex mutex_;
    std::unordered_map<int32_t, NodeInfo> registered_nodes_;
    bool all_registered_ = false;
    std::map<TestConfigKey, ConfigData> results_;
};

//==============================================================================
// CoordinatorService Implementation
//==============================================================================

tl::expected<RegisterResponse, ErrorCode> CoordinatorService::Register(
    const RegisterRequest& req) {
    LOG(INFO) << "[Server<-Client] Register RPC Request: "
              << "rpc_address=" << req.rpc_address
              << ", hostname=" << req.hostname;

    std::lock_guard<std::mutex> lock(mutex_);

    if (registered_nodes_.size() >= static_cast<size_t>(total_nodes_)) {
        LOG(INFO)
            << "[Server->Client] Register RPC Response: ERROR (already full)";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    int32_t assigned_rank = static_cast<int32_t>(registered_nodes_.size());
    NodeInfo info{assigned_rank, req.rpc_address, req.hostname};
    registered_nodes_[assigned_rank] = info;

    LOG(INFO) << "[Server] Registered node " << assigned_rank << " at "
              << req.rpc_address << " (" << registered_nodes_.size() << "/"
              << total_nodes_ << ")";

    RegisterResponse resp{assigned_rank, total_nodes_};
    LOG(INFO)
        << "[Server->Client] Register RPC Response: SUCCESS (assigned_rank="
        << resp.assigned_rank << ", total_nodes=" << resp.total_nodes << ")";

    if (registered_nodes_.size() >= static_cast<size_t>(total_nodes_)) {
        all_registered_ = true;
        LOG(INFO) << "[Server] All " << total_nodes_ << " nodes registered!";
    }

    return resp;
}

tl::expected<WaitResponse, ErrorCode> CoordinatorService::Wait(
    const WaitRequest& req) {
    LOG(INFO) << "[Server<-Client] Wait RPC Request: my_rank=" << req.my_rank;

    std::lock_guard<std::mutex> lock(mutex_);

    // Non-blocking check: are all nodes registered?
    if (!all_registered_) {
        std::ostringstream oss;
        oss << "Waiting for more nodes. Currently " << registered_nodes_.size()
            << " of " << total_nodes_ << " nodes registered.";
        WaitResponse resp{false, oss.str(), {}};
        LOG(INFO) << "[Server->Client] Wait RPC Response: NOT_READY ("
                  << registered_nodes_.size() << "/" << total_nodes_
                  << " nodes)";
        return resp;
    }

    LOG(INFO) << "[Server] All nodes registered, returning node list";

    // Build the list of all registered nodes
    std::vector<NodeInfo> all_nodes;
    all_nodes.reserve(registered_nodes_.size());
    for (const auto& [rank, info] : registered_nodes_) {
        all_nodes.push_back(info);
    }

    WaitResponse resp{true, "All nodes ready", all_nodes};
    LOG(INFO) << "[Server->Client] Wait RPC Response: SUCCESS (ready=true, "
              << all_nodes.size() << " nodes)";
    return resp;
}

tl::expected<ResultAck, ErrorCode> CoordinatorService::Report(
    const ResultReport& req) {
    LOG(INFO) << "[Server<-Client] Report RPC Request: "
              << "node_rank=" << req.node_rank
              << ", num_threads=" << req.num_threads
              << ", block_size=" << req.block_size
              << ", batch_size=" << req.batch_size
              << ", samples=" << req.total_samples << ", bw_gb_s=" << std::fixed
              << std::setprecision(2)
              << ((req.block_size * req.batch_size * req.total_samples / 1e9) /
                  (req.total_duration_avg / 1e6));

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = registered_nodes_.find(req.node_rank);
    if (it == registered_nodes_.end()) {
        LOG(INFO) << "[Server->Client] Report RPC Response: ERROR (node not "
                     "registered)";
        return tl::make_unexpected(ErrorCode::NOT_REGISTERED);
    }

    TestConfigKey key{req.num_threads, req.block_size, req.batch_size};
    auto& config_data = results_[key];
    config_data.reports.push_back(req);
    config_data.num_nodes_reported++;

    ResultAck resp{true};
    LOG(INFO) << "[Server->Client] Report RPC Response: SUCCESS (stored, "
                 "config_reported="
              << config_data.num_nodes_reported << "/"
              << registered_nodes_.size() << " nodes)";

    return resp;
}

tl::expected<PingResponse, ErrorCode> CoordinatorService::Ping(
    const PingRequest& req) {
    PingResponse resp{true};
    return resp;
}

void CoordinatorService::PrintResults() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (results_.empty()) {
        LOG(INFO) << "No results to display";
        return;
    }

    std::cout << "\n\033[32m===== All-to-All Aggregated Results ("
              << registered_nodes_.size() << "/" << total_nodes_
              << " nodes) =====\033[0m" << std::endl;

    std::cout << std::left << std::fixed << std::setprecision(6)
              << std::setw(14) << "BlkSize (B)" << std::setw(8) << "Batch"
              << std::setw(10) << "Threads" << std::setw(20)
              << "Total BW (GB/S)" << std::setw(14) << "Avg Lat (us)"
              << std::setw(14) << "Avg Tx (us)" << std::setw(14)
              << "P99 Tx (us)" << std::setw(14) << "P999 Tx (us)" << std::endl;
    std::cout << std::string(160, '-') << std::endl;
    for (auto& [key, config_data] : results_) {
        if (config_data.num_nodes_reported ==
            static_cast<int32_t>(registered_nodes_.size())) {
            double total_bw = 0, total_lat = 0, total_tx = 0;
            double total_p99 = 0, total_p999 = 0;

            for (const auto& r : config_data.reports) {
                total_bw += CalculateBandwidth(r);
                // Avg Lat: average wall-clock time per test run
                // Since total_duration_avg is accumulated across threads,
                // divide by num_threads to get actual execution time per run
                double node_avg_lat =
                    (r.total_duration_avg * r.num_threads) / r.total_samples;
                total_lat += node_avg_lat;
                // Avg Tx: single transfer time to one target (already correct)
                total_tx += r.transfer_duration_avg;
                total_p99 += r.transfer_duration_p99;
                total_p999 += r.transfer_duration_p999;
            }

            int32_t num_nodes = config_data.reports.size();

            std::cout << std::left << std::fixed << std::setprecision(6)
                      << std::setw(14) << key.block_size << std::setw(8)
                      << key.batch_size << std::setw(10) << key.num_threads
                      << std::setw(20) << total_bw << std::setprecision(1)
                      << std::setw(14) << (total_lat / num_nodes)
                      << std::setw(14) << (total_tx / num_nodes)
                      << std::setw(14) << (total_p99 / num_nodes)
                      << std::setw(14) << (total_p999 / num_nodes) << std::endl;
        }
    }

    std::cout << "\033[32m" << std::string(160, '=') << "\033[0m" << std::endl;
}

bool CoordinatorService::AllNodesRegistered() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return all_registered_;
}

//==============================================================================
// CoordinatorServer (RPC wrapper) - always defined, members conditional
//==============================================================================

class CoordinatorServer {
   public:
    CoordinatorServer(int32_t total_nodes, int port = 12345);
    ~CoordinatorServer();

    // Always available (for client reference)
    tl::expected<RegisterResponse, ErrorCode> RegisterHandler(
        const RegisterRequest& req);
    tl::expected<WaitResponse, ErrorCode> WaitHandler(const WaitRequest& req);
    tl::expected<ResultAck, ErrorCode> ReportHandler(const ResultReport& req);
    tl::expected<PingResponse, ErrorCode> PingHandler(const PingRequest& req);

    bool Start();
    void Stop();
    CoordinatorService& GetService() { return service_; }

   private:
    CoordinatorService service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_;
    std::atomic<bool> running_{false};
};

CoordinatorServer::CoordinatorServer(int32_t total_nodes, int port)
    : service_(total_nodes),
      rpc_server_(
          std::make_unique<coro_rpc::coro_rpc_server>(1, port, "0.0.0.0")) {}

CoordinatorServer::~CoordinatorServer() { Stop(); }

tl::expected<RegisterResponse, ErrorCode> CoordinatorServer::RegisterHandler(
    const RegisterRequest& req) {
    return service_.Register(req);
}

tl::expected<WaitResponse, ErrorCode> CoordinatorServer::WaitHandler(
    const WaitRequest& req) {
    return service_.Wait(req);
}

tl::expected<ResultAck, ErrorCode> CoordinatorServer::ReportHandler(
    const ResultReport& req) {
    return service_.Report(req);
}

tl::expected<PingResponse, ErrorCode> CoordinatorServer::PingHandler(
    const PingRequest& req) {
    return service_.Ping(req);
}

bool CoordinatorServer::Start() {
    rpc_server_->register_handler<&CoordinatorServer::RegisterHandler>(this);
    rpc_server_->register_handler<&CoordinatorServer::WaitHandler>(this);
    rpc_server_->register_handler<&CoordinatorServer::ReportHandler>(this);
    rpc_server_->register_handler<&CoordinatorServer::PingHandler>(this);

    auto ec = rpc_server_->start();
    if (ec) {
        LOG(ERROR) << "Failed to start RPC server: " << ec.message();
        return false;
    }

    running_ = true;
    LOG(INFO) << "RPC server started";

    // Monitor thread to print results when tests are complete
    std::thread([this]() {
        // Wait for all nodes to register
        while (running_ && !service_.AllNodesRegistered()) {
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        // Give time for results to be reported
        if (running_) {
            std::this_thread::sleep_for(std::chrono::seconds(15));
            if (running_) {
                service_.PrintResults();
            }
        }
    }).detach();

    return true;
}

void CoordinatorServer::Stop() {
    if (running_) {
        running_ = false;
        rpc_server_->stop();
        LOG(INFO) << "RPC server stopped";
    }
}

//==============================================================================
// CoordinatorClient Implementation
//==============================================================================

class CoordinatorClient::Impl {
   public:
    Impl(const std::string& server_addr)
        : server_addr_(server_addr),
          client_(std::make_unique<coro_rpc::coro_rpc_client>()) {}

    bool Connect() {
        auto ec = async_simple::coro::syncAwait(client_->connect(server_addr_));
        if (ec) {
            LOG(ERROR) << "Failed to connect: " << ec.message();
            return false;
        }
        connected_ = true;
        LOG(INFO) << "Connected to coordinator";
        return true;
    }

    void Disconnect() {
        if (connected_) {
            client_->close();
            connected_ = false;
        }
    }

    tl::expected<RegisterResponse, ErrorCode> RegisterNode(
        const RegisterRequest& req) {
        LOG(INFO) << "[Client->Server] Register RPC Request: "
                  << "rpc_address=" << req.rpc_address
                  << ", hostname=" << req.hostname;

        auto result = async_simple::coro::syncAwait(
            client_->call<&CoordinatorServer::RegisterHandler>(req));

        if (!result.has_value()) {
            LOG(INFO)
                << "[Client<-Server] Register RPC Response: ERROR (internal)";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        if (!result.value().has_value()) {
            LOG(INFO) << "[Client<-Server] Register RPC Response: ERROR (code="
                      << static_cast<int>(result.value().error()) << ")";
            return tl::make_unexpected(result.value().error());
        }

        const auto& resp = result.value().value();
        LOG(INFO) << "[Client<-Server] Register RPC Response: SUCCESS "
                  << "assigned_rank=" << resp.assigned_rank
                  << ", total_nodes=" << resp.total_nodes;
        return resp;
    }

    tl::expected<std::vector<NodeInfo>, ErrorCode> WaitForAll(int32_t node_rank,
                                                              int timeout_sec) {
        WaitRequest req{node_rank};
        auto start = std::chrono::steady_clock::now();
        auto deadline = start + std::chrono::seconds(timeout_sec);

        LOG(INFO) << "[Client->Server] Wait RPC Request: my_rank=" << node_rank
                  << ", timeout_sec=" << timeout_sec;

        int retry_count = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            auto result = async_simple::coro::syncAwait(
                client_->call<&CoordinatorServer::WaitHandler>(req));
            if (!result.has_value()) {
                // RPC failed, check if we should retry
                if (std::chrono::steady_clock::now() >= deadline) {
                    LOG(INFO) << "[Client<-Server] Wait RPC Response: ERROR "
                                 "(timeout after "
                              << retry_count << " retries)";
                    return tl::make_unexpected(ErrorCode::TIMEOUT);
                }
                LOG(WARNING)
                    << "[Client] Wait RPC failed (internal), retrying...";
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                retry_count++;
                continue;
            }

            const auto& resp = result.value();
            if (!resp.has_value()) {
                // Server returned an error, likely timeout
                LOG(INFO) << "[Client<-Server] Wait RPC Response: ERROR (code="
                          << static_cast<int>(resp.error()) << ", message='"
                          << (resp.error() == ErrorCode::TIMEOUT ? "timeout"
                                                                 : "unknown")
                          << "')";
                return tl::make_unexpected(resp.error());
            }

            if (resp->ready) {
                LOG(INFO) << "[Client<-Server] Wait RPC Response: SUCCESS "
                             "(ready=true, "
                          << resp->all_nodes.size() << " nodes)";
                return resp->all_nodes;
            }

            LOG(INFO) << "[Client<-Server] Wait RPC Response: NOT_READY "
                         "(retrying...)";
            // Check if we've exceeded our deadline
            if (std::chrono::steady_clock::now() >= deadline) {
                LOG(INFO) << "[Client<-Server] Wait RPC Response: ERROR "
                             "(client timeout)";
                return tl::make_unexpected(ErrorCode::TIMEOUT);
            }

            // Not ready yet, wait a bit before retrying
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            retry_count++;
        }

        LOG(INFO)
            << "[Client<-Server] Wait RPC Response: ERROR (deadline exceeded)";
        return tl::make_unexpected(ErrorCode::TIMEOUT);
    }

    tl::expected<ResultAck, ErrorCode> ReportResult(const ResultReport& req) {
        LOG(INFO) << "[Client->Server] Report RPC Request: "
                  << "node_rank=" << req.node_rank
                  << ", num_threads=" << req.num_threads
                  << ", block_size=" << req.block_size
                  << ", batch_size=" << req.batch_size
                  << ", samples=" << req.total_samples
                  << ", bw_gb_s=" << std::fixed << std::setprecision(2)
                  << ((req.block_size * req.batch_size * req.total_samples /
                       1e9) /
                      (req.total_duration_avg / 1e6));

        auto result = async_simple::coro::syncAwait(
            client_->call<&CoordinatorServer::ReportHandler>(req));

        if (!result.has_value()) {
            LOG(INFO)
                << "[Client<-Server] Report RPC Response: ERROR (internal)";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        if (!result.value().has_value()) {
            LOG(INFO) << "[Client<-Server] Report RPC Response: ERROR (code="
                      << static_cast<int>(result.value().error()) << ")";
            return tl::make_unexpected(result.value().error());
        }

        const auto& resp = result.value().value();
        LOG(INFO) << "[Client<-Server] Report RPC Response: SUCCESS (success="
                  << (resp.success ? "true" : "false") << ")";
        return resp;
    }

    tl::expected<PingResponse, ErrorCode> Ping(const PingRequest& req) {
        auto result = async_simple::coro::syncAwait(
            client_->call<&CoordinatorServer::PingHandler>(req));

        if (!result.has_value()) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        if (!result.value().has_value()) {
            return tl::make_unexpected(result.value().error());
        }

        return result.value().value();
    }

   private:
    std::string server_addr_;
    std::unique_ptr<coro_rpc::coro_rpc_client> client_;
    bool connected_ = false;
};

CoordinatorClient::CoordinatorClient(const std::string& server_addr)
    : impl_(new Impl(server_addr)) {}

CoordinatorClient::~CoordinatorClient() { delete impl_; }

bool CoordinatorClient::Connect() { return impl_->Connect(); }
void CoordinatorClient::Disconnect() { impl_->Disconnect(); }
tl::expected<RegisterResponse, ErrorCode> CoordinatorClient::RegisterNode(
    const RegisterRequest& req) {
    return impl_->RegisterNode(req);
}
tl::expected<std::vector<NodeInfo>, ErrorCode> CoordinatorClient::WaitForAll(
    int32_t node_rank, int timeout_sec) {
    return impl_->WaitForAll(node_rank, timeout_sec);
}
tl::expected<ResultAck, ErrorCode> CoordinatorClient::ReportResult(
    const ResultReport& req) {
    return impl_->ReportResult(req);
}
tl::expected<PingResponse, ErrorCode> CoordinatorClient::Ping(
    const PingRequest& req) {
    return impl_->Ping(req);
}

}  // namespace bench
}  // namespace mooncake

//==============================================================================
// Main Entry Point (server only)
//==============================================================================

#ifdef COORDINATOR_ENTRYPOINT

#include <gflags/gflags.h>
#include <csignal>

namespace mooncake {
namespace bench {

DEFINE_int32(port, 12345, "RPC server port");
DEFINE_int32(num_nodes, 2, "Total number of nodes expected");

static CoordinatorServer* g_coordinator = nullptr;

void signal_handler(int signum) {
    LOG(INFO) << "Shutting down...";
    if (g_coordinator) {
        g_coordinator->Stop();
        g_coordinator->GetService().PrintResults();
    }
    exit(0);
}

}  // namespace bench
}  // namespace mooncake

int main(int argc, char* argv[]) {
    gflags::SetUsageMessage(
        "Mooncake Benchmark Coordinator Server\n"
        "Coordinates multi-node all-to-all benchmark tests.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    // Get local IP address
    std::string local_ip;
    bool ipv6 = false;
    auto status = mooncake::tent::discoverLocalIpAddress(local_ip, ipv6);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to discover local IP: " << status.ToString();
        local_ip = "IP_Address";
    }

    std::cout << "\033[33mCluster size: " << mooncake::bench::FLAGS_num_nodes
              << std::endl
              << "For each test node, run " << std::endl
              << "  ./tebench --coordinator=" << local_ip << ":"
              << mooncake::bench::FLAGS_port << std::endl
              << "Press Ctrl-C to terminate\033[0m" << std::endl;

    auto coordinator = std::make_unique<mooncake::bench::CoordinatorServer>(
        mooncake::bench::FLAGS_num_nodes, mooncake::bench::FLAGS_port);
    mooncake::bench::g_coordinator = coordinator.get();

    std::signal(SIGINT, mooncake::bench::signal_handler);
    std::signal(SIGTERM, mooncake::bench::signal_handler);

    if (!coordinator->Start()) {
        LOG(ERROR) << "Failed to start coordinator";
        return 1;
    }

    while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
    return 0;
}

#endif  // COORDINATOR_ENTRYPOINT
