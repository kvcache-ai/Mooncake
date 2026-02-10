#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <chrono>
#include <iomanip>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <async_simple/coro/Collect.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <cstring>

#include "client_rpc_service.h"
#include "client_rpc_types.h"
#include "data_manager.h"
#include "peer_client.h"
#include "tiered_cache/tiered_backend.h"
#include "transfer_engine.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

static bool parseJsonString(const std::string& json_str, Json::Value& value,
                            std::string* error_msg = nullptr) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;
    bool success = reader->parse(
        json_str.data(), json_str.data() + json_str.size(), &value, &errs);
    if (!success && error_msg) {
        *error_msg = errs;
    }
    return success;
}

// Performance benchmark fixture comparing async vs sync PeerClient RPC.
//
// Architecture:
//   PeerClient --[RPC]--> coro_rpc_server --[ClientRpcService]--> DataManager
//
// Since TransferEngine is not fully initialized, all operations that find a
// key will fail at the transfer stage (INTERNAL_ERROR), and operations with
// missing keys will fail with INVALID_KEY. Either way, the full RPC
// round-trip (serialization, network, deserialization, server-side dispatch,
// response) is exercised. This is exactly what we want to measure: RPC
// throughput and concurrency benefits.
class PeerClientPerfTest : public ::testing::Test {
   protected:
    static constexpr uint16_t kTestPort = 50052;

    void SetUp() override {
        google::InitGoogleLogging("PeerClientPerfTest");
        FLAGS_logtostderr = 1;
        FLAGS_minloglevel = 2;  // Suppress INFO/WARNING logs during bench

        transfer_engine_ = std::make_shared<TransferEngine>(false);

        std::string json_config_str = R"({
            "tiers": [
                {
                    "type": "DRAM",
                    "capacity": 1073741824,
                    "priority": 10,
                    "tags": ["fast", "local"],
                    "allocator_type": "OFFSET"
                }
            ]
        })";
        Json::Value config;
        ASSERT_TRUE(parseJsonString(json_config_str, config));

        tiered_backend_ = std::make_unique<TieredBackend>();
        auto init_result = tiered_backend_->Init(config, nullptr, nullptr);
        ASSERT_TRUE(init_result.has_value())
            << "Failed to initialize TieredBackend: " << init_result.error();

        data_manager_ = std::make_unique<DataManager>(
            std::move(tiered_backend_), transfer_engine_);

        rpc_service_ = std::make_unique<ClientRpcService>(*data_manager_);

        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/4, kTestPort);
        RegisterClientRpcService(*server_, *rpc_service_);

        server_thread_ = std::thread([this]() {
            auto ec = server_->start();
            if (ec) {
                LOG(ERROR) << "Server start failed: " << ec.message();
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        peer_client_ = std::make_unique<PeerClient>();
        std::string endpoint =
            "127.0.0.1:" + std::to_string(kTestPort);
        auto connect_result = peer_client_->Connect(endpoint);
        ASSERT_TRUE(connect_result.has_value())
            << "PeerClient::Connect failed";
    }

    void TearDown() override {
        peer_client_.reset();
        if (server_) {
            server_->stop();
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
        server_.reset();
        rpc_service_.reset();
        data_manager_.reset();
        tiered_backend_.reset();
        transfer_engine_.reset();
        google::ShutdownGoogleLogging();
    }

    RemoteReadRequest MakeReadRequest(size_t index) {
        RemoteReadRequest request;
        request.key = "perf_key_" + std::to_string(index);
        RemoteBufferDesc desc;
        desc.segment_name = "perf_segment";
        desc.addr = 0x1000 + index * 0x100;
        desc.size = 64;
        request.dest_buffers.push_back(desc);
        return request;
    }

    RemoteWriteRequest MakeWriteRequest(size_t index) {
        RemoteWriteRequest request;
        request.key = "perf_write_key_" + std::to_string(index);
        RemoteBufferDesc desc;
        desc.segment_name = "perf_segment";
        desc.addr = 0x1000 + index * 0x100;
        desc.size = 64;
        request.src_buffers.push_back(desc);
        request.target_tier_id = std::nullopt;
        return request;
    }

    struct BenchResult {
        size_t num_requests;
        double sync_total_ms;
        double async_total_ms;
        double sync_rps;     // requests per second
        double async_rps;
        double speedup;
    };

    // Run N sync read RPCs sequentially
    double RunSyncReads(size_t n) {
        auto start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < n; ++i) {
            auto req = MakeReadRequest(i);
            auto result = peer_client_->ReadRemoteData(req);
            (void)result;
        }
        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    }

    // Run N async read RPCs concurrently using collectAllPara
    double RunAsyncReads(size_t n) {
        auto start = std::chrono::steady_clock::now();

        auto coro = [this, n]()
            -> async_simple::coro::Lazy<void> {
            // Requests must outlive all coroutines since
            // AsyncReadRemoteData takes const& references.
            std::vector<RemoteReadRequest> requests;
            requests.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                requests.push_back(MakeReadRequest(i));
            }

            std::vector<
                async_simple::coro::Lazy<tl::expected<void, ErrorCode>>>
                tasks;
            tasks.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                tasks.push_back(
                    peer_client_->AsyncReadRemoteData(requests[i]));
            }
            auto results =
                co_await async_simple::coro::collectAllPara(std::move(tasks));
            (void)results;
            co_return;
        };

        async_simple::coro::syncAwait(coro());

        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    }

    // Run N sync write RPCs sequentially
    double RunSyncWrites(size_t n) {
        auto start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < n; ++i) {
            auto req = MakeWriteRequest(i);
            auto result = peer_client_->WriteRemoteData(req);
            (void)result;
        }
        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    }

    // Run N async write RPCs concurrently using collectAllPara
    double RunAsyncWrites(size_t n) {
        auto start = std::chrono::steady_clock::now();

        auto coro = [this, n]()
            -> async_simple::coro::Lazy<void> {
            std::vector<RemoteWriteRequest> requests;
            requests.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                requests.push_back(MakeWriteRequest(i));
            }

            std::vector<
                async_simple::coro::Lazy<tl::expected<void, ErrorCode>>>
                tasks;
            tasks.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                tasks.push_back(
                    peer_client_->AsyncWriteRemoteData(requests[i]));
            }
            auto results =
                co_await async_simple::coro::collectAllPara(std::move(tasks));
            (void)results;
            co_return;
        };

        async_simple::coro::syncAwait(coro());

        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    }

    BenchResult RunBench(size_t n) {
        // Warmup
        RunSyncReads(std::min(n, size_t(10)));
        RunAsyncReads(std::min(n, size_t(10)));

        double sync_ms = RunSyncReads(n);
        double async_ms = RunAsyncReads(n);

        BenchResult r;
        r.num_requests = n;
        r.sync_total_ms = sync_ms;
        r.async_total_ms = async_ms;
        r.sync_rps = (n / sync_ms) * 1000.0;
        r.async_rps = (n / async_ms) * 1000.0;
        r.speedup = sync_ms / async_ms;
        return r;
    }

    void PrintResult(const char* label, const BenchResult& r) {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << label << " (N=" << r.num_requests << "):\n"
                  << "  Sync:  " << r.sync_total_ms << " ms, "
                  << r.sync_rps << " req/s\n"
                  << "  Async: " << r.async_total_ms << " ms, "
                  << r.async_rps << " req/s\n"
                  << "  Speedup: " << r.speedup << "x\n\n";
    }

    std::unique_ptr<DataManager> data_manager_;
    std::unique_ptr<TieredBackend> tiered_backend_;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::unique_ptr<ClientRpcService> rpc_service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::thread server_thread_;
    std::unique_ptr<PeerClient> peer_client_;
};

// ============================================================================
// Read Performance: Async vs Sync at different concurrency levels
// ============================================================================

TEST_F(PeerClientPerfTest, ReadConcurrencyComparison) {
    std::cout << "\n========================================\n"
              << "  Read RPC: Async vs Sync Performance\n"
              << "========================================\n\n";

    std::vector<size_t> levels = {1, 10, 50, 100, 200, 500};
    std::vector<BenchResult> results;

    for (size_t n : levels) {
        auto r = RunBench(n);
        PrintResult("Read", r);
        results.push_back(r);
    }

    // Summary table
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "+---------+------------+------------+----------+---------+\n"
              << "|    N    | Sync(ms)   | Async(ms)  | Async RPS| Speedup |\n"
              << "+---------+------------+------------+----------+---------+\n";
    for (auto& r : results) {
        std::cout << "| " << std::setw(7) << r.num_requests << " | "
                  << std::setw(10) << r.sync_total_ms << " | "
                  << std::setw(10) << r.async_total_ms << " | "
                  << std::setw(8) << r.async_rps << " | "
                  << std::setw(6) << r.speedup << "x |\n";
    }
    std::cout << "+---------+------------+------------+----------+---------+\n";
}

// ============================================================================
// Write Performance: Async vs Sync at different concurrency levels
// ============================================================================

TEST_F(PeerClientPerfTest, WriteConcurrencyComparison) {
    std::cout << "\n=========================================\n"
              << "  Write RPC: Async vs Sync Performance\n"
              << "=========================================\n\n";

    std::vector<size_t> levels = {1, 10, 50, 100, 200, 500};

    // Warmup
    RunSyncWrites(10);
    RunAsyncWrites(10);

    struct WriteResult {
        size_t n;
        double sync_ms;
        double async_ms;
        double sync_rps;
        double async_rps;
        double speedup;
    };
    std::vector<WriteResult> results;

    for (size_t n : levels) {
        double sync_ms = RunSyncWrites(n);
        double async_ms = RunAsyncWrites(n);

        WriteResult r;
        r.n = n;
        r.sync_ms = sync_ms;
        r.async_ms = async_ms;
        r.sync_rps = (n / sync_ms) * 1000.0;
        r.async_rps = (n / async_ms) * 1000.0;
        r.speedup = sync_ms / async_ms;
        results.push_back(r);

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Write (N=" << n << "):\n"
                  << "  Sync:  " << sync_ms << " ms, "
                  << r.sync_rps << " req/s\n"
                  << "  Async: " << async_ms << " ms, "
                  << r.async_rps << " req/s\n"
                  << "  Speedup: " << r.speedup << "x\n\n";
    }

    // Summary table
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "+---------+------------+------------+----------+---------+\n"
              << "|    N    | Sync(ms)   | Async(ms)  | Async RPS| Speedup |\n"
              << "+---------+------------+------------+----------+---------+\n";
    for (auto& r : results) {
        std::cout << "| " << std::setw(7) << r.n << " | "
                  << std::setw(10) << r.sync_ms << " | "
                  << std::setw(10) << r.async_ms << " | "
                  << std::setw(8) << r.async_rps << " | "
                  << std::setw(6) << r.speedup << "x |\n";
    }
    std::cout << "+---------+------------+------------+----------+---------+\n";
}

// ============================================================================
// Windowed Async: Measure effect of concurrency cap
// ============================================================================

TEST_F(PeerClientPerfTest, WindowedAsyncConcurrency) {
    std::cout << "\n================================================\n"
              << "  Windowed Async: Effect of Concurrency Cap\n"
              << "  (N=200 total requests, varying window size)\n"
              << "================================================\n\n";

    const size_t total = 200;
    std::vector<size_t> windows = {1, 5, 10, 25, 50, 100, 200};

    // Baseline: sync sequential
    double sync_ms = RunSyncReads(total);
    double sync_rps = (total / sync_ms) * 1000.0;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Baseline (sync sequential): "
              << sync_ms << " ms, " << sync_rps << " req/s\n\n";

    struct WindowResult {
        size_t window;
        double ms;
        double rps;
        double speedup;
    };
    std::vector<WindowResult> results;

    for (size_t w : windows) {
        auto start = std::chrono::steady_clock::now();

        auto coro = [this, total, w]()
            -> async_simple::coro::Lazy<void> {
            std::vector<RemoteReadRequest> requests;
            requests.reserve(total);
            for (size_t i = 0; i < total; ++i) {
                requests.push_back(MakeReadRequest(i));
            }

            std::vector<
                async_simple::coro::Lazy<tl::expected<void, ErrorCode>>>
                tasks;
            tasks.reserve(total);
            for (size_t i = 0; i < total; ++i) {
                tasks.push_back(
                    peer_client_->AsyncReadRemoteData(requests[i]));
            }
            auto results = co_await async_simple::coro::collectAllWindowedPara(
                w, /*yield=*/false, std::move(tasks));
            (void)results;
            co_return;
        };

        async_simple::coro::syncAwait(coro());

        auto end = std::chrono::steady_clock::now();
        double ms =
            std::chrono::duration<double, std::milli>(end - start).count();
        double rps = (total / ms) * 1000.0;
        double speedup = sync_ms / ms;

        results.push_back({w, ms, rps, speedup});

        std::cout << "Window=" << std::setw(3) << w
                  << ": " << std::setw(8) << ms << " ms, "
                  << std::setw(8) << rps << " req/s, "
                  << speedup << "x vs sync\n";
    }

    std::cout << "\n+--------+----------+----------+---------+\n"
              << "| Window | Time(ms) |  Req/s   | Speedup |\n"
              << "+--------+----------+----------+---------+\n";
    for (auto& r : results) {
        std::cout << "| " << std::setw(6) << r.window << " | "
                  << std::setw(8) << r.ms << " | "
                  << std::setw(8) << r.rps << " | "
                  << std::setw(6) << r.speedup << "x |\n";
    }
    std::cout << "+--------+----------+----------+---------+\n";
}

// ============================================================================
// RDMA-Enabled Performance Benchmark Fixture
// ============================================================================

// Benchmarks real RDMA data transfer at various buffer sizes and concurrency
// levels, comparing async vs sync PeerClient interfaces.
// Requires MC_METADATA_ADDR and MC_LOCAL_HOSTNAME env vars; gracefully skips
// when unavailable.
class PeerClientRdmaPerfTest : public ::testing::Test {
   protected:
    static constexpr uint16_t kTestPort = 50053;
    static constexpr uint64_t kRpcPort = 12351;
    static constexpr size_t kRdmaBufSize = 128 * 1024 * 1024;  // 128MB

    void SetUp() override {
        google::InitGoogleLogging("PeerClientRdmaPerfTest");
        FLAGS_logtostderr = 1;
        FLAGS_minloglevel = 2;

        const char* metadata_addr = std::getenv("MC_METADATA_ADDR");
        const char* local_hostname = std::getenv("MC_LOCAL_HOSTNAME");

        if (!metadata_addr || !local_hostname) {
            skip_reason_ =
                "MC_METADATA_ADDR and MC_LOCAL_HOSTNAME not set";
            return;
        }

        local_hostname_ = std::string(local_hostname);

        // Initialize TransferEngine with RDMA.
        // Use MC_RDMA_DEVICE env to filter to a single NIC for loopback;
        // multi-NIC loopback causes cross-port QP handshake failures.
        std::vector<std::string> nic_filter;
        const char* rdma_dev = std::getenv("MC_RDMA_DEVICE");
        if (rdma_dev) {
            nic_filter.push_back(std::string(rdma_dev));
        }
        transfer_engine_ =
            std::make_shared<TransferEngine>(true, nic_filter);
        int init_rc = transfer_engine_->init(metadata_addr,
                                             local_hostname_, "",
                                             kRpcPort);
        if (init_rc != 0) {
            skip_reason_ =
                "TransferEngine init failed (error " +
                std::to_string(init_rc) + ")";
            transfer_engine_.reset();
            return;
        }

        // Allocate and register RDMA buffer
        rdma_buffer_ = allocate_buffer_allocator_memory(kRdmaBufSize);
        if (!rdma_buffer_) {
            skip_reason_ = "Failed to allocate RDMA buffer";
            return;
        }
        std::memset(rdma_buffer_, 0, kRdmaBufSize);

        int reg_rc = transfer_engine_->registerLocalMemory(
            rdma_buffer_, kRdmaBufSize, "cpu:0");
        if (reg_rc != 0) {
            free_memory("", rdma_buffer_);
            rdma_buffer_ = nullptr;
            skip_reason_ = "Failed to register RDMA memory";
            return;
        }

        // Create TieredBackend with TransferEngine so DRAM is RDMA-registered
        std::string json_config_str = R"({
            "tiers": [
                {
                    "type": "DRAM",
                    "capacity": 1073741824,
                    "priority": 10,
                    "allocator_type": "OFFSET"
                }
            ]
        })";
        Json::Value config;
        ASSERT_TRUE(parseJsonString(json_config_str, config));

        tiered_backend_ = std::make_unique<TieredBackend>();
        auto backend_rc = tiered_backend_->Init(
            config, transfer_engine_.get(), nullptr);
        ASSERT_TRUE(backend_rc.has_value())
            << "Failed to initialize TieredBackend";

        data_manager_ = std::make_unique<DataManager>(
            std::move(tiered_backend_), transfer_engine_);

        rpc_service_ =
            std::make_unique<ClientRpcService>(*data_manager_);

        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/4, kTestPort);
        RegisterClientRpcService(*server_, *rpc_service_);

        server_thread_ = std::thread([this]() {
            auto ec = server_->start();
            if (ec) {
                LOG(ERROR) << "RDMA server start failed: "
                           << ec.message();
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        peer_client_ = std::make_unique<PeerClient>();
        std::string endpoint =
            "127.0.0.1:" + std::to_string(kTestPort);
        auto connect_result = peer_client_->Connect(endpoint);
        ASSERT_TRUE(connect_result.has_value())
            << "PeerClient::Connect failed";

        rdma_available_ = true;
    }

    void TearDown() override {
        peer_client_.reset();
        if (server_) {
            server_->stop();
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
        server_.reset();
        rpc_service_.reset();
        data_manager_.reset();
        tiered_backend_.reset();

        if (rdma_buffer_ && transfer_engine_) {
            transfer_engine_->unregisterLocalMemory(rdma_buffer_);
            free_memory("", rdma_buffer_);
            rdma_buffer_ = nullptr;
        }

        transfer_engine_.reset();
        google::ShutdownGoogleLogging();
    }

    bool ShouldSkip() const {
        if (!rdma_available_) {
            std::cout << "[  SKIPPED ] " << skip_reason_ << "\n";
            return true;
        }
        return false;
    }

    // ---- Helpers ----

    void PopulateKey(const std::string& key, size_t data_size) {
        auto data = std::make_unique<char[]>(data_size);
        std::memset(data.get(), 'A', data_size);
        auto rc = data_manager_->Put(key, std::move(data), data_size);
        ASSERT_TRUE(rc.has_value())
            << "PopulateKey failed for " << key;
    }

    RemoteReadRequest MakeRdmaReadRequest(size_t index,
                                          size_t data_size) {
        RemoteReadRequest request;
        request.key =
            "rdma_perf_key_" + std::to_string(index);
        RemoteBufferDesc desc;
        desc.segment_name = local_hostname_;
        desc.addr = reinterpret_cast<uintptr_t>(
            static_cast<char*>(rdma_buffer_) + index * data_size);
        desc.size = data_size;
        request.dest_buffers.push_back(desc);
        return request;
    }

    RemoteWriteRequest MakeRdmaWriteRequest(size_t index,
                                            size_t data_size) {
        RemoteWriteRequest request;
        request.key =
            "rdma_perf_write_key_" + std::to_string(index);
        // Pre-fill source region with data
        char* src = static_cast<char*>(rdma_buffer_) +
                    index * data_size;
        std::memset(src, 'B', data_size);

        RemoteBufferDesc desc;
        desc.segment_name = local_hostname_;
        desc.addr = reinterpret_cast<uintptr_t>(src);
        desc.size = data_size;
        request.src_buffers.push_back(desc);
        request.target_tier_id = std::nullopt;
        return request;
    }

    struct RdmaBenchResult {
        size_t data_size;
        size_t concurrency;
        double sync_ms;
        double async_ms;
        double sync_throughput_mbps;
        double async_throughput_mbps;
        double speedup;
    };

    // Run N sync reads with RDMA-backed requests
    double RunRdmaSyncReads(size_t n, size_t data_size) {
        auto start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < n; ++i) {
            auto req = MakeRdmaReadRequest(i, data_size);
            auto result = peer_client_->ReadRemoteData(req);
            (void)result;
        }
        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(end - start)
            .count();
    }

    // Run N async reads with RDMA-backed requests
    double RunRdmaAsyncReads(size_t n, size_t data_size) {
        auto start = std::chrono::steady_clock::now();

        auto coro = [this, n, data_size]()
            -> async_simple::coro::Lazy<void> {
            std::vector<RemoteReadRequest> requests;
            requests.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                requests.push_back(
                    MakeRdmaReadRequest(i, data_size));
            }

            std::vector<async_simple::coro::Lazy<
                tl::expected<void, ErrorCode>>>
                tasks;
            tasks.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                tasks.push_back(
                    peer_client_->AsyncReadRemoteData(requests[i]));
            }
            auto results =
                co_await async_simple::coro::collectAllPara(
                    std::move(tasks));
            (void)results;
            co_return;
        };

        async_simple::coro::syncAwait(coro());

        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(end - start)
            .count();
    }

    // Run N sync writes with RDMA-backed requests
    double RunRdmaSyncWrites(size_t n, size_t data_size) {
        auto start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < n; ++i) {
            auto req = MakeRdmaWriteRequest(i, data_size);
            auto result = peer_client_->WriteRemoteData(req);
            (void)result;
        }
        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(end - start)
            .count();
    }

    // Run N async writes with RDMA-backed requests
    double RunRdmaAsyncWrites(size_t n, size_t data_size) {
        auto start = std::chrono::steady_clock::now();

        auto coro = [this, n, data_size]()
            -> async_simple::coro::Lazy<void> {
            std::vector<RemoteWriteRequest> requests;
            requests.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                requests.push_back(
                    MakeRdmaWriteRequest(i, data_size));
            }

            std::vector<async_simple::coro::Lazy<
                tl::expected<void, ErrorCode>>>
                tasks;
            tasks.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                tasks.push_back(
                    peer_client_->AsyncWriteRemoteData(requests[i]));
            }
            auto results =
                co_await async_simple::coro::collectAllPara(
                    std::move(tasks));
            (void)results;
            co_return;
        };

        async_simple::coro::syncAwait(coro());

        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(end - start)
            .count();
    }

    static double ThroughputMBps(size_t n, size_t data_size,
                                 double ms) {
        double total_bytes = static_cast<double>(n) * data_size;
        double seconds = ms / 1000.0;
        return (total_bytes / (1024.0 * 1024.0)) / seconds;
    }

    std::string local_hostname_;
    void* rdma_buffer_ = nullptr;
    bool rdma_available_ = false;
    std::string skip_reason_;
    std::unique_ptr<DataManager> data_manager_;
    std::unique_ptr<TieredBackend> tiered_backend_;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::unique_ptr<ClientRpcService> rpc_service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::thread server_thread_;
    std::unique_ptr<PeerClient> peer_client_;
};

// ============================================================================
// RDMA Read: Async vs Sync across data sizes and concurrency levels
// ============================================================================

TEST_F(PeerClientRdmaPerfTest, RdmaReadConcurrencyBySizeComparison) {
    if (ShouldSkip()) return;

    std::cout
        << "\n============================================================\n"
        << "  RDMA Read: Async vs Sync (real data transfer)\n"
        << "============================================================\n\n";

    std::vector<size_t> data_sizes = {
        4 * 1024,        // 4KB
        64 * 1024,       // 64KB
        256 * 1024,      // 256KB
        1 * 1024 * 1024  // 1MB
    };
    std::vector<size_t> concurrency_levels = {1, 10, 50, 100};

    std::vector<PeerClientRdmaPerfTest::RdmaBenchResult> all_results;

    for (size_t data_size : data_sizes) {
        for (size_t n : concurrency_levels) {
            // Ensure we don't exceed RDMA buffer: n * data_size <= 128MB
            if (n * data_size > kRdmaBufSize) {
                std::cout << "  [SKIP] size="
                          << (data_size / 1024) << "KB, N=" << n
                          << " exceeds 128MB RDMA buffer\n";
                continue;
            }

            // Pre-populate keys in DataManager
            for (size_t i = 0; i < n; ++i) {
                std::string key =
                    "rdma_perf_key_" + std::to_string(i);
                PopulateKey(key, data_size);
            }

            // Zero out destination region before reads
            std::memset(rdma_buffer_, 0, n * data_size);

            // Sync reads
            double sync_ms = RunRdmaSyncReads(n, data_size);

            // Verify at least one transfer for correctness
            char* first_dest = static_cast<char*>(rdma_buffer_);
            bool first_correct = true;
            for (size_t b = 0; b < data_size; ++b) {
                if (first_dest[b] != 'A') {
                    first_correct = false;
                    break;
                }
            }

            // Zero out again for async test
            std::memset(rdma_buffer_, 0, n * data_size);

            // Async reads
            double async_ms = RunRdmaAsyncReads(n, data_size);

            double sync_tp = ThroughputMBps(n, data_size, sync_ms);
            double async_tp = ThroughputMBps(n, data_size, async_ms);
            double speedup = sync_ms / async_ms;

            RdmaBenchResult r{data_size, n, sync_ms, async_ms,
                              sync_tp, async_tp, speedup};
            all_results.push_back(r);

            std::cout << std::fixed << std::setprecision(2);
            std::cout << "  size=" << std::setw(7)
                      << (data_size / 1024) << "KB, N="
                      << std::setw(3) << n
                      << " | Sync: " << std::setw(9) << sync_ms
                      << " ms (" << std::setw(9) << sync_tp
                      << " MB/s)"
                      << " | Async: " << std::setw(9) << async_ms
                      << " ms (" << std::setw(9) << async_tp
                      << " MB/s)"
                      << " | Speedup: " << std::setw(5) << speedup
                      << "x";
            if (!first_correct) {
                std::cout << " [DATA MISMATCH]";
            }
            std::cout << "\n";

            // Clean up keys for next iteration
            for (size_t i = 0; i < n; ++i) {
                std::string key =
                    "rdma_perf_key_" + std::to_string(i);
                data_manager_->Delete(key);
            }
        }
    }

    // Summary table
    std::cout
        << "\n+---------+-----+------------+------------+-----------"
           "+-----------+---------+\n"
        << "|  Size   |  N  | Sync(ms)   | Async(ms)  | Sync MB/s "
           "| Async MB/s| Speedup |\n"
        << "+---------+-----+------------+------------+-----------"
           "+-----------+---------+\n";
    for (auto& r : all_results) {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "| " << std::setw(5) << (r.data_size / 1024)
                  << "KB | " << std::setw(3) << r.concurrency
                  << " | " << std::setw(10) << r.sync_ms << " | "
                  << std::setw(10) << r.async_ms << " | "
                  << std::setw(9) << r.sync_throughput_mbps << " | "
                  << std::setw(9) << r.async_throughput_mbps << " | "
                  << std::setw(6) << r.speedup << "x |\n";
    }
    std::cout
        << "+---------+-----+------------+------------+-----------"
           "+-----------+---------+\n";
}

// ============================================================================
// RDMA Write: Async vs Sync across data sizes and concurrency levels
// ============================================================================

TEST_F(PeerClientRdmaPerfTest, RdmaWriteConcurrencyBySizeComparison) {
    if (ShouldSkip()) return;

    std::cout
        << "\n============================================================\n"
        << "  RDMA Write: Async vs Sync (real data transfer)\n"
        << "============================================================\n\n";

    std::vector<size_t> data_sizes = {
        4 * 1024,        // 4KB
        64 * 1024,       // 64KB
        256 * 1024,      // 256KB
        1 * 1024 * 1024  // 1MB
    };
    std::vector<size_t> concurrency_levels = {1, 10, 50, 100};

    std::vector<PeerClientRdmaPerfTest::RdmaBenchResult> all_results;

    for (size_t data_size : data_sizes) {
        for (size_t n : concurrency_levels) {
            if (n * data_size > kRdmaBufSize) {
                std::cout << "  [SKIP] size="
                          << (data_size / 1024) << "KB, N=" << n
                          << " exceeds 128MB RDMA buffer\n";
                continue;
            }

            // Sync writes
            double sync_ms = RunRdmaSyncWrites(n, data_size);

            // Clean up written keys before async test
            for (size_t i = 0; i < n; ++i) {
                std::string key =
                    "rdma_perf_write_key_" + std::to_string(i);
                data_manager_->Delete(key);
            }

            // Async writes
            double async_ms = RunRdmaAsyncWrites(n, data_size);

            // Verify at least one key was written
            std::string first_key = "rdma_perf_write_key_0";
            auto get_rc = data_manager_->Get(first_key);
            bool write_ok = get_rc.has_value();

            double sync_tp = ThroughputMBps(n, data_size, sync_ms);
            double async_tp = ThroughputMBps(n, data_size, async_ms);
            double speedup = sync_ms / async_ms;

            RdmaBenchResult r{data_size, n, sync_ms, async_ms,
                              sync_tp, async_tp, speedup};
            all_results.push_back(r);

            std::cout << std::fixed << std::setprecision(2);
            std::cout << "  size=" << std::setw(7)
                      << (data_size / 1024) << "KB, N="
                      << std::setw(3) << n
                      << " | Sync: " << std::setw(9) << sync_ms
                      << " ms (" << std::setw(9) << sync_tp
                      << " MB/s)"
                      << " | Async: " << std::setw(9) << async_ms
                      << " ms (" << std::setw(9) << async_tp
                      << " MB/s)"
                      << " | Speedup: " << std::setw(5) << speedup
                      << "x";
            if (!write_ok) {
                std::cout << " [WRITE VERIFY FAILED]";
            }
            std::cout << "\n";

            // Clean up keys for next iteration
            for (size_t i = 0; i < n; ++i) {
                std::string key =
                    "rdma_perf_write_key_" + std::to_string(i);
                data_manager_->Delete(key);
            }
        }
    }

    // Summary table
    std::cout
        << "\n+---------+-----+------------+------------+-----------"
           "+-----------+---------+\n"
        << "|  Size   |  N  | Sync(ms)   | Async(ms)  | Sync MB/s "
           "| Async MB/s| Speedup |\n"
        << "+---------+-----+------------+------------+-----------"
           "+-----------+---------+\n";
    for (auto& r : all_results) {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "| " << std::setw(5) << (r.data_size / 1024)
                  << "KB | " << std::setw(3) << r.concurrency
                  << " | " << std::setw(10) << r.sync_ms << " | "
                  << std::setw(10) << r.async_ms << " | "
                  << std::setw(9) << r.sync_throughput_mbps << " | "
                  << std::setw(9) << r.async_throughput_mbps << " | "
                  << std::setw(6) << r.speedup << "x |\n";
    }
    std::cout
        << "+---------+-----+------------+------------+-----------"
           "+-----------+---------+\n";
}

}  // namespace mooncake
