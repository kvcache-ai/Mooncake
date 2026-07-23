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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// -------------------------------------------------------------------
// LocalDiskDrainTest
//
// In-process master (non-HA) + two offload-capable RealClients over
// TCP that verifies the LOCAL_DISK drain job path introduced by the
// "LOCAL_DISK drain migration via Move flow (Option A)" change:
//
//   Put (MEMORY + LOCAL_DISK replica on client1's SSD)
//     -> drain job moves the LOCAL_DISK replica off client1's segment
//       -> data is now served from client2, byte-for-byte identical
//
// This complements task_integration_test.DrainJobCompleteFlow, which
// only exercises in-memory (DRAM) segment drains. Here the source
// replica is physically written to the client's local disk
// (FileStorage) and the drain job must read it back via the new
// LOCAL_DISK branch of Client::ExecuteReplicaTransfer.
// -------------------------------------------------------------------

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>
#include <ylt/reflection/user_reflect_macro.hpp>

#include "real_client.h"
#include "rpc_types.h"
#include "master_metric_manager.h"
#include "test_server_helpers.h"
#include "types.h"
#include "utils.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace fs = std::filesystem;

namespace mooncake {
namespace testing {

// ----- HTTP drain-job API response structs (mirrors master_admin_service)
// -----
struct HttpCreateDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCreateDrainJobResponse, success, job_id, status, error_code,
         error_message);

struct HttpQueryDrainJobResponse {
    bool success{false};
    std::string job_id;
    int32_t type{0};
    std::string type_name;
    int32_t status{0};
    std::string status_name;
    int64_t created_at_ms_epoch{0};
    int64_t last_updated_at_ms_epoch{0};
    std::vector<std::string> segments;
    uint64_t succeeded_units{0};
    uint64_t failed_units{0};
    uint64_t blocked_units{0};
    uint64_t active_units{0};
    uint64_t migrated_bytes{0};
    std::string message;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpQueryDrainJobResponse, success, job_id, type, type_name, status,
         status_name, created_at_ms_epoch, last_updated_at_ms_epoch, segments,
         succeeded_units, failed_units, blocked_units, active_units,
         migrated_bytes, message, error_code, error_message);

struct HttpSegmentStatusResponse {
    bool success{false};
    std::string segment;
    int32_t status{0};
    std::string status_name;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpSegmentStatusResponse, success, segment, status, status_name,
         error_code, error_message);

tl::expected<std::string, int> HttpPostJson(const std::string& url,
                                            const std::string& body) {
    coro_http::coro_http_client client;
    auto response = client.post(url, body, coro_http::req_content_type::json);
    if (response.status != 200) {
        return tl::unexpected(response.status);
    }
    return std::string(response.resp_body);
}

tl::expected<std::string, int> HttpGet(const std::string& url) {
    coro_http::coro_http_client client;
    auto response = client.get(url);
    if (response.status != 200) {
        return tl::unexpected(response.status);
    }
    return std::string(response.resp_body);
}

// Build a deterministic value whose bytes vary with BOTH position and a
// per-key seed. A byte-for-byte comparison of the drained data then detects
// intra-object slice mis-ordering or a wrong O_DIRECT read offset -- bugs a
// single-repeated-byte fill (e.g. string(size, 'A')) would silently pass.
std::string MakePatternValue(size_t size, uint32_t seed) {
    std::string v;
    v.resize(size);
    for (size_t j = 0; j < size; ++j) {
        v[j] = static_cast<char>((j * 31u + seed * 131u + 7u) & 0xFFu);
    }
    return v;
}

class LocalDiskDrainTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("LocalDiskDrainTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        tmp_root_ = fs::temp_directory_path() /
                    ("mc_lddrain_" + std::to_string(::getpid()));
        fs::create_directories(tmp_root_);

        // Force the client-side BucketStorageBackend to seal (flush) a bucket
        // to disk after every single offloaded object. By default a bucket
        // only flushes once it accumulates 500 keys or 256 MB, so the few
        // small objects this test writes would stay buffered in memory and
        // never appear on disk, making the LOCAL_DISK file assertion time out.
        // Setting the key limit to 1 makes each offloaded object flush
        // immediately, so the offload actually materializes on the client SSD.
        ::setenv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "1", /*overwrite=*/1);

        // Master must have offload enabled so the clients can offload their
        // MEMORY replicas to LOCAL_DISK (client-side SSD). We intentionally do
        // NOT set root_fs_dir: that would enable the master's *server-side*
        // DISK (NoF) offload, which writes DISK replicas under
        // root_fs_dir/mooncake_cluster and would shadow the LOCAL_DISK path
        // this test exercises.
        InProcMasterConfig config =
            InProcMasterConfigBuilder().set_enable_offload(true).build();
        ASSERT_TRUE(master_.Start(config))
            << "Failed to start in-proc master with offload enabled";
        master_address_ = master_.master_address();
        LOG(INFO) << "Started in-proc master at " << master_address_;
    }

    void TearDown() override {
        if (client1_) client1_->tearDownAll();
        if (client2_) client2_->tearDownAll();
        if (client3_) client3_->tearDownAll();
        master_.Stop();
        std::error_code ec;
        fs::remove_all(tmp_root_, ec);
    }

    void SetupOffloadClient(const std::string& addr, const fs::path& ssd_path,
                            std::shared_ptr<RealClient>& out) {
        fs::create_directories(ssd_path);
        const std::string rdma_devices =
            (FLAGS_protocol == "rdma") ? FLAGS_device_name : std::string("");
        out = RealClient::create();
        ASSERT_NE(out, nullptr);
        constexpr size_t kGlobalSegmentSize = 64 * 1024 * 1024;
        constexpr size_t kLocalBufferSize = 64 * 1024 * 1024;
        ASSERT_EQ(
            out->setup_real(addr, "P2PHANDSHAKE",
                            /*global_segment_size=*/kGlobalSegmentSize,
                            /*local_buffer_size=*/kLocalBufferSize,
                            FLAGS_protocol, rdma_devices, master_address_,
                            /*transfer_engine=*/nullptr,
                            /*ipc_socket_path=*/"",
                            /*enable_ssd_offload=*/true, ssd_path.string(),
                            /*tenant_id=*/"default"),
            0)
            << "Failed to setup offload client at " << addr;
        // Config trace: MEMORY segment + LOCAL_DISK SSD path/capacity.
        // total_size_limit for the SSD backend defaults to 2 TB
        // (storage_backend.h kDefault) when not explicitly overridden,
        // which is effectively unbounded for this test's 16 KB payload.
        LOG(INFO) << "[Setup] client=" << addr
                  << " MEMORY_segment_size=" << kGlobalSegmentSize << " ("
                  << kGlobalSegmentSize / 1024 << " KiB)"
                  << " local_buffer_size=" << kLocalBufferSize << " ("
                  << kLocalBufferSize / 1024 << " KiB)"
                  << " SSD_offload_path=" << ssd_path.string()
                  << " SSD_total_size_limit=default(2TB)";
    }

    tl::expected<HttpCreateDrainJobResponse, int> CreateDrainJobViaHttp(
        const CreateDrainJobRequest& request) {
        std::string body;
        struct_json::to_json(request, body);
        auto response = HttpPostJson(
            master_.http_metrics_base() + "/api/v1/drain_jobs", body);
        if (!response.has_value()) return tl::unexpected(response.error());
        HttpCreateDrainJobResponse parsed;
        struct_json::from_json(parsed, response.value());
        return parsed;
    }

    tl::expected<HttpQueryDrainJobResponse, int> QueryDrainJobViaHttp(
        const std::string& job_id) {
        auto response = HttpGet(master_.http_metrics_base() +
                                "/api/v1/drain_jobs/query?job_id=" + job_id);
        if (!response.has_value()) return tl::unexpected(response.error());
        HttpQueryDrainJobResponse parsed;
        struct_json::from_json(parsed, response.value());
        return parsed;
    }

    tl::expected<HttpSegmentStatusResponse, int> QuerySegmentStatusViaHttp(
        const std::string& segment_name) {
        auto response =
            HttpGet(master_.http_metrics_base() +
                    "/api/v1/segments/status?segment=" + segment_name);
        if (!response.has_value()) return tl::unexpected(response.error());
        HttpSegmentStatusResponse parsed;
        struct_json::from_json(parsed, response.value());
        return parsed;
    }

    bool WaitForJobCompletionViaHttp(
        const std::string& job_id, HttpQueryDrainJobResponse* final_job,
        std::chrono::seconds timeout = std::chrono::seconds(120)) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            auto query_result = QueryDrainJobViaHttp(job_id);
            if (query_result.has_value()) {
                if (query_result->status_name == "SUCCEEDED" ||
                    query_result->status_name == "FAILED" ||
                    query_result->status_name == "CANCELED") {
                    if (final_job != nullptr) *final_job = query_result.value();
                    return query_result->status_name == "SUCCEEDED";
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    // Poll until the local-disk directory contains at least one regular file.
    // This proves the offload path actually wrote LOCAL_DISK replica data to
    // the client's SSD. We cannot use get_replica_desc()/Query() for this:
    // that RPC is for transfer planning and intentionally returns only
    // transferable replicas (MEMORY + DISK/NoF), never LOCAL_DISK replicas.
    bool WaitForLocalDiskFiles(
        const fs::path& ssd_path,
        std::chrono::seconds timeout = std::chrono::seconds(120)) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        int poll = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            std::error_code ec;
            bool has_files = false;
            for (auto it = fs::recursive_directory_iterator(ssd_path, ec);
                 it != fs::recursive_directory_iterator(); ++it) {
                if (it->is_regular_file()) {
                    has_files = true;
                    break;
                }
            }
            if (has_files) return true;
            if ((++poll % 10) == 0) {
                LOG(INFO) << "[WaitForLocalDiskFiles] poll=" << poll
                          << " ssd_path=" << ssd_path.string()
                          << " still empty";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        LOG(ERROR) << "[WaitForLocalDiskFiles] TIMEOUT: no LOCAL_DISK files in "
                   << ssd_path.string();
        // Dump the whole temp tree so we can see where the offloaded files
        // actually landed.
        {
            std::error_code ec;
            std::string tree;
            for (auto it = fs::recursive_directory_iterator(tmp_root_, ec);
                 it != fs::recursive_directory_iterator(); ++it) {
                tree += "\n  " + it->path().string();
            }
            LOG(ERROR) << "[WaitForLocalDiskFiles] tmp_root tree:" << tree;
        }
        return false;
    }

    // Count regular files and total bytes under an SSD offload directory.
    // Used to leave a trace of how many LOCAL_DISK replica files physically
    // landed on each client's SSD before/after drain.
    struct SsdFileStats {
        size_t file_count{0};
        uint64_t total_bytes{0};
    };
    SsdFileStats CountSsdFiles(const fs::path& ssd_path) {
        SsdFileStats stats;
        std::error_code ec;
        for (auto it = fs::recursive_directory_iterator(ssd_path, ec);
             it != fs::recursive_directory_iterator(); ++it) {
            if (it->is_regular_file()) {
                stats.file_count++;
                stats.total_bytes += it->file_size(ec);
            }
        }
        return stats;
    }

    InProcMaster master_;
    std::string master_address_;
    std::shared_ptr<RealClient> client1_;
    std::shared_ptr<RealClient> client2_;
    std::shared_ptr<RealClient> client3_;
    fs::path tmp_root_;
};

TEST_F(LocalDiskDrainTest, DrainLocalDiskReplicasToPeer) {
    const std::string client1_addr = "127.0.0.1:17813";
    const std::string client2_addr = "127.0.0.1:17814";
    fs::path ssd1 = tmp_root_ / "client1_ssd";
    fs::path ssd2 = tmp_root_ / "client2_ssd";

    SetupOffloadClient(client1_addr, ssd1, client1_);
    SetupOffloadClient(client2_addr, ssd2, client2_);

    // Register a LOCAL_DISK (SSD offload) segment for client1 on the master.
    // Without this, the master never allocates LOCAL_DISK replicas for
    // client1's objects and never pushes offload tasks to client1's
    // FileStorage, so nothing is ever written to ssd1. This is the missing
    // piece that makes this test exercise real LOCAL_DISK data, not just
    // MEMORY replicas.
    ASSERT_EQ(client1_->mountLocalDiskSegment(true), 0)
        << "Failed to mount LOCAL_DISK segment on client1";

    // Phase 1: Put keys on client1. With offload enabled the master assigns
    // LOCAL_DISK replicas and client1 writes them to its local disk.
    // CRITICAL: set preferred_segment = client1_addr so the master allocator
    // places the MEMORY replica on client1 (not client2). Without this, the
    // allocator may spread replicas across both clients, causing only a subset
    // of keys to be on the drain source and making succeeded_units flaky.
    constexpr int kKeyCount = 4;
    constexpr size_t kValueSize = 4096;
    constexpr const char* kSourceSegment = "127.0.0.1:17813";
    std::vector<std::string> keys;
    std::vector<std::string> values;
    for (int i = 0; i < kKeyCount; ++i) {
        std::string key = "ld_drain_key_" + std::to_string(i);
        std::string value = MakePatternValue(kValueSize, i);
        std::span<const char> span(value.data(), value.size());
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kSourceSegment;
        ASSERT_EQ(client1_->put(key, span, config), 0)
            << "Put failed for " << key;
        keys.push_back(key);
        values.push_back(value);
    }
    // Data trace: total payload and per-replica footprint.
    // With offload enabled, each key produces 1 MEMORY + 1 LOCAL_DISK
    // replica on client1, so the on-wire footprint is 2x payload.
    LOG(INFO) << "[Phase 1] Put " << kKeyCount << " keys x " << kValueSize
              << " bytes = " << (uint64_t)kKeyCount * kValueSize << " bytes ("
              << (uint64_t)kKeyCount * kValueSize / 1024 << " KiB)"
              << " preferred_segment=" << kSourceSegment
              << "; with offload, client1 holds MEMORY+LOCAL_DISK replicas = "
              << 2 * (uint64_t)kKeyCount * kValueSize << " bytes ("
              << 2 * (uint64_t)kKeyCount * kValueSize / 1024 << " KiB)";

    // Phase 2: Wait for the LOCAL_DISK replicas to be physically written to
    // client1's SSD. This is the core assertion that distinguishes this test
    // from the in-memory drain test. We verify it via the filesystem (the
    // offload path writes object files under ssd1), because get_replica_desc/
    // Query intentionally does NOT expose LOCAL_DISK replicas.
    ASSERT_TRUE(WaitForLocalDiskFiles(ssd1, std::chrono::seconds(120)))
        << "LOCAL_DISK replicas were not offloaded to client1's disk";

    // Trace: confirm how many LOCAL_DISK files landed on client1's SSD
    // before drain, and that client2's SSD is still empty.
    {
        const auto stats1 = CountSsdFiles(ssd1);
        const auto stats2 = CountSsdFiles(ssd2);
        LOG(INFO) << "[Phase 2] Pre-drain SSD footprint:"
                  << " client1_ssd files=" << stats1.file_count
                  << " bytes=" << stats1.total_bytes
                  << " | client2_ssd files=" << stats2.file_count
                  << " bytes=" << stats2.total_bytes;
    }

    // Wait for the KV lease to expire before creating the drain job.
    // ScheduleDrainJobTasks blocks any key whose lease is still active
    // (master_service.cpp ScheduleDrainJobTasks: IsHardPinned ||
    // !IsLeaseExpired || !AllReplicas || has_repl_task). The default TTL is
    // DEFAULT_DEFAULT_KV_LEASE_TTL = 5000ms (types.h). Sleeping 6s guarantees
    // all 4 keys are eligible on the first scheduling pass, so
    // succeeded_units converges to kKeyCount instead of fluctuating.
    std::this_thread::sleep_for(std::chrono::seconds(6));

    // The drain job is keyed by the client's *MEMORY* segment name (the
    // address the segment is registered under on the master), NOT the offload
    // RPC address. CreateDrainJob validates the source against the registered
    // segment table and sets it to DRAINING; the offload RPC address is not a
    // registered segment and would be rejected. The drain scheduler maps this
    // MEMORY segment to its owning client_id and, once the MEMORY replica has
    // moved, drains that client's LOCAL_DISK replica by its transport_endpoint
    // internally (see MasterService drain Option A logic).
    const std::string source_segment = client1_addr;
    const std::string target_segment = client2_addr;
    LOG(INFO) << "Draining source_segment=" << source_segment
              << " -> target_segment=" << target_segment;

    // Phase 3: Sanity check - data is readable on the source before drain.
    {
        auto alloc = client1_->client_buffer_allocator_->allocate(kValueSize);
        ASSERT_TRUE(alloc.has_value());
        auto* dst = static_cast<char*>(alloc->ptr());
        auto n = client1_->get_into(keys[0], dst, kValueSize);
        ASSERT_EQ(n, static_cast<int64_t>(kValueSize))
            << "Pre-drain get from source failed";
        ASSERT_EQ(std::string(dst, kValueSize), values[0]);
        LOG(INFO) << "[Phase 3] Sanity check OK: key=" << keys[0]
                  << " readable from client1 (pre-drain)";
    }

    // Phase 4: Create the drain job over the master admin HTTP API.
    CreateDrainJobRequest request;
    request.segments = {source_segment};
    request.target_segments = {target_segment};
    request.max_concurrency = 1;
    auto create_res = CreateDrainJobViaHttp(request);
    ASSERT_TRUE(create_res.has_value()) << "CreateDrainJob HTTP call failed";
    ASSERT_TRUE(create_res->success) << create_res->error_message;
    EXPECT_EQ(create_res->status, "CREATED");

    // Phase 5: Wait for the drain job to finish.
    HttpQueryDrainJobResponse final_job;
    ASSERT_TRUE(WaitForJobCompletionViaHttp(create_res->job_id, &final_job,
                                            std::chrono::seconds(120)))
        << "Drain job did not complete (status="
        << (final_job.status_name.empty() ? "<timeout>" : final_job.status_name)
        << ")";
    EXPECT_EQ(final_job.status_name, "SUCCEEDED");
    EXPECT_EQ(final_job.failed_units, 0u);
    // All kKeyCount keys must have been migrated. With preferred_segment set
    // and lease expired, ScheduleDrainJobTasks should not block or skip any
    // key. A lower value would indicate a regression (e.g. lease not expired,
    // allocator scattered replicas, or MaybeCompleteDrainJob terminating
    // early).
    EXPECT_EQ(final_job.succeeded_units, static_cast<uint64_t>(kKeyCount))
        << "Expected all " << kKeyCount
        << " keys to be migrated, got succeeded_units="
        << final_job.succeeded_units;
    // Trace: drain result summary from master's POV.
    LOG(INFO) << "[Phase 5] Drain job " << final_job.job_id
              << " status=" << final_job.status_name
              << " succeeded_units=" << final_job.succeeded_units
              << " failed_units=" << final_job.failed_units
              << " blocked_units=" << final_job.blocked_units
              << " migrated_bytes=" << final_job.migrated_bytes
              << " message=" << final_job.message;

    // The source segment must transition to DRAINED, which only happens when
    // the master sees *no* replicas (neither MEMORY nor LOCAL_DISK) belonging
    // to the drained client. This proves the redundant LOCAL_DISK metadata was
    // dropped on the source (its SSD file is intentionally retained as a safety
    // net) and the client is fully drained.
    {
        auto drained_status = QuerySegmentStatusViaHttp(source_segment);
        ASSERT_TRUE(drained_status.has_value())
            << "Failed to query source segment status over HTTP";
        EXPECT_TRUE(drained_status->success);
        EXPECT_EQ(drained_status->status_name, "DRAINED");
    }

    // Phase 6: After drain, every key must be byte-for-byte readable from the
    // target client (data survived the LOCAL_DISK -> peer migration).
    for (int i = 0; i < kKeyCount; ++i) {
        auto alloc = client2_->client_buffer_allocator_->allocate(kValueSize);
        ASSERT_TRUE(alloc.has_value());
        auto* dst = static_cast<char*>(alloc->ptr());
        auto n = client2_->get_into(keys[i], dst, kValueSize);
        ASSERT_EQ(n, static_cast<int64_t>(kValueSize))
            << "Get from target failed for " << keys[i];
        ASSERT_EQ(std::string(dst, kValueSize), values[i])
            << "Data mismatch after drain for " << keys[i];
    }
    // Trace: confirm migration destination = client2 MEMORY.
    // Data moved: 4 keys x 4 KiB = 16 KiB from client1 (MEMORY+LOCAL_DISK)
    // to client2 (MEMORY only, per MEMORY-first dedup design).
    {
        const auto stats1 = CountSsdFiles(ssd1);
        const auto stats2 = CountSsdFiles(ssd2);
        LOG(INFO) << "[Phase 6] Post-drain footprint:"
                  << " client1_ssd residual_files=" << stats1.file_count
                  << " bytes=" << stats1.total_bytes
                  << " (safety-net retained, expected non-zero)"
                  << " | client2_ssd files=" << stats2.file_count
                  << " bytes=" << stats2.total_bytes
                  << " (expected 0: target stores MEMORY replicas only)";
        LOG(INFO)
            << "[Phase 6] Data migrated: " << kKeyCount << " keys x "
            << kValueSize << " bytes = " << (uint64_t)kKeyCount * kValueSize
            << " bytes (" << (uint64_t)kKeyCount * kValueSize / 1024 << " KiB)"
            << " from client1 MEMORY -> client2 MEMORY"
            << " (LOCAL_DISK dedup: only MEMORY moved, LD metadata dropped)";
    }

    // Phase 7: Source-side SSD cleanup after LOCAL_DISK migration is a known
    // follow-up (see RFC: "Source SSD cleanup ... residual SSD data is
    // harmless"). So we only log whether residual files remain; we do NOT
    // assert their absence. The correctness guarantee verified above is that
    // the LOCAL_DISK replica was migrated (drain SUCCEEDED) and the data is
    // byte-for-byte readable from the target client.
    {
        const auto stats1 = CountSsdFiles(ssd1);
        LOG(INFO)
            << "[Phase 7] Source client1 residual LOCAL_DISK files on ssd1: "
            << stats1.file_count << " files / " << stats1.total_bytes
            << " bytes "
            << (stats1.file_count > 0
                    ? "(present, expected - safety net, not cleaned)"
                    : "(none)");
    }
}

// -------------------------------------------------------------------
// LocalDiskDrainEvictTest
//
// Large-data eviction tests: fill a 64 MB MEMORY segment with 4 MB
// objects, trigger eviction via allocation failure, then drain the
// resulting LOCAL_DISK replicas to a peer client.
//
// Two scenarios:
//   1. offload_on_evict=true  鈥?offload deferred to eviction time
//   2. offload_on_evict=false 鈥?offload immediate at PutEnd
// -------------------------------------------------------------------
// A glog sink that captures log messages in-process so the test can assert
// that the LOCAL_DISK drain path emitted its success log
// (action=replica_move_local_disk_transfer_success). This is DIRECT evidence
// that data flowed local-disk SSD -> staging buffer -> TransferWrite, rather
// than an inference from the memory ratio.
class CapturingLogSink : public google::LogSink {
   public:
    void send(google::LogSeverity /*severity*/, const char* /*full_filename*/,
              const char* /*base_filename*/, int /*line*/,
              const struct ::tm* /*tm_time*/, const char* message,
              size_t message_len) override {
        std::lock_guard<std::mutex> lock(mutex_);
        messages_.emplace_back(message, message_len);
    }
    int CountContaining(const std::string& substr) {
        std::lock_guard<std::mutex> lock(mutex_);
        int count = 0;
        for (const auto& m : messages_) {
            if (m.find(substr) != std::string::npos) ++count;
        }
        return count;
    }

   private:
    std::vector<std::string> messages_;
    std::mutex mutex_;
};

// -------------------------------------------------------------------
// Manual (non-drain) Move must NOT drop the source client's LOCAL_DISK
// backup. Only drain-originated moves drop the redundant LOCAL_DISK
// metadata (MoveEnd is gated on is_drain). This guards the public
// create_move_task API against silently discarding a durable SSD backup
// (and leaking its file) on a still-live node.
//
// Strategy: manually move every key's MEMORY replica client1 -> client2,
// then drain client1 -> client3. If the manual move preserved client1's
// LOCAL_DISK replicas, the drain migrates them via the LOCAL_DISK path
// (emitting local_disk_transfer_success) and the data becomes readable on
// client3. If the LOCAL_DISK metadata had been wrongly dropped by the
// manual move, the drain would find nothing to migrate, succeeded_units
// would be 0, and client3 would never receive the data.
// -------------------------------------------------------------------
TEST_F(LocalDiskDrainTest, ManualMovePreservesLocalDiskBackup) {
    const std::string client1_addr = "127.0.0.1:17813";
    const std::string client2_addr = "127.0.0.1:17814";
    const std::string client3_addr = "127.0.0.1:17815";
    fs::path ssd1 = tmp_root_ / "c1_ssd";
    fs::path ssd2 = tmp_root_ / "c2_ssd";
    fs::path ssd3 = tmp_root_ / "c3_ssd";

    SetupOffloadClient(client1_addr, ssd1, client1_);
    SetupOffloadClient(client2_addr, ssd2, client2_);
    SetupOffloadClient(client3_addr, ssd3, client3_);
    ASSERT_EQ(client1_->mountLocalDiskSegment(true), 0)
        << "Failed to mount LOCAL_DISK segment on client1";

    // Phase 1: Put keys on client1 (MEMORY + LOCAL_DISK on client1).
    constexpr int kKeyCount = 4;
    // Mix of 4096-aligned and deliberately NON-4096-aligned object sizes.
    // These keys become LOCAL_DISK-only after the manual move below, so the
    // drain migrates them through LoadBatchFromLocalDisk ->
    // BucketStorageBackend::BatchLoad, exercising the O_DIRECT offset/size
    // correction that a uniform 4096 size would never stress.
    const size_t kSizes[kKeyCount] = {4096, 7001, 12000, 5000};
    const size_t kMaxSize = 12000;
    std::vector<std::string> keys, values;
    std::vector<size_t> sizes;
    for (int i = 0; i < kKeyCount; ++i) {
        std::string key = "manual_move_key_" + std::to_string(i);
        // Position- AND key-varying pattern so a byte-for-byte compare detects
        // intra-object slice mis-ordering / wrong O_DIRECT offset.
        std::string value = MakePatternValue(kSizes[i], i);
        std::span<const char> span(value.data(), value.size());
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = client1_addr;
        ASSERT_EQ(client1_->put(key, span, config), 0)
            << "Put failed for " << key;
        keys.push_back(key);
        values.push_back(value);
        sizes.push_back(kSizes[i]);
    }
    ASSERT_TRUE(WaitForLocalDiskFiles(ssd1, std::chrono::seconds(120)))
        << "LOCAL_DISK replicas were not offloaded to client1";

    // Phase 2: Manually move each key's MEMORY replica client1 -> client2.
    // create_move_task => is_drain=false, so the LOCAL_DISK replica on
    // client1 must be preserved by MoveEnd.
    for (int i = 0; i < kKeyCount; ++i) {
        auto task =
            client1_->create_move_task(keys[i], client1_addr, client2_addr);
        ASSERT_TRUE(task.has_value())
            << "create_move_task failed for " << keys[i];
        bool done = false;
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline) {
            auto q = client1_->query_task(task.value());
            if (q.has_value() && (q->status == TaskStatus::SUCCESS ||
                                  q->status == TaskStatus::FAILED)) {
                ASSERT_EQ(q->status, TaskStatus::SUCCESS)
                    << "manual move task did not succeed for " << keys[i];
                done = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        ASSERT_TRUE(done) << "manual move task timed out for " << keys[i];
    }

    // Phase 3: Verify each key moved to client2 (MEMORY).
    for (int i = 0; i < kKeyCount; ++i) {
        auto alloc = client2_->client_buffer_allocator_->allocate(kMaxSize);
        ASSERT_TRUE(alloc.has_value());
        auto* dst = static_cast<char*>(alloc->ptr());
        auto n = client2_->get_into(keys[i], dst, sizes[i]);
        ASSERT_EQ(n, static_cast<int64_t>(sizes[i]))
            << "post-manual-move get from client2 failed for " << keys[i];
        ASSERT_EQ(std::string(dst, sizes[i]), values[i]);
    }
    LOG(INFO) << "[ManualMove] moved " << kKeyCount
              << " MEMORY replicas client1 -> client2 (is_drain=false); "
                 "client1 LOCAL_DISK backups expected to be preserved";

    // Phase 4: Wait for KV leases to expire so the drain scheduler will not
    // block the keys (default TTL 5s).
    std::this_thread::sleep_for(std::chrono::seconds(6));

    // Phase 5: Drain client1 -> client3. Capture logs to prove the
    // LOCAL_DISK path ran.
    CapturingLogSink sink;
    google::AddLogSink(&sink);
    HttpQueryDrainJobResponse final_job;
    {
        CreateDrainJobRequest request;
        request.segments = {client1_addr};
        request.target_segments = {client3_addr};
        request.max_concurrency = 1;
        auto create_res = CreateDrainJobViaHttp(request);
        ASSERT_TRUE(create_res.has_value());
        ASSERT_TRUE(create_res->success) << create_res->error_message;
        bool ok = WaitForJobCompletionViaHttp(create_res->job_id, &final_job,
                                              std::chrono::seconds(120));
        google::RemoveLogSink(&sink);
        ASSERT_TRUE(ok) << "Drain did not complete (status="
                        << (final_job.status_name.empty()
                                ? "<timeout>"
                                : final_job.status_name)
                        << ")";
    }
    EXPECT_EQ(final_job.status_name, "SUCCEEDED");
    EXPECT_EQ(final_job.failed_units, 0u);
    // All keys must migrate via the LOCAL_DISK path. If the manual move had
    // wrongly dropped the LOCAL_DISK metadata, there would be nothing to
    // migrate and succeeded_units would be 0.
    EXPECT_EQ(final_job.succeeded_units, static_cast<uint64_t>(kKeyCount))
        << "Expected all " << kKeyCount
        << " LOCAL_DISK replicas (preserved by the manual move) to be drained";

    // Direct evidence the LOCAL_DISK drain path ran for every key.
    const int local_disk_transfers =
        sink.CountContaining("local_disk_transfer_success");
    LOG(INFO) << "[ManualMove] local_disk_transfer_success count="
              << local_disk_transfers << " (expect >= " << kKeyCount << ")";
    EXPECT_GE(local_disk_transfers, kKeyCount)
        << "Manual move must preserve LOCAL_DISK; drain should then migrate "
           "all "
        << kKeyCount << " keys via the LOCAL_DISK path";

    // Source segment must reach DRAINED (no MEMORY, no LOCAL_DISK left).
    {
        auto st = QuerySegmentStatusViaHttp(client1_addr);
        ASSERT_TRUE(st.has_value());
        EXPECT_EQ(st->status_name, "DRAINED");
    }

    // Phase 6: Data must be byte-for-byte readable from client3 (the drain
    // target), proving the preserved LOCAL_DISK data survived migration.
    for (int i = 0; i < kKeyCount; ++i) {
        auto alloc = client3_->client_buffer_allocator_->allocate(kMaxSize);
        ASSERT_TRUE(alloc.has_value());
        auto* dst = static_cast<char*>(alloc->ptr());
        auto n = client3_->get_into(keys[i], dst, sizes[i]);
        ASSERT_EQ(n, static_cast<int64_t>(sizes[i]))
            << "get from drain target client3 failed for " << keys[i];
        ASSERT_EQ(std::string(dst, sizes[i]), values[i])
            << "data mismatch on client3 for " << keys[i];
    }
}

// -------------------------------------------------------------------
// Failure path: when a LOCAL_DISK-only replica's backing SSD file is gone at
// drain time, LoadBatchFromLocalDisk fails and the drain unit must fail
// *gracefully*: the job must reach a terminal FAILED state (never hang), the
// source segment must NOT be marked DRAINED, no phantom success is counted,
// and the live (MEMORY) copy of the data must remain intact. This is the ONLY
// test that exercises the LOCAL_DISK transfer error/revoke branch in
// Client::ExecuteReplicaTransfer -- the happy-path tests never reach it.
//
// Setup: put keys on client1 (MEMORY + LOCAL_DISK), manually move the MEMORY
// replica client1 -> client2 (is_drain=false, so client1 keeps its
// LOCAL_DISK-only replica), DELETE client1's backing SSD files to simulate a
// lost/corrupt file, then drain client1 -> client3.
// -------------------------------------------------------------------
TEST_F(LocalDiskDrainTest, DrainFailsGracefullyWhenLocalDiskDataMissing) {
    const std::string client1_addr = "127.0.0.1:17813";
    const std::string client2_addr = "127.0.0.1:17814";
    const std::string client3_addr = "127.0.0.1:17815";
    fs::path ssd1 = tmp_root_ / "c1_ssd";
    fs::path ssd2 = tmp_root_ / "c2_ssd";
    fs::path ssd3 = tmp_root_ / "c3_ssd";

    SetupOffloadClient(client1_addr, ssd1, client1_);
    SetupOffloadClient(client2_addr, ssd2, client2_);
    SetupOffloadClient(client3_addr, ssd3, client3_);
    ASSERT_EQ(client1_->mountLocalDiskSegment(true), 0);

    constexpr int kKeyCount = 4;
    constexpr size_t kValueSize = 4096;
    std::vector<std::string> keys, values;
    for (int i = 0; i < kKeyCount; ++i) {
        std::string key = "missing_ld_key_" + std::to_string(i);
        std::string value = MakePatternValue(kValueSize, 100 + i);
        std::span<const char> span(value.data(), value.size());
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = client1_addr;
        ASSERT_EQ(client1_->put(key, span, config), 0) << "Put failed " << key;
        keys.push_back(key);
        values.push_back(value);
    }
    ASSERT_TRUE(WaitForLocalDiskFiles(ssd1)) << "offload did not materialize";

    // Move the MEMORY replica off client1 so client1 is LOCAL_DISK-only.
    for (int i = 0; i < kKeyCount; ++i) {
        auto task =
            client1_->create_move_task(keys[i], client1_addr, client2_addr);
        ASSERT_TRUE(task.has_value());
        bool done = false;
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline) {
            auto q = client1_->query_task(task.value());
            if (q.has_value() && (q->status == TaskStatus::SUCCESS ||
                                  q->status == TaskStatus::FAILED)) {
                ASSERT_EQ(q->status, TaskStatus::SUCCESS);
                done = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        ASSERT_TRUE(done) << "manual move timed out for " << keys[i];
    }

    // Inject the fault: delete every backing file under client1's SSD dir.
    // client1 is now LOCAL_DISK-only for these keys; the live MEMORY copy is
    // on client2. A subsequent LOCAL_DISK load on client1 must now fail at
    // open()/read() (BucketStorageBackend::BatchLoad reopens the file fresh).
    {
        std::error_code ec;
        size_t removed = 0;
        for (auto it = fs::recursive_directory_iterator(ssd1, ec);
             it != fs::recursive_directory_iterator(); ++it) {
            if (it->is_regular_file()) {
                std::error_code rec;
                if (fs::remove(it->path(), rec)) ++removed;
            }
        }
        LOG(INFO) << "[MissingLD] deleted " << removed
                  << " backing file(s) under " << ssd1.string();
        ASSERT_GT(removed, 0u) << "no SSD files to delete; setup invalid";
    }

    // Let KV leases expire so the scheduler will actually plan the units.
    std::this_thread::sleep_for(std::chrono::seconds(6));

    // Drain client1 -> client3. The LOCAL_DISK load must fail for every key.
    CapturingLogSink sink;
    google::AddLogSink(&sink);
    HttpQueryDrainJobResponse final_job;
    bool completed_ok = false;
    {
        CreateDrainJobRequest request;
        request.segments = {client1_addr};
        request.target_segments = {client3_addr};
        request.max_concurrency = 1;
        auto create_res = CreateDrainJobViaHttp(request);
        ASSERT_TRUE(create_res.has_value());
        ASSERT_TRUE(create_res->success) << create_res->error_message;
        // Poll for ANY terminal state; we expect FAILED, not SUCCEEDED.
        completed_ok = WaitForJobCompletionViaHttp(
            create_res->job_id, &final_job, std::chrono::seconds(150));
        google::RemoveLogSink(&sink);
    }

    // 1) The job must reach a TERMINAL state (no hang). final_job.status_name
    //    is populated only when a terminal state was observed.
    ASSERT_FALSE(final_job.status_name.empty())
        << "drain job never reached a terminal state (hang?)";
    // 2) It must be FAILED, not SUCCEEDED: the LOCAL_DISK data is unreadable.
    EXPECT_FALSE(completed_ok);
    EXPECT_EQ(final_job.status_name, "FAILED");
    EXPECT_EQ(final_job.succeeded_units, 0u)
        << "no key could migrate; succeeded_units must be 0";
    EXPECT_GT(final_job.failed_units, 0u);

    // 3) The LOCAL_DISK load-failure branch must have been taken.
    EXPECT_GT(sink.CountContaining("local_disk_load_failed"), 0)
        << "expected the LOCAL_DISK load-failure error branch to be hit";

    // 4) The source segment must NOT be DRAINED (it still owns LD replicas
    //    that could not be migrated).
    {
        auto st = QuerySegmentStatusViaHttp(client1_addr);
        ASSERT_TRUE(st.has_value());
        EXPECT_NE(st->status_name, "DRAINED")
            << "segment must not be DRAINED when LOCAL_DISK migration failed";
    }

    // 5) The live copy is intact: client2 still serves the data byte-for-byte,
    //    proving the failed drain did not corrupt or drop the surviving
    //    replica.
    for (int i = 0; i < kKeyCount; ++i) {
        auto alloc = client2_->client_buffer_allocator_->allocate(kValueSize);
        ASSERT_TRUE(alloc.has_value());
        auto* dst = static_cast<char*>(alloc->ptr());
        auto n = client2_->get_into(keys[i], dst, kValueSize);
        ASSERT_EQ(n, static_cast<int64_t>(kValueSize))
            << "live MEMORY copy on client2 lost for " << keys[i];
        ASSERT_EQ(std::string(dst, kValueSize), values[i])
            << "live copy corrupted for " << keys[i];
    }
}

class LocalDiskDrainEvictTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("LocalDiskDrainEvictTest");
        FLAGS_logtostderr = 1;
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        // Reset the process-wide memory metrics so this test starts from a
        // clean slate. MasterMetricManager is a singleton; residual capacity
        // left over from earlier tests (whose segment unmounts can fail
        // during teardown with RPC_FAIL) would otherwise inflate
        // mem_total_capacity_ and skew get_global_mem_used_ratio(), which
        // would prevent the watermark-based eviction from triggering.
        MasterMetricManager::instance().reset_total_mem_capacity();
        MasterMetricManager::instance().reset_allocated_mem_size();
        google::AddLogSink(&log_sink_);
        tmp_root_ = fs::temp_directory_path() /
                    ("mc_ldevict_" + std::to_string(::getpid()));
        fs::create_directories(tmp_root_);
        ::setenv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "1", 1);
    }

    void TearDown() override {
        google::RemoveLogSink(&log_sink_);
        if (client1_) client1_->tearDownAll();
        if (client2_) client2_->tearDownAll();
        master_.Stop();
        std::error_code ec;
        fs::remove_all(tmp_root_, ec);
    }

    void SetupClient(const std::string& addr, const fs::path& ssd_path,
                     std::shared_ptr<RealClient>& out,
                     size_t seg_size = 64 * 1024 * 1024) {
        fs::create_directories(ssd_path);
        const std::string rdma_devices =
            (FLAGS_protocol == "rdma") ? FLAGS_device_name : std::string("");
        out = RealClient::create();
        ASSERT_NE(out, nullptr);
        constexpr size_t kBufSize = 64 * 1024 * 1024;
        ASSERT_EQ(out->setup_real(addr, "P2PHANDSHAKE", seg_size, kBufSize,
                                  FLAGS_protocol, rdma_devices,
                                  master_.master_address(), nullptr, "", true,
                                  ssd_path.string(), "default"),
                  0)
            << "setup_real failed for " << addr;
    }

    struct SsdStats {
        size_t files{0};
        uint64_t bytes{0};
    };
    SsdStats CountSsd(const fs::path& p) {
        SsdStats s;
        std::error_code ec;
        for (auto it = fs::recursive_directory_iterator(p, ec);
             it != fs::recursive_directory_iterator(); ++it) {
            if (it->is_regular_file()) {
                s.files++;
                s.bytes += it->file_size(ec);
            }
        }
        return s;
    }

    bool WaitFiles(const fs::path& p,
                   std::chrono::seconds timeout = std::chrono::seconds(120)) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::error_code ec;
            for (auto it = fs::recursive_directory_iterator(p, ec);
                 it != fs::recursive_directory_iterator(); ++it) {
                if (it->is_regular_file()) return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return false;
    }

    // Fill client1's 64 MB segment (the ONLY mounted segment) with 4 MB
    // objects. Since total capacity = 64 MB, writing 64 MB pushes the
    // global mem ratio to 1.0 > watermark (0.95), triggering eviction
    // automatically via the eviction thread (10 ms cycle). No "trigger
    // Put" needed.
    void RunEvictionDrainTest(bool offload_on_evict) {
        const std::string src_addr = "127.0.0.1:17813";
        const std::string dst_addr = "127.0.0.1:17814";
        fs::path ssd1 = tmp_root_ / "c1_ssd";
        fs::path ssd2 = tmp_root_ / "c2_ssd";

        InProcMasterConfig cfg = InProcMasterConfigBuilder()
                                     .set_enable_offload(true)
                                     .set_offload_on_evict(offload_on_evict)
                                     .set_eviction_high_watermark_ratio(0.5)
                                     .build();
        ASSERT_TRUE(master_.Start(cfg));

        // Only set up client1 initially. Total capacity = 64 MB.
        SetupClient(src_addr, ssd1, client1_);
        ASSERT_EQ(client1_->mountLocalDiskSegment(true), 0);

        constexpr size_t kValSize = 4 * 1024 * 1024;  // 4 MB
        constexpr int kKeyCount = 16;                 // 16 x 4 MB = 64 MB
        const std::string src_seg = src_addr;

        // Phase 1: Fill the segment. Ratio reaches 1.0 > 0.95 watermark.
        std::vector<std::string> keys, vals;
        for (int i = 0; i < kKeyCount; ++i) {
            std::string k = "evict_key_" + std::to_string(i);
            std::string v = MakePatternValue(kValSize, i);
            std::span<const char> sp(v.data(), v.size());
            ReplicateConfig rc;
            rc.replica_num = 1;
            rc.preferred_segment = src_seg;
            ASSERT_EQ(client1_->put(k, sp, rc), 0) << "Put failed for " << k;
            keys.push_back(k);
            vals.push_back(v);
        }
        LOG(INFO) << "[Phase 1] offload_on_evict=" << offload_on_evict
                  << " stored=" << kKeyCount << " x " << kValSize
                  << " bytes = " << (uint64_t)kKeyCount * kValSize
                  << " bytes (segment full, ratio=1.0 > watermark 0.95)";

        // Phase 1.5: Verify every key is readable from client1 right after
        // Put. This proves the first leg of EVERY key's route: Put lands the
        // data in client1's MEMORY (segment 17813), regardless of whether the
        // key is later evicted to LOCAL_DISK or stays in memory.
        {
            auto alloc = client1_->client_buffer_allocator_->allocate(kValSize);
            ASSERT_TRUE(alloc.has_value());
            auto* dst = static_cast<char*>(alloc->ptr());
            for (int i = 0; i < kKeyCount; ++i) {
                auto n = client1_->get_into(keys[i], dst, kValSize);
                ASSERT_EQ(n, static_cast<int64_t>(kValSize))
                    << "post-Put get_into failed for " << keys[i];
                ASSERT_EQ(std::string(dst, kValSize), vals[i])
                    << "post-Put data mismatch for " << keys[i];
            }
            LOG(INFO) << "[Phase 1.5] All " << kKeyCount
                      << " keys verified in client1 MEMORY right after Put "
                         "(route leg 1: Put -> client1 mem)";
        }

        // Phase 2: For immediate-offload, SSD files exist right away.
        if (!offload_on_evict) {
            ASSERT_TRUE(WaitFiles(ssd1))
                << "Immediate offload: SSD files not found";
            auto s = CountSsd(ssd1);
            LOG(INFO) << "[Phase 2] Immediate offload SSD: files=" << s.files
                      << " bytes=" << s.bytes;
        }

        // Phase 3: Wait for KV leases to expire (TTL = 5 s). The eviction
        // thread (10 ms cycle) will detect ratio > 0.5 and evict objects
        // whose leases have expired.
        std::this_thread::sleep_for(std::chrono::seconds(6));

        // Phase 4: Wait for the offload to materialise on SSD.
        // With offload_on_evict=true, eviction pushes objects to the
        // offloading queue; the client writes them to SSD asynchronously.
        // With offload_on_evict=false, SSD files already exist (Phase 2).
        ASSERT_TRUE(WaitFiles(ssd1, std::chrono::seconds(120)))
            << "SSD files not found after eviction";
        {
            auto s = CountSsd(ssd1);
            LOG(INFO) << "[Phase 4] Post-eviction SSD: files=" << s.files
                      << " bytes=" << s.bytes;
        }

        // Phase 4.5: Wait until eviction has actually FREED the MEMORY
        // replicas. CRITICAL: this must happen BEFORE client2 is mounted.
        // The eviction watermark is GLOBAL (get_global_mem_used_ratio =
        // allocated / capacity summed across ALL mounted segments). With only
        // client1's 64 MB segment mounted, the ratio sits at 1.0 while MEMORY
        // is pinned for offload; once offload completes, eviction removes the
        // MEMORY replicas (the offload_on_evict two-step) and the ratio falls.
        // Polling until it drops below the 0.5 watermark proves some keys are
        // now LOCAL_DISK-only. If client2 (256 MB) were mounted first, the
        // ratio would be diluted to ~0.2 and eviction would stop BEFORE freeing
        // MEMORY -- exactly the bug this phase guards against.
        double post_evict_ratio = 1.0;
        {
            auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(60);
            post_evict_ratio =
                MasterMetricManager::instance().get_global_mem_used_ratio();
            while (post_evict_ratio > 0.5 &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                post_evict_ratio =
                    MasterMetricManager::instance().get_global_mem_used_ratio();
            }
            LOG(INFO)
                << "[Phase 4.5] Post-eviction global mem ratio="
                << post_evict_ratio
                << " (<= 0.5 proves MEMORY replicas were freed; some keys "
                   "are now LOCAL_DISK-only)";
            ASSERT_LE(post_evict_ratio, 0.5)
                << "Eviction did not free MEMORY before timeout; the "
                   "LOCAL_DISK-only state needed to exercise the LOCAL_DISK "
                   "drain path was not reached";
        }

        // Phase 5: Now set up client2 as the drain target. Use a LARGE
        // segment (256 MB) so that after the drain the global mem ratio
        // stays well below the 0.5 watermark and the eviction thread does NOT
        // evict the freshly-migrated keys off client2 (they have only a MEMORY
        // replica and an expired lease, so a low watermark would otherwise
        // evict them -> OBJECT_NOT_FOUND). Mounting it only AFTER Phase 4.5
        // keeps the ratio high during eviction so MEMORY actually gets freed.
        SetupClient(dst_addr, ssd2, client2_, /*seg_size=*/256 * 1024 * 1024);

        // Phase 6: Drain source -> target.
        CreateDrainJobRequest req;
        req.segments = {src_seg};
        req.target_segments = {dst_addr};
        req.max_concurrency = 1;
        auto cr = CreateDrainJob(req);
        ASSERT_TRUE(cr.has_value()) << "CreateDrainJob HTTP failed";
        ASSERT_TRUE(cr->success) << cr->error_message;

        HttpQueryDrainJobResponse fj;
        ASSERT_TRUE(WaitJob(cr->job_id, &fj))
            << "Drain timed out (status="
            << (fj.status_name.empty() ? "<timeout>" : fj.status_name) << ")";
        EXPECT_EQ(fj.status_name, "SUCCEEDED");
        EXPECT_EQ(fj.failed_units, 0u);
        LOG(INFO) << "[Phase 6] Drain " << fj.status_name
                  << " succeeded=" << fj.succeeded_units
                  << " failed=" << fj.failed_units
                  << " blocked=" << fj.blocked_units
                  << " migrated_bytes=" << fj.migrated_bytes;

        // Phase 7: Every stored key must be byte-for-byte readable on target.
        for (int i = 0; i < kKeyCount; ++i) {
            auto alloc = client2_->client_buffer_allocator_->allocate(kValSize);
            ASSERT_TRUE(alloc.has_value());
            auto* dst = static_cast<char*>(alloc->ptr());
            auto n = client2_->get_into(keys[i], dst, kValSize);
            ASSERT_EQ(n, static_cast<int64_t>(kValSize))
                << "get_into failed for " << keys[i];
            ASSERT_EQ(std::string(dst, kValSize), vals[i])
                << "Data mismatch for " << keys[i];
        }
        LOG(INFO) << "[Phase 7] All " << kKeyCount
                  << " keys verified on target (offload_on_evict="
                  << offload_on_evict << ")";

        // Route proof -- the LOCAL_DISK drain path WAS exercised:
        // Phase 4.5 established the global mem ratio fell to post_evict_ratio
        // (<= 0.5). With a 64 MB segment holding 16 x 4 MB keys, ratio <= 0.5
        // means <= 32 MB stayed resident, so at least 8 keys had their MEMORY
        // replica FREED and became LOCAL_DISK-only (data only on client1 SSD).
        // Phase 6 established succeeded_units == kKeyCount, i.e. ALL 16 keys
        // were migrated. A LOCAL_DISK-only key has NO memory replica to move,
        // so it can only have travelled the LOCAL_DISK path: client1 SSD ->
        // staging buffer (LoadBatchFromLocalDisk) -> TransferWrite -> client2
        // MEMORY. Hence the drain necessarily exercised the LOCAL_DISK path.
        const int min_local_disk_only =
            kKeyCount -
            static_cast<int>(std::ceil(post_evict_ratio * kKeyCount));
        LOG(INFO) << "[Route] MEMORY-key route (x"
                  << (kKeyCount - min_local_disk_only)
                  << "): Put->client1 mem -> MEMORY-path TransferWrite -> "
                     "client2 mem";
        LOG(INFO) << "[Route] LOCAL_DISK-key route (x" << min_local_disk_only
                  << "): Put->client1 mem -> evict->client1 SSD -> "
                     "LoadBatchFromLocalDisk->staging buffer -> TransferWrite "
                     "-> client2 mem";
        EXPECT_GE(min_local_disk_only, 1)
            << "Expected at least one LOCAL_DISK-only key to exercise the "
               "LOCAL_DISK drain path";

        // Direct log evidence: each LOCAL_DISK-only key migrated via the
        // LOCAL_DISK path emits action=replica_move_local_disk_transfer_success
        // (SSD -> staging buffer via LoadBatchFromLocalDisk -> TransferWrite).
        // The captured count must reach min_local_disk_only, proving the data
        // really passed through LoadBatchFromLocalDisk + TransferWrite (not
        // merely inferred from the memory ratio).
        const int local_disk_transfers =
            log_sink_.CountContaining("local_disk_transfer_success");
        LOG(INFO) << "[Route] local_disk_transfer_success log count="
                  << local_disk_transfers
                  << " (expect >= " << min_local_disk_only << ")";
        EXPECT_GE(local_disk_transfers, min_local_disk_only)
            << "Expected the LOCAL_DISK drain path to emit at least "
            << min_local_disk_only
            << " local_disk_transfer_success logs "
               "(SSD->staging buffer->TransferWrite)";
    }

    // ----- HTTP helpers (same as LocalDiskDrainTest) -----
    tl::expected<HttpCreateDrainJobResponse, int> CreateDrainJob(
        const CreateDrainJobRequest& request) {
        std::string body;
        struct_json::to_json(request, body);
        auto r = HttpPostJson(
            master_.http_metrics_base() + "/api/v1/drain_jobs", body);
        if (!r.has_value()) return tl::unexpected(r.error());
        HttpCreateDrainJobResponse p;
        struct_json::from_json(p, r.value());
        return p;
    }

    tl::expected<HttpQueryDrainJobResponse, int> QueryJob(
        const std::string& id) {
        auto r = HttpGet(master_.http_metrics_base() +
                         "/api/v1/drain_jobs/query?job_id=" + id);
        if (!r.has_value()) return tl::unexpected(r.error());
        HttpQueryDrainJobResponse p;
        struct_json::from_json(p, r.value());
        return p;
    }

    bool WaitJob(const std::string& id, HttpQueryDrainJobResponse* out,
                 std::chrono::seconds timeout = std::chrono::seconds(120)) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            auto qr = QueryJob(id);
            if (qr.has_value()) {
                if (qr->status_name == "SUCCEEDED" ||
                    qr->status_name == "FAILED" ||
                    qr->status_name == "CANCELED") {
                    if (out) *out = qr.value();
                    return qr->status_name == "SUCCEEDED";
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    InProcMaster master_;
    std::shared_ptr<RealClient> client1_;
    std::shared_ptr<RealClient> client2_;
    fs::path tmp_root_;
    CapturingLogSink log_sink_;
};

// Scenario 1: offload_on_evict=true 鈥?offload deferred to eviction time.
// Objects are NOT offloaded at PutEnd; they are offloaded only when the
// eviction thread selects them. After eviction + offload, some keys have
// only LOCAL_DISK replicas. The drain must migrate them via the LOCAL_DISK
// path (BatchLoad -> TransferWrite).
TEST_F(LocalDiskDrainEvictTest, DrainAfterEviction_OffloadOnEvict) {
    RunEvictionDrainTest(/*offload_on_evict=*/true);
}

// Scenario 2: offload_on_evict=false 鈥?offload immediate at PutEnd.
// Objects are offloaded to SSD right after Put. When eviction fires,
// keys already have LOCAL_DISK replicas, so eviction just removes the
// MEMORY replica. The drain handles a mix of MEMORY-only and
// LOCAL_DISK-only keys.
TEST_F(LocalDiskDrainEvictTest, DrainAfterEviction_ImmediateOffload) {
    RunEvictionDrainTest(/*offload_on_evict=*/false);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false /* remove_flags */);
    return RUN_ALL_TESTS();
}
