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

// The network route chosen for a staged request must carry every one of its
// host-to-host chunks. ProxyManager submits each chunk as a transfer of its
// own, between host buffers and a chunk long; selected afresh it could match
// another policy or lose the request's transport hint and scheduling
// metadata.
//
// Two engines in this process, over loopback, stage TPU<->TPU transfers
// against the mock PJRT adapter. Each can carry the network hop over TCP or
// HP TCP (TENT enables at most one of them), or over RDMA, which a loopback
// stand-in replaces because RDMA needs a NIC. The initiator's network and TPU
// transports are wrapped to record what each one is actually given.

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

#ifndef MOCK_TPU_PJRT_LIB
#error "MOCK_TPU_PJRT_LIB must be defined by the build (path to mock adapter)"
#endif

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static void smallStagePool(tent::TransferEngineImpl& engine) {
        engine.staging_proxy_ =
            std::make_unique<tent::ProxyManager>(&engine, 1UL << 20, 4);
    }
    static tent::Status stopStaging(tent::TransferEngineImpl& engine) {
        if (!engine.staging_proxy_) return tent::Status::OK();
        return engine.staging_proxy_->deconstruct();
    }
    static std::shared_ptr<tent::Transport> transport(
        tent::TransferEngineImpl& engine, tent::TransportType type) {
        return engine.transport_list_[type];
    }
    // What the RDMA transport would install at construction.
    static void setBandwidthProvider(tent::TransferEngineImpl& engine,
                                     double bytes_per_second) {
        engine.runtime_queue_->setDegradationPolicy(
            [bytes_per_second]() { return bytes_per_second; },
            tent::DegradationHooks{}, nullptr);
    }
};

namespace tent {
namespace {

using Params = std::tuple<bool, bool>;  // legacy selection, runtime queue
constexpr size_t kChunk = 1UL << 20;    // smallStagePool() slot size
// Five staging chunks, the last one short: more than the four-slot pool.
constexpr size_t kLength = 4 * kChunk + 4096 + 3;
constexpr size_t kChunks = 5;

// Forwards everything to a real transport, recording each request it is
// asked to submit. It can also refuse submissions, to drive failover.
class RecordingTransport : public Transport {
   public:
    struct Submission {
        Request request;
        std::string qp_pool;
    };

    explicit RecordingTransport(std::shared_ptr<Transport> real)
        : real_(std::move(real)) {}

    std::vector<Submission> submissions() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return submissions_;
    }
    void refuseSubmissions() { refuse_ = true; }
    void failStatusWithRpcServiceError() { fail_status_rpc_error_ = true; }

    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const auto& request : requests)
                submissions_.push_back({request, batch->qp_pool});
        }
        if (refuse_) return Status::InternalError("refused by test" LOC_MARK);
        return real_->submitTransferTasks(batch, requests);
    }

    Status uninstall() override { return real_->uninstall(); }
    Status quiesce() override { return real_->quiesce(); }
    const Capabilities capabilities() const override {
        return real_->capabilities();
    }
    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override {
        return real_->allocateSubBatch(batch, max_size);
    }
    Status freeSubBatch(SubBatchRef& batch) override {
        return real_->freeSubBatch(batch);
    }
    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto st = real_->getTransferStatus(batch, task_id, status);
        if (fail_status_rpc_error_ && st.ok() && status.s != PENDING) {
            status = {FAILED, 0};
            return Status::RpcServiceError("uncertain write" LOC_MARK);
        }
        return st;
    }
    Status retryTransferTask(SubBatchRef batch, int task_id,
                             const Request& request) override {
        return real_->retryTransferTask(batch, task_id, request);
    }
    bool supportsCancellation() const override {
        return real_->supportsCancellation();
    }
    Status cancelTransferTask(SubBatchRef batch, int task_id) override {
        return real_->cancelTransferTask(batch, task_id);
    }
    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions& options) override {
        return real_->allocateLocalMemory(addr, size, options);
    }
    Status freeLocalMemory(void* addr, size_t size) override {
        return real_->freeLocalMemory(addr, size);
    }
    bool warmupMemory(void* addr, size_t length) override {
        return real_->warmupMemory(addr, length);
    }
    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override {
        return real_->addMemoryBuffer(desc, options);
    }
    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override {
        return real_->addMemoryBuffer(desc_list, options);
    }
    Status removeMemoryBuffer(BufferDesc& desc) override {
        return real_->removeMemoryBuffer(desc);
    }
    bool tracksLocalBuffer(const BufferDesc& desc) const override {
        return real_->tracksLocalBuffer(desc);
    }
    bool supportNotification() const override {
        return real_->supportNotification();
    }
    Status sendNotification(SegmentID target_id,
                            const Notification& notify) override {
        return real_->sendNotification(target_id, notify);
    }
    Status receiveNotification(
        std::vector<Notification>& notify_list) override {
        return real_->receiveNotification(notify_list);
    }
    const char* getName() const override { return real_->getName(); }

   private:
    std::shared_ptr<Transport> real_;
    mutable std::mutex mutex_;
    std::vector<Submission> submissions_;
    std::atomic<bool> refuse_{false};
    std::atomic<bool> fail_status_rpc_error_{false};
};

// Stands in for RDMA. Both engines live in this process, so a remote
// address is directly usable: each request is a memcpy, complete at once.
class LoopbackRdma : public Transport {
   public:
    LoopbackRdma() { caps.dram_to_dram = true; }

    Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new Statuses();
        return Status::OK();
    }
    Status freeSubBatch(SubBatchRef& batch) override {
        delete static_cast<Statuses*>(batch);
        batch = nullptr;
        return Status::OK();
    }
    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        auto* statuses = static_cast<Statuses*>(batch);
        for (const auto& request : requests) {
            auto* remote = reinterpret_cast<void*>(request.target_offset);
            if (request.opcode == Request::WRITE)
                std::memcpy(remote, request.source, request.length);
            else
                std::memcpy(request.source, remote, request.length);
            statuses->list.push_back({COMPLETED, request.length});
        }
        return Status::OK();
    }
    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        status = static_cast<Statuses*>(batch)->list.at(task_id);
        return Status::OK();
    }
    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(RDMA);
        return Status::OK();
    }
    Status removeMemoryBuffer(BufferDesc&) override { return Status::OK(); }
    const char* getName() const override { return "loopback-rdma"; }

   private:
    struct Statuses : SubBatch {
        size_t size() const override { return list.size(); }
        std::vector<TransferStatus> list;
    };
};

// An absolute steady_clock deadline `delay` from now, as Request wants it.
uint64_t deadlineIn(std::chrono::nanoseconds delay) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               (std::chrono::steady_clock::now() + delay).time_since_epoch())
        .count();
}

class TpuStagedRouteTest : public ::testing::TestWithParam<Params> {
   protected:
    static void SetUpTestSuite() {
        ::setenv("MC_TPU_PJRT_LIB", MOCK_TPU_PJRT_LIB, /*overwrite=*/1);
    }

    bool legacy() const { return std::get<0>(GetParam()); }
    bool queue() const { return std::get<1>(GetParam()); }

    void SetUp() override {
        mock_ = dlopen(MOCK_TPU_PJRT_LIB, RTLD_NOW | RTLD_GLOBAL);
        ASSERT_NE(mock_, nullptr) << dlerror();
        register_ = reinterpret_cast<void* (*)(size_t, int)>(
            dlsym(mock_, "mock_tpu_pjrt_register_device"));
        device_data_ = reinterpret_cast<void* (*)(const void*)>(
            dlsym(mock_, "mock_tpu_pjrt_device_data"));
        reset_ =
            reinterpret_cast<void (*)()>(dlsym(mock_, "mock_tpu_pjrt_reset"));
        ASSERT_NE(register_, nullptr);
        ASSERT_NE(device_data_, nullptr);
        ASSERT_NE(reset_, nullptr);
        reset_();
    }

    void TearDown() override {
        for (auto* engine : {a_.get(), b_.get()}) {
            if (engine) {
                EXPECT_TRUE(
                    TransferEngineImplTestPeer::stopStaging(*engine).ok());
            }
        }
        for (auto [engine, address] : registrations_)
            EXPECT_TRUE(engine->unregisterLocalMemory(address).ok());
        a_.reset();
        b_.reset();
        if (reset_) reset_();
        if (mock_) dlclose(mock_);
    }

    // Starts both engines, with `network` (TCP or HP TCP) and the RDMA
    // stand-in (skipped on peer when `peer_rdma` is false). `extra` is merged
    // into the configuration.
    void start(TransportType network, const json& extra = json::object(),
               bool peer_rdma = true) {
        auto conf = std::make_shared<Config>();
        conf->set("metadata_type", "p2p");
        conf->set("metadata_servers", "");
        conf->set("rpc_server_hostname", "127.0.0.1");
        conf->set("rpc_server_port", "0");
        conf->set("log_level", "warning");
        conf->set("transports/tcp/enable", network == TCP);
        conf->set("transports/hp_tcp/enable", network == HP_TCP);
        conf->set("transports/rdma/enable", false);  // LoopbackRdma instead.
        conf->set("transports/shm/enable", false);
        conf->set("transports/io_uring/enable", false);
        conf->set("transports/mpcomm/enable", false);
        conf->set("transports/tpu/enable", true);
        conf->set("use_legacy_transport_selection", legacy());
        conf->set("enable_runtime_queue", queue());
        conf->set("max_failover_attempts", 1);
        for (const auto& item : extra.items())
            conf->set(item.key(), item.value());
        ASSERT_EQ(Platform::getLoader(conf).type(), "tpu");
        a_ = std::make_unique<TransferEngineImpl>(conf);
        b_ = std::make_unique<TransferEngineImpl>(conf);
        ASSERT_TRUE(a_->available());
        ASSERT_TRUE(b_->available());
        for (auto* engine : {a_.get(), b_.get()}) {
            if (engine == a_.get() || peer_rdma) {
                auto rdma = std::make_shared<LoopbackRdma>();
                auto name = engine->getSegmentName();
                ASSERT_TRUE(rdma->install(name, nullptr, nullptr).ok());
                engine->swapTransportForTest(RDMA, rdma);
            }
            TransferEngineImplTestPeer::smallStagePool(*engine);
        }
        for (auto type : {RDMA, network, TPU}) {
            recorders_[type] = std::make_shared<RecordingTransport>(
                TransferEngineImplTestPeer::transport(*a_, type));
            a_->swapTransportForTest(type, recorders_[type]);
        }
        // The peer runs the HBM<->host copies of delegated stages.
        peer_tpu_ = std::make_shared<RecordingTransport>(
            TransferEngineImplTestPeer::transport(*b_, TPU));
        b_->swapTransportForTest(TPU, peer_tpu_);
        local_ = tpu(*a_);
        remote_ = tpu(*b_);
        ASSERT_TRUE(a_->openSegment(peer_, b_->getSegmentName()).ok());
    }

    void* tpu(TransferEngineImpl& engine) {
        void* address = register_(kLength, /*index=*/0);
        EXPECT_NE(address, nullptr);
        MemoryOptions opts;
        EXPECT_TRUE(
            engine.registerLocalMemory({address}, {kLength}, opts).ok());
        registrations_.push_back({&engine, address});
        return address;
    }

    Request request(Request::OpCode opcode, TransportType hint = UNSPEC) {
        Request r{};
        r.opcode = opcode;
        r.source = local_;
        r.target_id = peer_;
        r.target_offset = reinterpret_cast<uint64_t>(remote_);
        r.length = kLength;
        r.transport_hint = hint;
        return r;
    }

    // Runs `r` between the two TPU buffers, which start out different. A
    // completed transfer must have copied every byte; any other outcome must
    // have left the destination untouched.
    TransferStatusEnum transfer(const Request& r) {
        const size_t len = r.length;
        auto* source = static_cast<uint8_t*>(
            device_data_(r.opcode == Request::WRITE ? local_ : remote_));
        auto* dest = static_cast<uint8_t*>(
            device_data_(r.opcode == Request::WRITE ? remote_ : local_));
        ++seed_;
        for (size_t i = 0; i < len; ++i) source[i] = (i * 13 + seed_) % 251;
        std::memset(dest, 0, len);
        auto batch = a_->allocateBatch(1);
        EXPECT_NE(batch, 0u);
        if (!a_->submitTransfer(batch, {r}).ok()) {
            EXPECT_TRUE(a_->freeBatch(batch).ok());
            return INVALID;
        }
        TransferStatus status{};
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(15);
        do {
            auto s = a_->progressBatch(batch, status);
            EXPECT_TRUE(s.ok()) << s.ToString();
            if (!s.ok() || status.s != PENDING) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        EXPECT_TRUE(a_->freeBatch(batch).ok());
        if (status.s == COMPLETED) {
            EXPECT_EQ(0, std::memcmp(dest, source, len));
        } else {
            for (size_t i = 0; i < len; ++i)
                if (dest[i] != 0) {
                    ADD_FAILURE() << "destination byte " << i << " changed";
                    break;
                }
        }
        return status.s;
    }

    std::vector<RecordingTransport::Submission> submitted(TransportType type) {
        if (!recorders_[type]) return {};
        return recorders_[type]->submissions();
    }

    // Each of `transfers` transfers sent all its chunks over `type`, and no
    // other network transport was given any.
    void expectChunksOnlyOn(TransportType type, size_t transfers) {
        for (auto network : {RDMA, TCP, HP_TCP}) {
            EXPECT_EQ(submitted(network).size(),
                      network == type ? transfers * kChunks : 0u)
                << transportTypeName(network);
        }
    }

    void* mock_ = nullptr;
    void* (*register_)(size_t, int) = nullptr;
    void* (*device_data_)(const void*) = nullptr;
    void (*reset_)() = nullptr;
    std::unique_ptr<TransferEngineImpl> a_, b_;
    std::array<std::shared_ptr<RecordingTransport>, kSupportedTransportTypes>
        recorders_;
    std::shared_ptr<RecordingTransport> peer_tpu_;
    std::vector<std::pair<TransferEngineImpl*, void*>> registrations_;
    void* local_ = nullptr;
    void* remote_ = nullptr;
    SegmentID peer_ = LOCAL_SEGMENT_ID;
    int seed_ = 0;
};

// Policies that route a large TPU request and a chunk-sized host transfer
// differently. The HBM<->host copies are chunk-sized transfers too, so the
// chunk-sized policy lists `tpu` first, ahead of TCP's device-copy fallback.
json tpuPolicies(const json& tpu_bulk_transports,
                 const json& small_transports) {
    json tpu_bulk = {{"name", "tpu_bulk"},
                     {"segment_type", "memory"},
                     {"local_memory", "tpu"},
                     {"min_size", 2 * kChunk},
                     {"transports", tpu_bulk_transports}};
    json small = {{"name", "small"},
                  {"segment_type", "memory"},
                  {"max_size", kChunk},
                  {"transports", small_transports}};
    return json::array({tpu_bulk, small});
}

TEST_P(TpuStagedRouteTest, ExplicitTcpCarriesEveryChunk) {
    ASSERT_NO_FATAL_FAILURE(start(TCP));
    for (auto opcode : {Request::WRITE, Request::READ})
        EXPECT_EQ(transfer(request(opcode, TCP)), COMPLETED);
    expectChunksOnlyOn(TCP, 2);
}

TEST_P(TpuStagedRouteTest, ExplicitHpTcpCarriesEveryChunk) {
    ASSERT_NO_FATAL_FAILURE(start(HP_TCP));
    for (auto opcode : {Request::WRITE, Request::READ})
        EXPECT_EQ(transfer(request(opcode, HP_TCP)), COMPLETED);
    expectChunksOnlyOn(HP_TCP, 2);
}

TEST_P(TpuStagedRouteTest, TpuRequestPolicyRoutesEveryChunk) {
    if (legacy()) GTEST_SKIP() << "Legacy selection ignores policies";
    auto policies = tpuPolicies({"rdma"}, {"tpu", "tcp"});
    policies[0]["qp_pool"] = "kv";
    ASSERT_NO_FATAL_FAILURE(start(TCP, {{"policy", policies}}));
    for (auto opcode : {Request::WRITE, Request::READ})
        EXPECT_EQ(transfer(request(opcode)), COMPLETED);
    expectChunksOnlyOn(RDMA, 2);
    for (const auto& chunk : submitted(RDMA)) EXPECT_EQ(chunk.qp_pool, "kv");
}

TEST_P(TpuStagedRouteTest, SchedulingMetadataReachesEveryChunk) {
    ASSERT_NO_FATAL_FAILURE(start(TCP));
    auto r = request(Request::WRITE, TCP);
    r.priority = PRIO_LOW;
    r.deadline_ns = deadlineIn(std::chrono::seconds(60));
    EXPECT_EQ(transfer(r), COMPLETED);
    auto chunks = submitted(TCP);
    EXPECT_EQ(chunks.size(), kChunks);
    for (const auto& chunk : chunks) {
        EXPECT_EQ(chunk.request.priority, PRIO_LOW);
        EXPECT_EQ(chunk.request.deadline_ns, r.deadline_ns);
    }
}

TEST_P(TpuStagedRouteTest, RejectedRouteSendsNoChunk) {
    if (legacy()) GTEST_SKIP() << "Legacy selection ignores policies";
    ASSERT_NO_FATAL_FAILURE(
        start(TCP, {{"policy", tpuPolicies({"tcp"}, {"tpu", "tcp"})}}));
    // The policy for TPU requests does not allow RDMA.
    EXPECT_EQ(transfer(request(Request::WRITE, RDMA)), FAILED);
    expectChunksOnlyOn(UNSPEC, 0);
}

TEST_P(TpuStagedRouteTest, ChunkFailoverFollowsTheStagedRequestCandidates) {
    ASSERT_NO_FATAL_FAILURE(start(TCP));
    recorders_[TCP]->refuseSubmissions();
    EXPECT_EQ(transfer(request(Request::WRITE, TCP)), COMPLETED);
    EXPECT_EQ(submitted(TCP).size(), kChunks);   // Each chunk tried TCP...
    EXPECT_EQ(submitted(RDMA).size(), kChunks);  // ...then went over RDMA.
    // Staged chunks, not HBM copies, made those submissions.
    for (auto type : {TCP, RDMA})
        for (const auto& chunk : submitted(type))
            EXPECT_NE(Platform::getLoader().getMemoryType(chunk.request.source),
                      MTYPE_TPU);
}

TEST_P(TpuStagedRouteTest, ChunkFailoverStaysWithinTheAllowedTransports) {
    if (legacy()) GTEST_SKIP() << "Legacy selection ignores policies";
    ASSERT_NO_FATAL_FAILURE(
        start(TCP, {{"policy", tpuPolicies({"tcp"}, {"tpu", "rdma"})}}));
    recorders_[TCP]->refuseSubmissions();
    EXPECT_EQ(transfer(request(Request::WRITE)), FAILED);
    EXPECT_FALSE(submitted(TCP).empty());
    EXPECT_TRUE(submitted(RDMA).empty());
}

// A network hint never reaches the HBM<->host copies: they stay on the TPU
// transport, and only host addresses reach the network.
TEST_P(TpuStagedRouteTest, LocalHbmCopiesStayOnTheTpuTransport) {
    ASSERT_NO_FATAL_FAILURE(start(TCP));
    for (auto opcode : {Request::WRITE, Request::READ})
        EXPECT_EQ(transfer(request(opcode, TCP)), COMPLETED);
    auto copies = submitted(TPU);
    EXPECT_EQ(copies.size(), 2 * kChunks);
    for (const auto& copy : copies) {
        EXPECT_EQ(copy.request.target_id, LOCAL_SEGMENT_ID);
        EXPECT_EQ(Platform::getLoader().getMemoryType(copy.request.source),
                  MTYPE_TPU);
    }
    for (const auto& chunk : submitted(TCP)) {
        auto* target = reinterpret_cast<void*>(chunk.request.target_offset);
        EXPECT_NE(Platform::getLoader().getMemoryType(chunk.request.source),
                  MTYPE_TPU);
        EXPECT_NE(Platform::getLoader().getMemoryType(target), MTYPE_TPU);
    }
}

// protocol="tcp" sets force_tcp. The HBM<->host copies are not network hops:
// on both engines they stay on the TPU transport.
TEST_P(TpuStagedRouteTest, ForcedTcpKeepsLocalHbmCopiesOnTheTpuTransport) {
    ASSERT_NO_FATAL_FAILURE(start(TCP, {{"transports/force_tcp", true}}));
    for (auto opcode : {Request::WRITE, Request::READ})
        EXPECT_EQ(transfer(request(opcode)), COMPLETED);
    expectChunksOnlyOn(TCP, 2);
    EXPECT_EQ(submitted(TPU).size(), 2 * kChunks);
    EXPECT_EQ(peer_tpu_->submissions().size(), 2 * kChunks);
    for (const auto& chunk : submitted(TCP)) {
        auto* target = reinterpret_cast<void*>(chunk.request.target_offset);
        EXPECT_NE(Platform::getLoader().getMemoryType(chunk.request.source),
                  MTYPE_TPU);
        EXPECT_NE(Platform::getLoader().getMemoryType(target), MTYPE_TPU);
    }
}

// Chunks carry the staged request's deadline, but the runtime queue must not
// drop one as infeasible: the staged request was already accepted, and
// losing a chunk would fail it part-way.
TEST_P(TpuStagedRouteTest, RuntimeQueueDoesNotDropDeadlinedChunks) {
    if (!queue()) GTEST_SKIP() << "Needs the runtime queue";
    ASSERT_NO_FATAL_FAILURE(
        start(TCP, {{"runtime_queue/deadline_aware", true},
                    {"runtime_queue/mlu_local_threshold", 1.0}}));
    // One byte per second: every deadlined RDMA owner looks infeasible.
    TransferEngineImplTestPeer::setBandwidthProvider(*a_, 1.0);
    auto r = request(Request::WRITE, RDMA);
    r.deadline_ns = deadlineIn(std::chrono::seconds(1));
    EXPECT_EQ(transfer(r), COMPLETED);
    expectChunksOnlyOn(RDMA, 1);
}

TEST_P(TpuStagedRouteTest, AsymmetricPeerSkipsTransportsPeerDoesNotRun) {
    ASSERT_NO_FATAL_FAILURE(
        start(TCP, {{"max_failover_attempts", 0}}, /*peer_rdma=*/false));
    for (auto opcode : {Request::WRITE, Request::READ})
        EXPECT_EQ(transfer(request(opcode)), COMPLETED);
    expectChunksOnlyOn(TCP, 2);
}

TEST_P(TpuStagedRouteTest, AsymmetricPeerSkipsUnavailablePolicyTransport) {
    if (legacy()) GTEST_SKIP() << "Legacy selection ignores policies";
    ASSERT_NO_FATAL_FAILURE(
        start(TCP,
              {{"max_failover_attempts", 0},
               {"policy", tpuPolicies({"rdma", "tcp"}, {"tpu", "rdma"})}},
              /*peer_rdma=*/false));
    for (auto opcode : {Request::WRITE, Request::READ})
        EXPECT_EQ(transfer(request(opcode)), COMPLETED);
    expectChunksOnlyOn(TCP, 2);
}

TEST_P(TpuStagedRouteTest, NonReplayableTcpWriteDoesNotRetryStaging) {
    ASSERT_NO_FATAL_FAILURE(start(TCP, {{"max_failover_attempts", 3}}));
    recorders_[TCP]->failStatusWithRpcServiceError();
    auto r = request(Request::WRITE, TCP);
    r.length = 4096;
    EXPECT_EQ(transfer(r), FAILED);
    EXPECT_EQ(submitted(TCP).size(), 1u);
    EXPECT_TRUE(submitted(RDMA).empty());
}

TEST_P(TpuStagedRouteTest, PersistentStagingFailureCapsRetries) {
    ASSERT_NO_FATAL_FAILURE(start(TCP, {{"max_failover_attempts", 3}}));
    recorders_[TCP]->refuseSubmissions();
    recorders_[RDMA]->refuseSubmissions();
    auto r = request(Request::WRITE, TCP);
    r.length = 4096;
    EXPECT_EQ(transfer(r), FAILED);
    // Initial staged attempt plus at most one restage, not
    // max_failover_attempts.
    EXPECT_EQ(submitted(TCP).size(), 2u);
    EXPECT_EQ(submitted(RDMA).size(), 2u);
}

TEST_P(TpuStagedRouteTest,
       CustomTpuPolicyWithoutTpuKeepsLocalHbmCopiesOnTpuTransport) {
    if (legacy()) GTEST_SKIP() << "Legacy selection ignores policies";
    json policy = json::array({{{"name", "all_rdma"},
                                {"segment_type", "memory"},
                                {"transports", {"rdma"}}}});
    ASSERT_NO_FATAL_FAILURE(start(TCP, {{"policy", policy}}));
    for (auto opcode : {Request::WRITE, Request::READ})
        EXPECT_EQ(transfer(request(opcode)), COMPLETED);
    expectChunksOnlyOn(RDMA, 2);
    EXPECT_EQ(submitted(TPU).size(), 2 * kChunks);
    EXPECT_EQ(peer_tpu_->submissions().size(), 2 * kChunks);
}

TEST_P(TpuStagedRouteTest, IntentAndPolicyNameRouteEveryChunk) {
    if (legacy()) GTEST_SKIP() << "Legacy selection ignores policies";
    json policies = json::array({{{"name", "prefetch_tcp"},
                                  {"segment_type", "memory"},
                                  {"intent_type", "background_prefetch"},
                                  {"transports", {"tcp"}}},
                                 {{"name", "small_rdma"},
                                  {"segment_type", "memory"},
                                  {"max_size", kChunk},
                                  {"transports", {"rdma"}}},
                                 {{"name", "default_rdma"},
                                  {"segment_type", "memory"},
                                  {"transports", {"rdma"}}},
                                 {{"name", "named_tcp"},
                                  {"segment_type", "memory"},
                                  {"transports", {"tcp"}}}});
    ASSERT_NO_FATAL_FAILURE(start(TCP, {{"policy", policies}}));

    auto prefetch = request(Request::WRITE);
    prefetch.intent_type = IntentType::BACKGROUND_PREFETCH;
    EXPECT_EQ(transfer(prefetch), COMPLETED);
    expectChunksOnlyOn(TCP, 1);

    auto by_name = request(Request::WRITE);
    by_name.policy_name = "named_tcp";
    EXPECT_EQ(transfer(by_name), COMPLETED);
    expectChunksOnlyOn(TCP, 2);

    EXPECT_EQ(transfer(request(Request::WRITE)), COMPLETED);
    EXPECT_EQ(submitted(TCP).size(), 2 * kChunks);
    EXPECT_EQ(submitted(RDMA).size(), kChunks);
}

INSTANTIATE_TEST_SUITE_P(
    SelectionAndDispatchModes, TpuStagedRouteTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const ::testing::TestParamInfo<Params>& info) {
        return std::string(std::get<0>(info.param) ? "Legacy" : "Selector") +
               (std::get<1>(info.param) ? "RuntimeQueue" : "Direct");
    });

}  // namespace
}  // namespace tent
}  // namespace mooncake
