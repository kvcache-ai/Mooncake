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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>
#ifdef USE_TENT
#include <infiniband/verbs.h>
#endif

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <iomanip>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

#include "multi_transport.h"
#include "transfer_engine.h"
#include "transfer_engine_c.h"
#include "transfer_engine_impl.h"
#include "transport/transport.h"
#ifdef USE_TENT
#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transfer_engine.h"
#endif

using namespace mooncake;

namespace mooncake {

#ifdef USE_TENT
class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        const char* old_value = std::getenv(name);
        if (old_value) old_value_ = old_value;
        if (value)
            setenv(name, value, 1);
        else
            unsetenv(name);
    }

    ~ScopedEnvVar() {
        if (old_value_)
            setenv(name_.c_str(), old_value_->c_str(), 1);
        else
            unsetenv(name_.c_str());
    }

   private:
    std::string name_;
    std::optional<std::string> old_value_;
};
#endif

class TransferEngineImplTestPeer {
   public:
    static void replaceTransports(TransferEngineImpl& engine,
                                  std::shared_ptr<Transport> transport) {
        engine.multi_transports_->transport_map_.clear();
        engine.multi_transports_->transport_map_.emplace("blocking",
                                                         std::move(transport));
    }

    static void replaceTransports(
        TransferEngineImpl& engine,
        const std::vector<std::pair<std::string, std::shared_ptr<Transport>>>&
            transports) {
        engine.multi_transports_->transport_map_.clear();
        for (const auto& [name, transport] : transports) {
            engine.multi_transports_->transport_map_.emplace(name, transport);
        }
    }

    static TransferEngineImpl& implementation(TransferEngine& engine) {
        return *engine.impl_;
    }

    static AutoDiscoverConfig autoDiscoverConfig(
        const TransferEngineImpl& engine) {
        return engine.auto_discover_config_;
    }

    static std::string autoDiscoverTransport(const TransferEngineImpl& engine) {
        return engine.autoDiscoverTransport();
    }

    static void setUseBarex(TransferEngineImpl& engine, bool use_barex) {
        engine.use_barex_ = use_barex;
    }

    static size_t pendingNotifyCount(TransferEngineImpl& engine) {
        RWSpinlock::ReadGuard guard(engine.send_notifies_lock_);
        return engine.notifies_to_send_.size();
    }

    static Status freeBatchWithCallback(
        TransferEngineImpl& engine, BatchID batch_id,
        const std::function<void()>& before_delete) {
        return engine.multi_transports_->freeBatchID(batch_id, before_delete);
    }

#ifdef USE_TENT
    static const tent::Config* tentConfig(const TransferEngine& engine) {
        if (!engine.impl_tent_ || !engine.impl_tent_->impl_) return nullptr;
        return engine.impl_tent_->impl_->conf_.get();
    }

    static std::shared_ptr<tent::Config> buildTentConfig(
        const TransferEngine& engine, const std::string& metadata,
        const std::string& segment) {
        return engine.buildTentConfig(metadata, segment);
    }
#endif
};

#ifdef USE_TENT
class ScopedUnsetEnvVar {
   public:
    explicit ScopedUnsetEnvVar(const char* name) : name_(name) {
        if (const char* old = std::getenv(name)) old_value_ = old;
        unsetenv(name);
    }

    ~ScopedUnsetEnvVar() {
        if (old_value_.has_value())
            setenv(name_.c_str(), old_value_->c_str(), 1);
    }

   private:
    std::string name_;
    std::optional<std::string> old_value_;
};

TEST(TransferEngineTentCompatibilityTest,
     ConstructorDeviceFilterIsForwardedToTentConfig) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    const std::vector<std::string> filter{"mlx5_0", "mlx5_2"};
    TransferEngine engine(/*auto_discover=*/true, filter);
    ASSERT_TRUE(engine.isUsingTent());

    auto config = TransferEngineImplTestPeer::buildTentConfig(
        engine, P2PHANDSHAKE, "local-segment");

    EXPECT_EQ(config->getArray<std::string>("topology/rdma_whitelist"), filter);
}

TEST(TransferEngineTentCompatibilityTest,
     SetterDeviceFilterIsForwardedBeforeInit) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    TransferEngine engine(/*auto_discover=*/true);
    engine.setWhitelistFilters({"mlx5_1"});

    auto config = TransferEngineImplTestPeer::buildTentConfig(
        engine, P2PHANDSHAKE, "local-segment");

    EXPECT_EQ(config->getArray<std::string>("topology/rdma_whitelist"),
              (std::vector<std::string>{"mlx5_1"}));
}

TEST(TransferEngineTentCompatibilityTest,
     DeviceFilterSurvivesMoveConstructionAndAssignment) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    const std::vector<std::string> filter{"mlx5_move"};
    TransferEngine source(/*auto_discover=*/true, filter);
    TransferEngine moved(std::move(source));

    auto moved_config = TransferEngineImplTestPeer::buildTentConfig(
        moved, P2PHANDSHAKE, "local-segment");
    EXPECT_EQ(moved_config->getArray<std::string>("topology/rdma_whitelist"),
              filter);

    TransferEngine assigned(/*auto_discover=*/true);
    assigned = std::move(moved);
    auto assigned_config = TransferEngineImplTestPeer::buildTentConfig(
        assigned, P2PHANDSHAKE, "local-segment");
    EXPECT_EQ(assigned_config->getArray<std::string>("topology/rdma_whitelist"),
              filter);
}

TEST(TransferEngineTentCompatibilityTest,
     ConstructorDeviceFilterRestrictsDiscoveredTopology) {
    int count = 0;
    ibv_device** devices = ibv_get_device_list(&count);
    if (!devices || count < 2) {
        if (devices) ibv_free_device_list(devices);
        GTEST_SKIP() << "Requires at least two RDMA devices";
    }
    const std::string selected = ibv_get_device_name(devices[0]);
    ibv_free_device_list(devices);

    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    ScopedEnvVar hostname("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1");
    ScopedUnsetEnvVar tent_conf("MC_TENT_CONF");
    ScopedUnsetEnvVar custom_topology("MC_CUSTOM_TOPO_JSON");
    TransferEngine engine(/*auto_discover=*/true, {selected});
    ASSERT_EQ(engine.init(P2PHANDSHAKE, ""), 0);

    const auto topology = engine.getLocalTopology();
    ASSERT_NE(topology, nullptr);
    EXPECT_EQ(topology->getHcaList(), (std::vector<std::string>{selected}));
}

#endif

TEST(TransferEngineAutoDiscoverTest, SelectsEfaForEfaProtocol) {
    TransferEngineImpl engine(false);
    engine.setAutoDiscover({.enabled = true, .protocol = "efa"});

    const auto config = TransferEngineImplTestPeer::autoDiscoverConfig(engine);
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.protocol, "efa");
    EXPECT_EQ(TransferEngineImplTestPeer::autoDiscoverTransport(engine), "efa");
}

TEST(TransferEngineAutoDiscoverTest, BarexOverrideTakesPrecedence) {
    TransferEngineImpl engine(false);
    engine.setAutoDiscover({.enabled = true, .protocol = "efa"});
    TransferEngineImplTestPeer::setUseBarex(engine, true);

    EXPECT_EQ(TransferEngineImplTestPeer::autoDiscoverTransport(engine),
              "barex");
}

TEST(TransferEngineAutoDiscoverTest, BoolSetterPreservesDefaultSelection) {
    TransferEngineImpl engine(false);
    engine.setAutoDiscover({.enabled = true, .protocol = "efa"});
    engine.setAutoDiscover(true);

    const auto config = TransferEngineImplTestPeer::autoDiscoverConfig(engine);
    EXPECT_TRUE(config.enabled);
    EXPECT_TRUE(config.protocol.empty());
    EXPECT_EQ(TransferEngineImplTestPeer::autoDiscoverTransport(engine),
              "rdma");
}

#ifdef USE_TENT
// RDMA is left enabled and TCP disabled so a regression that drops forceTcp()
// still comes up with RDMA selected. forceTcp() must flip both flags.
constexpr const char* kTentConfPrefersRdma =
    R"({"transports":{"tcp":{"enable":false},"rdma":{"enable":true},"shm":{"enable":false},"hp_tcp":{"enable":false},"mpcomm":{"enable":false},"io_uring":{"enable":false}},"metrics":{"enabled":false}})";

void expectForcedTcpConstraint(const tent::Config& config) {
    EXPECT_TRUE(config.get("transports/force_tcp", false));
    EXPECT_TRUE(config.get("transports/tcp/enable", false));
    EXPECT_FALSE(config.get("transports/rdma/enable", true));
}

TEST(TransferEngineTentCompatibilityTest, CheckSegmentStatusRejectsDeadPeer) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    ScopedEnvVar force_tcp("MC_FORCE_TCP", nullptr);
    ScopedEnvVar hostname("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1");
    ScopedEnvVar conf("MC_TENT_CONF", kTentConfPrefersRdma);

    TransferEngine engine(true);
    ASSERT_TRUE(engine.isUsingTent());
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "compat-stale-handle"), 0);

    // Segment handles are handed out from 1 upward when a peer is opened, so a
    // large handle that was never opened has no id->name mapping. That is the
    // same "peer gone / handle stale" situation the eviction path must detect:
    // probePeerAliveByID fails on it, so CheckSegmentStatus must report non-OK
    // and let the caller close + re-open the segment. Before the fix the shim
    // returned Status::OK() unconditionally under MC_USE_TENT, so the Python
    // wrapper's handle_map_ never dropped a dead peer and kept reusing the same
    // stale handle forever. Refs #3995 (P0-stale-handle).
    EXPECT_FALSE(engine.CheckSegmentStatus(1ull << 40).ok());
}

TEST(TransferEngineTentCompatibilityTest, TcpProtocolForcesTcpTransport) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    ScopedEnvVar force_tcp("MC_FORCE_TCP", nullptr);
    ScopedEnvVar hostname("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1");
    ScopedEnvVar conf("MC_TENT_CONF", kTentConfPrefersRdma);

    TransferEngine engine(true);
    ASSERT_TRUE(engine.isUsingTent());
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "compat-protocol-tcp", "", 0, "tcp"),
              0);
    const auto* protocol_config =
        TransferEngineImplTestPeer::tentConfig(engine);
    ASSERT_NE(protocol_config, nullptr);
    expectForcedTcpConstraint(*protocol_config);

    std::array<char, 4096> buffer{};
    ASSERT_EQ(engine.registerLocalMemory(buffer.data(), buffer.size()), 0);
    EXPECT_EQ(engine.unregisterLocalMemory(buffer.data()), 0);
}

TEST(TransferEngineTentCompatibilityTest, ForceTcpEnvForcesTcpTransport) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    ScopedEnvVar force_tcp("MC_FORCE_TCP", "1");
    ScopedEnvVar hostname("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1");
    ScopedEnvVar conf("MC_TENT_CONF", kTentConfPrefersRdma);

    TransferEngine engine(true);
    ASSERT_TRUE(engine.isUsingTent());
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "compat-force-tcp"), 0);
    const auto* env_config = TransferEngineImplTestPeer::tentConfig(engine);
    ASSERT_NE(env_config, nullptr);
    expectForcedTcpConstraint(*env_config);

    std::array<char, 4096> buffer{};
    ASSERT_EQ(engine.registerLocalMemory(buffer.data(), buffer.size()), 0);
    EXPECT_EQ(engine.unregisterLocalMemory(buffer.data()), 0);
}
#endif

class BatchResultTransport : public Transport {
   public:
    explicit BatchResultTransport(int unregister_result = 0)
        : unregister_result_(unregister_result) {}

    int unregisterBatchCalls() const { return unregister_batch_calls_; }
    size_t registeredBufferCount() const { return registered_buffers_.size(); }
    void setRegisterResult(int result) { register_result_ = result; }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    Status getTransferStatus(BatchID, size_t, TransferStatus&) override {
        return Status::OK();
    }

   private:
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }

    int unregisterLocalMemory(void*, bool) override { return 0; }

    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string&) override {
        if (register_result_) {
            if (!buffer_list.empty()) {
                registered_buffers_.push_back(buffer_list.front().addr);
            }
            return register_result_;
        }
        for (const auto& buffer : buffer_list) {
            registered_buffers_.push_back(buffer.addr);
        }
        return 0;
    }

    int unregisterLocalMemoryBatch(
        const std::vector<void*>& addr_list) override {
        ++unregister_batch_calls_;
        for (void* addr : addr_list) {
            registered_buffers_.erase(
                std::remove(registered_buffers_.begin(),
                            registered_buffers_.end(), addr),
                registered_buffers_.end());
        }
        return unregister_result_;
    }

    const char* getName() const override { return "batch-result"; }

    int register_result_ = 0;
    int unregister_result_;
    int unregister_batch_calls_ = 0;
    std::vector<void*> registered_buffers_;
};

class BlockingRegistrationTransport : public Transport {
   public:
    explicit BlockingRegistrationTransport(int first_registration_result = 0)
        : first_registration_result_(first_registration_result) {}

    void waitForFirstRegistration() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return first_registration_started_; });
    }

    void releaseFirstRegistration() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            release_first_registration_ = true;
        }
        cv_.notify_all();
    }

    int registrationCalls() {
        std::lock_guard<std::mutex> lock(mutex_);
        return registration_calls_;
    }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    Status getTransferStatus(BatchID, size_t, TransferStatus&) override {
        return Status::OK();
    }

   private:
    int waitOnFirstRegistration() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++registration_calls_;
        if (registration_calls_ == 1) {
            first_registration_started_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this] { return release_first_registration_; });
            return first_registration_result_;
        }
        return 0;
    }

    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return waitOnFirstRegistration();
    }

    int unregisterLocalMemory(void*, bool) override { return 0; }

    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return waitOnFirstRegistration();
    }

    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }

    const char* getName() const override { return "blocking"; }

    std::mutex mutex_;
    std::condition_variable cv_;
    int first_registration_result_;
    int registration_calls_ = 0;
    bool first_registration_started_ = false;
    bool release_first_registration_ = false;
};

class PartialFailureSubmissionTransport : public BatchResultTransport {
   public:
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        for (auto* task : tasks) {
            request_counts.push_back(task->request_count);
            task->slice_count = task->request_count;
            task->success_slice_count = 1;
            task->failed_slice_count = task->request_count - 1;
            for (size_t i = 0; i < task->request_count; ++i) {
                auto* slice = new Slice{};
                slice->length = task->request[i].length;
                slice->status = i == 0 ? Slice::SUCCESS : Slice::FAILED;
                task->slice_list.push_back(slice);
            }
            if (extra_slice_) task->slice_list.push_back(new Slice{});
            task->is_finished = true;
        }
        return Status::InvalidArgument("synthetic submit failure");
    }

    Status getTransferStatus(BatchID, size_t, TransferStatus& status) override {
        status.s = TransferStatusEnum::FAILED;
        return Status::OK();
    }

    bool supportsGroupedScatter() const override { return true; }

    void addExtraSlice() { extra_slice_ = true; }

    std::vector<size_t> request_counts;

   private:
    bool extra_slice_ = false;
};

class TerminalFailureTransport : public BatchResultTransport {
   public:
    explicit TerminalFailureTransport(bool initially_finished)
        : initially_finished_(initially_finished) {}

    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        tasks_ = tasks;
        for (auto* task : tasks_) {
            task->is_finished = initially_finished_;
        }
        return Status::OK();
    }

    Status getTransferStatus(BatchID, size_t, TransferStatus& status) override {
        status.s = TransferStatusEnum::FAILED;
        return Status::OK();
    }

    void finishTasks() {
        for (auto* task : tasks_) {
            task->is_finished = true;
        }
    }

   private:
    bool initially_finished_;
    std::vector<TransferTask*> tasks_;
};

class TransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("TransportTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(TransportTest, SegmentBuffersClassicSnapshotAndErrors) {
#ifdef USE_TENT
    ScopedUnsetEnvVar use_tent("MC_USE_TENT");
    ScopedUnsetEnvVar use_tev1("MC_USE_TEV1");
#endif
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:0"), 0);
    auto metadata = engine.getMetadata();
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "buffers";
    desc->protocol = "rdma";
    desc->buffers.resize(2);
    desc->buffers[0].name = "cpu:0";
    desc->buffers[0].addr = 8192;
    desc->buffers[0].length = 128;
    desc->buffers[1].name = "cpu:1";
    desc->buffers[1].addr = 4096;
    desc->buffers[1].length = 256;
    metadata->addLocalSegment(LOCAL_SEGMENT_ID, "buffers", std::move(desc));

    std::vector<SegmentBufferInfo> buffers{{1, 1, "old"}};
    ASSERT_EQ(engine.getSegmentBuffers(LOCAL_SEGMENT_ID, buffers), 0);
    ASSERT_EQ(buffers.size(), 2);
    EXPECT_EQ(buffers[0].addr, 8192);
    EXPECT_EQ(buffers[0].length, 128);
    EXPECT_EQ(buffers[0].location, "cpu:0");
    EXPECT_EQ(buffers[1].addr, 4096);
    EXPECT_EQ(buffers[1].length, 256);
    EXPECT_EQ(buffers[1].location, "cpu:1");

    // Replacing the descriptor leaves an already returned snapshot intact.
    desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "buffers";
    desc->protocol = "rdma";
    metadata->addLocalSegment(LOCAL_SEGMENT_ID, "buffers", std::move(desc));
    EXPECT_EQ(buffers[0].length, 128);
    EXPECT_EQ(engine.getSegmentBuffers(LOCAL_SEGMENT_ID, buffers), 0);
    EXPECT_TRUE(buffers.empty());

    buffers.push_back({1, 1, "old"});
    EXPECT_EQ(engine.getSegmentBuffers(
                  static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT), buffers),
              ERR_METADATA);
    EXPECT_TRUE(buffers.empty());

    desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "buffers";
    desc->protocol = "nvmeof";
    metadata->addLocalSegment(LOCAL_SEGMENT_ID, "buffers", std::move(desc));
    buffers.push_back({1, 1, "old"});
    EXPECT_EQ(engine.getSegmentBuffers(LOCAL_SEGMENT_ID, buffers),
              ERR_NOT_IMPLEMENTED);
    EXPECT_TRUE(buffers.empty());
}

#ifdef USE_TENT
TEST_F(TransportTest, SegmentBuffersTentMemoryAndInvalidHandle) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    ScopedEnvVar hostname("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1");
    ScopedEnvVar config(
        "MC_TENT_CONF",
        R"({"transports":{"rdma":{"enable":false},"tcp":{"enable":true},"gds":{"enable":false},"shm":{"enable":false}}})");
    std::array<char, 128> memory{};
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, ""), 0);
    std::vector<SegmentBufferInfo> buffers{{1, 1, "old"}};
    ASSERT_EQ(engine.getSegmentBuffers(LOCAL_SEGMENT_ID, buffers), 0);
    EXPECT_TRUE(buffers.empty());

    ASSERT_EQ(engine.registerLocalMemory(memory.data(), memory.size()), 0);
    ASSERT_EQ(engine.getSegmentBuffers(LOCAL_SEGMENT_ID, buffers), 0);
    ASSERT_EQ(buffers.size(), 1);
    EXPECT_EQ(buffers[0].addr, reinterpret_cast<uint64_t>(memory.data()));
    EXPECT_EQ(buffers[0].length, memory.size());
    EXPECT_FALSE(buffers[0].location.empty());
    EXPECT_EQ(engine.unregisterLocalMemory(memory.data()), 0);
    EXPECT_EQ(buffers[0].length, memory.size());
    EXPECT_EQ(engine.getSegmentBuffers(LOCAL_SEGMENT_ID, buffers), 0);
    EXPECT_TRUE(buffers.empty());

    buffers.push_back({1, 1, "old"});
    EXPECT_EQ(engine.getSegmentBuffers(
                  static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT), buffers),
              ERR_METADATA);
    EXPECT_TRUE(buffers.empty());
}
#endif

static int CreateTempFile() {
    char temp_filename[] = "/tmp/testfileXXXXXX";
    int fd = mkstemp(temp_filename);
    if (fd == -1) {
        return -1;
    }
    unlink(temp_filename);
    return fd;
}

int CreateTempFileWithContent(const char* content) {
    char temp_filename[] = "/tmp/testfileXXXXXX";
    int fd = mkstemp(temp_filename);
    if (fd == -1) {
        return -1;
    }
    unlink(temp_filename);

    ssize_t nbytes = write(fd, content, strlen(content));
    (void)nbytes;
    lseek(fd, 0, SEEK_SET);

    return fd;
}

TEST_F(TransportTest, parseHostNameWithPortTest) {
    std::string local_server_name = "0.0.0.0:1234";
    auto res = parseHostNameWithPort(local_server_name);
    ASSERT_EQ(res.first, "0.0.0.0");
    ASSERT_EQ(res.second, 1234);

    local_server_name = "1.2.3.4:111111";
    res = parseHostNameWithPort(local_server_name);
    ASSERT_EQ(res.first, "1.2.3.4");
    ASSERT_EQ(res.second, 12001);
}

TEST_F(TransportTest, TransferTaskDestructorRunsSliceCleanup) {
    int cleanup_count = 0;
    {
        Transport::TransferTask task;
        auto* slice = new Transport::Slice();
        slice->source_addr = &cleanup_count;
        slice->cleanup_callback = [](Transport::Slice* released) {
            auto* count = static_cast<int*>(released->source_addr);
            ++*count;
        };
        task.slice_list.push_back(slice);
    }

    EXPECT_EQ(cleanup_count, 1);
}

TEST_F(TransportTest, SliceCleanupRunsOnceBeforeCacheReuse) {
    Transport::ThreadLocalSliceCache cache;
    int cleanup_count = 0;

    Transport::Slice* slice = cache.allocate();
    slice->source_addr = &cleanup_count;
    slice->cleanup_callback = [](Transport::Slice* released) {
        auto* count = static_cast<int*>(released->source_addr);
        ++*count;
    };

    cache.deallocate(slice);
    EXPECT_EQ(cleanup_count, 1);

    Transport::Slice* reused = cache.allocate();
    EXPECT_EQ(reused, slice);
    EXPECT_EQ(reused->cleanup_callback, nullptr);

    // A backend that does not install a callback must not inherit the callback
    // from the previous owner of this cached slice.
    cache.deallocate(reused);
    EXPECT_EQ(cleanup_count, 1);
}

TEST_F(TransportTest, WriteSuccess) {
    int fd = CreateTempFile();
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    const char* testData = "Hello, World!";
    size_t testDataLen = strlen(testData);

    ssize_t result = writeFully(fd, testData, testDataLen);
    EXPECT_EQ(result, static_cast<ssize_t>(testDataLen));

    char buffer[256] = {0};
    ssize_t nbytes = lseek(fd, 0, SEEK_SET);
    (void)nbytes;
    nbytes = read(fd, buffer, testDataLen);
    (void)nbytes;
    EXPECT_STREQ(buffer, testData);

    close(fd);
}

TEST_F(TransportTest, WriteInvalidFD) {
    const char* testData = "Hello, World!";
    size_t testDataLen = strlen(testData);

    ssize_t result = writeFully(-1, testData, testDataLen);
    ASSERT_EQ(result, -1);
    ASSERT_EQ(errno, EBADF);
}

TEST_F(TransportTest, PartialWrite) {
    int fd = CreateTempFile();
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    const char* testData = "Hello, World!";
    size_t testDataLen = strlen(testData);

    ssize_t result = writeFully(fd, testData, testDataLen / 2);

    ASSERT_EQ(result, static_cast<ssize_t>(testDataLen / 2));

    char buffer[256] = {0};
    lseek(fd, 0, SEEK_SET);
    ssize_t nbytes = read(fd, buffer, result);
    (void)nbytes;
    ASSERT_EQ(strncmp(buffer, testData, result), 0);
    close(fd);
}

TEST_F(TransportTest, ReadSuccess) {
    const char* testData = "Hello, World!";
    int fd = CreateTempFileWithContent(testData);
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    char buffer[256] = {0};
    ssize_t bytesRead = readFully(fd, buffer, sizeof(buffer));

    EXPECT_EQ(bytesRead, static_cast<ssize_t>(strlen(testData)));
    EXPECT_STREQ(buffer, testData);

    close(fd);
}

TEST_F(TransportTest, ReadInvalidFD) {
    char buffer[256] = {0};
    ssize_t bytesRead = readFully(-1, buffer, sizeof(buffer));
    EXPECT_EQ(bytesRead, -1);
    EXPECT_EQ(errno, EBADF);
}

TEST_F(TransportTest, PartialRead) {
    const char* testData = "Hello, World!";
    int fd = CreateTempFileWithContent(testData);
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    char buffer[256] = {0};
    size_t half_len = strlen(testData) / 2;
    ssize_t bytesRead = readFully(fd, buffer, half_len);

    EXPECT_EQ(bytesRead, static_cast<ssize_t>(half_len));
    EXPECT_EQ(strncmp(buffer, testData, half_len), 0);

    close(fd);
}

TEST_F(TransportTest, ReadEmptyFile) {
    int fd = CreateTempFileWithContent("");
    ASSERT_NE(fd, -1) << "Failed to create temporary file";

    char buffer[256] = {0};
    ssize_t bytesRead = readFully(fd, buffer, sizeof(buffer));

    EXPECT_EQ(bytesRead, static_cast<ssize_t>(0));

    close(fd);
}

TEST_F(TransportTest, RegisterLocalMemoryBatchRejectsOverlappingBuffers) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);

    std::array<char, 256> buffer{};
    std::vector<BufferEntry> entries = {
        {buffer.data() + 64, 128},
        {buffer.data(), 128},
    };

    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"),
              ERR_ADDRESS_OVERLAPPED);
}

TEST_F(TransportTest, RegisterLocalMemoryBatchRejectsZeroLengthBuffer) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);

    std::array<char, 1> buffer{};
    std::vector<BufferEntry> entries = {
        {buffer.data(), 0},
    };

    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"),
              ERR_INVALID_ARGUMENT);
}

TEST_F(TransportTest, RegisterLocalMemoryBatchAllowsAdjacentBuffers) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);

    std::array<char, 256> buffer{};
    std::vector<BufferEntry> entries = {
        {buffer.data() + 128, 128},
        {buffer.data(), 128},
    };

    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"), 0);
}

TEST_F(TransportTest, ConcurrentRegisterLocalMemoryRejectsOverlap) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport = std::make_shared<BlockingRegistrationTransport>();
    TransferEngineImplTestPeer::replaceTransports(engine, transport);

    std::array<char, 128> buffer{};
    auto first = std::async(std::launch::async, [&] {
        return engine.registerLocalMemory(buffer.data(), buffer.size(),
                                          "cpu:0");
    });
    transport->waitForFirstRegistration();

    int second =
        engine.registerLocalMemory(buffer.data(), buffer.size(), "cpu:0");
    int registration_calls = transport->registrationCalls();
    transport->releaseFirstRegistration();

    EXPECT_EQ(second, ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(registration_calls, 1);
    EXPECT_EQ(first.get(), 0);
    EXPECT_EQ(engine.unregisterLocalMemory(buffer.data()), 0);
}

TEST_F(TransportTest, ConcurrentRegisterLocalMemoryBatchRejectsOverlap) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport = std::make_shared<BlockingRegistrationTransport>();
    TransferEngineImplTestPeer::replaceTransports(engine, transport);

    std::array<char, 128> buffer{};
    std::vector<BufferEntry> entries = {{buffer.data(), buffer.size()}};
    auto first = std::async(std::launch::async, [&] {
        return engine.registerLocalMemoryBatch(entries, "cpu:0");
    });
    transport->waitForFirstRegistration();

    int second = engine.registerLocalMemoryBatch(entries, "cpu:0");
    int registration_calls = transport->registrationCalls();
    transport->releaseFirstRegistration();

    EXPECT_EQ(second, ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(registration_calls, 1);
    EXPECT_EQ(first.get(), 0);
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({buffer.data()}), 0);
}

TEST_F(TransportTest, FailedRegistrationReleasesReservedRegion) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport =
        std::make_shared<BlockingRegistrationTransport>(ERR_MEMORY);
    TransferEngineImplTestPeer::replaceTransports(engine, transport);

    std::array<char, 128> buffer{};
    auto first = std::async(std::launch::async, [&] {
        return engine.registerLocalMemory(buffer.data(), buffer.size(),
                                          "cpu:0");
    });
    transport->waitForFirstRegistration();
    transport->releaseFirstRegistration();

    EXPECT_EQ(first.get(), ERR_MEMORY);
    EXPECT_EQ(engine.registerLocalMemory(buffer.data(), buffer.size(), "cpu:0"),
              0);
    EXPECT_EQ(engine.unregisterLocalMemory(buffer.data()), 0);
}

TEST_F(TransportTest, UnregisterLocalMemoryBatchPropagatesTransportError) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);

    std::array<char, 1> buffer{};
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({buffer.data()}),
              ERR_ADDRESS_NOT_REGISTERED);
}

TEST_F(TransportTest, UnregisterLocalMemoryBatchContinuesAcrossTransports) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto failing = std::make_shared<BatchResultTransport>(ERR_MEMORY);
    auto succeeding = std::make_shared<BatchResultTransport>();
    TransferEngineImplTestPeer::replaceTransports(
        engine, {{"a-failing", failing}, {"b-succeeding", succeeding}});

    std::array<char, 1> buffer{};
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({buffer.data()}), ERR_MEMORY);
    EXPECT_EQ(failing->unregisterBatchCalls(), 1);
    EXPECT_EQ(succeeding->unregisterBatchCalls(), 1);
}

TEST_F(TransportTest, UnregisterLocalMemoryBatchContinuesAfterAddressError) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);

    std::array<char, 2> registered{};
    std::array<char, 1> missing{};
    std::vector<BufferEntry> entries = {
        {registered.data(), 1},
        {registered.data() + 1, 1},
    };
    ASSERT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"), 0);

    auto metadata = engine.getMetadata();
    ASSERT_NE(metadata, nullptr);
    auto contains_buffer = [&](void* addr) {
        auto desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!desc) return false;
        auto value = reinterpret_cast<uintptr_t>(addr);
        return std::any_of(
            desc->buffers.begin(), desc->buffers.end(),
            [value](const auto& buffer) { return buffer.addr == value; });
    };
    ASSERT_TRUE(contains_buffer(registered.data()));
    ASSERT_TRUE(contains_buffer(registered.data() + 1));

    EXPECT_EQ(engine.unregisterLocalMemoryBatch(
                  {missing.data(), registered.data(), registered.data() + 1}),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_FALSE(contains_buffer(registered.data()));
    EXPECT_FALSE(contains_buffer(registered.data() + 1));
}

TEST_F(TransportTest, RegisterLocalMemoryBatchRollsBackAttemptedTransports) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto succeeding = std::make_shared<BatchResultTransport>();
    auto failing = std::make_shared<BatchResultTransport>();
    failing->setRegisterResult(ERR_MEMORY);
    TransferEngineImplTestPeer::replaceTransports(
        engine, {{"a-succeeding", succeeding}, {"b-failing", failing}});

    std::array<char, 2> buffer{};
    std::vector<BufferEntry> entries = {
        {buffer.data(), 1},
        {buffer.data() + 1, 1},
    };
    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"), ERR_MEMORY);
    EXPECT_EQ(succeeding->registeredBufferCount(), 0);
    EXPECT_EQ(failing->registeredBufferCount(), 0);
    EXPECT_EQ(succeeding->unregisterBatchCalls(), 1);
    EXPECT_EQ(failing->unregisterBatchCalls(), 1);

    failing->setRegisterResult(0);
    EXPECT_EQ(engine.registerLocalMemoryBatch(entries, "cpu:0"), 0);
    EXPECT_EQ(succeeding->registeredBufferCount(), entries.size());
    EXPECT_EQ(failing->registeredBufferCount(), entries.size());
    EXPECT_EQ(
        engine.unregisterLocalMemoryBatch({buffer.data(), buffer.data() + 1}),
        0);
}

TEST_F(TransportTest, FreeBatchClearsPendingNotify) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport = std::make_shared<TerminalFailureTransport>(true);
    TransferEngineImplTestPeer::replaceTransports(
        engine, {{"terminal-failure", transport}});

    constexpr SegmentID kSegmentId = 12;
    auto descriptor = std::make_shared<TransferMetadata::SegmentDesc>();
    descriptor->name = "remote";
    descriptor->protocol = "terminal-failure";
    engine.getMetadata()->addLocalSegment(kSegmentId, "remote",
                                          std::move(descriptor));

    std::array<char, 1> buffer{};
    TransferRequest request{.opcode = TransferRequest::READ,
                            .source = buffer.data(),
                            .target_id = kSegmentId,
                            .target_offset = 0,
                            .length = buffer.size()};
    auto batch_id = engine.allocateBatchID(1);
    ASSERT_TRUE(
        engine
            .submitTransferWithNotify(batch_id, {request}, {"name", "payload"})
            .ok());
    ASSERT_EQ(TransferEngineImplTestPeer::pendingNotifyCount(engine), 1);

    TransferStatus status;
    ASSERT_TRUE(engine.getBatchTransferStatus(batch_id, status).ok());
    ASSERT_EQ(status.s, TransferStatusEnum::FAILED);
    ASSERT_TRUE(engine.freeBatchID(batch_id).ok());
    EXPECT_EQ(TransferEngineImplTestPeer::pendingNotifyCount(engine), 0);
}

TEST_F(TransportTest, BusyBatchKeepsPendingNotify) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport = std::make_shared<TerminalFailureTransport>(false);
    TransferEngineImplTestPeer::replaceTransports(
        engine, {{"terminal-failure", transport}});

    constexpr SegmentID kSegmentId = 12;
    auto descriptor = std::make_shared<TransferMetadata::SegmentDesc>();
    descriptor->name = "remote";
    descriptor->protocol = "terminal-failure";
    engine.getMetadata()->addLocalSegment(kSegmentId, "remote",
                                          std::move(descriptor));

    std::array<char, 1> buffer{};
    TransferRequest request{.opcode = TransferRequest::READ,
                            .source = buffer.data(),
                            .target_id = kSegmentId,
                            .target_offset = 0,
                            .length = buffer.size()};
    auto batch_id = engine.allocateBatchID(1);
    ASSERT_TRUE(
        engine
            .submitTransferWithNotify(batch_id, {request}, {"name", "payload"})
            .ok());

    EXPECT_TRUE(engine.freeBatchID(batch_id).IsBatchBusy());
    EXPECT_EQ(TransferEngineImplTestPeer::pendingNotifyCount(engine), 1);

    transport->finishTasks();
    ASSERT_TRUE(engine.freeBatchID(batch_id).ok());
    EXPECT_EQ(TransferEngineImplTestPeer::pendingNotifyCount(engine), 0);
}

TEST_F(TransportTest, BatchCleanupRunsAfterBeforeDeleteCallback) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);

    bool before_delete_finished = false;
    bool cleanup_observed_callback = false;
    struct CleanupProbe {
        bool* before_delete_finished;
        bool* cleanup_observed_callback;
    };
    CleanupProbe probe{&before_delete_finished, &cleanup_observed_callback};

    auto batch_id = engine.allocateBatchID(1);
    auto& batch = Transport::toBatchDesc(batch_id);
    auto& task = batch.task_list.emplace_back();
    task.is_finished = true;
    auto* slice = new Transport::Slice();
    slice->source_addr = &probe;
    slice->cleanup_callback = [](Transport::Slice* released) {
        auto* probe = static_cast<CleanupProbe*>(released->source_addr);
        *probe->cleanup_observed_callback = *probe->before_delete_finished;
    };
    task.slice_list.push_back(slice);

    auto status = TransferEngineImplTestPeer::freeBatchWithCallback(
        engine, batch_id, [&] { before_delete_finished = true; });

    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(cleanup_observed_callback);
}

#ifdef USE_TCP
TEST_F(TransportTest, TcpScatterCombinesRequestsIntoOneTask) {
    constexpr size_t kRequestCount = 256;
    constexpr size_t kRequestBytes = 128;
    constexpr size_t kBytes = kRequestCount * kRequestBytes;
    // Keep request metadata and buffers alive until after engine shutdown,
    // including when an unexpected transport failure aborts the test.
    std::array<char, 2 * kBytes> buffer{};
    std::vector<TransferRequest> requests;
    requests.reserve(kRequestCount);
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    ASSERT_EQ(engine.registerLocalMemory(buffer.data(), buffer.size(), "cpu:0"),
              0);
    auto segment = engine.openSegment(engine.getLocalIpAndPort());
    ASSERT_NE(engine.getMetadata()->getSegmentDescByID(segment), nullptr);

    for (auto opcode : {TransferRequest::READ, TransferRequest::WRITE}) {
        SCOPED_TRACE(opcode == TransferRequest::READ ? "READ" : "WRITE");
        auto* source = opcode == TransferRequest::READ ? buffer.data() + kBytes
                                                       : buffer.data();
        auto* destination = opcode == TransferRequest::READ
                                ? buffer.data()
                                : buffer.data() + kBytes;
        std::fill(buffer.begin(), buffer.end(), 0);
        std::fill_n(source, kBytes, 37);
        requests.clear();
        for (size_t i = 0; i < kRequestCount; ++i) {
            requests.push_back(TransferRequest{
                .opcode = opcode,
                .source = buffer.data() + i * kRequestBytes,
                .target_id = segment,
                .target_offset =
                    reinterpret_cast<uint64_t>(buffer.data() + kBytes) +
                    i * kRequestBytes,
                .length = kRequestBytes,
                .task_group_id = 1,
            });
        }

        MultiTransport::ScatterSubmission submission;
        const auto submitted = engine.submitScatter(requests, submission);
        ASSERT_NE(submission.batch_id, INVALID_BATCH_ID);
        const auto& tasks =
            Transport::toBatchDesc(submission.batch_id).task_list;
        const size_t task_count = tasks.size();
        std::vector<size_t> request_counts;
        for (const auto& task : tasks)
            request_counts.push_back(task.request_count);

        // Drain every actual task before asserting grouping, so the regression
        // case (one task per request) also releases its batch normally.
        std::vector<bool> done(task_count, false);
        bool successful = true;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(15);
        size_t remaining = task_count;
        while (remaining != 0 && std::chrono::steady_clock::now() < deadline) {
            for (size_t i = 0; i < task_count; ++i) {
                if (done[i]) continue;
                TransferStatus status;
                auto result =
                    engine.getTransferStatus(submission.batch_id, i, status);
                if (!result.ok()) continue;
                if (status.s == TransferStatusEnum::COMPLETED ||
                    status.s == TransferStatusEnum::FAILED) {
                    successful &= status.s == TransferStatusEnum::COMPLETED;
                    done[i] = true;
                    --remaining;
                }
            }
            if (remaining != 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_EQ(remaining, 0u)
            << "TCP scatter did not reach physical completion";
        EXPECT_TRUE(engine.freeBatchID(submission.batch_id).ok());
        EXPECT_TRUE(submitted.ok());
        EXPECT_TRUE(successful);
        EXPECT_EQ(memcmp(source, destination, kBytes), 0);
        EXPECT_EQ(task_count, 1u);
        EXPECT_EQ(request_counts, (std::vector<size_t>{kRequestCount}));
        EXPECT_EQ(submission.task_sizes, (std::vector<size_t>{kRequestCount}));
    }
    EXPECT_EQ(engine.closeSegment(segment), 0);
}
#endif

TEST_F(TransportTest, ScatterSubmitFailurePreservesCompletedFragments) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    auto transport = std::make_shared<PartialFailureSubmissionTransport>();
    auto& impl = TransferEngineImplTestPeer::implementation(engine);
    TransferEngineImplTestPeer::replaceTransports(
        impl, {{"partial-failure", transport}});

    constexpr SegmentID kSegmentId = 12;
    auto descriptor = std::make_shared<TransferMetadata::SegmentDesc>();
    descriptor->name = "remote";
    descriptor->protocol = "partial-failure";
    impl.getMetadata()->addLocalSegment(kSegmentId, "remote",
                                        std::move(descriptor));
    std::array<char, 2> buffer{};
    std::array<size_t, 1> offsets{0};
    std::array<size_t, 1> lengths{1};

    auto run = [&] {
        std::vector<bool> fragment_ok;
        TransferEngine::ScatterTransferRange range{
            .opcode = TransferRequest::READ,
            .remote_segment = "remote",
            .remote_base_offset = 0,
            .remote_size = buffer.size(),
            .local_buffer = buffer.data(),
            .local_capacity = 1,
            .local_offsets = offsets,
            .remote_offsets = offsets,
            .lengths = lengths,
            .on_fragment_complete =
                [&](size_t, const Status& status) {
                    fragment_ok.push_back(status.ok());
                },
        };
        auto operation = engine.submitScatter({range, range});
        EXPECT_FALSE(operation.wait().ok());
        return fragment_ok;
    };

    EXPECT_EQ(run(), (std::vector<bool>{true, false}));
    EXPECT_EQ(transport->request_counts, (std::vector<size_t>{2}));
    transport->addExtraSlice();
    EXPECT_EQ(run(), (std::vector<bool>{false, false}));
}

// Model the state left by completion events explicitly so these regressions
// also run in default builds without USE_EVENT_DRIVEN_COMPLETION.
TEST_F(TransportTest, FailedCompletionEventDoesNotReportSuccess) {
    std::string server_name = "localhost";
    MultiTransport transport(nullptr, server_name);
    Transport::BatchDesc batch{};
    batch.id = reinterpret_cast<Transport::BatchID>(&batch);
    batch.batch_size = 1;
    batch.task_list.resize(1);
    auto& task = batch.task_list.front();
    task.batch_id = batch.id;
    task.slice_count = 1;
    task.failed_slice_count = 1;
    task.is_finished = true;
    batch.has_failure.store(true);
    batch.is_finished.store(true);
    ASSERT_FALSE(batch.status_cached.load());

    Transport::TransferStatus status{};
    ASSERT_TRUE(transport.getBatchTransferStatus(batch.id, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_FALSE(batch.status_cached.load());
}

TEST_F(TransportTest, SuccessfulCompletionEventPreservesTransferredBytes) {
    std::string server_name = "localhost";
    MultiTransport transport(nullptr, server_name);
    Transport::BatchDesc batch{};
    batch.id = reinterpret_cast<Transport::BatchID>(&batch);
    batch.batch_size = 1;
    batch.task_list.resize(1);
    auto& task = batch.task_list.front();
    task.batch_id = batch.id;
    task.slice_count = 1;
    task.transferred_bytes = 65536;
    task.success_slice_count = 1;
    task.is_finished = true;
    batch.is_finished.store(true);
    ASSERT_FALSE(batch.has_failure.load());
    ASSERT_FALSE(batch.status_cached.load());
    ASSERT_EQ(batch.finished_transfer_bytes.load(), 0);

    Transport::TransferStatus status{};
    ASSERT_TRUE(transport.getBatchTransferStatus(batch.id, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 65536);
}

TEST_F(TransportTest, RepeatedBatchQueryPreservesAggregatedBytes) {
    std::string server_name = "localhost";
    MultiTransport transport(nullptr, server_name);
    Transport::BatchDesc batch{};
    batch.id = reinterpret_cast<Transport::BatchID>(&batch);
    batch.batch_size = 2;
    batch.task_list.resize(2);
    const std::array<uint64_t, 2> task_bytes{65536, 131072};
    for (size_t i = 0; i < batch.task_list.size(); ++i) {
        auto& task = batch.task_list[i];
        task.batch_id = batch.id;
        task.slice_count = 1;
        task.transferred_bytes = task_bytes[i];
        task.success_slice_count = 1;
    }
    const auto total_bytes = task_bytes[0] + task_bytes[1];
    ASSERT_FALSE(batch.is_finished.load());
    ASSERT_FALSE(batch.status_cached.load());

    Transport::TransferStatus status{};
    ASSERT_TRUE(transport.getBatchTransferStatus(batch.id, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, total_bytes);
    EXPECT_TRUE(batch.is_finished.load());
    ASSERT_TRUE(batch.status_cached.load());
    EXPECT_EQ(batch.finished_transfer_bytes.load(), total_bytes);

    status = {};
    ASSERT_TRUE(transport.getBatchTransferStatus(batch.id, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, total_bytes);
}

#ifdef USE_EVENT_DRIVEN_COMPLETION
TEST_F(TransportTest, GroupedTaskCompletionWaitsForSubmissionSeal) {
    Transport::BatchDesc batch{};
    batch.id = reinterpret_cast<Transport::BatchID>(&batch);
    batch.batch_size = 1;
    Transport::TransferTask task;
    task.batch_id = batch.id;
    task.request_count = 2;
    task.submission_sealed = false;

    auto complete_slice = [&] {
        __atomic_add_fetch(&task.slice_count, 1, __ATOMIC_ACQ_REL);
        Transport::Slice slice{};
        slice.task = &task;
        slice.length = 1;
        slice.markSuccess();
    };
    complete_slice();
    EXPECT_FALSE(task.is_finished);
    complete_slice();
    EXPECT_FALSE(task.is_finished);

    Transport::Slice::sealTaskSubmission(&task);
    EXPECT_TRUE(task.is_finished);
    EXPECT_TRUE(batch.is_finished.load());
    EXPECT_EQ(batch.finished_task_count.load(), 1);

    Transport::Slice::sealTaskSubmission(&task);
    EXPECT_EQ(batch.finished_task_count.load(), 1);
}
#endif

#ifdef USE_TENT
TEST(TransferEngineTentCompatibilityTest,
     InstallTransportReturnsCompatibilityHandle) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    ScopedEnvVar force_tcp("MC_FORCE_TCP", "1");
    ScopedEnvVar hostname("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1");

    TransferEngine engine;
    ASSERT_TRUE(engine.isUsingTent());
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "tent-install-transport"), 0);

    Transport* transport = nullptr;
    for (const auto* protocol : {"tcp", "efa", "cxi", "ascend"}) {
        auto* current = engine.installTransport(protocol, nullptr);
        ASSERT_NE(current, nullptr) << protocol;
        if (transport)
            EXPECT_EQ(current, transport) << protocol;
        else
            transport = current;
    }

    EXPECT_EQ(transport->allocateBatchID(1), INVALID_BATCH_ID);
    EXPECT_EQ(transport->freeBatchID(INVALID_BATCH_ID).code(),
              Status::Code::kNotImplemented);
    EXPECT_EQ(transport->submitTransfer(0, {}).code(),
              Status::Code::kNotImplemented);
    TransferStatus status{};
    EXPECT_EQ(transport->getTransferStatus(0, 0, status).code(),
              Status::Code::kNotImplemented);
    EXPECT_EQ(engine.uninstallTransport("tcp"), 0);
}

TEST(TransferEngineTentCompatibilityTest,
     CApiInstallTransportReturnsCompatibilityHandle) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    ScopedEnvVar force_tcp("MC_FORCE_TCP", "1");
    ScopedEnvVar hostname("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1");

    transfer_engine_t engine = createTransferEngine(
        P2PHANDSHAKE, "tent-c-api-install-transport", "", 0, false);
    ASSERT_NE(engine, nullptr);

    transport_t transport = nullptr;
    for (const auto* protocol : {"tcp", "efa", "cxi", "ascend"}) {
        auto current = installTransport(engine, protocol, nullptr);
        ASSERT_NE(current, nullptr) << protocol;
        if (transport)
            EXPECT_EQ(current, transport) << protocol;
        else
            transport = current;
    }
    EXPECT_EQ(uninstallTransport(engine, "tcp"), 0);
    destroyTransferEngine(engine);
}

TEST(TransferEngineTentCompatibilityTest,
     ConcurrentInstallTransportReturnsOneCompatibilityHandle) {
    ScopedEnvVar use_tent("MC_USE_TENT", "1");
    ScopedEnvVar force_tcp("MC_FORCE_TCP", "1");
    ScopedEnvVar hostname("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1");

    TransferEngine engine;
    ASSERT_TRUE(engine.isUsingTent());
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "tent-concurrent-install"), 0);

    constexpr size_t kThreadCount = 16;
    std::array<Transport*, kThreadCount> transports{};
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&engine, &transports, i] {
            transports[i] = engine.installTransport("tcp", nullptr);
        });
    }
    for (auto& thread : threads) thread.join();

    ASSERT_NE(transports[0], nullptr);
    for (const auto* transport : transports) {
        ASSERT_NE(transport, nullptr);
        EXPECT_EQ(transport, transports[0]);
    }
}
#endif

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
