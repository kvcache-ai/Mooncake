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

#include <acl/acl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "shared_segment/host_memfd.h"
#ifdef TENT_HAS_ASCEND_ALLOCATOR
#include "ascend_allocator.h"
#endif
#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/ascend/ascend_direct_transport.h"
#include "tent/transport/ascend/hixl_engine.h"
#include "tent/transport/ascend/resource_config.h"

namespace {

thread_local int g_device_id = 0;
thread_local aclrtContext g_context = reinterpret_cast<aclrtContext>(0x10000);
int g_device_count = 1;
// False stands for a thread that never bound a context, which is what the
// signal handling thread looks like when it tears the client down.
bool g_context_bound = true;
// What aclrtPointerGetAttributes() reports for unknown locations; tests change
// it to exercise the HIXL host/device classification.
aclrtMemLocationType g_pointer_location_type = ACL_MEM_LOCATION_TYPE_HOST;
int g_stream_creates = 0;
int g_stream_destroys = 0;
int g_memcpy_async_calls = 0;
bool g_memcpy_async_fail = false;
bool g_skip_memcpy_copy = false;
uintptr_t g_next_stream = 0x3000;
// Ordered ACL events, so a test can assert that the streams were aborted
// before the host flags of the failed group were released.
int g_acl_event_seq = 0;
int g_last_stream_abort_seq = 0;
std::vector<void*> g_host_allocs;
std::map<void*, int> g_host_free_seq;

constexpr uintptr_t kContextBase = 0x10000;

aclrtContext ContextForDevice(int device_id) {
    return reinterpret_cast<aclrtContext>(kContextBase +
                                          static_cast<uintptr_t>(device_id));
}

int DeviceForContext(aclrtContext context) {
    const auto value = reinterpret_cast<uintptr_t>(context);
    if (value >= kContextBase && value < kContextBase + 1024) {
        return static_cast<int>(value - kContextBase);
    }
    return -1;
}

void ResetAcl() {
    g_device_id = 0;
    g_device_count = 1;
    g_context_bound = true;
    g_context = ContextForDevice(0);
    g_pointer_location_type = ACL_MEM_LOCATION_TYPE_HOST;
    g_stream_creates = 0;
    g_stream_destroys = 0;
    g_memcpy_async_calls = 0;
    g_memcpy_async_fail = false;
    g_skip_memcpy_copy = false;
    g_next_stream = 0x3000;
    g_acl_event_seq = 0;
    g_last_stream_abort_seq = 0;
    g_host_allocs.clear();
    g_host_free_seq.clear();
}

}  // namespace

extern "C" {

aclError aclrtGetDeviceCount(uint32_t* count) {
    *count = static_cast<uint32_t>(g_device_count);
    return ACL_ERROR_NONE;
}

aclError aclrtGetDevice(int* deviceId) {
    *deviceId = g_device_id;
    return ACL_ERROR_NONE;
}

aclError aclrtSetDevice(int deviceId) {
    if (deviceId >= g_device_count) {
        return 1;
    }
    g_device_id = deviceId;
    g_context = ContextForDevice(deviceId);
    return ACL_ERROR_NONE;
}

aclError aclrtGetCurrentContext(aclrtContext* context) {
    if (!g_context_bound) {
        return 1;
    }
    *context = g_context;
    return ACL_ERROR_NONE;
}

aclError aclrtSetCurrentContext(aclrtContext context) {
    g_context = context;
    const int device_id = DeviceForContext(context);
    if (device_id >= 0) {
        g_device_id = device_id;
    }
    return ACL_ERROR_NONE;
}

aclError aclrtPointerGetAttributes(const void* ptr,
                                   aclrtPtrAttributes* attributes) {
    (void)ptr;
    attributes->location.id = 0;
    attributes->location.type = g_pointer_location_type;
    return ACL_ERROR_NONE;
}

aclError aclrtMemcpy(void* dst, size_t destMax, const void* src, size_t count,
                     aclrtMemcpyKind kind) {
    (void)kind;
    if (dst == nullptr || src == nullptr || count == 0) {
        return ACL_ERROR_NONE;
    }
    std::memcpy(dst, src, std::min(destMax, count));
    return ACL_ERROR_NONE;
}

aclError aclrtMemcpyAsync(void* dst, size_t destMax, const void* src,
                          size_t count, aclrtMemcpyKind kind,
                          aclrtStream stream) {
    (void)kind;
    (void)stream;
    ++g_memcpy_async_calls;
    if (g_memcpy_async_fail) {
        return 1;
    }
    if (g_skip_memcpy_copy || dst == nullptr || src == nullptr || count == 0) {
        return ACL_ERROR_NONE;
    }
    const auto dst_addr = reinterpret_cast<uintptr_t>(dst);
    const auto src_addr = reinterpret_cast<uintptr_t>(src);
    if (dst_addr < 0x10000 || src_addr < 0x10000) {
        return ACL_ERROR_NONE;
    }
    std::memcpy(dst, src, std::min(destMax, count));
    return ACL_ERROR_NONE;
}

aclError aclrtMalloc(void** devPtr, size_t size, aclrtMemMallocPolicy policy) {
    (void)policy;
    *devPtr = std::malloc(size);
    return *devPtr != nullptr ? ACL_ERROR_NONE : 1;
}

aclError aclrtFree(void* devPtr) {
    std::free(devPtr);
    return ACL_ERROR_NONE;
}

aclError aclrtMallocHost(void** hostPtr, size_t size) {
    *hostPtr = std::malloc(size);
    if (*hostPtr != nullptr) {
        g_host_allocs.push_back(*hostPtr);
    }
    return *hostPtr != nullptr ? ACL_ERROR_NONE : 1;
}

aclError aclrtFreeHost(void* hostPtr) {
    g_host_free_seq[hostPtr] = ++g_acl_event_seq;
    std::free(hostPtr);
    return ACL_ERROR_NONE;
}

aclError aclrtCreateStreamWithConfig(aclrtStream* stream, uint32_t priority,
                                     uint32_t flag) {
    (void)priority;
    (void)flag;
    *stream = reinterpret_cast<aclrtStream>(g_next_stream++);
    g_stream_creates++;
    return ACL_ERROR_NONE;
}

aclError aclrtDestroyStream(aclrtStream stream) {
    (void)stream;
    g_stream_destroys++;
    return ACL_ERROR_NONE;
}

aclError aclrtStreamAbort(aclrtStream stream) {
    (void)stream;
    g_last_stream_abort_seq = ++g_acl_event_seq;
    return ACL_ERROR_NONE;
}

const char* aclGetRecentErrMsg() { return "mock acl error message"; }

aclError aclrtGetPhyDevIdByLogicDevId(int32_t logic_dev_id,
                                      int32_t* physical_dev_id) {
    *physical_dev_id = logic_dev_id;
    return ACL_ERROR_NONE;
}

// The unit test process has no NPU. Report that the logic id cannot be
// resolved so the affinity plan stays empty instead of falling through to the
// real driver's DSMI query.
aclError aclrtGetLogicDevIdByUserDevId(int32_t user_dev_id,
                                       int32_t* logic_dev_id) {
    (void)user_dev_id;
    *logic_dev_id = -1;
    return 1;
}

}  // extern "C"

namespace mooncake {
namespace tent {
namespace {

const char* kEnvKeys[] = {
    "ASCEND_TRANSFER_TIMEOUT",
    "ASCEND_LOCAL_COMM_RES",
    "ASCEND_ENABLE_USE_FABRIC_MEM",
    "ASCEND_GLOBAL_RESOURCE_CONFIG",
    "ASCEND_RDMA_TC",
    "ASCEND_RDMA_SL",
    "HCCL_RDMA_TC",
    "HCCL_RDMA_SL",
    "HCCL_INTRA_ROCE_ENABLE",
    "ASCEND_BASE_PORT",
};

void ClearAscendEnv() {
    for (const char* key : kEnvKeys) {
        unsetenv(key);
    }
}

struct MockHixlState {
    std::mutex mu;
    int initialize_calls = 0;
    int connect_calls = 0;
    int disconnect_calls = 0;
    int transfer_async_calls = 0;
    int get_status_calls = 0;
    int register_calls = 0;
    int deregister_calls = 0;
    std::vector<bool> register_local_only;
    std::vector<bool> register_host;
    int concurrent_transfer = 0;
    int max_concurrent_transfer = 0;
    bool fail_next_transfer = false;
    int fail_register_on = -1;
    // How long TransferAsync stays inside HIXL before returning a handle. The
    // queued-timeout and concurrency tests change it; 5 ms is the historical
    // value the other tests were written against.
    int transfer_delay_ms = 5;
    HixlXferState xfer_state = HixlXferState::Completed;
    std::deque<uint32_t> initialize_results;
    std::vector<std::string> initialize_names;
    // Return value of Disconnect; kHixlNotConnected is what a peer whose
    // session was already torn down reports, and it must be tolerated.
    uint32_t disconnect_status = kHixlOk;
    std::map<std::string, std::string> last_options;
    std::string last_remote;
    std::vector<std::map<std::string, std::string>> all_options;
    std::atomic<uintptr_t> next_handle{0x2000};
};

class MockHixlVendor final : public HixlEngine::Vendor {
   public:
    explicit MockHixlVendor(std::shared_ptr<MockHixlState> state)
        : state_(std::move(state)) {}

    uint32_t Initialize(
        const std::string& local_name,
        const std::map<std::string, std::string>& options) override {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->initialize_calls++;
        state_->initialize_names.push_back(local_name);
        state_->last_options = options;
        state_->all_options.push_back(options);
        if (!state_->initialize_results.empty()) {
            const auto result = state_->initialize_results.front();
            state_->initialize_results.pop_front();
            return result;
        }
        return kHixlOk;
    }

    void Finalize() override {}

    uint32_t RegisterMem(uint64_t addr, size_t len, bool host, bool local_only,
                         void*& handle) override {
        (void)addr;
        (void)len;
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->register_calls++;
        state_->register_host.push_back(host);
        state_->register_local_only.push_back(local_only);
        if (state_->fail_register_on == state_->register_calls) {
            handle = nullptr;
            return 1;
        }
        handle = reinterpret_cast<void*>(
            state_->next_handle.fetch_add(1, std::memory_order_relaxed));
        return kHixlOk;
    }

    uint32_t DeregisterMem(void* handle) override {
        (void)handle;
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->deregister_calls++;
        return kHixlOk;
    }

    uint32_t TransferAsync(const std::string& remote, bool write,
                           const std::vector<HixlOpDesc>& descs,
                           void*& req) override {
        (void)write;
        (void)descs;
        int delay_ms = 0;
        {
            std::lock_guard<std::mutex> lock(state_->mu);
            state_->transfer_async_calls++;
            state_->concurrent_transfer++;
            state_->max_concurrent_transfer = std::max(
                state_->max_concurrent_transfer, state_->concurrent_transfer);
            state_->last_remote = remote;
            if (state_->fail_next_transfer) {
                state_->fail_next_transfer = false;
                state_->concurrent_transfer--;
                req = nullptr;
                return 1;
            }
            delay_ms = state_->transfer_delay_ms;
        }
        if (delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        }
        {
            std::lock_guard<std::mutex> lock(state_->mu);
            state_->concurrent_transfer--;
            req = reinterpret_cast<void*>(
                state_->next_handle.fetch_add(1, std::memory_order_relaxed));
        }
        return kHixlOk;
    }

    uint32_t GetTransferStatus(void* req, HixlXferState& status) override {
        (void)req;
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->get_status_calls++;
        status = state_->xfer_state;
        return kHixlOk;
    }

    uint32_t Disconnect(const std::string& remote,
                        int32_t timeout_ms) override {
        (void)remote;
        (void)timeout_ms;
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->disconnect_calls++;
        return state_->disconnect_status;
    }

    uint32_t Connect(const std::string& remote, int32_t timeout_ms) override {
        (void)remote;
        (void)timeout_ms;
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->connect_calls++;
        return kHixlOk;
    }

   private:
    std::shared_ptr<MockHixlState> state_;
};

class AscendDirectTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ClearAscendEnv();
        ResetAcl();
        state_ = std::make_shared<MockHixlState>();
        HixlEngine::SetVendorFactory([state = state_]() {
            return std::make_unique<MockHixlVendor>(state);
        });
        metadata_ = std::make_shared<ControlService>("p2p", "", nullptr);
        transport_ = std::make_unique<AscendDirectTransport>();
    }

    void TearDown() override {
        if (batch_ != nullptr) {
            (void)transport_->freeSubBatch(batch_);
        }
        if (transport_) {
            (void)transport_->uninstall();
        }
        HixlEngine::SetVendorFactory(nullptr);
        ClearAscendEnv();
    }

    Status Install(std::shared_ptr<Config> conf = nullptr) {
        name_ = "127.0.0.1:23456";
        return transport_->install(name_, metadata_, nullptr, conf);
    }

    Request MakeWrite(void* src, uint64_t dest, size_t len) {
        Request request{};
        request.opcode = Request::WRITE;
        request.source = src;
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = dest;
        request.length = len;
        return request;
    }

    Status InstallAgentTwoEngines(std::shared_ptr<Config> extra = nullptr) {
        g_device_count = 2;
        g_device_id = 0;
        auto conf = extra ? extra : std::make_shared<Config>();
        conf->set("transports/ascend_direct/agent_mode", true);
        return Install(conf);
    }

    void PublishEngineBuffers() {
        BufferDesc buf0{};
        buf0.addr = 0x10000;
        buf0.length = 4096;
        buf0.transport_attrs[AscendDirect] = "0";
        BufferDesc buf1{};
        buf1.addr = 0x20000;
        buf1.length = 4096;
        buf1.transport_attrs[AscendDirect] = "1";
        ASSERT_TRUE(metadata_->segmentManager()
                        .updateLocal([&](SegmentDesc& segment) {
                            auto& mem =
                                std::get<MemorySegmentDesc>(segment.detail);
                            mem.buffers.push_back(buf0);
                            mem.buffers.push_back(buf1);
                            return Status::OK();
                        })
                        .ok());
    }

    Request MakeRemoteWrite(void* src, size_t len, uint64_t dest = 0x20010) {
        return MakeWrite(src, dest, len);
    }

    // Publishes a buffer that advertises the owner's shared-memfd handle, as
    // TENT's addMemoryBuffer() does for ASCEND_USE_SHMEM host memory.
    void PublishSharedHostBuffer(void* base, uint64_t length,
                                 const std::string& handle) {
        BufferDesc buffer{};
        buffer.addr = reinterpret_cast<uint64_t>(base);
        buffer.length = length;
        buffer.location = "cpu:0";
        buffer.shm_path = handle;
        buffer.transport_attrs[AscendDirect] = "0";
        ASSERT_TRUE(metadata_->segmentManager()
                        .updateLocal([&](SegmentDesc& segment) {
                            auto& mem =
                                std::get<MemorySegmentDesc>(segment.detail);
                            mem.buffers.push_back(buffer);
                            return Status::OK();
                        })
                        .ok());
    }

    std::shared_ptr<MockHixlState> state_;
    std::shared_ptr<ControlService> metadata_;
    std::unique_ptr<AscendDirectTransport> transport_;
    Transport::SubBatchRef batch_{nullptr};
    std::string name_;
};

// Polls `predicate` every millisecond until it holds or the timeout expires.
template <typename Predicate>
bool WaitUntil(Predicate predicate, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

// Polls one task until it leaves PENDING. Submission is asynchronous now, so a
// test must not expect the first poll to carry the final status.
void WaitUntilTerminal(AscendDirectTransport& transport,
                       Transport::SubBatchRef batch, int task_id,
                       int timeout_ms = 2000) {
    TransferStatus status{};
    const bool terminal = WaitUntil(
        [&] {
            if (!transport.getTransferStatus(batch, task_id, status).ok()) {
                return false;
            }
            return status.s != TransferStatusEnum::PENDING;
        },
        timeout_ms);
    ASSERT_TRUE(terminal) << "task " << task_id
                          << " did not reach a terminal state";
}

TEST_F(AscendDirectTransportTest, InitializeIsAutoConnectOnly) {
    ASSERT_TRUE(Install().ok());
    const auto& opts = AscendDirectTransportTestPeer::initOptions(*transport_);
    EXPECT_EQ(opts.at("AutoConnect"), "1");
    EXPECT_EQ(opts.at("LocalCommRes"), kDefaultLocalCommRes);
    EXPECT_EQ(opts.count("BufferPool"), 0u);
    EXPECT_EQ(opts.count("ShortConnection"), 0u);
    EXPECT_EQ(state_->connect_calls, 0);
    EXPECT_FALSE(transport_->supportNotification());
}

TEST_F(AscendDirectTransportTest, LocalCommResEnvOverride) {
    setenv("ASCEND_LOCAL_COMM_RES", R"({"version":"9.9"})", 1);
    ASSERT_TRUE(Install().ok());
    const auto& opts = AscendDirectTransportTestPeer::initOptions(*transport_);
    EXPECT_EQ(opts.at("LocalCommRes"), R"({"version":"9.9"})");
}

TEST_F(AscendDirectTransportTest, FabricOptionInjectedFromConfig) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/fabric_mem", true);
    ASSERT_TRUE(Install(conf).ok());
    const auto& opts = AscendDirectTransportTestPeer::initOptions(*transport_);
    EXPECT_EQ(opts.at("EnableUseFabricMem"), "1");
    EXPECT_TRUE(
        AscendDirectTransportTestPeer::options(*transport_).use_fabric_mem);
}

TEST_F(AscendDirectTransportTest, FabricEnvDoesNotLeakWithoutStoreTeInit) {
    setenv("ASCEND_ENABLE_USE_FABRIC_MEM", "1", 1);
    auto options = LoadAscendDirectOptions(nullptr);
    EXPECT_FALSE(options.use_fabric_mem);
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/store_te_init", true);
    options = LoadAscendDirectOptions(conf);
    EXPECT_TRUE(options.use_fabric_mem);
}

TEST_F(AscendDirectTransportTest, TimeoutMsFromEnv) {
    setenv("ASCEND_TRANSFER_TIMEOUT", "2500", 1);
    auto options = LoadAscendDirectOptions(nullptr);
    EXPECT_EQ(options.transfer_timeout_ms, 2500);
}

TEST_F(AscendDirectTransportTest, BuildHixlInitOptionsNeverSetsBufferPool) {
    AscendDirectOptions options;
    options.use_fabric_mem = true;
    options.rdma_tc = "192";
    auto init = BuildHixlInitOptions(options);
    EXPECT_EQ(init.at("AutoConnect"), "1");
    EXPECT_EQ(init.at("EnableUseFabricMem"), "1");
    EXPECT_EQ(init.at("RdmaTrafficClass"), "192");
    EXPECT_EQ(init.count("BufferPool"), 0u);
    EXPECT_EQ(init.count("Connect"), 0u);
}

TEST_F(AscendDirectTransportTest, InitializeInjectsPerEngineListenPort) {
    setenv("ASCEND_GLOBAL_RESOURCE_CONFIG",
           R"({"comm_resource_config":{"protocol_desc":"roce:device"}})", 1);
    ASSERT_TRUE(Install().ok());
    ASSERT_EQ(state_->last_options.count("GlobalResourceConfig"), 1u);
    const auto& cfg = state_->last_options.at("GlobalResourceConfig");
    EXPECT_NE(cfg.find("listen_port"), std::string::npos);
    EXPECT_NE(cfg.find("roce:device"), std::string::npos);
}

TEST_F(AscendDirectTransportTest, HccsInitDoesNotInjectRoceListenPort) {
    setenv("ASCEND_GLOBAL_RESOURCE_CONFIG",
           R"({"comm_resource_config":{"protocol_desc":"hccs:device"}})", 1);
    ASSERT_TRUE(Install().ok());
    ASSERT_EQ(state_->last_options.count("GlobalResourceConfig"), 1u);
    const auto& cfg = state_->last_options.at("GlobalResourceConfig");
    EXPECT_EQ(cfg.find("listen_port"), std::string::npos);
    EXPECT_NE(cfg.find("hccs:device"), std::string::npos);
}

TEST_F(AscendDirectTransportTest, InitializeRetriesWithDistinctHixlPorts) {
    state_->initialize_results = {1, 1, kHixlOk};
    ASSERT_TRUE(Install().ok());
    ASSERT_EQ(state_->initialize_names.size(), 3u);
    EXPECT_NE(state_->initialize_names[0], state_->initialize_names[1]);
    EXPECT_NE(state_->initialize_names[0], state_->initialize_names[2]);
    EXPECT_NE(state_->initialize_names[1], state_->initialize_names[2]);
    EXPECT_EQ(AscendDirectTransportTestPeer::engineName(*transport_, 0),
              state_->initialize_names[2]);
}

TEST_F(AscendDirectTransportTest, InitializeFailureIsBounded) {
    state_->initialize_results = {1, 1, 1, 1};
    EXPECT_FALSE(Install().ok());
    EXPECT_EQ(state_->initialize_calls, 4);
}

TEST_F(AscendDirectTransportTest, SubmitRollbackLeavesBatchUnchanged) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 8).ok());
    char src[64]{};
    std::vector<Request> requests = {MakeWrite(src, 0x1000, 64),
                                     MakeWrite(src, 0x2000, 64)};
    requests[1].target_id = 99;
    auto status = transport_->submitTransferTasks(batch_, requests);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(batch_->size(), 0u);
    EXPECT_EQ(state_->transfer_async_calls, 0);
}

TEST_F(AscendDirectTransportTest, SameEngineUsesLocalCopy) {
    ASSERT_TRUE(Install().ok());
    EXPECT_EQ(g_stream_creates, 6)
        << "a single engine keeps its six copy streams ready from install";
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());
    char src[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 32u);
    EXPECT_EQ(state_->transfer_async_calls, 0);
    EXPECT_EQ(AscendDirectTransportTestPeer::inflight(*transport_), 0);
    EXPECT_GE(g_stream_creates, 1);
}

// A process with several local engines creates each engine's six copy streams
// on the first submit that needs them, not at install: it copies locally only
// on the devices it serves, so most engines would otherwise hold six unused
// streams from install on.
TEST_F(AscendDirectTransportTest, MultiEngineDefersLocalCopyStreams) {
    ASSERT_TRUE(InstallAgentTwoEngines().ok());
    // Publish one destination buffer per engine so a destination address
    // resolves to that engine's hixl name. The addresses stay below 0x10000:
    // the ACL mock treats anything above that as real memory and memcpy()s
    // into it, which would fault on a made-up device address.
    BufferDesc buf0{};
    buf0.addr = 0x1000;
    buf0.length = 0x1000;
    buf0.transport_attrs[AscendDirect] = "0";
    BufferDesc buf1{};
    buf1.addr = 0x2000;
    buf1.length = 0x1000;
    buf1.transport_attrs[AscendDirect] = "1";
    ASSERT_TRUE(metadata_->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        auto& mem = std::get<MemorySegmentDesc>(segment.detail);
                        mem.buffers.push_back(buf0);
                        mem.buffers.push_back(buf1);
                        return Status::OK();
                    })
                    .ok());
    EXPECT_EQ(g_stream_creates, 0)
        << "a multi-engine install must not create the copy streams";

    char src[32]{};
    g_device_id = 0;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1010, 32)})
            .ok());
    // The copy runs on engine 0 (the current device), so only its streams
    // appear.
    EXPECT_EQ(g_stream_creates, 6);
    WaitUntilTerminal(*transport_, batch_, 0);
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 32u);
    EXPECT_EQ(state_->transfer_async_calls, 0);

    // A copy on the other engine creates that engine's streams as well.
    Transport::SubBatchRef other_batch = nullptr;
    g_device_id = 1;
    ASSERT_TRUE(transport_->allocateSubBatch(other_batch, 4).ok());
    ASSERT_TRUE(
        transport_
            ->submitTransferTasks(other_batch, {MakeWrite(src, 0x2010, 32)})
            .ok());
    EXPECT_EQ(g_stream_creates, 12);
    WaitUntilTerminal(*transport_, other_batch, 0);
    TransferStatus other_status{};
    ASSERT_TRUE(
        transport_->getTransferStatus(other_batch, 0, other_status).ok());
    EXPECT_EQ(other_status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(other_status.transferred_bytes, 32u);
    EXPECT_EQ(state_->transfer_async_calls, 0);
    EXPECT_TRUE(transport_->freeSubBatch(other_batch).ok());
}

TEST_F(AscendDirectTransportTest, LocalCopyFailureFailsAllStreamTasks) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());
    char src[32]{};
    g_skip_memcpy_copy = true;
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 16)})
            .ok());
    TransferStatus pending{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, pending).ok());
    EXPECT_EQ(pending.s, TransferStatusEnum::PENDING);
    ASSERT_EQ(g_host_allocs.size(), 1U);
    void* const first_group_flags = g_host_allocs[0];

    const int destroys_before = g_stream_destroys;
    g_memcpy_async_fail = true;
    Transport::SubBatchRef batch_b = nullptr;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_b, 2).ok());
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_b, {MakeWrite(src, 0x2000, 16)})
            .ok());
    ASSERT_EQ(g_host_allocs.size(), 2U);
    void* const failed_group_flags = g_host_allocs[1];
    // The failure path must abort/destroy the streams before it releases the
    // failed group's host flags, so no stream can still write into freed
    // memory. The mock records both events in one ordered sequence.
    ASSERT_EQ(g_host_free_seq.count(failed_group_flags), 1U);
    EXPECT_GT(g_host_free_seq[failed_group_flags], g_last_stream_abort_seq);

    TransferStatus first{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::FAILED);
    TransferStatus second{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_b, 0, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::FAILED);
    EXPECT_GT(g_stream_destroys, destroys_before);
    EXPECT_EQ(state_->transfer_async_calls, 0);
    // The failed leader's flags are released through the normal group release
    // path once the first group is polled to completion.
    EXPECT_EQ(g_host_free_seq.count(first_group_flags), 1U);
    EXPECT_TRUE(transport_->freeSubBatch(batch_b).ok());
}

// When publishing a copy group fails after the streams were issued, the
// request's host flags must not be freed while a queued D2H can still write
// them: the streams are aborted first (failAndRecreate), the sibling groups on
// them are failed through markGroupHandles, and only then are the flags
// released.
TEST_F(AscendDirectTransportTest, LocalCopyPublishFailureAbortsBeforeRelease) {
    ASSERT_TRUE(Install().ok());
    char src[16]{};
    g_skip_memcpy_copy = true;
    // A first local copy group stays in flight, so its handle is still tracked
    // when the second publication fails.
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 16)})
            .ok());
    ASSERT_EQ(g_host_allocs.size(), 1U);
    void* const sibling_flags = g_host_allocs[0];

    Transport::SubBatchRef failed_batch = nullptr;
    ASSERT_TRUE(transport_->allocateSubBatch(failed_batch, 2).ok());
    AscendDirectTransportTestPeer::failNextLocalCopyPublish(*transport_);
    const int destroys_before = g_stream_destroys;
    ASSERT_TRUE(
        transport_
            ->submitTransferTasks(failed_batch, {MakeWrite(src, 0x2000, 16)})
            .ok());
    ASSERT_EQ(g_host_allocs.size(), 2U);
    void* const failed_flags = g_host_allocs[1];
    // The abort must come before the flags of the failed request are released.
    ASSERT_EQ(g_host_free_seq.count(failed_flags), 1U);
    EXPECT_GT(g_host_free_seq[failed_flags], g_last_stream_abort_seq);
    EXPECT_GT(g_stream_destroys, destroys_before);
    // The sibling group on the recreated streams was failed, not lost, and its
    // flags are released through the normal poll path.
    TransferStatus sibling{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, sibling).ok());
    EXPECT_EQ(sibling.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(g_host_free_seq.count(sibling_flags), 1U);
    EXPECT_GT(g_host_free_seq[sibling_flags], g_last_stream_abort_seq);
    TransferStatus failed{};
    ASSERT_TRUE(transport_->getTransferStatus(failed_batch, 0, failed).ok());
    EXPECT_EQ(failed.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(AscendDirectTransportTestPeer::inflight(*transport_), 0);
    EXPECT_EQ(state_->transfer_async_calls, 0);
    EXPECT_TRUE(transport_->freeSubBatch(failed_batch).ok());
}

TEST_F(AscendDirectTransportTest, LocalCopyTimeoutRecreatesStream) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/transfer_timeout_ms", 1);
    ASSERT_TRUE(Install(conf).ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[16]{};
    g_skip_memcpy_copy = true;
    const int destroys_before = g_stream_destroys;
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 16)})
            .ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::TIMEOUT);
    EXPECT_GT(g_stream_destroys, destroys_before);
    EXPECT_EQ(state_->transfer_async_calls, 0);
    // A local copy has no HIXL route to tear down.
    EXPECT_EQ(state_->disconnect_calls, 0);
}

// A group larger than the six copy streams is spread over all of them, and the
// D2H completion flag of each stream makes the group terminal through the
// normal poll path (the poll cache must not hold it PENDING forever).
TEST_F(AscendDirectTransportTest, MultiStreamLocalCopyCompletes) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 16).ok());
    char src[16 * 8]{};
    const int creates_before = g_stream_creates;
    std::vector<Request> requests;
    for (int i = 0; i < 8; ++i) {
        requests.push_back(MakeWrite(src + i, 0x1000 + i * 64, 16));
    }
    ASSERT_TRUE(transport_->submitTransferTasks(batch_, requests).ok());
    // One group of eight ops over the six existing streams: no stream is
    // recreated on the success path.
    EXPECT_EQ(g_stream_creates, creates_before);
    for (int i = 0; i < 8; ++i) {
        WaitUntilTerminal(*transport_, batch_, i);
        TransferStatus status{};
        ASSERT_TRUE(transport_->getTransferStatus(batch_, i, status).ok());
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
        EXPECT_EQ(status.transferred_bytes, 16u);
    }
    EXPECT_EQ(g_stream_creates % 6, 0);
    EXPECT_EQ(AscendDirectTransportTestPeer::inflight(*transport_), 0);
}

// A task that waits for a transfer worker past the transfer timeout reports
// TIMEOUT instead of PENDING forever, and the group reservation is returned.
// The worker may itself be blocked inside HIXL longer than the timeout; the
// claim then keeps the task from being cancelled while its request is being
// accepted, and it reports TIMEOUT on the first poll after the group handle is
// published because the deadline is counted from submission.
TEST_F(AscendDirectTransportTest, QueuedTaskTimesOutBeforeSubmission) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/transfer_timeout_ms", 1);
    ASSERT_TRUE(InstallAgentTwoEngines(conf).ok());
    PublishEngineBuffers();
    g_device_id = 0;
    // Keep the worker inside HIXL long enough for the poller to observe the
    // claimed-but-unpublished state.
    state_->transfer_delay_ms = 100;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[16]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeRemoteWrite(src, 16)})
            .ok());
    TransferStatus status{};
    ASSERT_TRUE(WaitUntil(
        [&] {
            if (!transport_->getTransferStatus(batch_, 0, status).ok()) {
                return false;
            }
            return status.s != TransferStatusEnum::PENDING;
        },
        5000));
    EXPECT_EQ(status.s, TransferStatusEnum::TIMEOUT);
    // The cancellation must not leak the inflight reservation even when the
    // worker was already inside HIXL when the task timed out.
    EXPECT_TRUE(WaitUntil(
        [&] {
            return AscendDirectTransportTestPeer::inflight(*transport_) == 0;
        },
        5000));
    EXPECT_TRUE(transport_->quiesce().ok());
    ASSERT_TRUE(transport_->freeSubBatch(batch_).ok());
    batch_ = nullptr;
}

// While a transfer worker is inside HIXL the claimed task must not be reported
// TIMEOUT: its request is not tracked yet, so a terminal status would let the
// caller reuse a buffer HIXL is about to write. Once the handle is published
// the deadline still counts from submission, so the task reports TIMEOUT on the
// first poll instead of restarting its budget after the slow submit.
TEST_F(AscendDirectTransportTest,
       ClaimedTaskDoesNotTimeOutBeforeHandlePublished) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/transfer_timeout_ms", 1);
    ASSERT_TRUE(InstallAgentTwoEngines(conf).ok());
    PublishEngineBuffers();
    g_device_id = 0;
    state_->xfer_state = HixlXferState::Waiting;
    // Keep the worker inside TransferAsync so the task is claimed but has no
    // group handle yet.
    state_->transfer_delay_ms = 50;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[16]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeRemoteWrite(src, 16)})
            .ok());
    // The claim happens before TransferAsync is entered, so observing the call
    // proves that the polls below see a claimed task.
    ASSERT_TRUE(
        WaitUntil([&] { return state_->transfer_async_calls >= 1; }, 2000));
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(20);
    while (std::chrono::steady_clock::now() < deadline) {
        TransferStatus status{};
        ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
        EXPECT_EQ(status.s, TransferStatusEnum::PENDING)
            << "a claimed task must stay PENDING until its handle is published";
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // The submit finished well after the 1 ms deadline, so the published group
    // is timed out against the submission timestamp, not the end of the submit.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::TIMEOUT);
    EXPECT_EQ(AscendDirectTransportTestPeer::inflight(*transport_), 0);
}

// The queued-task timeout still protects a task that never reaches a worker: if
// every transfer worker is blocked inside HIXL, a newly queued task has not
// been claimed and reports TIMEOUT once the submission deadline passes instead
// of staying PENDING forever.
TEST_F(AscendDirectTransportTest, UnclaimedQueuedTaskStillTimesOut) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/transfer_timeout_ms", 1);
    ASSERT_TRUE(InstallAgentTwoEngines(conf).ok());
    PublishEngineBuffers();
    g_device_id = 0;
    state_->xfer_state = HixlXferState::Waiting;
    // Block every transfer worker so a further group stays queued. The pool has
    // one worker per submission slot; keep the count in sync with
    // kTransferThreadsPerEngine.
    constexpr int kTransferWorkers = 16;
    state_->transfer_delay_ms = 50;
    ASSERT_TRUE(
        transport_->allocateSubBatch(batch_, kTransferWorkers + 1).ok());
    char src[16]{};
    for (int i = 0; i < kTransferWorkers; ++i) {
        ASSERT_TRUE(
            transport_
                ->submitTransferTasks(
                    batch_, {MakeRemoteWrite(src, 16, 0x20010 + i * 16)})
                .ok());
    }
    ASSERT_TRUE(WaitUntil(
        [&] { return state_->transfer_async_calls >= kTransferWorkers; },
        2000));
    // Every worker is busy, so this group is queued without a claim.
    ASSERT_TRUE(
        transport_
            ->submitTransferTasks(batch_, {MakeRemoteWrite(src, 16, 0x20110)})
            .ok());
    // Past the 1 ms submission deadline, while the workers are still blocked.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    TransferStatus status{};
    ASSERT_TRUE(
        transport_->getTransferStatus(batch_, kTransferWorkers, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::TIMEOUT);
}

TEST_F(AscendDirectTransportTest, FailEntireRouteMarksSiblingTasks) {
    state_->xfer_state = HixlXferState::Failed;
    ASSERT_TRUE(InstallAgentTwoEngines().ok());
    PublishEngineBuffers();
    g_device_id = 0;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());
    char src[64]{};
    ASSERT_TRUE(
        transport_
            ->submitTransferTasks(batch_, {MakeRemoteWrite(src, 32, 0x20010),
                                           MakeRemoteWrite(src, 32, 0x20020)})
            .ok());
    WaitUntilTerminal(*transport_, batch_, 0);
    TransferStatus first{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::FAILED);
    const int polls_after_first = state_->get_status_calls;
    // The sibling task shares the failed route group, so its very next poll
    // reports failed -- the poll cache must only skip the HIXL query, not the
    // group status -- and it costs no extra HIXL query.
    TransferStatus second{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(state_->get_status_calls, polls_after_first);
    EXPECT_EQ(state_->disconnect_calls, 0);
    EXPECT_EQ(state_->transfer_async_calls, 1);
}

// A timeout reported by HIXL for one task marks the whole route group; the
// sibling task must see TIMEOUT on its next poll on the same thread instead of
// staying PENDING in the poll-cache window.
TEST_F(AscendDirectTransportTest, TimeoutRouteMarksSiblingTaskOnNextPoll) {
    state_->xfer_state = HixlXferState::Timeout;
    ASSERT_TRUE(InstallAgentTwoEngines().ok());
    PublishEngineBuffers();
    g_device_id = 0;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());
    char src[64]{};
    ASSERT_TRUE(
        transport_
            ->submitTransferTasks(batch_, {MakeRemoteWrite(src, 32, 0x20010),
                                           MakeRemoteWrite(src, 32, 0x20020)})
            .ok());
    WaitUntilTerminal(*transport_, batch_, 0);
    TransferStatus first{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::TIMEOUT);
    const int polls_after_first = state_->get_status_calls;
    TransferStatus second{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::TIMEOUT);
    EXPECT_EQ(state_->get_status_calls, polls_after_first);
    EXPECT_EQ(AscendDirectTransportTestPeer::inflight(*transport_), 0);
}

TEST_F(AscendDirectTransportTest, TimeoutMarksHungRouteTimedOut) {
    state_->xfer_state = HixlXferState::Waiting;
    state_->transfer_delay_ms = 0;
    auto conf = std::make_shared<Config>();
    // Long enough that the worker publishes the HIXL handle before the queued
    // deadline, so this exercises the route timeout rather than the queued one.
    conf->set("transports/ascend_direct/transfer_timeout_ms", 50);
    ASSERT_TRUE(InstallAgentTwoEngines(conf).ok());
    PublishEngineBuffers();
    g_device_id = 0;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[16]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeRemoteWrite(src, 16)})
            .ok());
    ASSERT_TRUE(
        WaitUntil([&] { return state_->transfer_async_calls >= 1; }, 2000));
    // Let the hung transfer outlive transfer_timeout_ms, then poll: the group
    // must be reported timed out and the peer Disconnected, because HIXL never
    // reports TIMEOUT itself and Disconnect is what aborts the request's DMA.
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    WaitUntilTerminal(*transport_, batch_, 0);
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::TIMEOUT);
    EXPECT_EQ(state_->transfer_async_calls, 1);
    EXPECT_GE(state_->disconnect_calls, 1);
    EXPECT_EQ(state_->connect_calls, 0);
}

// A route timeout on a peer whose session is already gone (an earlier timeout
// disconnected it) is not an error: the task still reports TIMEOUT.
TEST_F(AscendDirectTransportTest, TimeoutToleratesAlreadyDisconnectedPeer) {
    state_->xfer_state = HixlXferState::Waiting;
    state_->transfer_delay_ms = 0;
    state_->disconnect_status = kHixlNotConnected;
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/transfer_timeout_ms", 50);
    ASSERT_TRUE(InstallAgentTwoEngines(conf).ok());
    PublishEngineBuffers();
    g_device_id = 0;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[16]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeRemoteWrite(src, 16)})
            .ok());
    ASSERT_TRUE(
        WaitUntil([&] { return state_->transfer_async_calls >= 1; }, 2000));
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    WaitUntilTerminal(*transport_, batch_, 0);
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::TIMEOUT);
    EXPECT_GE(state_->disconnect_calls, 1);
}

TEST_F(AscendDirectTransportTest, MultiEnginePublishesNamesAndRoutesDest) {
    ASSERT_TRUE(InstallAgentTwoEngines().ok());
    ASSERT_EQ(AscendDirectTransportTestPeer::engineCount(*transport_), 2u);
    const auto name0 =
        AscendDirectTransportTestPeer::engineName(*transport_, 0);
    const auto name1 =
        AscendDirectTransportTestPeer::engineName(*transport_, 1);
    EXPECT_NE(name0, name1);

    auto local = metadata_->segmentManager().getLocal();
    const auto& detail = std::get<MemorySegmentDesc>(local->detail);
    EXPECT_EQ(detail.device_attrs.at(kHixlNameAttr), name0);
    auto names = ParseHixlNames(detail);
    ASSERT_EQ(names.size(), 2u);

    PublishEngineBuffers();
    g_device_id = 0;

    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeRemoteWrite(src, 16)})
            .ok());
    ASSERT_TRUE(
        WaitUntil([&] { return state_->transfer_async_calls >= 1; }, 2000));
    EXPECT_EQ(state_->last_remote, name1);
    WaitUntilTerminal(*transport_, batch_, 0);
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(state_->transfer_async_calls, 1);
}

TEST_F(AscendDirectTransportTest, DummyRealHostBuffersFollowCurrentDevice) {
    g_device_count = 2;
    g_device_id = 0;
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/agent_mode", true);
    ASSERT_TRUE(Install(conf).ok());
    MemoryOptions opts{};
    BufferDesc on0{};
    on0.addr = 0x30000;
    on0.length = 4096;
    on0.location = "cpu:0";
    g_device_id = 0;
    ASSERT_TRUE(transport_->addMemoryBuffer(on0, opts).ok());
    EXPECT_EQ(on0.transport_attrs[AscendDirect], "0");
    BufferDesc on1{};
    on1.addr = 0x40000;
    on1.length = 4096;
    on1.location = "cpu:0";
    g_device_id = 1;
    ASSERT_TRUE(transport_->addMemoryBuffer(on1, opts).ok());
    EXPECT_EQ(on1.transport_attrs[AscendDirect], "1");
}

TEST_F(AscendDirectTransportTest, RegisterPassesPermissionAsLocalOnly) {
    ASSERT_TRUE(Install().ok());
    BufferDesc shared{};
    shared.addr = 0x50000;
    shared.length = 4096;
    shared.location = "cpu:0";
    MemoryOptions global_opts{};
    global_opts.perm = kGlobalReadWrite;
    ASSERT_TRUE(transport_->addMemoryBuffer(shared, global_opts).ok());

    BufferDesc private_buf{};
    private_buf.addr = 0x60000;
    private_buf.length = 4096;
    private_buf.location = "cpu:0";
    MemoryOptions local_opts{};
    local_opts.perm = kLocalReadWrite;
    ASSERT_TRUE(transport_->addMemoryBuffer(private_buf, local_opts).ok());

    ASSERT_EQ(state_->register_local_only.size(), 2U);
    EXPECT_FALSE(state_->register_local_only[0]);
    EXPECT_TRUE(state_->register_local_only[1]);
}

TEST_F(AscendDirectTransportTest, UnregisterWithoutCallerContext) {
    ASSERT_TRUE(Install().ok());
    BufferDesc desc{};
    desc.addr = 0x50000;
    desc.length = 4096;
    desc.location = "cpu:0";
    MemoryOptions options{};
    ASSERT_TRUE(transport_->addMemoryBuffer(desc, options).ok());

    // Ctrl-C unmounts the store segments from the signal handling thread, which
    // has no context of its own to save and restore.
    g_context_bound = false;
    EXPECT_TRUE(transport_->removeMemoryBuffer(desc).ok());
    EXPECT_EQ(state_->deregister_calls, 1);
}

// A remote transfer opens a HIXL AutoConnect session. HIXL 9.2.0 allows
// DeregisterMem while that session is up (restriction removed in f4bbabd6), so
// unregistering a buffer must NOT disconnect anything: the engine keeps no peer
// bookkeeping.
TEST_F(AscendDirectTransportTest, UnregisterDeregistersWithoutDisconnect) {
    ASSERT_TRUE(InstallAgentTwoEngines().ok());
    PublishEngineBuffers();
    g_device_id = 0;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[16]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeRemoteWrite(src, 16)})
            .ok());
    WaitUntilTerminal(*transport_, batch_, 0);

    BufferDesc desc{};
    desc.addr = 0x70000;
    desc.length = 4096;
    desc.location = "cpu:0";
    MemoryOptions options{};
    ASSERT_TRUE(transport_->addMemoryBuffer(desc, options).ok());

    const int deregisters_before = state_->deregister_calls;
    ASSERT_TRUE(transport_->removeMemoryBuffer(desc).ok());

    EXPECT_EQ(state_->deregister_calls, deregisters_before + 1);
    EXPECT_EQ(state_->disconnect_calls, 0);
    EXPECT_EQ(state_->connect_calls, 0);
}

// Same for engine teardown: finalize() deregisters the remaining buffers and
// lets HIXL tear the sessions down, without disconnecting the peer first.
TEST_F(AscendDirectTransportTest, FinalizeDeregistersWithoutDisconnect) {
    ASSERT_TRUE(InstallAgentTwoEngines().ok());
    PublishEngineBuffers();
    g_device_id = 0;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[16]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeRemoteWrite(src, 16)})
            .ok());
    WaitUntilTerminal(*transport_, batch_, 0);

    BufferDesc desc{};
    desc.addr = 0x71000;
    desc.length = 4096;
    desc.location = "cpu:0";
    MemoryOptions options{};
    ASSERT_TRUE(transport_->addMemoryBuffer(desc, options).ok());

    const int deregisters_before = state_->deregister_calls;
    ASSERT_TRUE(transport_->uninstall().ok());

    EXPECT_GT(state_->deregister_calls, deregisters_before);
    EXPECT_EQ(state_->disconnect_calls, 0);
    EXPECT_EQ(state_->connect_calls, 0);
}

TEST_F(AscendDirectTransportTest, RegisterRollbackUnregistersPrefix) {
    ASSERT_TRUE(Install().ok());
    state_->fail_register_on = 2;
    std::vector<BufferDesc> desc_list(2);
    desc_list[0].addr = 0x1000;
    desc_list[0].length = 64;
    desc_list[0].location = "cpu:0";
    desc_list[1].addr = 0x2000;
    desc_list[1].length = 64;
    desc_list[1].location = "cpu:0";
    MemoryOptions options;
    auto status = transport_->addMemoryBuffer(desc_list, options);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(state_->register_calls, 2);
    EXPECT_EQ(state_->deregister_calls, 1);
}

TEST_F(AscendDirectTransportTest, QuiesceWaitsForInflight) {
    state_->xfer_state = HixlXferState::Waiting;
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/transfer_timeout_ms", 50);
    ASSERT_TRUE(InstallAgentTwoEngines(conf).ok());
    PublishEngineBuffers();
    g_device_id = 0;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[16]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeRemoteWrite(src, 16)})
            .ok());
    // The reservation is taken by the submitter before the group is queued, so
    // it is visible immediately even though the worker submits later.
    EXPECT_EQ(AscendDirectTransportTestPeer::inflight(*transport_), 1);
    state_->xfer_state = HixlXferState::Completed;
    // Make sure the worker published the handle before completing the state,
    // otherwise the poll would see a still-queued task.
    ASSERT_TRUE(
        WaitUntil([&] { return state_->transfer_async_calls >= 1; }, 2000));
    WaitUntilTerminal(*transport_, batch_, 0);
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(transport_->quiesce().ok());
    EXPECT_EQ(AscendDirectTransportTestPeer::inflight(*transport_), 0);
}

// Two groups submitted from different threads reach HIXL at the same time: the
// transfer pool has one worker per submission slot and AutoConnect sessions are
// HIXL's responsibility, so nothing in the engine serializes them.
TEST_F(AscendDirectTransportTest, ConcurrentSubmitsRunInParallel) {
    ASSERT_TRUE(InstallAgentTwoEngines().ok());
    PublishEngineBuffers();
    g_device_id = 0;
    // Slow HIXL submit makes the overlap observable.
    state_->transfer_delay_ms = 50;
    char src_a[16]{};
    char src_b[16]{};
    Transport::SubBatchRef batch_a = nullptr;
    Transport::SubBatchRef batch_b = nullptr;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_a, 2).ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_b, 2).ok());
    std::thread t1([&] {
        (void)transport_->submitTransferTasks(batch_a,
                                              {MakeRemoteWrite(src_a, 16)});
    });
    std::thread t2([&] {
        (void)transport_->submitTransferTasks(
            batch_b, {MakeRemoteWrite(src_b, 16, 0x20020)});
    });
    t1.join();
    t2.join();
    WaitUntilTerminal(*transport_, batch_a, 0, 5000);
    WaitUntilTerminal(*transport_, batch_b, 0, 5000);
    EXPECT_EQ(state_->max_concurrent_transfer, 2);
    TransferStatus status_a{};
    TransferStatus status_b{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_a, 0, status_a).ok());
    ASSERT_TRUE(transport_->getTransferStatus(batch_b, 0, status_b).ok());
    EXPECT_EQ(status_a.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status_b.s, TransferStatusEnum::COMPLETED);
    (void)transport_->freeSubBatch(batch_a);
    (void)transport_->freeSubBatch(batch_b);
}

TEST(AscendDirectResourceConfigTest, StoreJsonIsolation) {
    ClearAscendEnv();
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/global_resource_config",
              R"({"store":{"fabric_memory":{"a":1}},"other":1})");
    auto p2p = LoadAscendDirectOptions(conf);
    EXPECT_FALSE(p2p.use_fabric_mem);
    conf->set("transports/ascend_direct/store_te_init", true);
    auto store = LoadAscendDirectOptions(conf);
    EXPECT_TRUE(store.use_fabric_mem);
}

TEST(AscendDirectResourceConfigTest, DestRoutingUsesBufferEngineIndex) {
    MemorySegmentDesc detail;
    detail.device_attrs[kHixlNamesAttr] = R"(["a:1","b:2"])";
    BufferDesc buf{};
    buf.addr = 100;
    buf.length = 50;
    buf.transport_attrs[AscendDirect] = "1";
    detail.buffers.push_back(buf);
    EXPECT_EQ(ResolveRemoteHixlName(detail, 120), "b:2");
}

TEST(AscendDirectResourceConfigTest, HixlNameResolverMatchesPerRequestLookup) {
    MemorySegmentDesc detail;
    detail.device_attrs[kHixlNamesAttr] =
        R"(["e:0","e:1","e:2","e:3","e:4","e:5"])";
    const uint64_t base = 0x10000;
    const uint64_t stride = 0x1000;
    for (size_t i = 0; i < 6; ++i) {
        BufferDesc buf{};
        buf.addr = base + i * stride;
        buf.length = stride / 2;
        buf.transport_attrs[AscendDirect] = std::to_string(i);
        detail.buffers.push_back(buf);
    }
    // A buffer without an engine attribute falls back to the first name.
    BufferDesc plain{};
    plain.addr = base + 6 * stride;
    plain.length = stride;
    detail.buffers.push_back(plain);

    HixlNameResolver resolver(detail);
    // The cached resolver must answer exactly like the per-request lookup for
    // addresses inside the buffers, in the gaps and past the last buffer.
    for (uint64_t dest = base; dest <= base + 7 * stride; dest += 0x100) {
        EXPECT_EQ(resolver.Resolve(dest), ResolveRemoteHixlName(detail, dest))
            << "dest=" << dest;
    }
    EXPECT_EQ(resolver.Resolve(base + 5 * stride + 8), "e:5");
    EXPECT_EQ(resolver.Resolve(base + 6 * stride + 8), "e:0");

    MemorySegmentDesc single;
    single.device_attrs[kHixlNameAttr] = "only:1";
    HixlNameResolver single_resolver(single);
    EXPECT_EQ(single_resolver.Resolve(base), "only:1");

    HixlNameResolver empty;
    EXPECT_TRUE(empty.Resolve(base).empty());
}

TEST(AscendDirectResourceConfigTest, RoceListenPortAvoidsHixlControlPort) {
    EXPECT_EQ(HixlRoceListenPort(20000), 30000);
    EXPECT_EQ(HixlRoceListenPort(42067), 52067);
    EXPECT_NE(HixlRoceListenPort(20000), 20000u);
    EXPECT_EQ(HixlRoceListenPort(60000), 50000);
}

TEST(AscendDirectResourceConfigTest, ListenPortsDoNotShareDeviceBoundary) {
    constexpr int kPortsPerDevice = 100;
    std::vector<int> sockets;
    int base_port = 0;
    for (int candidate = 30000; candidate < 50000; candidate += 1000) {
        bool complete = true;
        for (int offset = 0; offset < kPortsPerDevice; ++offset) {
            const int fd = socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0) {
                complete = false;
                break;
            }
            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_port = htons(candidate + offset);
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            if (bind(fd, reinterpret_cast<sockaddr*>(&address),
                     sizeof(address)) != 0) {
                close(fd);
                complete = false;
                break;
            }
            sockets.push_back(fd);
        }
        if (complete) {
            base_port = candidate;
            break;
        }
        for (const int fd : sockets) close(fd);
        sockets.clear();
    }
    if (base_port == 0) {
        FAIL() << "Could not reserve a consecutive test port range";
    }

    const auto selected = FindHixlListenPort(base_port, 0);
    for (const int fd : sockets) close(fd);
    EXPECT_EQ(selected, 0);
}

TEST(AscendDirectResourceConfigTest, WithHixlListenPortKeepsProtocolDesc) {
    AscendDirectOptions options;
    options.global_resource_config =
        R"({"comm_resource_config":{"protocol_desc":"roce:device"}})";
    auto init = WithHixlListenPort(BuildHixlInitOptions(options), 52067);
    const auto& cfg = init.at("GlobalResourceConfig");
    EXPECT_NE(cfg.find("\"listen_port\":52067"), std::string::npos);
    EXPECT_NE(cfg.find("roce:device"), std::string::npos);
}

TEST(AscendDirectResourceConfigTest, WithHixlListenPortReplacesFlatPort) {
    AscendDirectOptions options;
    options.global_resource_config =
        R"({"comm_resource_config.listen_port":26666,"comm_resource_config.protocol_desc":["roce:device"]})";
    auto init = WithHixlListenPort(BuildHixlInitOptions(options), 52067);
    const auto parsed = nlohmann::json::parse(init.at("GlobalResourceConfig"));
    EXPECT_EQ(parsed.at("comm_resource_config.listen_port"), 52067);
}

TEST(AscendDirectResourceConfigTest, InjectNicTopoPath_EmptyConfigCreates) {
    auto out =
        InjectNicTopoPathFromCustomTopoTent("", "/etc/mooncake/nic_topo.json");
    const auto parsed = nlohmann::json::parse(out);
    EXPECT_EQ(parsed.at("comm_resource_config.nic_topo_path"),
              "/etc/mooncake/nic_topo.json");
}

TEST(AscendDirectResourceConfigTest, InjectNicTopoPath_ExplicitWins) {
    const std::string cfg =
        R"({"comm_resource_config.nic_topo_path":"/explicit.json"})";
    EXPECT_EQ(
        InjectNicTopoPathFromCustomTopoTent(cfg, "/etc/mooncake/nic_topo.json"),
        cfg);
}

TEST(AscendDirectResourceConfigTest, InjectNicTopoPath_NestedDropsFlat) {
    const std::string out = InjectNicTopoPathFromCustomTopoTent(
        R"({"comm_resource_config":{"protocol_desc":"roce:host"},)"
        R"("comm_resource_config.nic_topo_path":""})",
        "/etc/mooncake/nic_topo.json");
    const auto parsed = nlohmann::json::parse(out);
    EXPECT_EQ(parsed.at("comm_resource_config").at("nic_topo_path"),
              "/etc/mooncake/nic_topo.json");
    EXPECT_FALSE(parsed.contains("comm_resource_config.nic_topo_path"))
        << "the flat key must be dropped once the nested object is used";
}

TEST(AscendDirectResourceConfigTest, InjectNicTopoPath_InvalidJsonUnchanged) {
    const std::string cfg = "{not json";
    EXPECT_EQ(
        InjectNicTopoPathFromCustomTopoTent(cfg, "/etc/mooncake/nic_topo.json"),
        cfg);
}

TEST(AscendDirectResourceConfigTest, LoadOptionsInjectsCustomTopoJson) {
    ClearAscendEnv();
    auto conf = std::make_shared<Config>();
    conf->set("topology/custom_json_path", "/etc/mooncake/nic_topo.json");
    conf->set("transports/ascend_direct/global_resource_config",
              R"({"comm_resource_config":{"protocol_desc":"roce:host"}})");
    auto options = LoadAscendDirectOptions(conf);
    const auto parsed = nlohmann::json::parse(options.global_resource_config);
    EXPECT_EQ(parsed.at("comm_resource_config").at("nic_topo_path"),
              "/etc/mooncake/nic_topo.json");
    auto init = BuildHixlInitOptions(options);
    EXPECT_NE(init.at("GlobalResourceConfig").find("nic_topo_path"),
              std::string::npos);
}

// Injection is scenario-agnostic: HIXL decides whether to use nic_topo_path.
TEST(AscendDirectResourceConfigTest, LoadOptionsInjectsForNonRoceToo) {
    ClearAscendEnv();
    auto conf = std::make_shared<Config>();
    conf->set("topology/custom_json_path", "/etc/mooncake/nic_topo.json");
    conf->set("transports/ascend_direct/global_resource_config",
              R"({"comm_resource_config":{"protocol_desc":"hccs:device"}})");
    auto options = LoadAscendDirectOptions(conf);
    EXPECT_FALSE(options.roce_mode);
    const auto parsed = nlohmann::json::parse(options.global_resource_config);
    EXPECT_EQ(parsed.at("comm_resource_config").at("nic_topo_path"),
              "/etc/mooncake/nic_topo.json");
}

TEST(AscendDirectResourceConfigTest, LoadOptionsNoCustomTopoJsonUnchanged) {
    ClearAscendEnv();
    auto conf = std::make_shared<Config>();
    conf->set("transports/ascend_direct/global_resource_config",
              R"({"comm_resource_config":{"protocol_desc":"roce:host"}})");
    auto options = LoadAscendDirectOptions(conf);
    EXPECT_EQ(options.global_resource_config,
              R"({"comm_resource_config":{"protocol_desc":"roce:host"}})");
}

// A peer host segment with an importable memfd handle is served by the ACL
// local copy engine (H2D/D2H/H2H chosen by pointer type), never HIXL.
TEST_F(AscendDirectTransportTest, ShmHostReadUsesAclCopy) {
    ASSERT_TRUE(Install().ok());
    constexpr size_t kLength = 1024;
    void* base = nullptr;
    int fd = -1;
    std::string error;
    ASSERT_TRUE(host_memfd::Create(host_memfd::Granularity(), fd, base, error))
        << error;
    const uint64_t mapped_length = host_memfd::Granularity();
    std::memset(base, 0x5A, mapped_length);
    PublishSharedHostBuffer(base, mapped_length, host_memfd::ExportHandle(fd));

    std::vector<char> destination(kLength, 0);
    Request request{};
    request.opcode = Request::READ;
    request.source = destination.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(base) + 1024;
    request.length = kLength;
    const int acl_copies_before = g_memcpy_async_calls;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, std::vector<Request>{request})
            .ok());
    WaitUntilTerminal(*transport_, batch_, 0);
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kLength);
    EXPECT_EQ(std::memcmp(destination.data(), static_cast<char*>(base) + 1024,
                          kLength),
              0);
    EXPECT_EQ(state_->transfer_async_calls, 0)
        << "shared host transfer must not reach HIXL";
    EXPECT_GT(g_memcpy_async_calls, acl_copies_before)
        << "shared host transfers must go through the ACL local copy engine";

    (void)munmap(base, mapped_length);
    close(fd);
}

TEST_F(AscendDirectTransportTest, ShmHostWriteUsesAclCopy) {
    ASSERT_TRUE(Install().ok());
    constexpr size_t kLength = 512;
    void* base = nullptr;
    int fd = -1;
    std::string error;
    ASSERT_TRUE(host_memfd::Create(host_memfd::Granularity(), fd, base, error))
        << error;
    const uint64_t mapped_length = host_memfd::Granularity();
    std::memset(base, 0, mapped_length);
    PublishSharedHostBuffer(base, mapped_length, host_memfd::ExportHandle(fd));

    std::vector<char> source(kLength, 0xA5);
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(base) + 2048;
    request.length = kLength;
    const int acl_copies_before = g_memcpy_async_calls;
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, std::vector<Request>{request})
            .ok());
    WaitUntilTerminal(*transport_, batch_, 0);
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kLength);
    EXPECT_EQ(
        std::memcmp(static_cast<char*>(base) + 2048, source.data(), kLength),
        0);
    EXPECT_EQ(state_->transfer_async_calls, 0);
    EXPECT_GT(g_memcpy_async_calls, acl_copies_before)
        << "shared host transfers must go through the ACL local copy engine";

    (void)munmap(base, mapped_length);
    close(fd);
}

// Only ACL_MEM_LOCATION_TYPE_DEVICE means device memory. Current CANN marks
// ACL_MEM_LOCATION_TYPE_HOST reserved and reports real host allocations as
// UNREGISTERED/MANAGED/HOST_NUMA, which must still register as host in HIXL.
TEST_F(AscendDirectTransportTest, UnknownLocationUsesAclTypeNotHostEnum) {
    ASSERT_TRUE(Install().ok());
    auto add_with_type = [&](aclrtMemLocationType type, uint64_t addr) {
        g_pointer_location_type = type;
        BufferDesc desc{};
        desc.addr = addr;
        desc.length = 4096;  // empty location forces the ACL attribute query
        MemoryOptions options{};
        ASSERT_TRUE(transport_->addMemoryBuffer(desc, options).ok());
        ASSERT_FALSE(state_->register_host.empty());
        EXPECT_EQ(state_->register_host.back(),
                  type != ACL_MEM_LOCATION_TYPE_DEVICE)
            << "ACL location type " << static_cast<int>(type);
        (void)transport_->removeMemoryBuffer(desc);
    };
    add_with_type(ACL_MEM_LOCATION_TYPE_DEVICE, 0x12345000);
    add_with_type(ACL_MEM_LOCATION_TYPE_UNREGISTERED, 0x12346000);
    add_with_type(ACL_MEM_LOCATION_TYPE_MANAGED, 0x12347000);
    add_with_type(ACL_MEM_LOCATION_TYPE_HOST_NUMA, 0x12348000);
}

// addMemoryBuffer() must advertise the shared-memfd handle for host memory so a
// peer process can map the same pages and take the local-memcpy path above.
#ifdef TENT_HAS_ASCEND_ALLOCATOR
TEST_F(AscendDirectTransportTest, AddMemoryBufferPublishesSharedHandle) {
    setenv("ASCEND_USE_SHMEM", "1", 1);
    ASSERT_TRUE(Install().ok());
    void* host = ascend_allocate_memory(4096, "ascend");
    ASSERT_NE(host, nullptr);

    BufferDesc desc{};
    desc.addr = reinterpret_cast<uint64_t>(host);
    desc.length = 4096;
    desc.location = "cpu:0";
    MemoryOptions options{};
    ASSERT_TRUE(transport_->addMemoryBuffer(desc, options).ok());
    EXPECT_TRUE(host_memfd::IsHandle(desc.shm_path)) << desc.shm_path;
    EXPECT_EQ(state_->register_calls, 1);
    EXPECT_TRUE(state_->register_local_only.empty() ||
                !state_->register_local_only.back());

    (void)transport_->removeMemoryBuffer(desc);
    ascend_free_memory("ascend", host);
    unsetenv("ASCEND_USE_SHMEM");
}
#endif  // TENT_HAS_ASCEND_ALLOCATOR
}  // namespace
}  // namespace tent
}  // namespace mooncake
