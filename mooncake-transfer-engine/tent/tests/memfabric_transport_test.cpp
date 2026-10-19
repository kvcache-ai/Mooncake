// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <acl/acl.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <unordered_map>
#include <unordered_set>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/memfabric/memfabric_transport.h"
#include "tent/transport/memfabric/memfabric_under_api.h"

namespace {

thread_local int g_device_id = 0;
thread_local aclrtContext g_context = reinterpret_cast<aclrtContext>(0x10000);
int g_device_count = 1;

void ResetAcl() {
    g_device_id = 0;
    g_device_count = 1;
    g_context = reinterpret_cast<aclrtContext>(0x10000);
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
    if (deviceId >= g_device_count) return 1;
    g_device_id = deviceId;
    return ACL_ERROR_NONE;
}

aclError aclrtGetCurrentContext(aclrtContext* context) {
    *context = g_context;
    return ACL_ERROR_NONE;
}

aclError aclrtSetCurrentContext(aclrtContext context) {
    g_context = context;
    return ACL_ERROR_NONE;
}

aclError aclrtPointerGetAttributes(const void* ptr,
                                   aclrtPtrAttributes* attributes) {
    (void)ptr;
    attributes->location.id = 0;
    attributes->location.type = ACL_MEM_LOCATION_TYPE_HOST;
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
    return *hostPtr != nullptr ? ACL_ERROR_NONE : 1;
}

aclError aclrtFreeHost(void* hostPtr) {
    std::free(hostPtr);
    return ACL_ERROR_NONE;
}

aclError aclrtGetPhyDevIdByLogicDevId(int32_t, int32_t* phyDevId) {
    *phyDevId = 0;
    return ACL_SUCCESS;
}

aclError aclrtMallocPhysical(aclrtDrvMemHandle*, size_t,
                             const aclrtPhysicalMemProp*, uint64_t) {
    return ACL_SUCCESS;
}

aclError aclrtFreePhysical(aclrtDrvMemHandle) { return ACL_SUCCESS; }

aclError aclrtReserveMemAddress(void** va, size_t, uint64_t, void*, size_t) {
    *va = nullptr;
    return ACL_ERROR_NONE;
}

aclError aclrtMapMem(void*, size_t, uint64_t, aclrtDrvMemHandle, uint64_t) {
    return ACL_ERROR_NONE;
}

aclError aclrtUnmapMem(void*) { return ACL_ERROR_NONE; }

aclError aclrtReleaseMemAddress(void*) { return ACL_ERROR_NONE; }

aclError aclrtMemcpy(void*, size_t, const void*, size_t, aclrtMemcpyKind) {
    return ACL_SUCCESS;
}

const char* aclGetRecentErrMsg() { return ""; }

}  // extern "C"

namespace mooncake {
namespace tent {

struct MockMemFabricState {
    std::mutex mu;
    std::condition_variable cv;
    bool block_submit = false;
    int pending_queries = 0;
    bool defer_copy = false;
    struct Copy {
        smem_trans_opcode_t opcode;
        std::vector<void*> sources, destinations;
        std::vector<size_t> sizes;
    };
    std::unordered_map<uint64_t, Copy> copies;
    std::unordered_set<uint64_t> pending;
    std::unordered_map<uint64_t, std::pair<int32_t, int32_t>> results;
    int init_calls = 0;
    int32_t init_ret = 0;
    int uninit_calls = 0;
    int config_init_calls = 0;
    int32_t config_init_ret = 0;
    int create_calls = 0;
    int destroy_calls = 0;
    int get_rpc_port_calls = 0;
    int batch_copy_calls = 0;
    std::vector<smem_trans_opcode_t> batch_opcodes;
    int status_query_calls = 0;
    int register_mem_calls = 0;
    int deregister_mem_calls = 0;
    int32_t deregister_ret = 0;
    int set_log_level_calls = 0;
    bool block_log_level = false;
    std::atomic<uint64_t> next_batch_id{1};
    int32_t query_ret = SM_OK;
    int32_t batch_result = SM_OK;
    int32_t batch_copy_ret = 0;
    int32_t get_rpc_port_ret = 0;
    uint16_t rpc_port = 12345;
    std::string create_url;
    std::string create_nic;
    bool create_fail = false;
    bool copy_data = false;
};

static std::shared_ptr<MockMemFabricState> g_state;

int32_t mockSmemTransInit(uint32_t, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->init_calls++;
    return g_state->init_ret;
}

void mockSmemTransUninit(uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->uninit_calls++;
}

int32_t mockSmemTransConfigInit(smem_trans_config_t* config) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->config_init_calls++;
    std::memset(config, 0, sizeof(*config));
    return g_state->config_init_ret;
}

smem_trans_t mockSmemTransCreate(const smem_trans_config_t* config, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->create_calls++;
    g_state->create_url = config->url;
    g_state->create_nic = config->nic;
    if (g_state->create_fail) return nullptr;
    return reinterpret_cast<smem_trans_t>(0x2000);
}

void mockSmemTransDestroy(smem_trans_t, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->destroy_calls++;
    g_state->cv.notify_all();
}

int32_t mockSmemTransGetRpcPort(smem_trans_t, uint16_t* port) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->get_rpc_port_calls++;
    *port = g_state->rpc_port;
    return g_state->get_rpc_port_ret;
}

int32_t mockSmemTransRegisterMem(smem_trans_t, void*, size_t, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->register_mem_calls++;
    return 0;
}

int32_t mockSmemTransBatchRegisterMem(smem_trans_t, void*[], size_t[], uint32_t,
                                      uint32_t) {
    return 0;
}

int32_t mockSmemTransDeregisterMem(smem_trans_t, void*) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->deregister_mem_calls++;
    return g_state->deregister_ret;
}

void CopyData(const MockMemFabricState::Copy& copy) {
    for (size_t i = 0; i < copy.sizes.size(); ++i) {
        if (copy.opcode == SMEM_TRANS_OP_WRITE)
            std::memcpy(copy.destinations[i], copy.sources[i], copy.sizes[i]);
        else
            std::memcpy(copy.sources[i], copy.destinations[i], copy.sizes[i]);
    }
}

int32_t mockSmemTransBatchCopy(smem_trans_t, smem_trans_batch_params_t* params,
                               uint64_t* batchId) {
    std::unique_lock<std::mutex> lock(g_state->mu);
    g_state->batch_copy_calls++;
    g_state->cv.notify_all();
    g_state->cv.wait(lock, [] { return !g_state->block_submit; });
    if (params) g_state->batch_opcodes.push_back(params->opcode);
    if (g_state->batch_copy_ret != 0) return g_state->batch_copy_ret;
    *batchId = g_state->next_batch_id.fetch_add(1, std::memory_order_relaxed);
    g_state->pending.insert(*batchId);
    if (g_state->copy_data) {
        MockMemFabricState::Copy copy{
            params->opcode,
            std::vector<void*>(params->srcList,
                               params->srcList + params->batchSize),
            std::vector<void*>(params->destList,
                               params->destList + params->batchSize),
            std::vector<size_t>(params->sizeList,
                                params->sizeList + params->batchSize)};
        if (g_state->defer_copy)
            g_state->copies.emplace(*batchId, std::move(copy));
        else
            CopyData(copy);
    }
    return SM_OK;
}

int32_t mockSmemTransBatchStatusQuery(smem_trans_t, uint64_t batchId,
                                      int32_t* result, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->status_query_calls++;
    g_state->cv.notify_all();
    if (!g_state->pending.count(batchId)) return SM_INVALID_PARAM;
    if (g_state->pending_queries > 0) {
        --g_state->pending_queries;
        return SM_NEED_RETRY;
    }
    auto query_ret = g_state->query_ret;
    auto batch_result = g_state->batch_result;
    if (auto it = g_state->results.find(batchId);
        it != g_state->results.end()) {
        query_ret = it->second.first;
        batch_result = it->second.second;
    }
    *result = batch_result;
    if (query_ret == SM_OK) {
        auto copy = g_state->copies.find(batchId);
        if (copy != g_state->copies.end()) {
            if (batch_result == SM_OK) CopyData(copy->second);
            g_state->copies.erase(copy);
        }
        g_state->pending.erase(batchId);
    }
    return query_ret;
}

int32_t mockSmemSetLogLevel(int) {
    std::unique_lock<std::mutex> lock(g_state->mu);
    g_state->set_log_level_calls++;
    g_state->cv.notify_all();
    g_state->cv.wait(lock, [] { return !g_state->block_log_level; });
    return 0;
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

Request MakeRead(void* src, uint64_t dest, size_t len) {
    Request request{};
    request.opcode = Request::READ;
    request.source = src;
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = dest;
    request.length = len;
    return request;
}

class MemFabricTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ResetAcl();
        g_state = std::make_shared<MockMemFabricState>();
        state_ = g_state;
        InstallMockApi();
        metadata_ = std::make_shared<ControlService>("p2p", "", nullptr);
        uint16_t port = 0;
        ASSERT_TRUE(metadata_->start(port).ok());
        name_ = "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
        ASSERT_TRUE(SetRpcAddress(name_).ok());
        transport_ = std::make_unique<MemFabricTransport>();
    }

    void TearDown() override {
        if (batch_ != nullptr) {
            (void)transport_->freeSubBatch(batch_);
            batch_ = nullptr;
        }
        if (transport_) {
            (void)transport_->uninstall();
        }
        ResetMockApi();
        g_state.reset();
        unsetenv("MF_PROTOCOL");
    }

    Status Install() {
        setenv("MF_PROTOCOL", "device_rdma", 1);
        auto conf = std::make_shared<Config>();
        return transport_->install(name_, metadata_, nullptr, conf);
    }

    int InflightCount() const { return transport_->inflight_.load(); }

    int InitRefCount() const { return MemFabricTransport::g_init_ref_.load(); }

    void SetQueryResult(int32_t query_ret, int32_t result = SM_OK) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->query_ret = query_ret;
        state_->batch_result = result;
    }

    bool WaitForQueries(int count) {
        std::unique_lock<std::mutex> lock(state_->mu);
        return state_->cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return state_->status_query_calls >= count;
        });
    }

    void SetTransferTimeout(int32_t timeout_ms) {
        transport_->transfer_timeout_ms_ = timeout_ms;
    }

    Status SetRpcAddress(const std::string& address) {
        return metadata_->segmentManager().updateLocal(
            [&](SegmentDesc& segment) -> Status {
                segment.rpc_server_addr = address;
                return Status::OK();
            });
    }

    std::string PublishedEndpoint() const {
        auto segment = metadata_->segmentManager().getLocal();
        const auto& attrs =
            std::get<MemorySegmentDesc>(segment->detail).device_attrs;
        auto it = attrs.find("memfabric_unique_id");
        return it == attrs.end() ? "" : it->second;
    }

    void InstallMockApi();
    void ResetMockApi();

    std::shared_ptr<MockMemFabricState> state_;
    std::shared_ptr<ControlService> metadata_;
    std::unique_ptr<MemFabricTransport> transport_;
    Transport::SubBatchRef batch_{nullptr};
    std::string name_;
};

void MemFabricTransportTest::InstallMockApi() {
    MemFabricUnderApi::gLoaded = true;
    MemFabricUnderApi::gLibHandle = nullptr;
    MemFabricUnderApi::pSmemTransInit = &mockSmemTransInit;
    MemFabricUnderApi::pSmemTransUninit = &mockSmemTransUninit;
    MemFabricUnderApi::pSmemTransConfigInit = &mockSmemTransConfigInit;
    MemFabricUnderApi::pSmemTransCreate = &mockSmemTransCreate;
    MemFabricUnderApi::pSmemTransDestroy = &mockSmemTransDestroy;
    MemFabricUnderApi::pSmemTransGetRpcPort = &mockSmemTransGetRpcPort;
    MemFabricUnderApi::pSmemTransRegisterMem = &mockSmemTransRegisterMem;
    MemFabricUnderApi::pSmemTransBatchRegisterMem =
        &mockSmemTransBatchRegisterMem;
    MemFabricUnderApi::pSmemTransDeregisterMem = &mockSmemTransDeregisterMem;
    MemFabricUnderApi::pSmemTransBatchCopy = &mockSmemTransBatchCopy;
    MemFabricUnderApi::pSmemTransBatchStatusQuery =
        &mockSmemTransBatchStatusQuery;
    MemFabricUnderApi::pSmemSetLogLevel = &mockSmemSetLogLevel;
}

void MemFabricTransportTest::ResetMockApi() {
    MemFabricUnderApi::gLoaded = false;
    MemFabricUnderApi::gLibHandle = nullptr;
    MemFabricUnderApi::pSmemTransInit = nullptr;
    MemFabricUnderApi::pSmemTransUninit = nullptr;
    MemFabricUnderApi::pSmemTransConfigInit = nullptr;
    MemFabricUnderApi::pSmemTransCreate = nullptr;
    MemFabricUnderApi::pSmemTransDestroy = nullptr;
    MemFabricUnderApi::pSmemTransGetRpcPort = nullptr;
    MemFabricUnderApi::pSmemTransRegisterMem = nullptr;
    MemFabricUnderApi::pSmemTransBatchRegisterMem = nullptr;
    MemFabricUnderApi::pSmemTransDeregisterMem = nullptr;
    MemFabricUnderApi::pSmemTransBatchCopy = nullptr;
    MemFabricUnderApi::pSmemTransBatchStatusQuery = nullptr;
    MemFabricUnderApi::pSmemSetLogLevel = nullptr;
}

TEST_F(MemFabricTransportTest, InstallSucceeds) {
    ASSERT_TRUE(Install().ok());
    EXPECT_EQ(state_->init_calls, 1);
    EXPECT_EQ(state_->create_calls, 1);
    EXPECT_EQ(state_->get_rpc_port_calls, 1);
}

TEST_F(MemFabricTransportTest, InstallFailsWhenCreateReturnsNull) {
    g_state->create_fail = true;
    auto status = Install();
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(state_->create_calls, 1);
    EXPECT_EQ(state_->uninit_calls, 1);
    EXPECT_EQ(InitRefCount(), 0);
    EXPECT_FALSE(MemFabricUnderApi::IsLoaded());
    EXPECT_TRUE(PublishedEndpoint().empty());
}

TEST_F(MemFabricTransportTest, InitFailureUnloadsLibraryAndAllowsRetry) {
    state_->init_ret = -1;
    EXPECT_FALSE(Install().ok());
    EXPECT_EQ(InitRefCount(), 0);
    EXPECT_EQ(state_->uninit_calls, 0);
    EXPECT_EQ(state_->create_calls, 0);
    EXPECT_FALSE(MemFabricUnderApi::IsLoaded());

    InstallMockApi();
    state_->init_ret = 0;
    ASSERT_TRUE(Install().ok());
    EXPECT_EQ(state_->init_calls, 2);
    EXPECT_EQ(InitRefCount(), 1);
    EXPECT_TRUE(transport_->uninstall().ok());
    EXPECT_EQ(state_->uninit_calls, 1);
    EXPECT_EQ(InitRefCount(), 0);
    EXPECT_FALSE(MemFabricUnderApi::IsLoaded());
}

TEST_F(MemFabricTransportTest, ConfigFailureUnloadsLibrary) {
    state_->config_init_ret = -1;
    EXPECT_FALSE(Install().ok());
    EXPECT_EQ(state_->uninit_calls, 1);
    EXPECT_EQ(state_->create_calls, 0);
    EXPECT_EQ(InitRefCount(), 0);
    EXPECT_FALSE(MemFabricUnderApi::IsLoaded());
}

TEST_F(MemFabricTransportTest, FailedInstallKeepsOtherInstanceInitialized) {
    ASSERT_TRUE(Install().ok());
    state_->create_fail = true;
    MemFabricTransport other;
    EXPECT_FALSE(
        other.install(name_, metadata_, nullptr, std::make_shared<Config>())
            .ok());
    EXPECT_EQ(state_->init_calls, 1);
    EXPECT_EQ(state_->uninit_calls, 0);
    EXPECT_EQ(InitRefCount(), 1);
    EXPECT_TRUE(MemFabricUnderApi::IsLoaded());

    EXPECT_TRUE(transport_->uninstall().ok());
    EXPECT_EQ(state_->uninit_calls, 1);
    EXPECT_EQ(InitRefCount(), 0);
    EXPECT_FALSE(MemFabricUnderApi::IsLoaded());
}

TEST_F(MemFabricTransportTest, InstallKeepsLibraryWhileLastInstanceUninstalls) {
    ASSERT_TRUE(Install().ok());
    state_->block_log_level = true;
    MemFabricTransport other;
    auto installing = std::async(std::launch::async, [&] {
        return other.install(name_, metadata_, nullptr,
                             std::make_shared<Config>());
    });
    bool reached_log_level;
    {
        std::unique_lock<std::mutex> lock(state_->mu);
        reached_log_level = state_->cv.wait_for(
            lock, std::chrono::seconds(2),
            [&] { return state_->set_log_level_calls == 2; });
        if (!reached_log_level) {
            state_->block_log_level = false;
            state_->cv.notify_all();
        }
    }
    ASSERT_TRUE(reached_log_level);

    auto uninstalling =
        std::async(std::launch::async, [&] { return transport_->uninstall(); });
    bool reached_destroy;
    {
        std::unique_lock<std::mutex> lock(state_->mu);
        reached_destroy =
            state_->cv.wait_for(lock, std::chrono::seconds(2),
                                [&] { return state_->destroy_calls == 1; });
    }
    EXPECT_TRUE(reached_destroy);
    EXPECT_EQ(uninstalling.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->block_log_level = false;
        state_->cv.notify_all();
    }
    EXPECT_TRUE(installing.get().ok());
    EXPECT_TRUE(uninstalling.get().ok());
    EXPECT_EQ(state_->init_calls, 1);
    EXPECT_EQ(state_->uninit_calls, 0);
    EXPECT_EQ(InitRefCount(), 1);
    EXPECT_TRUE(MemFabricUnderApi::IsLoaded());

    EXPECT_TRUE(other.uninstall().ok());
    EXPECT_EQ(state_->uninit_calls, 1);
    EXPECT_EQ(InitRefCount(), 0);
    EXPECT_FALSE(MemFabricUnderApi::IsLoaded());
}

TEST_F(MemFabricTransportTest, InstallUsesRpcAddressForOpaqueSegmentName) {
    name_ = "segment_noname_memfabric_test";
    ASSERT_TRUE(SetRpcAddress("192.0.2.10:9000").ok());

    ASSERT_TRUE(Install().ok());

    EXPECT_EQ(state_->create_url, "192.0.2.10:0");
    EXPECT_EQ(state_->create_nic, "tcp://192.0.2.10:9000");
    EXPECT_EQ(PublishedEndpoint(), "192.0.2.10:12345");
    EXPECT_EQ(name_, "segment_noname_memfabric_test");
}

TEST_F(MemFabricTransportTest, InstallIgnoresAddressLikeSegmentName) {
    name_ = "198.51.100.20:8000";
    ASSERT_TRUE(SetRpcAddress("192.0.2.10:9000").ok());

    ASSERT_TRUE(Install().ok());

    EXPECT_EQ(state_->create_url, "192.0.2.10:0");
    EXPECT_EQ(state_->create_nic, "tcp://192.0.2.10:9000");
    EXPECT_EQ(PublishedEndpoint(), "192.0.2.10:12345");
}

TEST_F(MemFabricTransportTest, InstallFormatsIpv6RpcEndpoint) {
    name_ = "segment_noname_ipv6";
    ASSERT_TRUE(SetRpcAddress("[2001:db8::10]:9000").ok());

    ASSERT_TRUE(Install().ok());

    EXPECT_EQ(state_->create_url, "[2001:db8::10]:0");
    EXPECT_EQ(state_->create_nic, "tcp://[2001:db8::10]:9000");
    EXPECT_EQ(PublishedEndpoint(), "[2001:db8::10]:12345");
}

TEST_F(MemFabricTransportTest, InstallRejectsInvalidRpcAddress) {
    for (const auto& address :
         {"", ":9000", "127.0.0.1:bad", "127.0.0.1:0", "0.0.0.0:9000",
          "[::]:9000", "[2001::db8::1]:9000"}) {
        SCOPED_TRACE(address);
        ASSERT_TRUE(SetRpcAddress(address).ok());
        ASSERT_FALSE(Install().ok());
        EXPECT_EQ(state_->init_calls, 0);
        EXPECT_EQ(state_->create_calls, 0);
        EXPECT_TRUE(PublishedEndpoint().empty());
    }
}

TEST_F(MemFabricTransportTest, InstallRejectsOversizedRpcAddress) {
    ASSERT_TRUE(SetRpcAddress(std::string(64, 'a') + ":9000").ok());

    EXPECT_FALSE(Install().ok());
    EXPECT_EQ(state_->init_calls, 0);
    EXPECT_EQ(state_->create_calls, 0);
    EXPECT_TRUE(PublishedEndpoint().empty());
}

TEST_F(MemFabricTransportTest, InstallDoesNotPublishWhenPortQueryFails) {
    state_->get_rpc_port_ret = -1;

    EXPECT_FALSE(Install().ok());
    EXPECT_EQ(state_->get_rpc_port_calls, 1);
    EXPECT_EQ(state_->destroy_calls, 1);
    EXPECT_EQ(state_->uninit_calls, 1);
    EXPECT_EQ(InitRefCount(), 0);
    EXPECT_FALSE(MemFabricUnderApi::IsLoaded());
    EXPECT_TRUE(PublishedEndpoint().empty());
}

TEST_F(MemFabricTransportTest, InstallDoesNotPublishZeroPort) {
    state_->rpc_port = 0;

    EXPECT_FALSE(Install().ok());
    EXPECT_EQ(state_->get_rpc_port_calls, 1);
    EXPECT_EQ(state_->destroy_calls, 1);
    EXPECT_EQ(state_->uninit_calls, 1);
    EXPECT_TRUE(PublishedEndpoint().empty());
}

TEST_F(MemFabricTransportTest, MixedWriteAndReadToSamePeerSubmitsTwoBatches) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    state_->copy_data = true;
    for (bool read_first : {false, true}) {
        SCOPED_TRACE(read_first);
        std::array<char, 32> local_source, remote_source;
        local_source.fill(0x11);
        remote_source.fill(0x22);
        std::array<char, 32> remote_destination{}, local_destination{};
        const auto expected_write = local_source;
        const auto expected_read = remote_source;
        std::vector<Request> requests = {
            MakeWrite(local_source.data(),
                      reinterpret_cast<uint64_t>(remote_destination.data()),
                      local_source.size()),
            MakeRead(local_destination.data(),
                     reinterpret_cast<uint64_t>(remote_source.data()),
                     remote_source.size())};
        if (read_first) std::reverse(requests.begin(), requests.end());
        state_->batch_opcodes.clear();
        const auto previous_calls = state_->batch_copy_calls;
        const auto start_task = batch_->size();

        ASSERT_TRUE(transport_->submitTransferTasks(batch_, requests).ok());

        EXPECT_EQ(state_->batch_copy_calls, previous_calls + 2);
        const auto& opcodes = state_->batch_opcodes;
        ASSERT_EQ(opcodes.size(), 2u);
        EXPECT_EQ(
            std::count(opcodes.begin(), opcodes.end(), SMEM_TRANS_OP_READ), 1);
        EXPECT_EQ(
            std::count(opcodes.begin(), opcodes.end(), SMEM_TRANS_OP_WRITE), 1);
        EXPECT_EQ(remote_destination, expected_write);
        EXPECT_EQ(local_destination, expected_read);
        EXPECT_EQ(local_source, expected_write);
        EXPECT_EQ(remote_source, expected_read);
        for (size_t i = 0; i < requests.size(); ++i) {
            TransferStatus status{};
            ASSERT_TRUE(
                transport_
                    ->getTransferStatus(
                        batch_, static_cast<int>(start_task + i), status)
                    .ok());
            EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
            EXPECT_EQ(status.transferred_bytes, requests[i].length);
        }
    }
}

TEST_F(MemFabricTransportTest, SubmitAndGetStatusCompleted) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 128).ok());

    char src[64]{};
    std::vector<Request> requests;
    for (int i = 0; i < 4; ++i) {
        requests.push_back(MakeWrite(src, 0x1000 + i * 64, 64));
    }
    ASSERT_TRUE(transport_->submitTransferTasks(batch_, requests).ok());
    EXPECT_EQ(state_->batch_copy_calls, 1);

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    ASSERT_NE(mf_batch, nullptr);
    EXPECT_EQ(mf_batch->pending_batch_count, 1u);

    g_state->query_ret = SM_OK;
    g_state->batch_result = SM_OK;

    TransferStatus status0{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status0).ok());
    EXPECT_EQ(status0.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status0.transferred_bytes, 64u);
    EXPECT_EQ(state_->status_query_calls, 1);

    TransferStatus status1{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 1, status1).ok());
    EXPECT_EQ(status1.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status1.transferred_bytes, 64u);
    EXPECT_EQ(state_->status_query_calls, 1);

    EXPECT_EQ(mf_batch->pending_batch_count, 0u);
}

TEST_F(MemFabricTransportTest, SubmitAndGetStatusPending) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    char src[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());

    g_state->query_ret = SM_NEED_RETRY;

    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    EXPECT_EQ(mf_batch->pending_batch_count, 1u);
}

TEST_F(MemFabricTransportTest, InvalidDestinationLeavesEmptyBatchUnchanged) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    char src[32]{};
    auto valid = MakeWrite(src, 0x1000, sizeof(src));
    auto invalid = MakeWrite(src, 0x2000, sizeof(src));
    // The fixture has not opened any remote segments.
    invalid.target_id = LOCAL_SEGMENT_ID + 1;

    EXPECT_FALSE(
        transport_->submitTransferTasks(batch_, {valid, invalid}).ok());

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    ASSERT_NE(mf_batch, nullptr);
    EXPECT_EQ(mf_batch->size(), 0u);
    EXPECT_EQ(mf_batch->pending_batch_count, 0u);
    EXPECT_TRUE(mf_batch->batch_status_cache.empty());
    EXPECT_EQ(state_->batch_copy_calls, 0);
    EXPECT_EQ(state_->status_query_calls, 0);
}

TEST_F(MemFabricTransportTest, InvalidDestinationPreservesExistingTasks) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 8).ok());

    char src[64]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);

    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x2000, 16)})
            .ok());
    state_->query_ret = SM_NEED_RETRY;
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 1, status).ok());
    ASSERT_EQ(status.s, TransferStatusEnum::PENDING);

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    ASSERT_NE(mf_batch, nullptr);
    const auto original_tasks = mf_batch->task_list;
    const auto original_cache = mf_batch->batch_status_cache;
    const auto original_pending = mf_batch->pending_batch_count;
    const auto original_copy_calls = state_->batch_copy_calls;
    const auto original_query_calls = state_->status_query_calls;

    auto valid = MakeWrite(src, 0x3000, 8);
    auto invalid = MakeWrite(src, 0x4000, 8);
    invalid.target_id = LOCAL_SEGMENT_ID + 1;
    EXPECT_FALSE(
        transport_->submitTransferTasks(batch_, {valid, invalid}).ok());

    ASSERT_EQ(mf_batch->size(), original_tasks.size());
    EXPECT_EQ(mf_batch->pending_batch_count, original_pending);
    EXPECT_EQ(mf_batch->batch_status_cache, original_cache);
    EXPECT_EQ(state_->batch_copy_calls, original_copy_calls);
    EXPECT_EQ(state_->status_query_calls, original_query_calls);
    for (size_t i = 0; i < original_tasks.size(); ++i) {
        const auto& before = original_tasks[i];
        const auto& after = mf_batch->task_list[i];
        EXPECT_EQ(after.request.opcode, before.request.opcode);
        EXPECT_EQ(after.request.source, before.request.source);
        EXPECT_EQ(after.request.target_id, before.request.target_id);
        EXPECT_EQ(after.request.target_offset, before.request.target_offset);
        EXPECT_EQ(after.request.length, before.request.length);
        EXPECT_EQ(after.batch_id, before.batch_id);
        EXPECT_EQ(after.status_word, before.status_word);
        EXPECT_EQ(after.length, before.length);
        EXPECT_EQ(after.transferred_bytes, before.transferred_bytes);
    }

    // A subsequent valid append must use the next slot without a gap.
    ASSERT_TRUE(transport_->submitTransferTasks(batch_, {valid}).ok());
    ASSERT_EQ(mf_batch->size(), original_tasks.size() + 1);
    EXPECT_EQ(state_->batch_copy_calls, original_copy_calls + 1);
    EXPECT_EQ(mf_batch->pending_batch_count, original_pending + 1);

    state_->query_ret = SM_OK;
    state_->batch_result = SM_OK;
    const size_t expected_bytes[] = {32, 16, 8};
    for (int task_id = 0; task_id < 3; ++task_id) {
        ASSERT_TRUE(
            transport_->getTransferStatus(batch_, task_id, status).ok());
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
        EXPECT_EQ(status.transferred_bytes, expected_bytes[task_id]);
    }
    EXPECT_EQ(mf_batch->pending_batch_count, 0u);
}

TEST_F(MemFabricTransportTest, FailedBatchPropagatesToAllTasks) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    char src[32]{};
    std::vector<Request> requests = {MakeWrite(src, 0x1000, 32),
                                     MakeWrite(src, 0x2000, 32)};
    ASSERT_TRUE(transport_->submitTransferTasks(batch_, requests).ok());

    g_state->query_ret = SM_OK;
    g_state->batch_result = SM_ERROR;

    TransferStatus status0{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status0).ok());
    EXPECT_EQ(status0.s, TransferStatusEnum::FAILED);

    TransferStatus status1{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 1, status1).ok());
    EXPECT_EQ(status1.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(state_->status_query_calls, 1);

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    EXPECT_EQ(mf_batch->pending_batch_count, 0u);
}

TEST_F(MemFabricTransportTest, PendingBatchDestructorWarning) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    char src[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    EXPECT_EQ(mf_batch->pending_batch_count, 1u);

    transport_->freeSubBatch(batch_);
    batch_ = nullptr;
}

TEST_F(MemFabricTransportTest, QuiesceDrainsInflightBatches) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());
    SetTransferTimeout(1000);
    std::array<char, 32> src, dst{};
    src.fill(0x5a);
    state_->copy_data = true;
    state_->defer_copy = true;
    state_->pending_queries = 2;
    ASSERT_TRUE(
        transport_
            ->submitTransferTasks(
                batch_,
                {MakeWrite(src.data(), reinterpret_cast<uint64_t>(dst.data()),
                           src.size())})
            .ok());
    EXPECT_EQ(InflightCount(), 1);

    ASSERT_TRUE(transport_->quiesce().ok());
    EXPECT_EQ(InflightCount(), 0);
    EXPECT_EQ(state_->status_query_calls, 3);
    EXPECT_EQ(dst, src);
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, src.size());
    EXPECT_EQ(state_->status_query_calls, 3);
    ASSERT_TRUE(transport_->quiesce().ok());
}

TEST_F(MemFabricTransportTest, QuiesceTimeoutWithPendingBatches) {
    ASSERT_TRUE(Install().ok());
    SetTransferTimeout(100);
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    char src[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());
    EXPECT_EQ(InflightCount(), 1);

    SetQueryResult(SM_NEED_RETRY);

    auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(transport_->quiesce().ok());
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    EXPECT_GE(elapsed, 90);
    EXPECT_LT(elapsed, 1000);
    EXPECT_GT(state_->status_query_calls, 1);
    EXPECT_EQ(InflightCount(), 1);
    auto* batch = static_cast<MemFabricSubBatch*>(batch_);
    EXPECT_EQ(batch->pending_batch_count, 1u);
    EXPECT_EQ(batch->task_list[0].status_word, TransferStatusEnum::PENDING);
    EXPECT_FALSE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x2000, 32)})
            .ok());
    EXPECT_EQ(batch->size(), 1u);
    EXPECT_EQ(state_->batch_copy_calls, 1);

    // A later drain can finish existing work without reopening submission.
    SetQueryResult(SM_OK);
    EXPECT_TRUE(transport_->quiesce().ok());
    EXPECT_EQ(InflightCount(), 0);
    EXPECT_FALSE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x2000, 32)})
            .ok());
}

TEST_F(MemFabricTransportTest, QuiesceRejectsNewWorkWhenIdle) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 1).ok());
    ASSERT_TRUE(transport_->quiesce().ok());
    char src[32]{};
    EXPECT_FALSE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());
    EXPECT_EQ(batch_->size(), 0u);
    EXPECT_EQ(state_->batch_copy_calls, 0);
    EXPECT_EQ(InflightCount(), 0);
    Transport::SubBatchRef extra = nullptr;
    EXPECT_FALSE(transport_->allocateSubBatch(extra, 1).ok());
    EXPECT_EQ(extra, nullptr);
    if (extra) transport_->freeSubBatch(extra);
}

TEST_F(MemFabricTransportTest, QuiescePollsAllBatchesAndExecutionFailures) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    char src[32]{};
    ASSERT_TRUE(transport_
                    ->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32),
                                                   MakeWrite(src, 0x2000, 32)})
                    .ok());
    Transport::SubBatchRef extra = nullptr;
    ASSERT_TRUE(transport_->allocateSubBatch(extra, 1).ok());
    EXPECT_TRUE(
        transport_->submitTransferTasks(extra, {MakeRead(src, 0x3000, 32)})
            .ok());
    EXPECT_EQ(InflightCount(), 2);
    SetQueryResult(SM_OK, SM_ERROR);
    EXPECT_TRUE(transport_->quiesce().ok());
    EXPECT_EQ(InflightCount(), 0);
    EXPECT_EQ(state_->status_query_calls, 2);
    for (auto* sub_batch : {batch_, extra}) {
        auto* batch = static_cast<MemFabricSubBatch*>(sub_batch);
        EXPECT_EQ(batch->pending_batch_count, 0u);
        for (const auto& task : batch->task_list) {
            EXPECT_EQ(task.status_word, TransferStatusEnum::FAILED);
            EXPECT_EQ(task.transferred_bytes, 0u);
        }
    }
    EXPECT_TRUE(transport_->freeSubBatch(extra).ok());
}

TEST_F(MemFabricTransportTest, QuiesceRetainsQueryErrorsUntilTimeout) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    SetTransferTimeout(20);
    char src[32]{};
    ASSERT_TRUE(transport_
                    ->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32),
                                                   MakeRead(src, 0x2000, 32)})
                    .ok());
    auto* batch = static_cast<MemFabricSubBatch*>(batch_);
    state_->results[batch->task_list[0].batch_id] = {SM_OK, SM_ERROR};
    state_->results[batch->task_list[1].batch_id] = {SM_INVALID_PARAM, SM_OK};
    EXPECT_FALSE(transport_->quiesce().ok());
    EXPECT_EQ(batch->task_list[0].status_word, TransferStatusEnum::FAILED);
    EXPECT_EQ(batch->task_list[1].status_word, TransferStatusEnum::PENDING);
    EXPECT_EQ(batch->pending_batch_count, 1u);
    EXPECT_EQ(InflightCount(), 1);
    state_->results[batch->task_list[1].batch_id] = {SM_OK, SM_OK};
    EXPECT_TRUE(transport_->quiesce().ok());
    EXPECT_EQ(InflightCount(), 0);
}

TEST_F(MemFabricTransportTest, QuiesceSerializesWithAcceptedSubmission) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 1).ok());
    SetTransferTimeout(1000);
    state_->block_submit = true;
    char src[32]{};
    auto submit = std::async(std::launch::async, [&] {
        return transport_->submitTransferTasks(batch_,
                                               {MakeWrite(src, 0x1000, 32)});
    });
    {
        std::unique_lock<std::mutex> lock(state_->mu);
        EXPECT_TRUE(state_->cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return state_->batch_copy_calls == 1;
        }));
    }
    std::promise<void> started;
    auto close = std::async(std::launch::async, [&] {
        started.set_value();
        return transport_->quiesce();
    });
    started.get_future().wait();
    EXPECT_EQ(close.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->block_submit = false;
        state_->cv.notify_all();
    }
    EXPECT_TRUE(submit.get().ok());
    EXPECT_TRUE(close.get().ok());
    EXPECT_EQ(InflightCount(), 0);
    EXPECT_EQ(state_->status_query_calls, 1);
    EXPECT_FALSE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x2000, 32)})
            .ok());
    EXPECT_EQ(state_->batch_copy_calls, 1);
}

TEST_F(MemFabricTransportTest, QuiesceAndExternalPollConsumeCompletionOnce) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 1).ok());
    SetTransferTimeout(1000);
    state_->pending_queries = 1;
    char src[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());
    auto close =
        std::async(std::launch::async, [&] { return transport_->quiesce(); });
    EXPECT_TRUE(WaitForQueries(1));
    TransferStatus status{};
    EXPECT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(close.get().ok());
    EXPECT_EQ(InflightCount(), 0);
    EXPECT_EQ(state_->status_query_calls, 2);
}

TEST_F(MemFabricTransportTest, QueryErrorPreservesPending) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());
    char source[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(source, 0x1000, 32)})
            .ok());
    SetQueryResult(SM_INVALID_PARAM, SM_OK);
    TransferStatus status{};
    EXPECT_FALSE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(InflightCount(), 1u);
    SetQueryResult(SM_OK, -777);
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(InflightCount(), 0u);
    const int calls = state_->status_query_calls;
    EXPECT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(state_->status_query_calls, calls);
    EXPECT_TRUE(transport_->quiesce().ok());
}

TEST_F(MemFabricTransportTest, ExecutionRetryCodeIsTerminalFailure) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 1).ok());
    char source[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(source, 0x1000, 32)})
            .ok());
    SetQueryResult(SM_OK, SM_NEED_RETRY);
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(InflightCount(), 0u);
}

TEST_F(MemFabricTransportTest, MixedGroupsKeepIndependentResultsAndBytes) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 2).ok());
    state_->copy_data = true;
    state_->defer_copy = true;
    std::array<char, 32> write_source, remote_read_source;
    write_source.fill(0x11);
    remote_read_source.fill(0x22);
    std::array<char, 32> remote_write_destination{}, read_destination{};
    ASSERT_TRUE(
        transport_
            ->submitTransferTasks(
                batch_,
                {MakeWrite(write_source.data(),
                           reinterpret_cast<uint64_t>(
                               remote_write_destination.data()),
                           32),
                 MakeRead(read_destination.data(),
                          reinterpret_cast<uint64_t>(remote_read_source.data()),
                          32)})
            .ok());
    auto* batch = static_cast<MemFabricSubBatch*>(batch_);
    state_->results[batch->task_list[0].batch_id] = {SM_OK, -777};
    state_->results[batch->task_list[1].batch_id] = {SM_NEED_RETRY, SM_ERROR};
    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(batch->pending_batch_count, 1u);
    EXPECT_EQ(remote_write_destination, (std::array<char, 32>{}));
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 1, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(read_destination, (std::array<char, 32>{}));
    EXPECT_EQ(batch->pending_batch_count, 1u);
    state_->results[batch->task_list[1].batch_id] = {SM_OK, SM_OK};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 1, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 32u);
    EXPECT_EQ(read_destination, remote_read_source);
    EXPECT_EQ(batch->pending_batch_count, 0u);
    EXPECT_EQ(batch->batch_status_cache.at(batch->task_list[0].batch_id),
              TransferStatusEnum::FAILED);
}

TEST_F(MemFabricTransportTest, TimeoutBatchMapsToTimeout) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    char src[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());

    g_state->query_ret = SM_OK;
    g_state->batch_result = SM_TIMEOUT;

    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::TIMEOUT);

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    ASSERT_NE(mf_batch, nullptr);
    EXPECT_EQ(mf_batch->pending_batch_count, 0u);
    EXPECT_EQ(InflightCount(), 0);
}

TEST_F(MemFabricTransportTest, InvalidParamMapsToInvalid) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    char src[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());

    g_state->query_ret = SM_OK;
    g_state->batch_result = SM_INVALID_PARAM;

    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::INVALID);

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    ASSERT_NE(mf_batch, nullptr);
    EXPECT_EQ(mf_batch->pending_batch_count, 0u);
    EXPECT_EQ(InflightCount(), 0);
}

TEST_F(MemFabricTransportTest, AddRemoveMemoryBuffer) {
    ASSERT_TRUE(Install().ok());

    BufferDesc desc{};
    desc.addr = 0x10000;
    desc.length = 4096;
    MemoryOptions options;
    ASSERT_TRUE(transport_->addMemoryBuffer(desc, options).ok());
    EXPECT_EQ(state_->register_mem_calls, 1);

    bool found = false;
    for (auto t : desc.transports) {
        if (t == TransportType::MEMFABRIC) found = true;
    }
    EXPECT_TRUE(found);

    ASSERT_TRUE(transport_->removeMemoryBuffer(desc).ok());
    EXPECT_EQ(state_->deregister_mem_calls, 1);
}

TEST_F(MemFabricTransportTest, DeregisterFailurePropagatesError) {
    ASSERT_TRUE(Install().ok());

    BufferDesc desc{};
    desc.addr = 0x10000;
    desc.length = 4096;
    MemoryOptions options;
    ASSERT_TRUE(transport_->addMemoryBuffer(desc, options).ok());

    g_state->deregister_ret = -9;
    auto status = transport_->removeMemoryBuffer(desc);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(state_->deregister_mem_calls, 1);

    g_state->deregister_ret = 0;
    ASSERT_TRUE(transport_->removeMemoryBuffer(desc).ok());
    EXPECT_EQ(state_->deregister_mem_calls, 2);
}

TEST_F(MemFabricTransportTest, AddMemoryBufferFailsBeforeInstall) {
    BufferDesc desc{};
    desc.addr = 0x10000;
    desc.length = 4096;
    MemoryOptions options;
    auto status = transport_->addMemoryBuffer(desc, options);
    EXPECT_FALSE(status.ok());
}

TEST_F(MemFabricTransportTest, SubmitFailsWhenBatchCopyFails) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    g_state->batch_copy_ret = -1;

    char src[32]{};
    ASSERT_TRUE(
        transport_->submitTransferTasks(batch_, {MakeWrite(src, 0x1000, 32)})
            .ok());

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    ASSERT_EQ(mf_batch->task_list.size(), 1u);
    EXPECT_EQ(mf_batch->task_list[0].status_word, TransferStatusEnum::FAILED);
    EXPECT_EQ(mf_batch->pending_batch_count, 0u);
}

}  // namespace tent
}  // namespace mooncake
