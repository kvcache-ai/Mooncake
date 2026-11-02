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

#include "memfabric_allocator.h"
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
    int init_device = -1;
    int create_device = -1;
    int mem_free_calls = 0;
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
    int32_t register_mem_ret = 0;
    int fail_register_call = 0;
    std::vector<Copy> submitted_copies;
    int batch_register_mem_calls = 0;
    int32_t batch_register_ret = 0;
    std::vector<void*> registered_addrs;
    std::vector<size_t> registered_sizes;
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

int32_t mockSmemMemAlloc(uint16_t, uint64_t size, uint32_t, void** address) {
    *address = std::malloc(size);
    return *address ? 0 : -1;
}

int32_t mockSmemMemFree(void* address) {
    ++g_state->mem_free_calls;
    std::free(address);
    return 0;
}

int32_t mockSmemTransInit(uint32_t device, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->init_calls++;
    g_state->init_device = device;
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
    g_state->create_device = config->deviceId;
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

int32_t mockSmemTransRegisterMem(smem_trans_t, void* addr, size_t size,
                                 uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    ++g_state->register_mem_calls;
    if (g_state->register_mem_ret != 0 ||
        g_state->register_mem_calls == g_state->fail_register_call)
        return -9;
    const auto begin = reinterpret_cast<uint64_t>(addr);
    for (size_t i = 0; i < g_state->registered_addrs.size(); ++i) {
        const auto old =
            reinterpret_cast<uint64_t>(g_state->registered_addrs[i]);
        if (begin < old + g_state->registered_sizes[i] && old < begin + size)
            return -9;  // Native partial overlap must never be submitted.
    }
    g_state->registered_addrs.push_back(addr);
    g_state->registered_sizes.push_back(size);
    return 0;
}

int32_t mockSmemTransBatchRegisterMem(smem_trans_t, void* addrs[],
                                      size_t sizes[], uint32_t count,
                                      uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->batch_register_mem_calls++;
    g_state->registered_addrs.assign(addrs, addrs + count);
    g_state->registered_sizes.assign(sizes, sizes + count);
    return g_state->batch_register_ret;
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
    g_state->submitted_copies.push_back(
        {params->opcode,
         std::vector<void*>(params->srcList,
                            params->srcList + params->batchSize),
         std::vector<void*>(params->destList,
                            params->destList + params->batchSize),
         std::vector<size_t>(params->sizeList,
                             params->sizeList + params->batchSize)});
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
        unsetenv("MF_STORE_PROTOCOL");
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
        unsetenv("MF_STORE_PROTOCOL");
    }

    Status Initialize(MemFabricTransport& transport) {
        std::lock_guard<std::mutex> lock(transport.inflight_mu_);
        return transport.initializeLocked();
    }

    // Complete both phases for tests that exercise the native transport.
    Status Install(const char* protocol = "device_rdma", bool store = false) {
        if (!transport_->installed_) {
            CHECK_STATUS(Configure(protocol, store));
        }
        return Initialize(*transport_);
    }

    Status Configure(const char* protocol = "device_rdma", bool store = false) {
        if (protocol) {
            setenv("MF_PROTOCOL", protocol, 1);
        } else {
            unsetenv("MF_PROTOCOL");
        }
        auto conf = std::make_shared<Config>();
        conf->set("transports/memfabric/store_te_init", store);
        return transport_->install(name_, metadata_, nullptr, conf);
    }

    std::string Protocol() const {
        return transport_->config_.data_op_type_str;
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
    std::weak_ptr<const MemFabricUnderApi::MemoryApi> InstallMemoryApi(
        std::shared_ptr<void> library = nullptr) {
        auto api = std::make_shared<MemFabricUnderApi::MemoryApi>();
        api->library = std::move(library);
        api->allocate = mockSmemMemAlloc;
        api->free = mockSmemMemFree;
        MemFabricUnderApi::gMemoryApi = api;
        MemFabricUnderApi::gMemoryApiWeak = api;
        return api;
    }

    std::shared_ptr<MockMemFabricState> state_;
    std::shared_ptr<ControlService> metadata_;
    std::unique_ptr<MemFabricTransport> transport_;
    Transport::SubBatchRef batch_{nullptr};
    std::string name_;
};

void MemFabricTransportTest::InstallMockApi() {
    InstallMemoryApi();
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
    MemFabricUnderApi::CleanupLibrary();
    MemFabricUnderApi::gMemoryApiWeak.reset();
}

TEST_F(MemFabricTransportTest, SharedAllocationsRetainLibraryAfterUninstall) {
    auto closed = std::make_shared<int>(0);
    auto api = InstallMemoryApi(std::shared_ptr<void>(
        reinterpret_cast<void*>(1), [closed](void*) { ++*closed; }));
    ASSERT_TRUE(Install("device_sdma").ok());
    auto release = [](void* ptr) { memfabric_free_memory("ascend", ptr); };
    std::unique_ptr<void, decltype(release)> first(
        memfabric_allocate_memory(64, "ascend"), release);
    std::unique_ptr<void, decltype(release)> second(
        memfabric_allocate_memory(64, "ascend"), release);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ASSERT_TRUE(transport_->uninstall().ok());
    EXPECT_FALSE(MemFabricUnderApi::IsLoaded());
    EXPECT_EQ(*closed, 0);
    first.reset();
    EXPECT_FALSE(api.expired());
    EXPECT_EQ(*closed, 0);
    second.reset();
    EXPECT_TRUE(api.expired());
    EXPECT_EQ(*closed, 1);
}

TEST_F(MemFabricTransportTest, NativeHostMemoryIgnoresMmapHints) {
    InstallMemoryApi();
    setenv("MF_STORE_PROTOCOL", "device_sdma", 1);
    ASSERT_TRUE(Install("device_rdma").ok());
    EXPECT_TRUE(memfabric_use_fabric_mem("ascend"));
    EXPECT_FALSE(memfabric_use_fabric_mem("tcp"));
    size_t actual_size = 0;
    auto release = [](void* ptr) { memfabric_free_memory("ascend", ptr); };
    std::unique_ptr<void, decltype(release)> buffer(
        memfabric_allocate_memory_best_effort(
            64, "ascend", &actual_size,
            [](size_t, size_t, bool) -> void* {
                ADD_FAILURE() << "Native Host memory must not use mmap";
                return nullptr;
            },
            2 * 1024 * 1024, true),
        release);
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(actual_size, 64u);
    setenv("MF_STORE_PROTOCOL", "device_rdma", 1);
    EXPECT_FALSE(memfabric_use_fabric_mem("ascend"));
    buffer.reset();
    EXPECT_EQ(state_->mem_free_calls, 1);
}

TEST_F(MemFabricTransportTest, RdmaMemoryHonorsHostAllocationCallback) {
    setenv("MF_STORE_PROTOCOL", "device_rdma", 1);
    ASSERT_TRUE(Install("device_sdma").ok());
    size_t actual_size = 0;
    void* buffer = memfabric_allocate_memory_best_effort(
        64, "ascend", &actual_size,
        [](size_t size, size_t alignment, bool defer_population) -> void* {
            EXPECT_EQ(alignment, 4096u);
            EXPECT_TRUE(defer_population);
            return std::malloc(size);
        },
        4096, true);
    EXPECT_NE(buffer, nullptr);
    EXPECT_EQ(actual_size, 64u);
    std::free(buffer);
    EXPECT_EQ(state_->mem_free_calls, 0);
}

TEST_F(MemFabricTransportTest, ConcurrentRegistrationUsesRegistrationDevice) {
    g_device_count = 2;
    ASSERT_TRUE(Configure("device_sdma").ok());
    auto registration = [this](uint64_t addr) {
        g_device_id = 1;
        BufferDesc desc;
        desc.addr = addr;
        desc.length = 4096;
        MemoryOptions options;
        options.perm = kLocalReadWrite;
        return transport_->addMemoryBuffer(desc, options);
    };
    auto first = std::async(std::launch::async, registration, 0x10000);
    auto second = std::async(std::launch::async, registration, 0x20000);
    EXPECT_TRUE(first.get().ok());
    EXPECT_TRUE(second.get().ok());
    EXPECT_EQ(state_->init_calls, 1);
    EXPECT_EQ(state_->create_calls, 1);
    EXPECT_EQ(state_->init_device, 1);
    EXPECT_EQ(state_->create_device, 1);
    EXPECT_EQ(state_->register_mem_calls, 0);
    EXPECT_FALSE(PublishedEndpoint().empty());
    // Later callers start on another device and are rebound to the owner.
    BufferDesc desc;
    desc.addr = 0x30000;
    desc.length = 4096;
    g_device_id = 0;
    ASSERT_TRUE(transport_->addMemoryBuffer(desc, MemoryOptions()).ok());
    EXPECT_EQ(g_device_id, 1);
    EXPECT_EQ(state_->register_mem_calls, 1);
}

TEST_F(MemFabricTransportTest,
       FirstBatchRegistrationInitializesAndSharesCoverage) {
    g_device_count = 2;
    g_device_id = -1;
    ASSERT_TRUE(Configure("device_urma").ok());
    EXPECT_EQ(state_->init_calls, 0);
    EXPECT_EQ(state_->create_calls, 0);
    EXPECT_TRUE(PublishedEndpoint().empty());
    g_device_id = 1;
    const std::array<uint64_t, 3> addresses{0x700000, 0x300000, 0x500000};
    std::vector<BufferDesc> buffers(addresses.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        buffers[i].addr = addresses[i];
        buffers[i].length = 0x200000;
    }
    ASSERT_TRUE(transport_->addMemoryBuffer(buffers, MemoryOptions{}).ok());
    EXPECT_EQ(state_->init_calls, 1);
    EXPECT_EQ(state_->init_device, 1);
    EXPECT_EQ(state_->create_device, 1);
    EXPECT_EQ(state_->register_mem_calls, 1);
    EXPECT_EQ(state_->batch_register_mem_calls, 0);
    EXPECT_EQ(state_->registered_addrs,
              (std::vector<void*>{reinterpret_cast<void*>(0x200000)}));
    EXPECT_EQ(state_->registered_sizes, (std::vector<size_t>{0x800000}));
    EXPECT_FALSE(PublishedEndpoint().empty());
    for (size_t i = 0; i < buffers.size(); ++i) {
        const auto& buffer = buffers[i];
        EXPECT_EQ(buffer.addr, addresses[i]);
        EXPECT_EQ(buffer.transports, (std::vector<TransportType>{MEMFABRIC}));
    }
}

TEST_F(MemFabricTransportTest, FirstLocalSdmaBatchSkipsNativeRegistration) {
    ASSERT_TRUE(Configure("device_sdma").ok());
    g_device_id = 0;
    std::vector<BufferDesc> buffers(2);
    buffers[0].addr = 0x201000;
    buffers[0].length = 4096;
    buffers[1].addr = 0x202000;
    buffers[1].length = 8192;
    MemoryOptions options;
    options.perm = kLocalReadWrite;
    ASSERT_TRUE(transport_->addMemoryBuffer(buffers, options).ok());
    EXPECT_EQ(state_->init_calls, 1);
    EXPECT_EQ(state_->register_mem_calls, 0);
    EXPECT_EQ(state_->batch_register_mem_calls, 0);
    for (const auto& buffer : buffers)
        EXPECT_EQ(buffer.transports, (std::vector<TransportType>{MEMFABRIC}));
}

TEST_F(MemFabricTransportTest, InstallSucceeds) {
    ASSERT_TRUE(Install().ok());
    EXPECT_EQ(state_->init_calls, 1);
    EXPECT_EQ(state_->create_calls, 1);
    EXPECT_EQ(state_->get_rpc_port_calls, 1);
}

TEST_F(MemFabricTransportTest, StoreProtocolOverridesTransportAndAllocator) {
    for (const auto* store_protocol : {"device_rdma", "device_sdma"}) {
        SCOPED_TRACE(store_protocol);
        InstallMockApi();
        setenv("MF_STORE_PROTOCOL", store_protocol, 1);
        ASSERT_TRUE(Install("device_urma").ok());
        EXPECT_EQ(Protocol(), "device_urma");
        ASSERT_TRUE(transport_->uninstall().ok());
        InstallMockApi();
        ASSERT_TRUE(Install("device_urma", true).ok());
        EXPECT_EQ(Protocol(), store_protocol);
        EXPECT_EQ(memfabric_use_fabric_mem("ascend"),
                  std::string(store_protocol) == "device_sdma");
        EXPECT_STREQ(std::getenv("MF_PROTOCOL"), "device_urma");
        EXPECT_STREQ(std::getenv("MF_STORE_PROTOCOL"), store_protocol);
        ASSERT_TRUE(transport_->uninstall().ok());
    }
}

TEST_F(MemFabricTransportTest, StoreProtocolFallbackAndValidation) {
    ASSERT_TRUE(Install("device_sdma", true).ok());
    EXPECT_EQ(Protocol(), "device_sdma");
    ASSERT_TRUE(transport_->uninstall().ok());
    InstallMockApi();
    setenv("MF_STORE_PROTOCOL", "", 1);
    ASSERT_TRUE(Install("device_rdma", true).ok());
    EXPECT_EQ(Protocol(), "device_rdma");
    ASSERT_TRUE(transport_->uninstall().ok());
    InstallMockApi();
    setenv("MF_STORE_PROTOCOL", "device_sdma", 1);
    EXPECT_FALSE(Install(nullptr).ok());
    ASSERT_TRUE(Install(nullptr, true).ok());
    EXPECT_EQ(Protocol(), "device_sdma");
    EXPECT_TRUE(memfabric_use_fabric_mem("ascend"));
    EXPECT_EQ(std::getenv("MF_PROTOCOL"), nullptr);
    ASSERT_TRUE(transport_->uninstall().ok());
    InstallMockApi();
    setenv("MF_STORE_PROTOCOL", "invalid", 1);
    ASSERT_TRUE(Install("device_rdma").ok());
    ASSERT_TRUE(transport_->uninstall().ok());
    EXPECT_FALSE(Install("device_rdma", true).ok());
    EXPECT_STREQ(std::getenv("MF_PROTOCOL"), "device_rdma");
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
    ASSERT_TRUE(
        other.install(name_, metadata_, nullptr, std::make_shared<Config>())
            .ok());
    EXPECT_FALSE(Initialize(other).ok());
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
        auto status = other.install(name_, metadata_, nullptr,
                                    std::make_shared<Config>());
        return status.ok() ? Initialize(other) : status;
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

TEST_F(MemFabricTransportTest, BatchRegisterBuffersSharesAlignedCoverage) {
    ASSERT_TRUE(Install().ok());
    std::vector<BufferDesc> buffers(2);
    buffers[0].addr = 0x201000;
    buffers[0].length = 4096;
    buffers[1].addr = 0x202000;
    buffers[1].length = 8192;
    ASSERT_TRUE(transport_->addMemoryBuffer(buffers, MemoryOptions{}).ok());
    EXPECT_EQ(state_->register_mem_calls, 1);
    EXPECT_EQ(state_->batch_register_mem_calls, 0);
    EXPECT_EQ(state_->registered_addrs,
              (std::vector<void*>{reinterpret_cast<void*>(0x200000)}));
    EXPECT_EQ(state_->registered_sizes, (std::vector<size_t>{0x200000}));
    for (const auto& desc : buffers) {
        EXPECT_EQ(desc.transports, (std::vector<TransportType>{MEMFABRIC}));
        ASSERT_EQ(desc.transport_attrs.count(MEMFABRIC), 1u);
        EXPECT_TRUE(desc.transport_attrs.at(MEMFABRIC).empty());
    }
}

TEST_F(MemFabricTransportTest, BatchRegisterFailureDoesNotPublishBuffers) {
    std::vector<BufferDesc> buffers;
    EXPECT_TRUE(transport_->addMemoryBuffer(buffers, MemoryOptions{}).ok());
    buffers.resize(2);
    buffers[0].addr = 0x201000;
    buffers[0].length = 4096;
    buffers[1].addr = 0x202000;
    buffers[1].length = 4096;
    EXPECT_FALSE(transport_->addMemoryBuffer(buffers, MemoryOptions{}).ok());
    EXPECT_EQ(state_->batch_register_mem_calls, 0);
    ASSERT_TRUE(Install().ok());
    state_->register_mem_ret = -9;
    EXPECT_FALSE(transport_->addMemoryBuffer(buffers, MemoryOptions{}).ok());
    EXPECT_EQ(state_->register_mem_calls, 1);
    EXPECT_EQ(state_->batch_register_mem_calls, 0);
    for (const auto& desc : buffers) {
        EXPECT_TRUE(desc.transports.empty());
        EXPECT_TRUE(desc.transport_attrs.empty());
    }
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

TEST_F(MemFabricTransportTest,
       SequentialThreeMiBBuffersRegisterOnlyUncoveredTail) {
    ASSERT_TRUE(Install().ok());
    BufferDesc left{}, right{};
    left.addr = 0x200000;
    left.length = 0x300000;
    right.addr = 0x500000;
    right.length = 0x300000;
    ASSERT_TRUE(transport_->addMemoryBuffer(left, MemoryOptions{}).ok());
    ASSERT_TRUE(transport_->addMemoryBuffer(right, MemoryOptions{}).ok());
    EXPECT_EQ(state_->registered_addrs,
              (std::vector<void*>{reinterpret_cast<void*>(0x200000),
                                  reinterpret_cast<void*>(0x600000)}));
    EXPECT_EQ(state_->registered_sizes,
              (std::vector<size_t>{0x400000, 0x200000}));
    ASSERT_TRUE(transport_->removeMemoryBuffer(left).ok());
    ASSERT_TRUE(transport_->addMemoryBuffer(right, MemoryOptions{}).ok());
    EXPECT_EQ(state_->register_mem_calls, 2);
}

TEST_F(MemFabricTransportTest, InvalidRegistrationDoesNotReachNativeApi) {
    ASSERT_TRUE(Install().ok());
    for (const auto& range : std::vector<std::pair<uint64_t, uint64_t>>{
             {0, 1},
             {0x200000, 0},
             {UINT64_MAX - 1, 4},
             {UINT64_MAX - 0x100000, 1}}) {
        BufferDesc desc{};
        desc.addr = range.first;
        desc.length = range.second;
        EXPECT_FALSE(transport_->addMemoryBuffer(desc, MemoryOptions{}).ok());
    }
    EXPECT_EQ(state_->register_mem_calls, 0);
}

TEST_F(MemFabricTransportTest,
       ReadAndWriteSplitAtBothEndpointsAndPreserveTaskStatus) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());
    constexpr size_t length = 5 * 1024 * 1024 + 17;
    constexpr uint64_t page = 2 * 1024 * 1024;
    std::vector<char> source(length, 37), target(length, 0);
    state_->copy_data = true;
    for (bool write : {true, false}) {
        const auto request =
            write ? MakeWrite(source.data(),
                              reinterpret_cast<uint64_t>(target.data()), length)
                  : MakeRead(target.data(),
                             reinterpret_cast<uint64_t>(source.data()), length);
        const auto task = batch_->size();
        ASSERT_TRUE(transport_->submitTransferTasks(batch_, {request}).ok());
        const auto& copy = state_->submitted_copies.back();
        size_t copied = 0;
        ASSERT_GT(copy.sizes.size(), 1U);
        for (size_t i = 0; i < copy.sizes.size(); ++i) {
            EXPECT_LE(reinterpret_cast<uint64_t>(copy.sources[i]) % page +
                          copy.sizes[i],
                      page);
            EXPECT_LE(reinterpret_cast<uint64_t>(copy.destinations[i]) % page +
                          copy.sizes[i],
                      page);
            copied += copy.sizes[i];
        }
        EXPECT_EQ(copied, length);
        EXPECT_EQ(target, source);
        TransferStatus status{};
        ASSERT_TRUE(transport_->getTransferStatus(batch_, task, status).ok());
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
        EXPECT_EQ(status.transferred_bytes, length);
        std::fill(target.begin(), target.end(), 0);
    }
}

TEST_F(MemFabricTransportTest, AdjacentAlignedAllocationsStaySeparate) {
    ASSERT_TRUE(Install().ok());
    std::vector<BufferDesc> buffers(2);
    buffers[0].addr = 0x400000;
    buffers[1].addr = 0x200000;
    for (auto& buffer : buffers) buffer.length = 0x200000;
    ASSERT_TRUE(transport_->addMemoryBuffer(buffers, MemoryOptions{}).ok());
    EXPECT_EQ(state_->register_mem_calls, 2);
    EXPECT_EQ(state_->registered_sizes,
              (std::vector<size_t>{0x200000, 0x200000}));
}

TEST_F(MemFabricTransportTest, MergedGapFailureCleanupRetryRetainsCoverage) {
    ASSERT_TRUE(Install().ok());
    BufferDesc existing{};
    existing.addr = 0x400000;
    existing.length = 0x200000;
    ASSERT_TRUE(transport_->addMemoryBuffer(existing, MemoryOptions{}).ok());
    std::vector<BufferDesc> buffers(2);
    buffers[0].addr = 0x300000;
    buffers[0].length = 0x400000;
    buffers[1].addr = 0x700000;
    buffers[1].length = 0x200000;
    state_->fail_register_call = 3;
    EXPECT_FALSE(transport_->addMemoryBuffer(buffers, MemoryOptions{}).ok());
    for (auto& buffer : buffers) {
        EXPECT_TRUE(buffer.transports.empty());
        // Match the engine's defensive cleanup after a failed registration.
        ASSERT_TRUE(transport_->removeMemoryBuffer(buffer).ok());
    }
    state_->fail_register_call = 0;
    ASSERT_TRUE(transport_->addMemoryBuffer(buffers, MemoryOptions{}).ok());
    EXPECT_EQ(state_->register_mem_calls, 4);
    EXPECT_EQ(state_->registered_sizes,
              (std::vector<size_t>{0x200000, 0x200000, 0x400000}));
    ASSERT_TRUE(transport_->uninstall().ok());
    EXPECT_EQ(state_->destroy_calls, 1);
}

TEST_F(MemFabricTransportTest, ExcessiveSliceCountDispatchesNoWork) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 1).ok());
    const uint64_t length = uint64_t{UINT32_MAX} * 0x200000 + 1;
    EXPECT_FALSE(transport_
                     ->submitTransferTasks(
                         batch_, {MakeWrite(reinterpret_cast<void*>(0x200000),
                                            0x200000, length)})
                     .ok());
    EXPECT_EQ(batch_->size(), 0u);
    EXPECT_EQ(state_->batch_copy_calls, 0);
}

}  // namespace tent
}  // namespace mooncake
