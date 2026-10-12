// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <acl/acl.h>

#include <atomic>
#include <cstdint>
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
thread_local aclrtContext g_context =
    reinterpret_cast<aclrtContext>(0x10000);
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

aclError aclrtMalloc(void** devPtr, size_t size,
                     aclrtMemMallocPolicy policy) {
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

}  // extern "C"

namespace mooncake {
namespace tent {

struct MockMemFabricState {
    std::mutex mu;
    int init_calls = 0;
    int config_init_calls = 0;
    int create_calls = 0;
    int destroy_calls = 0;
    int get_rpc_port_calls = 0;
    int batch_copy_calls = 0;
    int status_query_calls = 0;
    int register_mem_calls = 0;
    int deregister_mem_calls = 0;
    int set_log_level_calls = 0;
    std::atomic<uint64_t> next_batch_id{1};
    int32_t status_query_result = SMEM_TRANS_BATCH_DONE;
    int32_t batch_copy_ret = 0;
    bool create_fail = false;
};

static std::shared_ptr<MockMemFabricState> g_state;

int32_t mockSmemTransInit(uint32_t, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->init_calls++;
    return 0;
}

void mockSmemTransUninit(uint32_t) {}

int32_t mockSmemTransConfigInit(smem_trans_config_t* config) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->config_init_calls++;
    std::memset(config, 0, sizeof(*config));
    return 0;
}

smem_trans_t mockSmemTransCreate(const smem_trans_config_t*, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->create_calls++;
    if (g_state->create_fail) return nullptr;
    return reinterpret_cast<smem_trans_t>(0x2000);
}

void mockSmemTransDestroy(smem_trans_t, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->destroy_calls++;
}

int32_t mockSmemTransGetRpcPort(smem_trans_t, uint16_t* port) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->get_rpc_port_calls++;
    *port = 12345;
    return 0;
}

int32_t mockSmemTransRegisterMem(smem_trans_t, void*, size_t, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->register_mem_calls++;
    return 0;
}

int32_t mockSmemTransBatchRegisterMem(smem_trans_t, void*[], size_t[],
                                      uint32_t, uint32_t) {
    return 0;
}

int32_t mockSmemTransDeregisterMem(smem_trans_t, void*) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->deregister_mem_calls++;
    return 0;
}

int32_t mockSmemTransBatchCopy(smem_trans_t, smem_trans_batch_params_t*,
                                uint64_t* batchId) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->batch_copy_calls++;
    if (g_state->batch_copy_ret != 0) return g_state->batch_copy_ret;
    *batchId = g_state->next_batch_id.fetch_add(1, std::memory_order_relaxed);
    return 0;
}

int32_t mockSmemTransBatchStatusQuery(smem_trans_t, uint64_t, uint32_t) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->status_query_calls++;
    return g_state->status_query_result;
}

int32_t mockSmemTransSetPeerDownCallback(smem_trans_t,
                                          smem_trans_peer_down_callback_t,
                                          void*) {
    return 0;
}

int32_t mockSmemSetLogLevel(int) {
    std::lock_guard<std::mutex> lock(g_state->mu);
    g_state->set_log_level_calls++;
    return 0;
}

void InstallMockApi() {
    MemFabricUnderApi::gLoaded = true;
    MemFabricUnderApi::gLibHandle = reinterpret_cast<void*>(0x1000);
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
    MemFabricUnderApi::pSmemTransSetPeerDownCallback =
        &mockSmemTransSetPeerDownCallback;
    MemFabricUnderApi::pSmemSetLogLevel = &mockSmemSetLogLevel;
}

void ResetMockApi() {
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
    MemFabricUnderApi::pSmemTransSetPeerDownCallback = nullptr;
    MemFabricUnderApi::pSmemSetLogLevel = nullptr;
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

class MemFabricTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ResetAcl();
        g_state = std::make_shared<MockMemFabricState>();
        InstallMockApi();
        setenv("MF_DATA_OP_TYPE", "device_sdma", 1);
        metadata_ = std::make_shared<ControlService>("p2p", "", nullptr);
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
        unsetenv("MF_DATA_OP_TYPE");
        g_state.reset();
    }

    Status Install() {
        name_ = "127.0.0.1:23456";
        auto conf = std::make_shared<Config>();
        return transport_->install(name_, metadata_, nullptr, conf);
    }

    std::shared_ptr<MockMemFabricState> state_;
    std::shared_ptr<ControlService> metadata_;
    std::unique_ptr<MemFabricTransport> transport_;
    Transport::SubBatchRef batch_{nullptr};
    std::string name_;
};

TEST_F(MemFabricTransportTest, InstallSucceeds) {
    ASSERT_TRUE(Install().ok());
    EXPECT_EQ(state_->init_calls, 1);
    EXPECT_EQ(state_->create_calls, 1);
    EXPECT_EQ(state_->get_rpc_port_calls, 1);
    EXPECT_TRUE(transport_->installed_);
    EXPECT_NE(transport_->handle_, nullptr);
}

TEST_F(MemFabricTransportTest, InstallFailsWhenCreateReturnsNull) {
    g_state->create_fail = true;
    auto status = Install();
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(state_->create_calls, 1);
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

    g_state->status_query_result = SMEM_TRANS_BATCH_DONE;

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

    g_state->status_query_result = SMEM_TRANS_BATCH_PENDING;

    TransferStatus status{};
    ASSERT_TRUE(transport_->getTransferStatus(batch_, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);

    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch_);
    EXPECT_EQ(mf_batch->pending_batch_count, 1u);
}

TEST_F(MemFabricTransportTest, FailedBatchPropagatesToAllTasks) {
    ASSERT_TRUE(Install().ok());
    ASSERT_TRUE(transport_->allocateSubBatch(batch_, 4).ok());

    char src[32]{};
    std::vector<Request> requests = {MakeWrite(src, 0x1000, 32),
                                     MakeWrite(src, 0x2000, 32)};
    ASSERT_TRUE(transport_->submitTransferTasks(batch_, requests).ok());

    g_state->status_query_result = -2000;

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
    EXPECT_EQ(mf_batch->task_list[0].status_word,
              TransferStatusEnum::FAILED);
    EXPECT_EQ(mf_batch->pending_batch_count, 0u);
}

}  // namespace tent
}  // namespace mooncake
