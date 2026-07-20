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
#include <glog/logging.h>

#include <acl/acl.h>
#include <acl/acl_rt.h>
#include <algorithm>
#include <cstdlib>
#include "transport/ascend_transport/ascend_direct_transport/ascend_direct_transport.h"
#include "transport/ascend_transport/ascend_direct_transport/context_manager.h"
#include "ascend_allocator.h"
#include "transport/ascend_transport/ascend_direct_transport/adxl_compat.h"
#include "transfer_metadata.h"
#include "config.h"
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

using namespace mooncake;

namespace {
constexpr int kDummyRealRoceEngineCount = 16;
constexpr size_t kTransferBufSize = 4096;
constexpr size_t kRegisterMemSize = 1024 * 1024;
constexpr int kTransferStatusMaxRetries = 1000;

static int g_device_id = 0;
static int g_device_count = 1;
static aclrtContext g_context = reinterpret_cast<aclrtContext>(0x1234);
static aclError g_memcpy_result = ACL_ERROR_NONE;
static aclError g_memcpy_async_result = ACL_ERROR_NONE;
static aclError g_memcpy_batch_result = ACL_ERROR_NONE;
static int g_get_device_call_count = 0;
static int g_set_device_call_count = 0;
static std::set<int> g_set_device_ids;
static std::map<const void*, aclrtMemLocation> g_memory_locations;
static std::mutex g_acl_mutex;

namespace mock_acl {
void reset() {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    g_device_id = 0;
    g_device_count = 1;
    g_memcpy_result = ACL_ERROR_NONE;
    g_memcpy_async_result = ACL_ERROR_NONE;
    g_memcpy_batch_result = ACL_ERROR_NONE;
    g_get_device_call_count = 0;
    g_set_device_call_count = 0;
    g_set_device_ids.clear();
    g_memory_locations.clear();
}

void set_device_count(int count) {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    g_device_count = count;
}

void set_pointer_location(void* ptr, aclrtMemLocationType type, uint32_t id) {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    aclrtMemLocation loc;
    loc.id = id;
    loc.type = type;
    g_memory_locations[ptr] = loc;
}

void set_memcpy_result(aclError result) {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    g_memcpy_result = result;
}

void set_memcpy_batch_result(aclError result) {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    g_memcpy_batch_result = result;
}

void set_memcpy_async_result(aclError result) {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    g_memcpy_async_result = result;
}

int get_get_device_call_count() {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    return g_get_device_call_count;
}

int get_set_device_call_count() {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    return g_set_device_call_count;
}

bool all_devices_covered(int begin, int end_exclusive) {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    for (int device = begin; device < end_exclusive; ++device) {
        if (g_set_device_ids.find(device) == g_set_device_ids.end()) {
            return false;
        }
    }
    return true;
}
}  // namespace mock_acl

extern "C" {

aclError aclrtGetDeviceCount(uint32_t* count) {
    *count = static_cast<uint32_t>(g_device_count);
    return ACL_ERROR_NONE;
}

aclError aclrtGetDevice(int* deviceId) {
    g_get_device_call_count++;
    *deviceId = g_device_id;
    return ACL_ERROR_NONE;
}

aclError aclrtSetDevice(int deviceId) {
    if (deviceId >= g_device_count) {
        return 1;
    }
    g_device_id = deviceId;
    g_set_device_call_count++;
    g_set_device_ids.insert(deviceId);
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

aclError aclrtCreateStreamWithConfig(aclrtStream* stream, uint32_t priority,
                                     uint32_t config) {
    (void)priority;
    (void)config;
    *stream = reinterpret_cast<aclrtStream>(0x5678);
    return ACL_ERROR_NONE;
}

aclError aclrtDestroyStream(aclrtStream stream) {
    (void)stream;
    return ACL_ERROR_NONE;
}

aclError aclrtSynchronizeStreamWithTimeout(aclrtStream stream,
                                           int32_t timeout) {
    (void)stream;
    (void)timeout;
    return ACL_ERROR_NONE;
}

aclError aclrtStreamAbort(aclrtStream stream) {
    (void)stream;
    return ACL_ERROR_NONE;
}

aclError aclrtMemcpy(void* dst, size_t destMax, const void* src, size_t count,
                     aclrtMemcpyKind kind) {
    (void)destMax;
    (void)kind;
    aclError result;
    {
        std::lock_guard<std::mutex> lock(g_acl_mutex);
        result = g_memcpy_result;
        if (result == ACL_ERROR_NONE) {
            memcpy(dst, src, count);
        }
    }
    return result;
}

aclError aclrtMemcpyAsync(void* dst, size_t destMax, const void* src,
                          size_t count, aclrtMemcpyKind kind,
                          aclrtStream stream) {
    (void)destMax;
    (void)kind;
    (void)stream;
    aclError result;
    {
        std::lock_guard<std::mutex> lock(g_acl_mutex);
        result = g_memcpy_async_result;
        if (result == ACL_ERROR_NONE) {
            memcpy(dst, src, count);
        }
    }
    return result;
}

aclError aclrtMemcpyBatch(void** dstList, size_t* dstSizes, void** srcList,
                          size_t* srcSizes, size_t count,
                          aclrtMemcpyBatchAttr* attrs, size_t* attrIds,
                          size_t attrCount, size_t* failIdx) {
    (void)attrs;
    (void)attrIds;
    (void)attrCount;

    std::lock_guard<std::mutex> lock(g_acl_mutex);

    if (g_memcpy_batch_result == ACL_ERROR_RT_FEATURE_NOT_SUPPORT) {
        return ACL_ERROR_RT_FEATURE_NOT_SUPPORT;
    }

    if (g_memcpy_batch_result != ACL_ERROR_NONE) {
        *failIdx = 0;
        return g_memcpy_batch_result;
    }

    for (size_t i = 0; i < count; i++) {
        memcpy(dstList[i], srcList[i], dstSizes[i]);
    }
    return ACL_ERROR_NONE;
}

aclError aclrtPointerGetAttributes(const void* ptr,
                                   aclrtPtrAttributes* attributes) {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    auto it = g_memory_locations.find(ptr);
    if (it != g_memory_locations.end()) {
        attributes->location = it->second;
    } else {
        attributes->location.id = 0;
        attributes->location.type = ACL_MEM_LOCATION_TYPE_HOST;
    }
    return ACL_ERROR_NONE;
}

const char* aclGetRecentErrMsg() { return "mock acl error message"; }

aclError aclrtGetPhyDevIdByLogicDevId(int32_t logic_dev_id,
                                      int32_t* physical_dev_id) {
    *physical_dev_id = logic_dev_id;
    return ACL_ERROR_NONE;
}

aclError aclrtMallocPhysical(aclrtDrvMemHandle* handle, size_t size,
                             aclrtPhysicalMemProp* prop, uint32_t flags) {
    (void)size;
    (void)prop;
    (void)flags;
    *handle = reinterpret_cast<aclrtDrvMemHandle>(malloc(1));
    return *handle ? ACL_ERROR_NONE : ACL_ERROR_FAILURE;
}

aclError aclrtReserveMemAddress(void** va, size_t size, size_t alignment,
                                void* hint_addr, uint32_t page_type) {
    (void)alignment;
    (void)hint_addr;
    (void)page_type;
    *va = malloc(size);
    return *va ? ACL_ERROR_NONE : ACL_ERROR_FAILURE;
}

aclError aclrtMapMem(void* va, size_t size, size_t offset,
                     aclrtDrvMemHandle handle, size_t map_offset) {
    (void)va;
    (void)size;
    (void)offset;
    (void)handle;
    (void)map_offset;
    return ACL_ERROR_NONE;
}

aclError aclrtFreePhysical(aclrtDrvMemHandle handle) {
    free(handle);
    return ACL_ERROR_NONE;
}

aclError aclrtReleaseMemAddress(void* va) {
    free(va);
    return ACL_ERROR_NONE;
}

aclError aclrtUnmapMem(void* va) {
    (void)va;
    return ACL_ERROR_NONE;
}

aclError aclrtMallocHost(void** host_ptr, size_t size) {
    *host_ptr = malloc(size);
    return *host_ptr ? ACL_ERROR_NONE : ACL_ERROR_FAILURE;
}

aclError aclrtFreeHost(void* host_ptr) {
    free(host_ptr);
    return ACL_ERROR_NONE;
}
}

static adxl::Status g_connect_result = adxl::SUCCESS;
static adxl::Status g_transfer_result = adxl::SUCCESS;
static adxl::Status g_initialize_result = adxl::SUCCESS;
static adxl::Status g_transfer_async_result = adxl::SUCCESS;
static adxl::Status g_get_transfer_status_result = adxl::SUCCESS;
static adxl::Status g_register_mem_result = adxl::SUCCESS;
static adxl::TransferStatus g_transfer_status_enum =
    adxl::TransferStatus::COMPLETED;
static std::deque<adxl::Status> g_transfer_results;
static std::deque<adxl::Status> g_transfer_async_results;
static std::vector<uintptr_t> g_registered_mem_handles;
static std::vector<uintptr_t> g_deregistered_mem_handles;
static std::set<std::string> g_connected;
static std::mutex g_mutex;
static bool g_was_initialize_called = false;
static int g_next_handle = 1;
static int g_connect_count = 0;
static int g_disconnect_count = 0;
static int g_transfer_count = 0;
static int g_transfer_async_count = 0;
static int g_register_mem_count = 0;
static int g_deregister_mem_count = 0;

namespace adxl_mock {
void reset() {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_connect_result = adxl::SUCCESS;
    g_transfer_result = adxl::SUCCESS;
    g_initialize_result = adxl::SUCCESS;
    g_transfer_async_result = adxl::SUCCESS;
    g_get_transfer_status_result = adxl::SUCCESS;
    g_register_mem_result = adxl::SUCCESS;
    g_transfer_status_enum = adxl::TransferStatus::COMPLETED;
    g_transfer_results.clear();
    g_transfer_async_results.clear();
    g_registered_mem_handles.clear();
    g_deregistered_mem_handles.clear();
    g_connected.clear();
    g_was_initialize_called = false;
    g_next_handle = 1;
    g_connect_count = 0;
    g_disconnect_count = 0;
    g_transfer_count = 0;
    g_transfer_async_count = 0;
    g_register_mem_count = 0;
    g_deregister_mem_count = 0;
}

void set_connect_result(adxl::Status status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_connect_result = status;
}

void set_transfer_result(adxl::Status status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_transfer_result = status;
    g_transfer_results.clear();
}

void set_transfer_sequence(const std::vector<adxl::Status>& statuses) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_transfer_results.assign(statuses.begin(), statuses.end());
}

void set_transfer_async_result(adxl::Status status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_transfer_async_result = status;
    g_transfer_async_results.clear();
}

void set_transfer_async_sequence(const std::vector<adxl::Status>& statuses) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_transfer_async_results.assign(statuses.begin(), statuses.end());
}

void set_get_transfer_status_result(adxl::Status status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_get_transfer_status_result = status;
}

void set_transfer_status_enum(adxl::TransferStatus status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_transfer_status_enum = status;
}

void set_initialize_result(adxl::Status status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_initialize_result = status;
}

void set_register_mem_result(adxl::Status status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_register_mem_result = status;
}

bool was_initialize_called() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_was_initialize_called;
}

int get_transfer_count() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_transfer_count;
}

int get_transfer_async_count() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_transfer_async_count;
}

int get_connect_count() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_connect_count;
}

int get_disconnect_count() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_disconnect_count;
}

int get_register_mem_count() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_register_mem_count;
}

int get_deregister_mem_count() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_deregister_mem_count;
}

std::vector<uintptr_t> get_registered_mem_handles() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_registered_mem_handles;
}

std::vector<uintptr_t> get_deregistered_mem_handles() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_deregistered_mem_handles;
}
}  // namespace adxl_mock

}  // namespace

// Override ADXL methods with strong symbols
namespace adxl {

class AdxlEngine::AdxlEngineImpl {};

AdxlEngine::AdxlEngine() = default;
AdxlEngine::~AdxlEngine() = default;

Status AdxlEngine::Initialize(
    const AscendString& name,
    const std::map<AscendString, AscendString>& options) {
    (void)name;
    (void)options;
    g_was_initialize_called = true;
    return g_initialize_result;
}

void AdxlEngine::Finalize() {}

Status AdxlEngine::Connect(const AscendString& remote_engine,
                           int32_t timeout_in_millis) {
    (void)timeout_in_millis;
    std::lock_guard<std::mutex> lock(g_mutex);
    g_connected.insert(std::string(remote_engine.GetString()));
    g_connect_count++;
    return g_connect_result;
}

Status AdxlEngine::Disconnect(const AscendString& remote_engine,
                              int32_t timeout_in_millis) {
    (void)timeout_in_millis;
    std::lock_guard<std::mutex> lock(g_mutex);
    g_connected.erase(std::string(remote_engine.GetString()));
    g_disconnect_count++;
    return SUCCESS;
}

Status AdxlEngine::TransferSync(const AscendString& remote_engine,
                                TransferOp operation,
                                const std::vector<TransferOpDesc>& op_descs,
                                int32_t timeout_in_millis) {
    (void)remote_engine;
    (void)timeout_in_millis;

    adxl::Status result;
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        g_transfer_count++;
        if (!g_transfer_results.empty()) {
            result = g_transfer_results.front();
            g_transfer_results.pop_front();
        } else {
            result = g_transfer_result;
        }
    }

    (void)operation;
    (void)op_descs;

    return result;
}

Status AdxlEngine::TransferAsync(const AscendString& remote_engine,
                                 TransferOp operation,
                                 const std::vector<TransferOpDesc>& op_descs,
                                 const TransferArgs& optional_args,
                                 TransferReq& req) {
    (void)remote_engine;
    (void)operation;
    (void)op_descs;
    (void)optional_args;

    adxl::Status result;
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        g_transfer_async_count++;
        if (!g_transfer_async_results.empty()) {
            result = g_transfer_async_results.front();
            g_transfer_async_results.pop_front();
        } else {
            result = g_transfer_async_result;
        }
        req = reinterpret_cast<TransferReq>(
            static_cast<uintptr_t>(g_next_handle++));
    }
    return result;
}

Status AdxlEngine::GetTransferStatus(const TransferReq& req,
                                     TransferStatus& status) {
    (void)req;

    adxl::Status result;
    adxl::TransferStatus status_val;
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        result = g_get_transfer_status_result;
        status_val = g_transfer_status_enum;
    }
    if (result == adxl::SUCCESS) {
        status = status_val;
    }
    return result;
}

Status AdxlEngine::RegisterMem(const MemDesc& mem, MemType type,
                               MemHandle& mem_handle) {
    (void)mem;
    (void)type;
    std::lock_guard<std::mutex> lock(g_mutex);
    g_register_mem_count++;
    if (g_register_mem_result != adxl::SUCCESS) {
        return g_register_mem_result;
    }
    mem_handle =
        reinterpret_cast<MemHandle>(static_cast<uintptr_t>(g_next_handle++));
    g_registered_mem_handles.push_back(reinterpret_cast<uintptr_t>(mem_handle));
    return SUCCESS;
}

Status AdxlEngine::DeregisterMem(MemHandle mem_handle) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_deregister_mem_count++;
    g_deregistered_mem_handles.push_back(
        reinterpret_cast<uintptr_t>(mem_handle));
    return SUCCESS;
}

}  // namespace adxl

class AscendDirectTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        mock_acl::reset();
        adxl_mock::reset();

        mock_acl::set_pointer_location(test_buffer_src_,
                                       ACL_MEM_LOCATION_TYPE_HOST, 0);
        mock_acl::set_pointer_location(test_buffer_dst_,
                                       ACL_MEM_LOCATION_TYPE_HOST, 0);

        // Initialize ContextManager so ascend transport install can succeed
        // when ascend_agent_mode or RoCE mode is used
        ASSERT_TRUE(ContextManager::getInstance().initialize())
            << "ContextManager must be initialized for ascend transport tests";

        // Initialize glog and set log output to stderr for visibility in docker
        google::InitGoogleLogging("AscendDirectTransportTest");
        FLAGS_minloglevel = 0;       // INFO level
        FLAGS_logtostderr = 1;       // Output to stderr instead of file
        FLAGS_colorlogtostderr = 1;  // Enable color output
    }

    void TearDown() override {
        ContextManager::getInstance().finalize();
        google::ShutdownGoogleLogging();
    }

    std::unique_ptr<AscendDirectTransport> createTransport(
        bool use_async = false) {
        if (use_async) {
            setenv("ASCEND_USE_ASYNC_TRANSFER", "1", 1);
        } else {
            unsetenv("ASCEND_USE_ASYNC_TRANSFER");
        }
        auto transport = std::make_unique<AscendDirectTransport>();
        std::string local_server_name = "127.0.0.1:20000";
        auto metadata = std::make_shared<TransferMetadata>("P2PHANDSHAKE");
        auto topology = std::make_shared<Topology>();

        if (transport->install(local_server_name, metadata, topology) != 0) {
            return nullptr;
        }
        return transport;
    }

    struct RemoteSegmentSetup {
        int segment_id = 1;
        std::string name = "remote_server";
        std::string host_ip = "192.168.1.100";
        int port = 30000;
    };

    void addRemoteSegment(std::shared_ptr<TransferMetadata> meta,
                          const RemoteSegmentSetup& s) {
        auto remote_desc = std::make_shared<TransferMetadata::SegmentDesc>();
        remote_desc->name = s.name;
        remote_desc->protocol = "ascend";
        remote_desc->rank_info.hostIp = s.host_ip;
        remote_desc->rank_info.hostPort = s.port;
        remote_desc->rank_info.endpoints.push_back(s.host_ip + ":" +
                                                   std::to_string(s.port));
        meta->addLocalSegment(s.segment_id, s.name, std::move(remote_desc));
    }

    std::shared_ptr<TransferMetadata> startRemoteMetadataServer(
        const RemoteSegmentSetup& s) {
        auto metadata = std::make_shared<TransferMetadata>("P2PHANDSHAKE");
        auto remote_desc = std::make_shared<TransferMetadata::SegmentDesc>();
        remote_desc->name = s.name;
        remote_desc->protocol = "ascend";
        remote_desc->rank_info.hostIp = s.host_ip;
        remote_desc->rank_info.hostPort = s.port;
        remote_desc->rank_info.endpoints.push_back(s.name);
        metadata->addLocalSegment(LOCAL_SEGMENT_ID, s.name,
                                  std::move(remote_desc));

        TransferMetadata::RpcMetaDesc rpc_desc;
        rpc_desc.ip_or_host_name = s.host_ip;
        rpc_desc.rpc_port = static_cast<uint16_t>(s.port);
        rpc_desc.sockfd = -1;
        EXPECT_EQ(metadata->addRpcMetaEntry(s.name, rpc_desc), 0);
        return metadata;
    }

    struct TransferWaitResult {
        bool finished;
        bool failed;
        Transport::TransferStatus status;
    };

    TransferWaitResult runLocalCopy(AscendDirectTransport* transport, void* src,
                                    void* dst, size_t len) {
        auto batch_id = transport->allocateBatchID(1);
        if (batch_id == 0) {
            return {false, false, {}};
        }
        std::vector<Transport::TransferRequest> requests;
        Transport::TransferRequest req;
        req.opcode = Transport::TransferRequest::WRITE;
        req.source = src;
        req.target_id = 0;
        req.target_offset = reinterpret_cast<uint64_t>(dst);
        req.length = len;
        requests.push_back(req);
        Status s = transport->submitTransfer(batch_id, requests);
        if (!s.ok()) {
            transport->freeBatchID(batch_id);
            return {false, false, {}};
        }
        auto result = waitForTransfer(transport, batch_id);
        transport->freeBatchID(batch_id);
        return result;
    }

    TransferWaitResult runRemoteTransfer(AscendDirectTransport* transport,
                                         void* src, int target_id,
                                         uint64_t target_offset, size_t len) {
        auto batch_id = transport->allocateBatchID(1);
        if (batch_id == 0) {
            return {false, false, {}};
        }
        std::vector<Transport::TransferRequest> requests;
        Transport::TransferRequest req;
        req.opcode = Transport::TransferRequest::WRITE;
        req.source = src;
        req.target_id = target_id;
        req.target_offset = target_offset;
        req.length = len;
        requests.push_back(req);
        Status s = transport->submitTransfer(batch_id, requests);
        if (!s.ok()) {
            transport->freeBatchID(batch_id);
            return {false, false, {}};
        }
        auto result = waitForTransfer(transport, batch_id);
        transport->freeBatchID(batch_id);
        return result;
    }

    void initTestData(size_t size) {
        for (size_t i = 0; i < size; ++i) {
            test_buffer_src_[i] = static_cast<char>(i % 256);
            test_buffer_dst_[i] = 0;
        }
    }

    bool verifyTestData(size_t size) {
        for (size_t i = 0; i < size; ++i) {
            if (test_buffer_dst_[i] != static_cast<char>(i % 256)) {
                return false;
            }
        }
        return true;
    }

    TransferWaitResult waitForTransfer(AscendDirectTransport* transport,
                                       uint64_t batch_id) {
        Transport::TransferStatus status{};
        for (int i = 0; i < kTransferStatusMaxRetries; ++i) {
            Status s = transport->getTransferStatus(batch_id, 0, status);
            if (!s.ok()) {
                ADD_FAILURE() << "getTransferStatus failed";
                return {false, false, status};
            }
            if (status.s == Transport::FAILED) {
                return {true, true, status};
            }
            if (status.s == Transport::COMPLETED) {
                return {true, false, status};
            }
            usleep(1000);
        }
        return {false, false, status};
    }

    size_t getLocalBufferCount(AscendDirectTransport* transport) {
        auto segment_desc =
            transport->meta()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!segment_desc) {
            return 0;
        }
        return segment_desc->buffers.size();
    }

    alignas(4096) char test_buffer_src_[16 * 1024 * 1024];
    alignas(4096) char test_buffer_dst_[16 * 1024 * 1024];
};

// -----------------------------------------------------------------------------
// Basic API tests
// -----------------------------------------------------------------------------

TEST_F(AscendDirectTransportTest, Basic_ConstructorAndName) {
    auto transport = std::make_unique<AscendDirectTransport>();
    ASSERT_NE(transport, nullptr);
    EXPECT_STREQ(transport->getName(), "ascend_direct");
}

TEST_F(AscendDirectTransportTest, Basic_InstallSuccess) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    EXPECT_TRUE(adxl_mock::was_initialize_called());
}

TEST_F(AscendDirectTransportTest, Basic_InstallFailsWhenAdxlInitializeFails) {
    adxl_mock::set_initialize_result(adxl::FAILED);

    auto transport = std::make_unique<AscendDirectTransport>();
    std::string local_server_name = "127.0.0.1:20000";
    auto metadata = std::make_shared<TransferMetadata>("P2PHANDSHAKE");
    auto topology = std::make_shared<Topology>();

    int ret = transport->install(local_server_name, metadata, topology);
    EXPECT_NE(ret, 0);
}

TEST_F(AscendDirectTransportTest,
       ContextManager_InitializePreservesCallerContext) {
    ContextManager::getInstance().finalize();
    constexpr int kDeviceCount = 4;
    constexpr int kCallerDeviceId = 1;
    auto* expected_context = reinterpret_cast<aclrtContext>(0x4321);

    mock_acl::set_device_count(kDeviceCount);
    ASSERT_EQ(aclrtSetDevice(kCallerDeviceId), ACL_ERROR_NONE);
    ASSERT_EQ(aclrtSetCurrentContext(expected_context), ACL_ERROR_NONE);

    ASSERT_TRUE(ContextManager::getInstance().initialize());

    int current_device_id = -1;
    ASSERT_EQ(aclrtGetDevice(&current_device_id), ACL_ERROR_NONE);
    EXPECT_EQ(current_device_id, kCallerDeviceId);

    aclrtContext current_context = nullptr;
    ASSERT_EQ(aclrtGetCurrentContext(&current_context), ACL_ERROR_NONE);
    EXPECT_EQ(current_context, expected_context);
}

TEST_F(AscendDirectTransportTest,
       ContextManager_SetCurrentContextByPhysicalId) {
    ContextManager::getInstance().finalize();
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    ASSERT_TRUE(ContextManager::getInstance().initialize());
    EXPECT_TRUE(ContextManager::getInstance().setCurrentContextByPhysicalId(2));
    EXPECT_FALSE(
        ContextManager::getInstance().setCurrentContextByPhysicalId(99));
}

// -----------------------------------------------------------------------------
// Install tests (dummy real mode, RoCE)
// -----------------------------------------------------------------------------

TEST_F(AscendDirectTransportTest, Install_DummyRealModeSkipsGetDevice) {
    globalConfig().ascend_agent_mode = true;
    const int get_device_calls_before = mock_acl::get_get_device_call_count();
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    // allocateLocalSegmentID uses ContextManager instead of aclrtGetDevice.
    // With ContextManager pre-initialized in SetUp, transport uses it, so
    // install should not introduce extra aclrtGetDevice calls.
    EXPECT_EQ(mock_acl::get_get_device_call_count(), get_device_calls_before);
    globalConfig().ascend_agent_mode = false;
}

TEST_F(AscendDirectTransportTest, Install_DummyRealRocePublishes16Endpoints) {
    globalConfig().ascend_agent_mode = true;
    ContextManager::getInstance().finalize();
    mock_acl::set_device_count(kDummyRealRoceEngineCount);
    ASSERT_TRUE(ContextManager::getInstance().initialize())
        << "Re-init ContextManager with 16 devices for RoCE test";
    setenv("HCCL_INTRA_ROCE_ENABLE", "1", 1);
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    auto metadata = transport->meta();
    ASSERT_NE(metadata, nullptr);
    auto local_desc = metadata->getSegmentDescByID(0);
    ASSERT_NE(local_desc, nullptr);
    EXPECT_EQ(local_desc->rank_info.endpoints.size(),
              kDummyRealRoceEngineCount);
    EXPECT_TRUE(mock_acl::all_devices_covered(0, kDummyRealRoceEngineCount));
    EXPECT_GE(mock_acl::get_set_device_call_count(), kDummyRealRoceEngineCount);
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    globalConfig().ascend_agent_mode = false;
}

// dummy-real + RoCE: per-engine thread dispatcher, each ADXL engine has
// dedicated thread for data transfer
TEST_F(AscendDirectTransportTest,
       Install_DummyRealModeWithRoce_PerEngineThread) {
    globalConfig().ascend_agent_mode = true;
    ContextManager::getInstance().finalize();
    constexpr int kEngineCount = 4;
    mock_acl::set_device_count(kEngineCount);
    ASSERT_TRUE(ContextManager::getInstance().initialize())
        << "Re-init ContextManager for per-engine thread test";
    setenv("HCCL_INTRA_ROCE_ENABLE", "1", 1);
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    auto metadata = transport->meta();
    ASSERT_NE(metadata, nullptr);
    auto local_desc = metadata->getSegmentDescByID(0);
    ASSERT_NE(local_desc, nullptr);
    EXPECT_EQ(local_desc->rank_info.endpoints.size(),
              static_cast<size_t>(kEngineCount))
        << "dummy-real+RoCE uses per-engine threads, one endpoint per engine";
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    globalConfig().ascend_agent_mode = false;
}

// Dummy real mode without RoCE: single engine endpoint, still skips
// aclrtGetDevice
TEST_F(AscendDirectTransportTest,
       Install_DummyRealModeWithoutRoce_SingleEndpoint) {
    globalConfig().ascend_agent_mode = true;
    mock_acl::set_device_count(1);
    unsetenv("HCCL_INTRA_ROCE_ENABLE");  // Ensure non-RoCE path
    const int get_device_calls_before = mock_acl::get_get_device_call_count();
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    // allocateLocalSegmentID uses ContextManager instead of aclrtGetDevice.
    EXPECT_EQ(mock_acl::get_get_device_call_count(), get_device_calls_before);
    auto metadata = transport->meta();
    ASSERT_NE(metadata, nullptr);
    auto local_desc = metadata->getSegmentDescByID(0);
    ASSERT_NE(local_desc, nullptr);
    EXPECT_EQ(local_desc->rank_info.endpoints.size(), 1u)
        << "dummy real without RoCE should publish single endpoint";
    globalConfig().ascend_agent_mode = false;
}

// -----------------------------------------------------------------------------
// Dummy-real flow tests (dummy registers memory, forwards to real, real
// executes transfer)
// -----------------------------------------------------------------------------

TEST_F(AscendDirectTransportTest, DummyReal_LocalCopy_Success) {
    globalConfig().ascend_agent_mode = true;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    auto result = runLocalCopy(transport.get(), test_buffer_src_,
                               test_buffer_dst_, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed)
        << "Local copy in dummy-real mode should succeed";
    EXPECT_TRUE(verifyTestData(kTransferBufSize));

    globalConfig().ascend_agent_mode = false;
}

TEST_F(AscendDirectTransportTest, DummyReal_RemoteTransfer_Success) {
    globalConfig().ascend_agent_mode = true;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.200", 40000});

    initTestData(kTransferBufSize);

    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed)
        << "Remote transfer in dummy-real mode should succeed";
    EXPECT_GT(adxl_mock::get_transfer_count(), 0)
        << "ADXL transfer should have been called";

    globalConfig().ascend_agent_mode = false;
}

TEST_F(AscendDirectTransportTest, DummyReal_RemoteTransfer_Async_Success) {
    adxl_mock::set_transfer_async_result(adxl::SUCCESS);
    adxl_mock::set_get_transfer_status_result(adxl::SUCCESS);
    adxl_mock::set_transfer_status_enum(adxl::TransferStatus::COMPLETED);

    globalConfig().ascend_agent_mode = true;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    auto transport = createTransport(true);
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.200", 40000});
    initTestData(kTransferBufSize);

    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed)
        << "Async remote transfer in dummy-real mode should succeed";
    EXPECT_GT(adxl_mock::get_transfer_async_count(), 0)
        << "ADXL TransferAsync should have been called";

    globalConfig().ascend_agent_mode = false;
}

TEST_F(AscendDirectTransportTest, DummyReal_SubmitTransfer_UsesDeviceId) {
    globalConfig().ascend_agent_mode = true;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    initTestData(kTransferBufSize);

    auto result = runLocalCopy(transport.get(), test_buffer_src_,
                               test_buffer_dst_, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_GE(mock_acl::get_get_device_call_count(), 1)
        << "submitTransfer in dummy-real mode must call aclrtGetDevice to get "
           "current device_id for slice dispatch";

    globalConfig().ascend_agent_mode = false;
}

TEST_F(AscendDirectTransportTest, DummyReal_Memory_RegisterAndUnregister) {
    globalConfig().ascend_agent_mode = true;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    EXPECT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    EXPECT_EQ(transport->unregisterLocalMemory(test_buffer_src_, true), 0);

    globalConfig().ascend_agent_mode = false;
}

TEST_F(AscendDirectTransportTest, DummyReal_Roce_LocalCopy_Success) {
    globalConfig().ascend_agent_mode = true;
    ContextManager::getInstance().finalize();
    constexpr int kEngineCount = 4;
    mock_acl::set_device_count(kEngineCount);
    ASSERT_TRUE(ContextManager::getInstance().initialize())
        << "Re-init ContextManager for dummy-real RoCE local copy test";
    setenv("HCCL_INTRA_ROCE_ENABLE", "1", 1);
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    auto result = runLocalCopy(transport.get(), test_buffer_src_,
                               test_buffer_dst_, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed)
        << "Local copy in dummy-real+RoCE mode (device 0) should succeed";
    EXPECT_TRUE(verifyTestData(kTransferBufSize));

    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    globalConfig().ascend_agent_mode = false;
}

// -----------------------------------------------------------------------------
// Memory registration tests
// -----------------------------------------------------------------------------

TEST_F(AscendDirectTransportTest, Memory_RegisterAndUnregister) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    EXPECT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    EXPECT_EQ(transport->unregisterLocalMemory(test_buffer_src_, true), 0);
}

TEST_F(AscendDirectTransportTest, Memory_RegisterFailureRollsBackMetadata) {
    adxl_mock::set_register_mem_result(adxl::FAILED);
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    const size_t buffer_count_before = getLocalBufferCount(transport.get());
    EXPECT_NE(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    EXPECT_EQ(getLocalBufferCount(transport.get()), buffer_count_before);
}

TEST_F(AscendDirectTransportTest, Memory_RegisterWithWildcardLocation) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    mock_acl::set_pointer_location(test_buffer_src_, ACL_MEM_LOCATION_TYPE_HOST,
                                   0);
    EXPECT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "*", true, true),
              0);
    EXPECT_EQ(transport->unregisterLocalMemory(test_buffer_src_, true), 0);
}

TEST_F(AscendDirectTransportTest, Memory_RegisterWithUnsupportedLocation) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    EXPECT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "invalid_location", true, true),
              -1);
}

TEST_F(AscendDirectTransportTest, Memory_RegisterWithDeviceLocation) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    mock_acl::set_pointer_location(test_buffer_src_,
                                   ACL_MEM_LOCATION_TYPE_DEVICE, 0);

    EXPECT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "*", true, true),
              0);
    EXPECT_EQ(transport->unregisterLocalMemory(test_buffer_src_, true), 0);
}

TEST_F(AscendDirectTransportTest, Memory_registerLocalMemoryBatch_Success) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    std::vector<Transport::BufferEntry> buffers = {
        {test_buffer_src_, kRegisterMemSize},
        {test_buffer_dst_, kRegisterMemSize},
    };
    EXPECT_EQ(transport->registerLocalMemoryBatch(buffers, "cpu:0"), 0);

    std::vector<void*> addrs = {test_buffer_src_, test_buffer_dst_};
    EXPECT_EQ(transport->unregisterLocalMemoryBatch(addrs), 0);
}

TEST_F(AscendDirectTransportTest, Memory_unregisterLocalMemoryBatch_Success) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    std::vector<Transport::BufferEntry> buffers = {
        {test_buffer_src_, kRegisterMemSize},
    };
    ASSERT_EQ(transport->registerLocalMemoryBatch(buffers, "cpu:0"), 0);

    std::vector<void*> addrs = {test_buffer_src_};
    EXPECT_EQ(transport->unregisterLocalMemoryBatch(addrs), 0);
}

TEST_F(AscendDirectTransportTest,
       DummyReal_Roce_RegisterAndUnregisterUsePerEngineHandles) {
    globalConfig().ascend_agent_mode = true;
    ContextManager::getInstance().finalize();
    constexpr int kEngineCount = 4;
    mock_acl::set_device_count(kEngineCount);
    ASSERT_TRUE(ContextManager::getInstance().initialize())
        << "Re-init ContextManager for RoCE memory handle test";
    setenv("HCCL_INTRA_ROCE_ENABLE", "1", 1);

    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    void* store_buffer = ascend_allocate_memory(kRegisterMemSize, "ascend");
    ASSERT_NE(store_buffer, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(store_buffer, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    EXPECT_EQ(adxl_mock::get_register_mem_count(), kEngineCount);

    ASSERT_EQ(transport->unregisterLocalMemory(store_buffer, true), 0);
    EXPECT_EQ(adxl_mock::get_deregister_mem_count(), kEngineCount);

    auto registered_handles = adxl_mock::get_registered_mem_handles();
    auto deregistered_handles = adxl_mock::get_deregistered_mem_handles();
    std::sort(registered_handles.begin(), registered_handles.end());
    std::sort(deregistered_handles.begin(), deregistered_handles.end());
    EXPECT_EQ(deregistered_handles, registered_handles);

    ascend_free_memory("ascend", store_buffer);
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    globalConfig().ascend_agent_mode = false;
}

// -----------------------------------------------------------------------------
// Batch tests
// -----------------------------------------------------------------------------

TEST_F(AscendDirectTransportTest, Batch_ExceedsCapacity) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    for (int i = 0; i < 2; ++i) {
        Transport::TransferRequest req;
        req.opcode = Transport::TransferRequest::WRITE;
        req.source = test_buffer_src_;
        req.target_id = 0;
        req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
        req.length = kTransferBufSize;
        requests.push_back(req);
    }

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_FALSE(s.ok());

    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, Batch_allocateBatchID_freeBatchID_Success) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0u);

    Status s = transport->freeBatchID(batch_id);
    EXPECT_TRUE(s.ok());
}

// -----------------------------------------------------------------------------
// Edge tests
// -----------------------------------------------------------------------------

TEST_F(AscendDirectTransportTest,
       Edge_getTransferStatus_InvalidTaskId_ReturnsError) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    initTestData(kTransferBufSize);
    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0u);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 0;
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = kTransferBufSize;
    requests.push_back(req);
    ASSERT_TRUE(transport->submitTransfer(batch_id, requests).ok());

    Transport::TransferStatus status{};
    Status s = transport->getTransferStatus(batch_id, 1, status);
    EXPECT_FALSE(s.ok())
        << "getTransferStatus with invalid task_id should fail";

    auto result = waitForTransfer(transport.get(), batch_id);
    ASSERT_TRUE(result.finished);
    transport->freeBatchID(batch_id);
}

// -----------------------------------------------------------------------------
// Local copy tests
// -----------------------------------------------------------------------------

TEST_F(AscendDirectTransportTest, LocalCopy_Sync_aclrtMemcpyFailure) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    mock_acl::set_memcpy_result(ACL_ERROR_RT_DEVICE_TASK_ABORT);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 0;
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = kTransferBufSize;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    auto result = waitForTransfer(transport.get(), batch_id);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed)
        << "Transfer should have failed due to ACL memcpy failure";
    EXPECT_EQ(result.status.transferred_bytes, 0)
        << "Transferred bytes should be 0 on failure";
    EXPECT_FALSE(verifyTestData(kTransferBufSize))
        << "Destination should not match source on failure";

    mock_acl::set_memcpy_result(ACL_ERROR_NONE);
    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, LocalCopy_Async_aclrtMemcpyAsyncFailure) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    mock_acl::set_pointer_location(test_buffer_src_,
                                   ACL_MEM_LOCATION_TYPE_DEVICE, 0);
    mock_acl::set_pointer_location(test_buffer_dst_,
                                   ACL_MEM_LOCATION_TYPE_DEVICE, 0);

    mock_acl::set_memcpy_async_result(ACL_ERROR_RT_DEVICE_TASK_ABORT);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 0;
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = kTransferBufSize;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    auto result = waitForTransfer(transport.get(), batch_id);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed)
        << "Transfer should have failed due to ACL async memcpy failure";
    EXPECT_EQ(result.status.transferred_bytes, 0)
        << "Transferred bytes should be 0 on failure";
    EXPECT_FALSE(verifyTestData(kTransferBufSize))
        << "Destination should not match source on failure";

    mock_acl::set_memcpy_async_result(ACL_ERROR_NONE);
    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, LocalCopy_Batch_aclrtMemcpyBatchFailure) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    mock_acl::set_pointer_location(test_buffer_src_, ACL_MEM_LOCATION_TYPE_HOST,
                                   0);
    mock_acl::set_pointer_location(test_buffer_dst_,
                                   ACL_MEM_LOCATION_TYPE_DEVICE, 0);

    mock_acl::set_memcpy_batch_result(ACL_ERROR_RT_DEVICE_TASK_ABORT);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 0;
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = kTransferBufSize;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    auto result = waitForTransfer(transport.get(), batch_id);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed)
        << "Transfer should fail when batch memcpy fails";
    EXPECT_EQ(result.status.transferred_bytes, 0)
        << "Transferred bytes should be 0 on failure";
    EXPECT_FALSE(verifyTestData(kTransferBufSize))
        << "Destination should not match source on failure";

    mock_acl::set_memcpy_batch_result(ACL_ERROR_NONE);
    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest,
       LocalCopy_Batch_FeatureNotSupport_FallbackToAsync) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    mock_acl::set_pointer_location(test_buffer_src_, ACL_MEM_LOCATION_TYPE_HOST,
                                   0);
    mock_acl::set_pointer_location(test_buffer_dst_,
                                   ACL_MEM_LOCATION_TYPE_DEVICE, 0);

    mock_acl::set_memcpy_batch_result(ACL_ERROR_RT_FEATURE_NOT_SUPPORT);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 0;
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = kTransferBufSize;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    auto result = waitForTransfer(transport.get(), batch_id);
    ASSERT_TRUE(result.finished);
    ASSERT_FALSE(result.failed) << "Transfer should fallback to async path";
    EXPECT_TRUE(verifyTestData(kTransferBufSize));

    mock_acl::set_memcpy_batch_result(ACL_ERROR_NONE);
    transport->freeBatchID(batch_id);
}

// -----------------------------------------------------------------------------
// Remote transfer tests (sync and async)
// -----------------------------------------------------------------------------

TEST_F(AscendDirectTransportTest, RemoteTransfer_Sync_TransferFailure) {
    adxl_mock::set_transfer_result(adxl::FAILED);

    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.100", 30000});

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    auto result = runRemoteTransfer(
        transport.get(), test_buffer_src_, 1,
        reinterpret_cast<uint64_t>(test_buffer_dst_), kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed)
        << "Transfer should have failed due to ADXL failure";
    EXPECT_GT(adxl_mock::get_transfer_count(), 0)
        << "ADXL transfer should have been called";
    EXPECT_EQ(test_buffer_dst_[0], static_cast<char>(0))
        << "Destination buffer should not be modified on transfer failure";
}

TEST_F(AscendDirectTransportTest, RemoteTransfer_Sync_ConnectTimeout) {
    adxl_mock::set_connect_result(adxl::TIMEOUT);

    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.100", 30000});

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    auto result = runRemoteTransfer(
        transport.get(), test_buffer_src_, 1,
        reinterpret_cast<uint64_t>(test_buffer_dst_), kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed)
        << "Transfer should have failed due to connection timeout";
    EXPECT_EQ(result.status.transferred_bytes, 0)
        << "Transferred bytes should be 0 on connection failure";
}

TEST_F(AscendDirectTransportTest, RemoteTransfer_Sync_Success) {
    adxl_mock::set_transfer_result(adxl::SUCCESS);

    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.200", 40000});

    initTestData(kTransferBufSize);

    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed) << "Remote transfer should not fail";
    EXPECT_GT(adxl_mock::get_transfer_count(), 0)
        << "ADXL transfer should have been called";
}

TEST_F(AscendDirectTransportTest,
       RemoteTransfer_Sync_RetryAfterMetadataRefresh_Success) {
    constexpr int kRemotePort = 43000;
    const RemoteSegmentSetup remote_setup = {1, "127.0.0.1:43000", "127.0.0.1",
                                             kRemotePort};
    auto remote_metadata = startRemoteMetadataServer(remote_setup);
    ASSERT_NE(remote_metadata, nullptr);

    adxl_mock::set_transfer_sequence({adxl::FAILED, adxl::SUCCESS});

    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    addRemoteSegment(transport->meta(), remote_setup);

    initTestData(kTransferBufSize);

    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed)
        << "Retry after metadata refresh should succeed";
    EXPECT_EQ(adxl_mock::get_transfer_count(), 2);
    EXPECT_EQ(adxl_mock::get_connect_count(), 2);
    EXPECT_EQ(adxl_mock::get_disconnect_count(), 1);
}

TEST_F(AscendDirectTransportTest, RemoteTransfer_Async_Success) {
    adxl_mock::set_transfer_async_result(adxl::SUCCESS);
    adxl_mock::set_get_transfer_status_result(adxl::SUCCESS);
    adxl_mock::set_transfer_status_enum(adxl::TransferStatus::COMPLETED);

    auto transport = createTransport(true);
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.200", 40000});

    initTestData(kTransferBufSize);

    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed) << "Async remote transfer should not fail";
    EXPECT_GT(adxl_mock::get_transfer_async_count(), 0)
        << "ADXL TransferAsync should have been called";
}

TEST_F(AscendDirectTransportTest,
       RemoteTransfer_Async_RetryAfterMetadataRefresh_Success) {
    constexpr int kRemotePort = 43001;
    const RemoteSegmentSetup remote_setup = {1, "127.0.0.1:43001", "127.0.0.1",
                                             kRemotePort};
    auto remote_metadata = startRemoteMetadataServer(remote_setup);
    ASSERT_NE(remote_metadata, nullptr);

    adxl_mock::set_transfer_async_sequence({adxl::FAILED, adxl::SUCCESS});
    adxl_mock::set_get_transfer_status_result(adxl::SUCCESS);
    adxl_mock::set_transfer_status_enum(adxl::TransferStatus::COMPLETED);

    auto transport = createTransport(true);
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    addRemoteSegment(transport->meta(), remote_setup);

    initTestData(kTransferBufSize);

    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed) << "Async retry after metadata refresh should "
                                   "succeed";
    EXPECT_EQ(adxl_mock::get_transfer_async_count(), 2);
    EXPECT_EQ(adxl_mock::get_connect_count(), 2);
    EXPECT_EQ(adxl_mock::get_disconnect_count(), 1);
}

TEST_F(AscendDirectTransportTest, RemoteTransfer_Async_TransferAsyncFailure) {
    adxl_mock::set_transfer_async_result(adxl::FAILED);

    auto transport = createTransport(true);
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.100", 30000});

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    auto result = runRemoteTransfer(
        transport.get(), test_buffer_src_, 1,
        reinterpret_cast<uint64_t>(test_buffer_dst_), kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed)
        << "Transfer should have failed due to TransferAsync failure";
    EXPECT_EQ(test_buffer_dst_[0], static_cast<char>(0))
        << "Destination buffer should not be modified on transfer failure";
}

TEST_F(AscendDirectTransportTest,
       RemoteTransfer_Async_GetTransferStatusFailure) {
    adxl_mock::set_transfer_async_result(adxl::SUCCESS);
    adxl_mock::set_get_transfer_status_result(adxl::FAILED);

    auto transport = createTransport(true);
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.100", 30000});

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    auto result = runRemoteTransfer(
        transport.get(), test_buffer_src_, 1,
        reinterpret_cast<uint64_t>(test_buffer_dst_), kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed)
        << "Transfer should have failed due to GetTransferStatus failure";
}

TEST_F(AscendDirectTransportTest, RemoteTransfer_Async_TransferTimeout) {
    setenv("ASCEND_TRANSFER_TIMEOUT", "100", 1);
    adxl_mock::set_transfer_async_result(adxl::SUCCESS);
    adxl_mock::set_get_transfer_status_result(adxl::SUCCESS);
    adxl_mock::set_transfer_status_enum(adxl::TransferStatus::WAITING);

    auto transport = createTransport(true);
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.100", 30000});

    initTestData(kTransferBufSize);
    memset(test_buffer_dst_, 0, kTransferBufSize);

    auto result = runRemoteTransfer(
        transport.get(), test_buffer_src_, 1,
        reinterpret_cast<uint64_t>(test_buffer_dst_), kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed) << "Transfer should have failed due to timeout "
                                  "(GetTransferStatus stays WAITING)";
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
