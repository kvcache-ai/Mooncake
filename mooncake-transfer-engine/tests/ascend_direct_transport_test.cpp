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
#include <cstdint>
#include <cstdlib>
#include "transport/ascend_transport/ascend_direct_transport/ascend_direct_transport.h"
#include "transport/ascend_transport/ascend_direct_transport/context_manager.h"
#include "transport/ascend_transport/ascend_direct_transport/utils.h"
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

// ACL device/context are per-thread; keep the mock that way so engine worker
// threads do not clobber the test thread's current device.
static thread_local int g_device_id = 0;
static int g_device_count = 1;
static thread_local aclrtContext g_context =
    reinterpret_cast<aclrtContext>(0x10000);
static aclError g_memcpy_result = ACL_ERROR_NONE;
static aclError g_memcpy_async_result = ACL_ERROR_NONE;
static aclError g_memcpy_batch_result = ACL_ERROR_NONE;
static int g_get_device_call_count = 0;
static int g_set_device_call_count = 0;
static std::set<int> g_set_device_ids;
static std::map<const void*, aclrtMemLocation> g_memory_locations;
static std::mutex g_acl_mutex;
// 0 = no size limit; otherwise aclrtMallocPhysical fails when size > threshold.
static size_t g_malloc_physical_max_success = 0;
static int g_malloc_physical_call_count = 0;

constexpr uintptr_t kDeviceContextBase = 0x10000;

aclrtContext ContextForDevice(int device_id) {
    return reinterpret_cast<aclrtContext>(kDeviceContextBase +
                                          static_cast<uintptr_t>(device_id));
}

int DeviceForContext(aclrtContext context) {
    const auto value = reinterpret_cast<uintptr_t>(context);
    if (value >= kDeviceContextBase && value < kDeviceContextBase + 1024) {
        return static_cast<int>(value - kDeviceContextBase);
    }
    return -1;
}

namespace mock_acl {
void reset() {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    g_device_id = 0;
    g_device_count = 1;
    g_context = ContextForDevice(0);
    g_memcpy_result = ACL_ERROR_NONE;
    g_memcpy_async_result = ACL_ERROR_NONE;
    g_memcpy_batch_result = ACL_ERROR_NONE;
    g_get_device_call_count = 0;
    g_set_device_call_count = 0;
    g_set_device_ids.clear();
    g_memory_locations.clear();
    g_malloc_physical_max_success = 0;
    g_malloc_physical_call_count = 0;
}

void set_malloc_physical_max_success(size_t max_success) {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    g_malloc_physical_max_success = max_success;
}

int malloc_physical_call_count() {
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    return g_malloc_physical_call_count;
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
    g_context = ContextForDevice(deviceId);
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
    const int device_id = DeviceForContext(context);
    if (device_id >= 0) {
        g_device_id = device_id;
    }
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
    (void)prop;
    (void)flags;
    std::lock_guard<std::mutex> lock(g_acl_mutex);
    ++g_malloc_physical_call_count;
    if (g_malloc_physical_max_success > 0 &&
        size > g_malloc_physical_max_success) {
        return ACL_ERROR_FAILURE;
    }
    *handle = reinterpret_cast<aclrtDrvMemHandle>(malloc(1));
    return *handle ? ACL_ERROR_NONE : ACL_ERROR_FAILURE;
}

aclError aclrtReserveMemAddress(void** va, size_t size, size_t alignment,
                                void* hint_addr, uint32_t page_type) {
    (void)alignment;
    (void)hint_addr;
    (void)page_type;
    // Stub VA only — do not malloc(size) (best-effort tests use multi-GB
    // sizes).
    (void)size;
    constexpr size_t kStubVaBytes = 64;
    *va = malloc(kStubVaBytes);
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
static std::string g_last_connect_target;
static std::vector<std::string> g_connect_targets;
static adxl::Status g_get_capability_result = adxl::SUCCESS;
static int32_t g_get_capability_value = 0;
static std::map<std::string, std::string> g_last_init_options;

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
    g_last_connect_target.clear();
    g_connect_targets.clear();
    g_get_capability_result = adxl::SUCCESS;
    g_get_capability_value = 0;
    g_last_init_options.clear();
}

void set_capability_result(adxl::Status status, int32_t value) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_get_capability_result = status;
    g_get_capability_value = value;
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

std::string get_last_connect_target() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_last_connect_target;
}

std::vector<std::string> get_connect_targets() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_connect_targets;
}

std::map<std::string, std::string> get_last_init_options() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_last_init_options;
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
    g_was_initialize_called = true;
    g_last_init_options.clear();
    for (const auto& kv : options) {
        g_last_init_options[std::string(kv.first.GetString())] =
            std::string(kv.second.GetString());
    }
    return g_initialize_result;
}

void AdxlEngine::Finalize() {}

Status AdxlEngine::Connect(const AscendString& remote_engine,
                           int32_t timeout_in_millis) {
    (void)timeout_in_millis;
    std::lock_guard<std::mutex> lock(g_mutex);
    g_connected.insert(std::string(remote_engine.GetString()));
    g_last_connect_target = remote_engine.GetString();
    g_connect_targets.push_back(g_last_connect_target);
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

Status AdxlEngine::GetCapability(FeatureType feature_type, int32_t& value) {
    (void)feature_type;
    std::lock_guard<std::mutex> lock(g_mutex);
    value = g_get_capability_value;
    return g_get_capability_result;
}

}  // namespace adxl

class AscendDirectTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Unit tests mock explicit Connect(); disable auto-connect by default.
        setenv("ASCEND_AUTO_CONNECT", "0", 1);
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
        unsetenv("ASCEND_AUTO_CONNECT");
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

    void addMultiEndpointRemoteSegment(
        std::shared_ptr<TransferMetadata> meta, int segment_id,
        const std::string& name, const std::string& host_ip,
        const std::vector<std::string>& endpoints, int32_t dest_device_id = -1,
        uint64_t dest_addr = 0, uint64_t dest_len = 0) {
        auto remote_desc = std::make_shared<TransferMetadata::SegmentDesc>();
        remote_desc->name = name;
        remote_desc->protocol = "ascend";
        remote_desc->rank_info.hostIp = host_ip;
        remote_desc->rank_info.endpoints = endpoints;
        if (dest_device_id >= 0 && dest_len > 0) {
            TransferMetadata::BufferDesc buffer;
            buffer.name = "store";
            buffer.addr = dest_addr;
            buffer.length = dest_len;
            buffer.device_id = dest_device_id;
            remote_desc->buffers.push_back(buffer);
        }
        meta->addLocalSegment(segment_id, name, std::move(remote_desc));
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
                                       uint64_t batch_id,
                                       size_t task_count = 1) {
        Transport::TransferStatus status{};
        for (size_t task_id = 0; task_id < task_count; ++task_id) {
            bool task_done = false;
            for (int i = 0; i < kTransferStatusMaxRetries; ++i) {
                Status s =
                    transport->getTransferStatus(batch_id, task_id, status);
                if (!s.ok()) {
                    ADD_FAILURE()
                        << "getTransferStatus failed for task " << task_id;
                    return {false, false, status};
                }
                if (status.s == Transport::FAILED) {
                    return {true, true, status};
                }
                if (status.s == Transport::COMPLETED) {
                    task_done = true;
                    break;
                }
                usleep(1000);
            }
            if (!task_done) {
                return {false, false, status};
            }
        }
        return {true, false, status};
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

    mock_acl::set_device_count(kDeviceCount);
    ASSERT_EQ(aclrtSetDevice(kCallerDeviceId), ACL_ERROR_NONE);

    ASSERT_TRUE(ContextManager::getInstance().initialize());

    int current_device_id = -1;
    ASSERT_EQ(aclrtGetDevice(&current_device_id), ACL_ERROR_NONE);
    EXPECT_EQ(current_device_id, kCallerDeviceId);
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

TEST_F(AscendDirectTransportTest,
       DummyReal_Roce_SameHostSelectsDestBufferEngine) {
    globalConfig().ascend_agent_mode = true;
    setenv("HCCL_INTRA_ROCE_ENABLE", "1", 1);
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    g_device_id = 1;
    ContextManager::getInstance().finalize();
    ASSERT_TRUE(ContextManager::getInstance().initialize());
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    std::vector<std::string> endpoints = {"127.0.0.1:9000", "127.0.0.1:9100",
                                          "127.0.0.1:9200", "127.0.0.1:9300"};
    addMultiEndpointRemoteSegment(transport->meta(), 1, "dummy_real_same_host",
                                  "127.0.0.1", endpoints, /*dest_device_id=*/2,
                                  0x10000, kTransferBufSize);

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_EQ(adxl_mock::get_last_connect_target(), "127.0.0.1:9200")
        << "Target engine follows dest buffer device_id, not local engine";

    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    globalConfig().ascend_agent_mode = false;
}

TEST_F(AscendDirectTransportTest,
       SameSegment_TwoDestBuffers_ConnectsEachEngine) {
    globalConfig().ascend_agent_mode = true;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    ContextManager::getInstance().finalize();
    ASSERT_TRUE(ContextManager::getInstance().initialize());
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    std::vector<std::string> endpoints = {"10.0.0.1:5000", "10.0.0.1:5100",
                                          "10.0.0.1:5200", "10.0.0.1:5300"};
    addMultiEndpointRemoteSegment(transport->meta(), 1, "two_dests", "10.0.0.1",
                                  endpoints, /*dest_device_id=*/1, 0x10000,
                                  kTransferBufSize);
    auto remote_desc = transport->meta()->getSegmentDescByID(1);
    ASSERT_NE(remote_desc, nullptr);
    TransferMetadata::BufferDesc second;
    second.name = "store";
    second.addr = 0x20000;
    second.length = kTransferBufSize;
    second.device_id = 2;
    remote_desc->buffers.push_back(second);

    auto batch_id = transport->allocateBatchID(2);
    ASSERT_NE(batch_id, 0);
    std::vector<Transport::TransferRequest> requests(2);
    requests[0].opcode = Transport::TransferRequest::WRITE;
    requests[0].source = test_buffer_src_;
    requests[0].target_id = 1;
    requests[0].target_offset = 0x10000;
    requests[0].length = kTransferBufSize;
    requests[1].opcode = Transport::TransferRequest::WRITE;
    requests[1].source = test_buffer_src_;
    requests[1].target_id = 1;
    requests[1].target_offset = 0x20000;
    requests[1].length = kTransferBufSize;
    ASSERT_TRUE(transport->submitTransfer(batch_id, requests).ok());
    auto result = waitForTransfer(transport.get(), batch_id, /*task_count=*/2);
    transport->freeBatchID(batch_id);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed);

    auto targets = adxl_mock::get_connect_targets();
    EXPECT_NE(std::find(targets.begin(), targets.end(), "10.0.0.1:5100"),
              targets.end());
    EXPECT_NE(std::find(targets.begin(), targets.end(), "10.0.0.1:5200"),
              targets.end());

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

TEST_F(AscendDirectTransportTest, Memory_RegisterStampsBufferDeviceId) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    constexpr int32_t kStampDeviceId = 2;
    g_device_id = kStampDeviceId;
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    auto segment_desc = transport->meta()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(segment_desc, nullptr);
    ASSERT_FALSE(segment_desc->buffers.empty());
    EXPECT_EQ(segment_desc->buffers.back().device_id, kStampDeviceId);
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
    EXPECT_EQ(adxl_mock::get_register_mem_count(), 1);
    auto segment_desc = transport->meta()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(segment_desc, nullptr);
    ASSERT_FALSE(segment_desc->buffers.empty());
    EXPECT_EQ(segment_desc->buffers.back().device_id, 0);

    ASSERT_EQ(transport->unregisterLocalMemory(store_buffer, true), 0);
    EXPECT_EQ(adxl_mock::get_deregister_mem_count(), 1);

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

TEST_F(AscendDirectTransportTest, Batch_AllocateFree_Success) {
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

TEST_F(AscendDirectTransportTest, LocalCopy_Async_MemcpyFailure) {
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

TEST_F(AscendDirectTransportTest, RemoteTransfer_Async_TransferFailure) {
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

TEST_F(AscendDirectTransportTest,
       RemoteTransfer_Sync_FailureWithAutoConnect_SkipsDisconnect) {
    setenv("ASCEND_AUTO_CONNECT", "1", 1);
    adxl_mock::set_transfer_result(adxl::FAILED);

    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.100", 30000});

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed);
    EXPECT_EQ(adxl_mock::get_disconnect_count(), 0);
}

TEST_F(AscendDirectTransportTest,
       RemoteTransfer_Async_SubmitFailureWithAutoConnect_SkipsDisconnect) {
    setenv("ASCEND_AUTO_CONNECT", "1", 1);
    adxl_mock::set_transfer_async_result(adxl::FAILED);

    auto transport = createTransport(true);
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);
    addRemoteSegment(transport->meta(),
                     {1, "remote_server", "192.168.1.100", 30000});

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed);
    EXPECT_EQ(adxl_mock::get_disconnect_count(), 0);
}

TEST_F(AscendDirectTransportTest,
       RemoteTransfer_Async_GetStatusFailureWithAutoConnect_SkipsDisconnect) {
    setenv("ASCEND_AUTO_CONNECT", "1", 1);
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
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed);
    EXPECT_EQ(adxl_mock::get_disconnect_count(), 0);
}

TEST_F(AscendDirectTransportTest,
       RemoteTransfer_Async_TimeoutWithAutoConnect_StillDisconnects) {
    setenv("ASCEND_AUTO_CONNECT", "1", 1);
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
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_TRUE(result.failed);
    EXPECT_EQ(adxl_mock::get_disconnect_count(), 1);
}

// -----------------------------------------------------------------------------
// Standalone mode tests (non-dummy-real, non-fabric-mem)
// -----------------------------------------------------------------------------

TEST_F(AscendDirectTransportTest,
       Standalone_RemoteHost_SelectsDestBufferEngine) {
    globalConfig().ascend_agent_mode = false;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    g_device_id = 2;
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    std::vector<std::string> endpoints = {"10.0.0.1:5000", "10.0.0.1:5100",
                                          "10.0.0.1:5200", "10.0.0.1:5300"};
    addMultiEndpointRemoteSegment(transport->meta(), 1, "remote_host",
                                  "10.0.0.1", endpoints, /*dest_device_id=*/2,
                                  0x10000, kTransferBufSize);

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed);
    EXPECT_EQ(adxl_mock::get_last_connect_target(), "10.0.0.1:5200");
}

TEST_F(AscendDirectTransportTest, Standalone_SameHost_SelectsDestBufferEngine) {
    globalConfig().ascend_agent_mode = false;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    g_device_id = 1;
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    std::vector<std::string> endpoints = {"127.0.0.1:6000", "127.0.0.1:6100",
                                          "127.0.0.1:6200", "127.0.0.1:6300"};
    addMultiEndpointRemoteSegment(transport->meta(), 1, "same_host_process",
                                  "127.0.0.1", endpoints, /*dest_device_id=*/2,
                                  0x10000, kTransferBufSize);

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed);
    EXPECT_EQ(adxl_mock::get_last_connect_target(), "127.0.0.1:6200")
        << "Same-host transfers must not apply a +1 engine offset";
}

TEST_F(AscendDirectTransportTest,
       Standalone_MissingDestBuffer_UsesFrontEndpoint) {
    globalConfig().ascend_agent_mode = false;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    g_device_id = 3;
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    std::vector<std::string> endpoints = {"127.0.0.1:7000", "127.0.0.1:7100",
                                          "127.0.0.1:7200", "127.0.0.1:7300"};
    addMultiEndpointRemoteSegment(transport->meta(), 1, "same_host_wrap",
                                  "127.0.0.1", endpoints);

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed);
    EXPECT_EQ(adxl_mock::get_last_connect_target(), "127.0.0.1:7000");
}

TEST_F(AscendDirectTransportTest,
       Standalone_SingleEndpoint_IgnoresDestDeviceId) {
    globalConfig().ascend_agent_mode = false;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    g_device_id = 3;
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    std::vector<std::string> endpoints = {"127.0.0.1:7400"};
    addMultiEndpointRemoteSegment(transport->meta(), 1, "single_endpoint",
                                  "127.0.0.1", endpoints, /*dest_device_id=*/2,
                                  0x10000, kTransferBufSize);

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed);
    EXPECT_EQ(adxl_mock::get_last_connect_target(), "127.0.0.1:7400")
        << "A single remote endpoint is used as-is, even when dest "
           "buffer device_id is not 0";
}

TEST_F(AscendDirectTransportTest,
       Standalone_FabricMem_SameHostSelectsDestBufferEngine) {
    // Fabric mem is a Store-TE feature, so the TE must be store-init for the
    // transport to capture use_fabric_mem_=true.
    globalConfig().ascend_agent_mode = false;
    globalConfig().ascend_store_te_init = true;
    globalConfig().ascend_use_fabric_mem = true;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    g_device_id = 1;
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    std::vector<std::string> endpoints = {"127.0.0.1:8000", "127.0.0.1:8100",
                                          "127.0.0.1:8200", "127.0.0.1:8300"};
    addMultiEndpointRemoteSegment(transport->meta(), 1, "fabric_same_host",
                                  "127.0.0.1", endpoints, /*dest_device_id=*/2,
                                  0x10000, kTransferBufSize);

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed);
    EXPECT_EQ(adxl_mock::get_last_connect_target(), "127.0.0.1:8200")
        << "Fabric-mem target engine follows dest buffer device_id";

    globalConfig().ascend_use_fabric_mem = false;
    globalConfig().ascend_store_te_init = false;
}

// A non-Store (e.g. P2P/HCCS) TE must NOT inherit a Store TE's fabric flag that
// leaked into the process-global config: with ascend_use_fabric_mem=true but
// ascend_store_te_init=false, the transport must capture use_fabric_mem_=false.
// Routing still follows dest buffer device_id (no same-host offset).
TEST_F(AscendDirectTransportTest,
       Standalone_FabricFlagWithoutStoreTe_DoesNotInheritFabric) {
    globalConfig().ascend_agent_mode = false;
    globalConfig().ascend_store_te_init = false;
    globalConfig().ascend_use_fabric_mem = true;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    g_device_id = 1;
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    std::vector<std::string> endpoints = {"127.0.0.1:9000", "127.0.0.1:9100",
                                          "127.0.0.1:9200", "127.0.0.1:9300"};
    addMultiEndpointRemoteSegment(transport->meta(), 1, "p2p_no_fabric",
                                  "127.0.0.1", endpoints, /*dest_device_id=*/2,
                                  0x10000, kTransferBufSize);

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed);
    EXPECT_EQ(adxl_mock::get_last_connect_target(), "127.0.0.1:9200")
        << "Non-Store TE still routes by dest buffer device_id";

    globalConfig().ascend_use_fabric_mem = false;
}

// -----------------------------------------------------------------------------
// Roce mode detection (HCCL_INTRA_ROCE_ENABLE / ASCEND_GLOBAL_RESOURCE_CONFIG)
// -----------------------------------------------------------------------------

TEST(FabricMemBestEffortAllocTest, PercentileLadderFindsFeasibleSize) {
    mock_acl::reset();
    constexpr size_t kGiB = 1024ULL * 1024 * 1024;
    constexpr size_t kTargetGiB = 40;
    constexpr size_t kMaxOkGiB = 35;
    constexpr size_t kExpectGiB = 32;  // 80% of 40GiB after 100%/90% fail
    constexpr size_t kTarget = kTargetGiB * kGiB;
    mock_acl::set_malloc_physical_max_success(kMaxOkGiB * kGiB);

    globalConfig().ascend_use_fabric_mem = true;
    size_t actual = 0;
    void* ptr = ascend_allocate_memory_best_effort(kTarget, "ascend", &actual);
    ASSERT_NE(ptr, nullptr);
    if (&adxl::AdxlEngine::MallocMem == nullptr) {
        EXPECT_EQ(actual, kExpectGiB * kGiB);
        EXPECT_GT(mock_acl::malloc_physical_call_count(), 1);
    } else {
        // AdxlEngine::MallocMem is linked in this CANN build, so the VMM
        // mock percentile ladder is unused. Still require a feasible step-down.
        EXPECT_LT(actual, kTarget);
        EXPECT_GE(actual, kTarget / 2);
    }

    ascend_free_memory("ascend", ptr);
    globalConfig().ascend_use_fabric_mem = false;
    mock_acl::reset();
}

TEST(FabricMemBestEffortAllocTest, BelowFiftyPercentReturnsNull) {
    mock_acl::reset();
    constexpr size_t kGiB = 1024ULL * 1024 * 1024;
    constexpr size_t kTargetGiB = 40;
    constexpr size_t kMaxOkGiB = 15;  // below 50% of 40GiB (=20GiB)
    mock_acl::set_malloc_physical_max_success(kMaxOkGiB * kGiB);

    globalConfig().ascend_use_fabric_mem = true;
    size_t actual = 0;
    void* ptr = ascend_allocate_memory_best_effort(kTargetGiB * kGiB, "ascend",
                                                   &actual);
    EXPECT_EQ(ptr, nullptr);
    EXPECT_EQ(actual, 0);

    globalConfig().ascend_use_fabric_mem = false;
    mock_acl::reset();
}

// 2.1 GiB target: 50% = 1.05 GiB. Align-down of lower percentiles yields 1 GiB
// (< 50%), which must be rejected even if physical alloc of 1 GiB would
// succeed.
TEST(FabricMemBestEffortAllocTest, AlignDownBelowMinPercentIsRejected) {
    mock_acl::reset();
    constexpr size_t kGiB = 1024ULL * 1024 * 1024;
    constexpr size_t kTarget = (2 * kGiB) + (kGiB / 10);  // 2.1 GiB
    mock_acl::set_malloc_physical_max_success(kGiB);  // 1 GiB ok, 2 GiB fails

    globalConfig().ascend_use_fabric_mem = true;
    size_t actual = 0;
    void* ptr = ascend_allocate_memory_best_effort(kTarget, "ascend", &actual);
    EXPECT_EQ(ptr, nullptr);
    EXPECT_EQ(actual, 0);

    globalConfig().ascend_use_fabric_mem = false;
    mock_acl::reset();
}

TEST(FabricMemBestEffortAllocTest, FullTargetFirstSuccess) {
    mock_acl::reset();
    constexpr size_t kGiB = 1024ULL * 1024 * 1024;
    constexpr size_t kTargetGiB = 40;
    constexpr size_t kTarget = kTargetGiB * kGiB;
    // Physical alloc is tried up to 3 attribute tiers per size.
    constexpr int kMaxPhysicalTiersPerSize = 3;
    mock_acl::set_malloc_physical_max_success(0);  // unlimited

    globalConfig().ascend_use_fabric_mem = true;
    size_t actual = 0;
    void* ptr = ascend_allocate_memory_best_effort(kTarget, "ascend", &actual);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(actual, kTarget);  // 100% first
    EXPECT_LE(mock_acl::malloc_physical_call_count(), kMaxPhysicalTiersPerSize);

    ascend_free_memory("ascend", ptr);
    globalConfig().ascend_use_fabric_mem = false;
    mock_acl::reset();
}

namespace {
int32_t CurrentAclDevice() {
    int32_t device_id = -1;
    EXPECT_EQ(aclrtGetDevice(&device_id), ACL_ERROR_NONE);
    return device_id;
}

struct AgentAllocEnv {
    explicit AgentAllocEnv(int device_count) {
        mock_acl::reset();
        globalConfig().ascend_agent_mode = true;
        globalConfig().ascend_use_fabric_mem = false;
        mock_acl::set_device_count(device_count);
        ContextManager::getInstance().finalize();
        EXPECT_TRUE(ContextManager::getInstance().initialize());
    }
    ~AgentAllocEnv() {
        ContextManager::getInstance().finalize();
        globalConfig().ascend_agent_mode = false;
        globalConfig().ascend_use_fabric_mem = false;
        mock_acl::reset();
    }
};
}  // namespace

TEST(AgentModeAllocTest, ConsecutiveStoreAllocsRotateDevices) {
    constexpr int kDeviceCount = 4;
    constexpr size_t kAllocSize = 4096;
    AgentAllocEnv env(kDeviceCount);

    int first = -1;
    for (int i = 0; i < kDeviceCount; ++i) {
        size_t actual = 0;
        void* ptr =
            ascend_allocate_memory_best_effort(kAllocSize, "ascend", &actual);
        ASSERT_NE(ptr, nullptr);
        EXPECT_EQ(actual, kAllocSize);
        if (i == 0) {
            first = CurrentAclDevice();
            ASSERT_GE(first, 0);
            ASSERT_LT(first, kDeviceCount);
        } else {
            EXPECT_EQ(CurrentAclDevice(), (first + i) % kDeviceCount);
        }
        ascend_free_memory("ascend", ptr);
    }

    size_t actual = 0;
    void* wrap =
        ascend_allocate_memory_best_effort(kAllocSize, "ascend", &actual);
    ASSERT_NE(wrap, nullptr);
    EXPECT_EQ(CurrentAclDevice(), first);
    ascend_free_memory("ascend", wrap);
}

TEST(AgentModeAllocTest, DirectVmmDoesNotRotateSequencer) {
    constexpr int kDeviceCount = 4;
    constexpr size_t kGiB = 1024ULL * 1024 * 1024;
    AgentAllocEnv env(kDeviceCount);

    size_t actual = 0;
    void* first = ascend_allocate_memory_best_effort(4096, "ascend", &actual);
    ASSERT_NE(first, nullptr);
    const int d0 = CurrentAclDevice();
    ascend_free_memory("ascend", first);

    void* vmm = ascend_allocate_vmm_memory_direct(kGiB);
    ASSERT_NE(vmm, nullptr);

    actual = 0;
    void* second = ascend_allocate_memory_best_effort(4096, "ascend", &actual);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(CurrentAclDevice(), (d0 + 1) % kDeviceCount)
        << "direct VMM must not consume the agent alloc sequencer";

    ascend_free_memory("ascend", second);
    ascend_free_memory("ascend", vmm);
}

TEST(AgentModeAllocTest, FailedBestEffortProbesDoNotSkipDevice) {
    constexpr int kDeviceCount = 4;
    constexpr size_t kGiB = 1024ULL * 1024 * 1024;
    AgentAllocEnv env(kDeviceCount);

    size_t actual = 0;
    void* marker = ascend_allocate_memory_best_effort(4096, "ascend", &actual);
    ASSERT_NE(marker, nullptr);
    const int d0 = CurrentAclDevice();
    ascend_free_memory("ascend", marker);

    globalConfig().ascend_use_fabric_mem = true;
    mock_acl::set_malloc_physical_max_success(35 * kGiB);
    actual = 0;
    void* first =
        ascend_allocate_memory_best_effort(40 * kGiB, "ascend", &actual);
    ASSERT_NE(first, nullptr);
    EXPECT_GT(actual, 0);
    EXPECT_EQ(CurrentAclDevice(), (d0 + 1) % kDeviceCount)
        << "failed 100%/90% probes must not consume a device slot";
    ascend_free_memory("ascend", first);

    actual = 0;
    void* second = ascend_allocate_memory_best_effort(kGiB, "ascend", &actual);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(CurrentAclDevice(), (d0 + 2) % kDeviceCount);
    ascend_free_memory("ascend", second);
}

TEST(AgentModeAllocTest, EntireBestEffortFailureDoesNotConsumeSlot) {
    constexpr int kDeviceCount = 4;
    constexpr size_t kGiB = 1024ULL * 1024 * 1024;
    AgentAllocEnv env(kDeviceCount);

    size_t actual = 0;
    void* marker = ascend_allocate_memory_best_effort(4096, "ascend", &actual);
    ASSERT_NE(marker, nullptr);
    const int d0 = CurrentAclDevice();
    ascend_free_memory("ascend", marker);

    globalConfig().ascend_use_fabric_mem = true;
    mock_acl::set_malloc_physical_max_success(15 * kGiB);
    actual = 0;
    void* failed =
        ascend_allocate_memory_best_effort(40 * kGiB, "ascend", &actual);
    EXPECT_EQ(failed, nullptr);
    EXPECT_EQ(actual, 0);

    mock_acl::set_malloc_physical_max_success(0);
    actual = 0;
    void* ok = ascend_allocate_memory_best_effort(kGiB, "ascend", &actual);
    ASSERT_NE(ok, nullptr);
    EXPECT_EQ(CurrentAclDevice(), (d0 + 1) % kDeviceCount);
    ascend_free_memory("ascend", ok);
}

TEST(RoceModeDetectionTest, GlobalResourceConfig_StringRoceDesc) {
    EXPECT_TRUE(HasRoceProtocolDescInGlobalResourceConfig(
        R"({"comm_resource_config.protocol_desc":"roce:device"})"));
}

TEST(RoceModeDetectionTest, GlobalResourceConfig_ArrayRoceDesc) {
    EXPECT_TRUE(HasRoceProtocolDescInGlobalResourceConfig(
        R"({"comm_resource_config.protocol_desc":["hccs:device","roce:host"]})"));
}

TEST(RoceModeDetectionTest, GlobalResourceConfig_NonRoceDesc) {
    EXPECT_FALSE(HasRoceProtocolDescInGlobalResourceConfig(
        R"({"comm_resource_config.protocol_desc":"hccs:device"})"));
    EXPECT_FALSE(HasRoceProtocolDescInGlobalResourceConfig(
        R"({"comm_resource_config.listen_port":26666})"));
}

TEST(RoceModeDetectionTest, GlobalResourceConfig_NestedRoceDesc) {
    EXPECT_TRUE(HasRoceProtocolDescInGlobalResourceConfig(
        R"({"comm_resource_config":{"protocol_desc":"roce:device"}})"));
}

TEST(RoceModeDetectionTest, GlobalResourceConfig_InvalidJson) {
    EXPECT_FALSE(HasRoceProtocolDescInGlobalResourceConfig("{not json"));
    EXPECT_FALSE(HasRoceProtocolDescInGlobalResourceConfig(
        R"("comm_resource_config.protocol_desc":"roce:device")"));
}

TEST(RoceModeDetectionTest, IsRoceModeEnabled_FromGlobalResourceConfig) {
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    setenv("ASCEND_GLOBAL_RESOURCE_CONFIG",
           R"({"comm_resource_config.protocol_desc":"roce:device"})", 1);
    EXPECT_TRUE(IsRoceModeEnabled());
    unsetenv("ASCEND_GLOBAL_RESOURCE_CONFIG");
}

TEST(RoceModeDetectionTest, IsRoceModeEnabled_FromHcclEnv) {
    unsetenv("ASCEND_GLOBAL_RESOURCE_CONFIG");
    setenv("HCCL_INTRA_ROCE_ENABLE", "1", 1);
    EXPECT_TRUE(IsRoceModeEnabled());
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
}

TEST(FabricMemConfigDetectionTest, GlobalResourceConfig_FlatFabricMemory) {
    EXPECT_TRUE(HasFabricMemoryInGlobalResourceConfig(
        R"({"fabric_memory.max_capacity":32})"));
    EXPECT_TRUE(HasFabricMemoryInGlobalResourceConfig(
        R"({"fabric_memory.start_address":"72","comm_resource_config.protocol_desc":"hccs:device"})"));
}

TEST(FabricMemConfigDetectionTest, GlobalResourceConfig_NestedFabricMemory) {
    EXPECT_TRUE(HasFabricMemoryInGlobalResourceConfig(
        R"({"fabric_memory":{"max_capacity":32,"start_address":40}})"));
}

TEST(FabricMemConfigDetectionTest, GlobalResourceConfig_NoFabricMemory) {
    EXPECT_FALSE(HasFabricMemoryInGlobalResourceConfig(
        R"({"comm_resource_config.protocol_desc":"hccs:device"})"));
    EXPECT_FALSE(HasFabricMemoryInGlobalResourceConfig("{}"));
    EXPECT_FALSE(HasFabricMemoryInGlobalResourceConfig(nullptr));
}

TEST(FabricMemConfigDetectionTest,
     IsFabricMemEnabled_FromGlobalResourceConfig) {
    globalConfig().ascend_store_te_init = false;
    globalConfig().ascend_use_fabric_mem = false;
    setenv("ASCEND_GLOBAL_RESOURCE_CONFIG",
           R"({"fabric_memory.max_capacity":32})", 1);
    EXPECT_TRUE(IsFabricMemEnabledFromGlobalResourceConfig());
    unsetenv("ASCEND_GLOBAL_RESOURCE_CONFIG");
}

TEST_F(AscendDirectTransportTest,
       Standalone_FabricMem_FromGlobalResourceConfig) {
    // Normal/P2P TE enables fabric mem when ASCEND_GLOBAL_RESOURCE_CONFIG
    // carries fabric_memory, without ASCEND_ENABLE_USE_FABRIC_MEM / store init.
    globalConfig().ascend_agent_mode = false;
    globalConfig().ascend_store_te_init = false;
    globalConfig().ascend_use_fabric_mem = false;
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    setenv("ASCEND_GLOBAL_RESOURCE_CONFIG",
           R"({"fabric_memory.max_capacity":32})", 1);
    constexpr int kDeviceCount = 4;
    mock_acl::set_device_count(kDeviceCount);
    g_device_id = 1;
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, kRegisterMemSize,
                                             "cpu:0", true, true),
              0);

    std::vector<std::string> endpoints = {"127.0.0.1:8000", "127.0.0.1:8100",
                                          "127.0.0.1:8200", "127.0.0.1:8300"};
    addMultiEndpointRemoteSegment(transport->meta(), 1, "fabric_via_grc",
                                  "127.0.0.1", endpoints, /*dest_device_id=*/2,
                                  0x10000, kTransferBufSize);

    initTestData(kTransferBufSize);
    auto result = runRemoteTransfer(transport.get(), test_buffer_src_, 1,
                                    0x10000, kTransferBufSize);
    ASSERT_TRUE(result.finished);
    EXPECT_FALSE(result.failed);
    EXPECT_EQ(adxl_mock::get_last_connect_target(), "127.0.0.1:8200")
        << "Normal TE with fabric_memory in GLOBAL_RESOURCE_CONFIG routes by "
           "dest buffer device_id";

    unsetenv("ASCEND_GLOBAL_RESOURCE_CONFIG");
}

// -----------------------------------------------------------------------------
// Store vs P2P protocol split via a single ASCEND_GLOBAL_RESOURCE_CONFIG:
// top-level = default (P2P, e.g. HCCS), optional "store" sub-object overrides
// it for a Store-init TE (e.g. RoCE). Resolution is gated on
// ascend_store_te_init.
// -----------------------------------------------------------------------------

namespace {
// Top-level default = HCCS (P2P), "store" override = RoCE (Store).
constexpr const char* kDualProtocolConfig =
    R"({"comm_resource_config":{"protocol_desc":"hccs:device"},)"
    R"("store":{"comm_resource_config":{"protocol_desc":"roce:device"}}})";

struct StoreTeInitScope {
    explicit StoreTeInitScope(bool v) {
        globalConfig().ascend_store_te_init = v;
    }
    ~StoreTeInitScope() { globalConfig().ascend_store_te_init = false; }
};
}  // namespace

TEST(StoreResourceConfigSplitTest, NoStoreKey_PassthroughVerbatimBothRoles) {
    const char* cfg =
        R"({"comm_resource_config":{"protocol_desc":"hccs:device"}})";
    {
        StoreTeInitScope store(true);
        EXPECT_EQ(ResolveAscendGlobalResourceConfig(cfg), std::string(cfg));
    }
    {
        StoreTeInitScope store(false);
        EXPECT_EQ(ResolveAscendGlobalResourceConfig(cfg), std::string(cfg));
    }
}

TEST(StoreResourceConfigSplitTest, EmptyOrNull_ReturnsEmpty) {
    StoreTeInitScope store(true);
    EXPECT_TRUE(ResolveAscendGlobalResourceConfig(nullptr).empty());
    EXPECT_TRUE(ResolveAscendGlobalResourceConfig("").empty());
}

TEST(StoreResourceConfigSplitTest, StoreRole_SelectsStoreSubtreeRoce) {
    StoreTeInitScope store(true);
    std::string resolved =
        ResolveAscendGlobalResourceConfig(kDualProtocolConfig);
    // Store TE gets the "store" subtree (RoCE), with the "store" key stripped.
    EXPECT_TRUE(HasRoceProtocolDescInGlobalResourceConfig(resolved.c_str()));
    EXPECT_EQ(resolved.find("hccs"), std::string::npos);
    EXPECT_EQ(resolved.find("\"store\""), std::string::npos);
}

TEST(StoreResourceConfigSplitTest, DefaultRole_StripsStoreKeyHccs) {
    StoreTeInitScope store(false);
    std::string resolved =
        ResolveAscendGlobalResourceConfig(kDualProtocolConfig);
    // P2P/default TE gets the top-level (HCCS) with the "store" key removed.
    EXPECT_FALSE(HasRoceProtocolDescInGlobalResourceConfig(resolved.c_str()));
    EXPECT_NE(resolved.find("hccs"), std::string::npos);
    EXPECT_EQ(resolved.find("roce"), std::string::npos);
    EXPECT_EQ(resolved.find("\"store\""), std::string::npos);
}

TEST(StoreResourceConfigSplitTest,
     StoreRoleMissingStoreKey_FallsBackToDefault) {
    const char* cfg =
        R"({"comm_resource_config":{"protocol_desc":"hccs:device"}})";
    StoreTeInitScope store(true);
    // No "store" key -> store TE falls back to the (verbatim) default config.
    EXPECT_EQ(ResolveAscendGlobalResourceConfig(cfg), std::string(cfg));
    EXPECT_FALSE(HasRoceProtocolDescInGlobalResourceConfig(
        ResolveAscendGlobalResourceConfig(cfg).c_str()));
}

// The headline case: one env var, Store TE -> RoCE, P2P TE -> HCCS.
TEST(StoreResourceConfigSplitTest, IsRoceModeEnabled_StoreRoceP2pHccs) {
    unsetenv("HCCL_INTRA_ROCE_ENABLE");
    setenv("ASCEND_GLOBAL_RESOURCE_CONFIG", kDualProtocolConfig, 1);
    {
        StoreTeInitScope store(true);
        EXPECT_TRUE(IsRoceModeEnabled()) << "Store TE should resolve to RoCE";
    }
    {
        StoreTeInitScope store(false);
        EXPECT_FALSE(IsRoceModeEnabled()) << "P2P TE should resolve to HCCS";
    }
    unsetenv("ASCEND_GLOBAL_RESOURCE_CONFIG");
}

// -----------------------------------------------------------------------------
// Client-Server mode: when capability is supported and user did not set
// ASCEND_LOCAL_COMM_RES, Mooncake auto-injects LocalCommRes={"version":"1.3"}
// so EngineFactory selects HixlEngine (HixlCS path).
// -----------------------------------------------------------------------------

class ClientServerModeTest : public AscendDirectTransportTest {
   protected:
    void SetUp() override {
        AscendDirectTransportTest::SetUp();
        unsetenv("ASCEND_LOCAL_COMM_RES");
    }
    void TearDown() override {
        unsetenv("ASCEND_LOCAL_COMM_RES");
        AscendDirectTransportTest::TearDown();
    }
};

TEST_F(ClientServerModeTest, AutoInjectLocalCommResWhenSupported) {
    adxl_mock::set_capability_result(adxl::SUCCESS, 1);
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    const auto opts = adxl_mock::get_last_init_options();
    auto it = opts.find("adxl.LocalCommRes");
    ASSERT_NE(it, opts.end());
    EXPECT_EQ(it->second, R"({"version":"1.3"})");
}

TEST_F(ClientServerModeTest, NoInjectWhenNotSupported) {
    adxl_mock::set_capability_result(adxl::SUCCESS, 0);
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    const auto opts = adxl_mock::get_last_init_options();
    EXPECT_EQ(opts.find("adxl.LocalCommRes"), opts.end());
}

TEST_F(ClientServerModeTest, UserEnvOverridesAutoInject) {
    adxl_mock::set_capability_result(adxl::SUCCESS, 1);
    setenv("ASCEND_LOCAL_COMM_RES", R"({"version":"1.2"})", 1);
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    const auto opts = adxl_mock::get_last_init_options();
    auto it = opts.find("adxl.LocalCommRes");
    ASSERT_NE(it, opts.end());
    EXPECT_EQ(it->second, R"({"version":"1.2"})");
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
