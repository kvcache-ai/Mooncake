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
#include <adxl/adxl_engine.h>
#include "transport/ascend_transport/ascend_direct_transport/ascend_direct_transport.h"
#include "transfer_metadata.h"
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>

using namespace mooncake;

namespace {

static int g_device_id = 0;
static int g_device_count = 1;
static aclrtContext g_context = reinterpret_cast<aclrtContext>(0x1234);
static aclError g_memcpy_result = ACL_ERROR_NONE;
static aclError g_memcpy_async_result = ACL_ERROR_NONE;
static aclError g_memcpy_batch_result = ACL_ERROR_NONE;
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
    g_memory_locations.clear();
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
}  // namespace mock_acl

extern "C" {

aclError aclrtGetDevice(int* deviceId) {
    *deviceId = g_device_id;
    return ACL_ERROR_NONE;
}

aclError aclrtSetDevice(int deviceId) {
    if (deviceId >= g_device_count) {
        return 1;
    }
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
}

static adxl::Status g_connect_result = adxl::SUCCESS;
static adxl::Status g_transfer_result = adxl::SUCCESS;
static adxl::Status g_initialize_result = adxl::SUCCESS;
static std::set<std::string> g_connected;
static std::mutex g_mutex;
static bool g_was_initialize_called = false;
static int g_next_handle = 1;
static int g_connect_count = 0;
static int g_disconnect_count = 0;
static int g_transfer_count = 0;

namespace adxl_mock {
void reset() {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_connect_result = adxl::SUCCESS;
    g_transfer_result = adxl::SUCCESS;
    g_initialize_result = adxl::SUCCESS;
    g_connected.clear();
    g_was_initialize_called = false;
    g_next_handle = 1;
    g_connect_count = 0;
    g_disconnect_count = 0;
    g_transfer_count = 0;
}

void set_connect_result(adxl::Status status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_connect_result = status;
}

void set_transfer_result(adxl::Status status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_transfer_result = status;
}

void set_initialize_result(adxl::Status status) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_initialize_result = status;
}

bool was_initialize_called() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_was_initialize_called;
}

int get_transfer_count() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_transfer_count;
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
        result = g_transfer_result;
    }

    (void)operation;
    (void)op_descs;

    return result;
}

Status AdxlEngine::RegisterMem(const MemDesc& mem, MemType type,
                               MemHandle& mem_handle) {
    (void)type;
    mem_handle =
        reinterpret_cast<MemHandle>(static_cast<uintptr_t>(g_next_handle++));
    return SUCCESS;
}

Status AdxlEngine::DeregisterMem(MemHandle mem_handle) {
    (void)mem_handle;
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

        // 初始化glog并设置日志输出到stderr，确保在docker中能看到日志
        google::InitGoogleLogging("AscendDirectTransportTest");
        FLAGS_minloglevel = 0;       // INFO级别
        FLAGS_logtostderr = 1;       // 输出到stderr而不是文件
        FLAGS_colorlogtostderr = 1;  // 启用颜色输出
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<AscendDirectTransport> createTransport() {
        auto transport = std::make_unique<AscendDirectTransport>();
        std::string local_server_name = "127.0.0.1:20000";
        auto metadata = std::make_shared<TransferMetadata>("P2PHANDSHAKE");
        auto topology = std::make_shared<Topology>();

        if (transport->install(local_server_name, metadata, topology) != 0) {
            return nullptr;
        }
        return transport;
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

    alignas(4096) char test_buffer_src_[16 * 1024 * 1024];
    alignas(4096) char test_buffer_dst_[16 * 1024 * 1024];
};

TEST_F(AscendDirectTransportTest, ConstructorAndName) {
    auto transport = std::make_unique<AscendDirectTransport>();
    ASSERT_NE(transport, nullptr);
    EXPECT_STREQ(transport->getName(), "ascend_direct");
}

TEST_F(AscendDirectTransportTest, InstallSuccess) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    EXPECT_TRUE(adxl_mock::was_initialize_called());
}

TEST_F(AscendDirectTransportTest, InstallFailsWhenAdxlInitializeFails) {
    adxl_mock::set_initialize_result(adxl::FAILED);

    auto transport = std::make_unique<AscendDirectTransport>();
    std::string local_server_name = "127.0.0.1:20000";
    auto metadata = std::make_shared<TransferMetadata>("P2PHANDSHAKE");
    auto topology = std::make_shared<Topology>();

    int ret = transport->install(local_server_name, metadata, topology);
    EXPECT_NE(ret, 0);
}

TEST_F(AscendDirectTransportTest, RegisterAndUnregisterMemory) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    EXPECT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);
    EXPECT_EQ(transport->unregisterLocalMemory(test_buffer_src_, true), 0);
}

TEST_F(AscendDirectTransportTest, RegisterMemoryWithWildcardLocation) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    mock_acl::set_pointer_location(test_buffer_src_, ACL_MEM_LOCATION_TYPE_HOST,
                                   0);
    EXPECT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024, "*",
                                             true, true),
              0);
    EXPECT_EQ(transport->unregisterLocalMemory(test_buffer_src_, true), 0);
}

TEST_F(AscendDirectTransportTest, RegisterMemoryWithUnsupportedLocation) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    // Test with unsupported location string
    EXPECT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "invalid_location", true, true),
              -1);
}

TEST_F(AscendDirectTransportTest, RegisterMemoryWithDeviceLocation) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    mock_acl::set_pointer_location(test_buffer_src_,
                                   ACL_MEM_LOCATION_TYPE_DEVICE, 0);

    EXPECT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024, "*",
                                             true, true),
              0);
    EXPECT_EQ(transport->unregisterLocalMemory(test_buffer_src_, true), 0);
}

TEST_F(AscendDirectTransportTest, TransferExceedsBatchCapacity) {
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
        req.length = 1024;
        requests.push_back(req);
    }

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_FALSE(s.ok());

    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, TransferWithADXLFailure) {
    // mock adxl FAILED
    adxl_mock::set_transfer_result(adxl::FAILED);

    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);

    auto metadata = transport->meta();
    ASSERT_NE(metadata, nullptr);

    // add remote meta
    auto remote_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    remote_desc->name = "remote_server";
    remote_desc->protocol = "ascend";
    remote_desc->rank_info.hostIp = "192.168.1.100";  // 远程IP
    remote_desc->rank_info.hostPort = 30000;          // 远程端口
    metadata->addLocalSegment(1, "remote_server", std::move(remote_desc));

    initTestData(4096);
    memset(test_buffer_dst_, 0, 4096);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 1;  // remote segment
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = 4096;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    Transport::TransferStatus status;
    int retries = 0;
    bool failed = false;
    while (!failed && retries < 1000) {
        s = transport->getTransferStatus(batch_id, 0, status);
        ASSERT_TRUE(s.ok());
        if (status.s == Transport::FAILED) {
            failed = true;
        } else if (status.s == Transport::COMPLETED) {
            break;
        }
        retries++;
        if (!failed) {
            usleep(1000);
        }
    }

    EXPECT_TRUE(failed) << "Transfer should have failed due to ADXL failure";
    EXPECT_GT(adxl_mock::get_transfer_count(), 0)
        << "ADXL transfer should have been called";
    EXPECT_EQ(test_buffer_dst_[0], static_cast<char>(0))
        << "Destination buffer should not be modified on transfer failure";

    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, LocalCopyWithACLMemcpyFailure) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);

    initTestData(4096);
    memset(test_buffer_dst_, 0, 4096);

    // Force ACL memcpy to fail
    mock_acl::set_memcpy_result(ACL_ERROR_RT_DEVICE_TASK_ABORT);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 0;
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = 4096;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    Transport::TransferStatus status;
    int retries = 0;
    bool failed = false;
    while (!failed && retries < 1000) {
        s = transport->getTransferStatus(batch_id, 0, status);
        if (!s.ok()) break;
        if (status.s == Transport::FAILED) {
            failed = true;
        }
        retries++;
        if (!failed) {
            usleep(1000);
        }
    }
    EXPECT_TRUE(failed)
        << "Transfer should have failed due to ACL memcpy failure";
    EXPECT_EQ(status.transferred_bytes, 0)
        << "Transferred bytes should be 0 on failure";
    EXPECT_FALSE(verifyTestData(4096))
        << "Destination should not match source on failure";

    mock_acl::set_memcpy_result(ACL_ERROR_NONE);
    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, LocalCopyWithACLAsyncFailure) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);

    initTestData(4096);
    memset(test_buffer_dst_, 0, 4096);

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
    req.length = 4096;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    Transport::TransferStatus status;
    int retries = 0;
    bool failed = false;
    while (!failed && retries < 1000) {
        s = transport->getTransferStatus(batch_id, 0, status);
        if (!s.ok()) break;
        if (status.s == Transport::FAILED) {
            failed = true;
        }
        retries++;
        if (!failed) {
            usleep(1000);
        }
    }
    EXPECT_TRUE(failed)
        << "Transfer should have failed due to ACL async memcpy failure";
    EXPECT_EQ(status.transferred_bytes, 0)
        << "Transferred bytes should be 0 on failure";
    EXPECT_FALSE(verifyTestData(4096))
        << "Destination should not match source on failure";

    mock_acl::set_memcpy_async_result(ACL_ERROR_NONE);
    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, CopyWithBatchFailure) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);

    initTestData(4096);
    memset(test_buffer_dst_, 0, 4096);

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
    req.length = 4096;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    Transport::TransferStatus status;
    int retries = 0;
    bool failed = false;
    while (!failed && retries < 1000) {
        s = transport->getTransferStatus(batch_id, 0, status);
        if (!s.ok()) break;
        if (status.s == Transport::FAILED) {
            failed = true;
        } else if (status.s == Transport::COMPLETED) {
            break;
        }
        retries++;
        if (!failed) {
            usleep(1000);
        }
    }

    EXPECT_TRUE(failed) << "Transfer should fail when batch memcpy fails";
    EXPECT_EQ(status.transferred_bytes, 0)
        << "Transferred bytes should be 0 on failure";
    EXPECT_FALSE(verifyTestData(4096))
        << "Destination should not match source on failure";

    mock_acl::set_memcpy_batch_result(ACL_ERROR_NONE);
    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, CopyWithBatchFeatureNotSupport) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);

    initTestData(4096);
    memset(test_buffer_dst_, 0, 4096);

    mock_acl::set_pointer_location(test_buffer_src_, ACL_MEM_LOCATION_TYPE_HOST,
                                   0);
    mock_acl::set_pointer_location(test_buffer_dst_,
                                   ACL_MEM_LOCATION_TYPE_DEVICE, 0);

    // Force batch memcpy to return FEATURE_NOT_SUPPORT, should fallback to
    // async
    mock_acl::set_memcpy_batch_result(ACL_ERROR_RT_FEATURE_NOT_SUPPORT);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 0;
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = 4096;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    // Wait for transfer to complete (fallback to async path)
    Transport::TransferStatus status;
    int retries = 0;
    bool completed = false;
    while (!completed && retries < 1000) {
        s = transport->getTransferStatus(batch_id, 0, status);
        ASSERT_TRUE(s.ok());
        if (status.s == Transport::COMPLETED) {
            completed = true;
        } else if (status.s == Transport::FAILED) {
            FAIL() << "Transfer failed";
        }
        retries++;
        if (!completed) {
            usleep(1000);
        }
    }
    EXPECT_TRUE(completed);
    EXPECT_TRUE(verifyTestData(4096));

    mock_acl::set_memcpy_batch_result(ACL_ERROR_NONE);
    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, TransferZeroLength) {
    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_dst_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);

    initTestData(4096);
    memset(test_buffer_dst_, 0, 4096);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 0;
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = 0;  // 零长度传输
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    Transport::TransferStatus status;
    int retries = 0;
    bool completed = false;
    while (!completed && retries < 100) {
        s = transport->getTransferStatus(batch_id, 0, status);
        ASSERT_TRUE(s.ok());
        if (status.s == Transport::COMPLETED) {
            completed = true;
        } else if (status.s == Transport::FAILED) {
            FAIL() << "Zero-length transfer should not fail";
        }
        retries++;
        if (!completed) {
            usleep(1000);
        }
    }
    EXPECT_TRUE(completed)
        << "Zero-length transfer should complete immediately";
    EXPECT_EQ(status.transferred_bytes, 0)
        << "Transferred bytes should be 0 for zero-length transfer";

    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, RemoteTransferConnectTimeout) {
    // mock adxl TIMEOUT
    adxl_mock::set_connect_result(adxl::TIMEOUT);

    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);

    auto metadata = transport->meta();
    ASSERT_NE(metadata, nullptr);

    auto remote_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    remote_desc->name = "remote_server";
    remote_desc->protocol = "ascend";
    remote_desc->rank_info.hostIp = "192.168.1.100";
    remote_desc->rank_info.hostPort = 30000;
    metadata->addLocalSegment(1, "remote_server", std::move(remote_desc));

    initTestData(4096);
    memset(test_buffer_dst_, 0, 4096);

    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 1;
    req.target_offset = reinterpret_cast<uint64_t>(test_buffer_dst_);
    req.length = 4096;
    requests.push_back(req);

    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    Transport::TransferStatus status;
    int retries = 0;
    bool failed = false;
    while (!failed && retries < 1000) {
        s = transport->getTransferStatus(batch_id, 0, status);
        ASSERT_TRUE(s.ok());
        if (status.s == Transport::FAILED) {
            failed = true;
        } else if (status.s == Transport::COMPLETED) {
            break;
        }
        retries++;
        if (!failed) {
            usleep(1000);
        }
    }

    EXPECT_TRUE(failed)
        << "Transfer should have failed due to connection timeout";
    EXPECT_EQ(status.transferred_bytes, 0)
        << "Transferred bytes should be 0 on connection failure";

    transport->freeBatchID(batch_id);
}

TEST_F(AscendDirectTransportTest, RemoteTransferSuccess) {
    adxl_mock::set_transfer_result(adxl::SUCCESS);

    auto transport = createTransport();
    ASSERT_NE(transport, nullptr);
    ASSERT_EQ(transport->registerLocalMemory(test_buffer_src_, 1024 * 1024,
                                             "cpu:0", true, true),
              0);

    auto metadata = transport->meta();
    ASSERT_NE(metadata, nullptr);
    auto remote_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    remote_desc->name = "remote_server";
    remote_desc->protocol = "ascend";
    remote_desc->rank_info.hostIp = "192.168.1.200";
    remote_desc->rank_info.hostPort = 40000;
    metadata->addLocalSegment(1, "remote_server", std::move(remote_desc));

    initTestData(4096);
    auto batch_id = transport->allocateBatchID(1);
    ASSERT_NE(batch_id, 0);

    std::vector<Transport::TransferRequest> requests;
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = test_buffer_src_;
    req.target_id = 1;
    req.target_offset = 0x10000;
    req.length = 4096;
    requests.push_back(req);
    Status s = transport->submitTransfer(batch_id, requests);
    EXPECT_TRUE(s.ok());

    Transport::TransferStatus status;
    int retries = 0;
    bool completed = false;
    while (!completed && retries < 1000) {
        s = transport->getTransferStatus(batch_id, 0, status);
        ASSERT_TRUE(s.ok());
        if (status.s == Transport::COMPLETED) {
            completed = true;
        } else if (status.s == Transport::FAILED) {
            FAIL() << "Remote transfer should not fail";
        }
        retries++;
        if (!completed) {
            usleep(1000);
        }
    }

    EXPECT_TRUE(completed) << "Remote transfer should complete successfully";
    EXPECT_GT(adxl_mock::get_transfer_count(), 0)
        << "ADXL transfer should have been called";

    transport->freeBatchID(batch_id);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
