// Copyright 2025 Huawei Technologies Co., Ltd
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

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <memory>
#include <string>
#include <map>
#include <gmock/gmock.h>
#define private public
#include "transport/ascend_transport/ascend_direct_transport/ascend_direct_transport.h"
#undef private 
#include "adxl/adxl_engine.h"
#include "transfer_engine.h"

using ::testing::_;
using ::testing::Return;

// Mock aclrtGetDevice function
extern "C" {
    int aclrtGetDevice(int* deviceId) {
        if (deviceId != nullptr) {
            *deviceId = 0;  // Set device ID to 0
        }
        return 0;  // Return success
    }
}

// Mock AdxlEngine
class MockAdxlEngine : public adxl::AdxlEngine {
public:
    MockAdxlEngine() : AdxlEngine("test_engine") {}
    MOCK_METHOD(adxl::Status, RegisterGlobalMem, (const adxl::MemDesc&, adxl::MemType, adxl::MemHandle&));
    MOCK_METHOD(adxl::Status, DeregisterGlobalMem, (adxl::MemHandle));
    MOCK_METHOD(adxl::Status, Connect, (const adxl::AscendString&, int32_t));
    MOCK_METHOD(adxl::Status, DisConnect, (int32_t));
    MOCK_METHOD(adxl::Status, TransferSync, (const adxl::AscendString&, adxl::TransferOp, const std::vector<adxl::TransferOpDesc> &, int32_t));
};

namespace mooncake {

class AscendDirectTransportTest : public ::testing::Test {
protected:
    void SetUp() override {
        google::InitGoogleLogging("AscendDirectTransportTest");
        FLAGS_logtostderr = 1;

        const char *env = std::getenv("MC_METADATA_SERVER");
        if (env)
            metadata_server = env;
        else
            metadata_server = "P2PHANDSHAKE";
        LOG(INFO) << "metadata_server: " << metadata_server;

        env = std::getenv("MC_LOCAL_SERVER_NAME");
        if (env)
            local_server_name = env;
        else
            local_server_name = "127.0.0.2:12345";
        LOG(INFO) << "local_server_name: " << local_server_name;
    }

    void TearDown() override {
        // 清理 glog
        google::ShutdownGoogleLogging();
    }

    std::string metadata_server;
    std::string local_server_name;
};

TEST_F(AscendDirectTransportTest, RegisterLocalMemory_Success) {
    auto engine = std::make_unique<TransferEngine>(false);
    auto hostname_port = parseHostNameWithPort(local_server_name);
    auto rc = engine->init(metadata_server, local_server_name,
                           hostname_port.first.c_str(), hostname_port.second);
    LOG_ASSERT(rc == 0);
    Transport *transport = nullptr;
    transport = engine->installTransport("ascend", nullptr);
    LOG_ASSERT(transport != nullptr);

    auto mock_data_dist = std::make_unique<MockAdxlEngine>();
    EXPECT_CALL(*mock_data_dist, RegisterGlobalMem(_, _, _)).WillOnce(Return(adxl::ADXL_SUCCESS));
    AscendDirectTransport *ascend_transport = dynamic_cast<AscendDirectTransport *>(transport);
    ascend_transport->data_dist_ = std::move(mock_data_dist);
    void* addr = reinterpret_cast<void*>(0x1000);
    size_t length = 4096;
    std::string location = "npu:0";
    int ret = ascend_transport->registerLocalMemory(addr, length, location, true, true);
    EXPECT_EQ(ret, 0);
}


TEST_F(AscendDirectTransportTest, CompleteTransferSuccess) {
    auto engine = std::make_unique<TransferEngine>(false);
    auto hostname_port = parseHostNameWithPort(local_server_name);
    auto rc = engine->init(metadata_server, local_server_name,
                           hostname_port.first.c_str(), hostname_port.second);
    LOG_ASSERT(rc == 0);
    Transport *transport = nullptr;
    transport = engine->installTransport("ascend", nullptr);
    LOG_ASSERT(transport != nullptr);

    auto mock_data_dist = std::make_unique<MockAdxlEngine>();
    // Mock所有关键方法
    EXPECT_CALL(*mock_data_dist, Connect(_, _))
        .WillOnce(testing::DoAll(testing::Return(adxl::ADXL_SUCCESS)));
    EXPECT_CALL(*mock_data_dist, TransferSync(_, _, _, _))
        .WillOnce(testing::Return(adxl::ADXL_SUCCESS));
    EXPECT_CALL(*mock_data_dist, DisConnect(_))
        .WillOnce(testing::Return(adxl::ADXL_SUCCESS));

    AscendDirectTransport *ascend_transport = dynamic_cast<AscendDirectTransport *>(transport);
    ascend_transport->data_dist_ = std::move(mock_data_dist);

    // 创建batch
    auto batch_id = engine->allocateBatchID(1);
    EXPECT_NE(batch_id, 0);

    // 创建传输请求
    TransferRequest request;
    request.source = reinterpret_cast<void*>(0x1000);
    request.target_id = 0;
    request.target_offset = 0x2000;
    request.length = 4096;
    request.opcode = TransferRequest::WRITE;
    std::vector<TransferRequest> entries = {request};

    // 提交传输
    Status status = ascend_transport->submitTransfer(batch_id, entries);
    EXPECT_TRUE(status.ok());

    // 检查初始状态（应为WAITING）
    TransferStatus transfer_status;
    status = ascend_transport->getTransferStatus(batch_id, 0, transfer_status);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(transfer_status.s, TransferStatusEnum::WAITING);

    // 这里可以模拟worker线程处理，实际项目中可用条件变量或更长sleep
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // 再次检查状态（理论上应为COMPLETED）
    status = ascend_transport->getTransferStatus(batch_id, 0, transfer_status);
    EXPECT_TRUE(status.ok());
    // 由于mock，状态可能还是WAITING，但mock调用已被验证
    EXPECT_EQ(transfer_status.s, TransferStatusEnum::COMPLETED);

    // 释放 batch 以避免 slice leak
    status = engine->freeBatchID(batch_id);
    EXPECT_TRUE(status.ok());

    LOG(INFO) << "Mock setup complete and transfer flow tested.";
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
