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

/*
 * RDMA Endpoint Re-establishment Test
 *
 * Purpose:
 * This test verifies that TE correctly handles endpoint re-establish
 * during simulated Initiator restarts.
 *
 * How to run:
 * 1. Start etcd:
 *    etcd --listen-client-urls http://127.0.0.1:18222 \
 *      --advertise-client-urls http://127.0.0.1:18222
 *
 * 2. Run test
 *    sudo env MC_METADATA_SERVER=127.0.0.1:18222 \
 *      MC_TARGET_SERVER_NAME=127.0.0.1:12345 \
 *      MC_INITIATOR_SERVER_NAME=127.0.0.1:12346 \
 *      MC_TARGET_DEVICE_NAME=erdma_0 MC_INITIATOR_DEVICE_NAME=erdma_1 \
 *      ./build/mooncake-transfer-engine/tests/rdma_endpoint_reestablish_test
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <numa.h>
#include <sys/time.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <vector>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "common.h"

using namespace mooncake;

// Size of the pre-registered memory.
constexpr size_t kRAMBufSize = 256ull << 24;  // 256 MB

// Actual data payload size for RDMA Read/Write.
constexpr size_t kDataLength = 16ull << 24;  // 16MB

std::string formatDeviceNames(const std::string &device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

std::string makeNicPriorityMatrix(const std::string &device_name) {
    auto formatted_devices = formatDeviceNames(device_name);
    return "{\"cpu:0\": [[" + formatted_devices +
           "],[]], "
           " \"cpu:1\": [[" +
           formatted_devices + "],[]]}";
}

void wait_for_transfer(TransferEngine *engine, BatchID batch_id,
                       const std::string &op_name) {
    bool completed = false;
    TransferStatus status;
    while (!completed) {
        Status s = engine->getTransferStatus(batch_id, 0, status);
        EXPECT_EQ(s, Status::OK());
        if (status.s == TransferStatusEnum::COMPLETED) {
            completed = true;
        } else if (status.s == TransferStatusEnum::FAILED) {
            FAIL() << op_name << " FAILED";
        }
    }
    Status s = engine->freeBatchID(batch_id);
    EXPECT_EQ(s, Status::OK());
}

struct TEContext {
    std::unique_ptr<TransferEngine> engine_{};
    uint8_t *local_addr_{};
    bool segment_opened_{false};
    SegmentHandle segment_handle_{};
    uint64_t remote_base_{};

    TEContext(const std::string &local_server_name,
              const std::string &metadata_server, const std::string &segment_id,
              const std::string &device_name) {
        engine_ = std::make_unique<TransferEngine>(false);
        auto hostname_port = parseHostNameWithPort(local_server_name);
        engine_->init(metadata_server, local_server_name, hostname_port.first,
                      hostname_port.second);

        auto nic_priority_matrix = makeNicPriorityMatrix(device_name);
        void *args[2] = {const_cast<char *>(nic_priority_matrix.c_str()),
                         nullptr};

        Transport *xport = engine_->installTransport("rdma", args);
        LOG_ASSERT(xport);

        local_addr_ = static_cast<uint8_t *>(numa_alloc_onnode(kRAMBufSize, 0));
        memset(local_addr_, 0, kDataLength);

        int rc =
            engine_->registerLocalMemory(local_addr_, kRAMBufSize, "cpu:0");
        LOG_ASSERT(!rc);

        if (!segment_id.empty()) {
            segment_opened_ = true;
            LOG(INFO) << "Opening segment " << segment_id << "...";
            segment_handle_ = engine_->openSegment(segment_id);
            auto segment_desc =
                engine_->getMetadata()->getSegmentDescByID(segment_handle_);
            remote_base_ = (uint64_t)segment_desc->buffers[0].addr;
        }
    }

    ~TEContext() {
        engine_->unregisterLocalMemory(local_addr_);
        numa_free(local_addr_, kRAMBufSize);
        if (segment_opened_) engine_->closeSegment(segment_handle_);
    }
};

class RDMAEndpointReestablishTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("RDMAEndpointReestablishTest");
        FLAGS_logtostderr = true;

        const char *env = std::getenv("MC_METADATA_SERVER");
        metadata_server = env ? env : "127.0.0.1:18222";
        LOG(INFO) << "metadata_server: " << metadata_server;

        env = std::getenv("MC_TARGET_SERVER_NAME");
        target_server_name = env ? env : "127.0.0.1:12345";
        LOG(INFO) << "target_server_name: " << target_server_name;

        env = std::getenv("MC_INITIATOR_SERVER_NAME");
        initiator_server_name = env ? env : "127.0.0.1:12346";
        LOG(INFO) << "initiator_server_name: " << initiator_server_name;

        env = std::getenv("MC_TARGET_DEVICE_NAME");
        target_device_name = env ? env : "erdma_0";
        LOG(INFO) << "target_device_name: " << target_device_name;

        env = std::getenv("MC_INITIATOR_DEVICE_NAME");
        initiator_device_name = env ? env : "erdma_1";
        LOG(INFO) << "initiator_device_name: " << initiator_device_name;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::string metadata_server;
    std::string target_server_name;
    std::string initiator_server_name;
    std::string target_device_name;
    std::string initiator_device_name;
};

TEST_F(RDMAEndpointReestablishTest, EndpointReestablish) {
    // 1. Setup Target (will stay alive until test function exits)
    LOG(INFO) << "========== Setting up Target ==========";
    TEContext target_ctx(target_server_name, metadata_server, "",
                         target_device_name);
    LOG(INFO) << "Target is up. Waiting for RDMA connections and operations...";

    // 2. Phase 1: Initiator Start, Connect & Write
    LOG(INFO) << "========== Phase 1: Start, Connect & Write ==========";
    {
        TEContext init_ctx(initiator_server_name, metadata_server,
                           target_server_name, initiator_device_name);

        // Fill buffer with test pattern
        for (size_t i = 0; i < kDataLength; ++i) {
            init_ctx.local_addr_[i] = static_cast<uint8_t>(i % 256);
        }

        LOG(INFO) << "Writing " << kDataLength << " bytes to Target...";
        auto batch_id = init_ctx.engine_->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = init_ctx.local_addr_;
        entry.target_id = init_ctx.segment_handle_;
        entry.target_offset = init_ctx.remote_base_;

        Status s = init_ctx.engine_->submitTransfer(batch_id, {entry});
        ASSERT_EQ(s, Status::OK());

        wait_for_transfer(init_ctx.engine_.get(), batch_id, "WRITE");

        LOG(INFO) << "Phase 1: Write Completed. Tearing down connection...";
    }

    LOG(INFO) << "Simulating Initiator Crash/Restart... Waiting 2 seconds.";
    sleep(2);

    // 3. Phase 2: Restart, Re-establish Endpoint & Read
    LOG(INFO) << "========== Phase 2: Restart, Re-establish Endpoint & Read "
                 "==========";
    {
        TEContext init_ctx(initiator_server_name, metadata_server,
                           target_server_name, initiator_device_name);

        LOG(INFO) << "Reading data back over new Endpoint...";
        auto batch_id = init_ctx.engine_->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = init_ctx.local_addr_;
        entry.target_id = init_ctx.segment_handle_;
        entry.target_offset = init_ctx.remote_base_;

        Status s = init_ctx.engine_->submitTransfer(batch_id, {entry});
        ASSERT_EQ(s, Status::OK());

        wait_for_transfer(init_ctx.engine_.get(), batch_id, "READ");

        bool ok = true;
        for (size_t i = 0; i < kDataLength; ++i) {
            if (init_ctx.local_addr_[i] != static_cast<uint8_t>(i % 256)) {
                ok = false;
                LOG(ERROR) << "Data mismatch at offset " << i << ", expected "
                           << (i % 256) << ", got "
                           << (int)init_ctx.local_addr_[i];
                break;
            }
        }

        ASSERT_TRUE(ok) << "Endpoint Reconstruction Verification Failed!";
        LOG(INFO)
            << ">>> ENDPOINT RECONSTRUCTION VERIFICATION: \033[32mSUCCESS\033";
    }
}
