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

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>

#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

DEFINE_string(metadata_server, "127.0.0.1:2379",
              "central metadata server for transfer engine");

class RDMALoopbackTest : public ::testing::Test {
   public:
    void *addr = nullptr;
    std::pair<std::string, uint16_t> hostname_port;
    std::unique_ptr<mooncake::TransferEngine> engine;
    const size_t ram_buffer_size = 1ull << 30;

   protected:
    void SetUp() override {
        google::InitGoogleLogging("RDMALoopbackTest");
        FLAGS_logtostderr = 1;
        engine = std::make_unique<TransferEngine>(true);
        engine->init(FLAGS_metadata_server, "test_node");
        addr = numa_alloc_onnode(ram_buffer_size, 0);
        int rc = engine->registerLocalMemory(addr, ram_buffer_size, "cpu:0");
        ASSERT_EQ(rc, 0);
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        engine->unregisterLocalMemory(addr);
        numa_free(addr, ram_buffer_size);
    }
};

TEST_F(RDMALoopbackTest, MultiWrite) {
    const size_t kDataLength = 4096000;
    int times = 200;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char *)(addr) + offset) = 'a' + lrand48() % 26;
        auto batch_id = engine->allocateBatchID(1);
        Status s;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = LOCAL_SEGMENT_ID;
        entry.target_offset = (uint64_t)addr + kDataLength;
        s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = engine->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                LOG(INFO) << "FAILED";
                completed = true;
            }
        }
        s = engine->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
        ASSERT_EQ(0, memcmp(addr, (char *)addr + kDataLength, kDataLength));
    }
}
}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}