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

// Registering HIP device memory for RDMA must not shrink the KFD free VRAM
// that hipMemGetInfo reports and new device allocations draw from. amdgpu
// before 6.19.14 charges a NIC's dma-buf pin of KFD memory a second time, so
// without the KFD pin anchor (MC_HIP_KFD_PIN_FIX) a prefill node whose KV
// cache exceeded its remaining headroom could no longer load a kernel
// after registering it (HSA OUT_OF_RESOURCES, surfaced as HIP error 209).
//
// Needs an AMD GPU, an RDMA NIC and the amdgpu peer-memory client. Pick NICs
// with MC_TEST_NICS=nic0[,nic1]; all NICs are used otherwise.

#include <gtest/gtest.h>
#include <hip/hip_runtime.h>

#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "transfer_engine.h"

using namespace mooncake;

namespace {

constexpr size_t kBufferSize = 4ull << 30;
// KFD reports availability in 2 MiB steps; allow for runtime allocations.
constexpr size_t kSlack = 64ull << 20;

size_t kfdFreeVram() {
    size_t free = 0, total = 0;
    EXPECT_EQ(hipMemGetInfo(&free, &total), hipSuccess);
    return free;
}

void expectUnchanged(size_t before, const char *step) {
    size_t now = kfdFreeVram();
    EXPECT_LE(before, now + kSlack)
        << step << ": registered VRAM charged twice";
    EXPECT_LE(now, before + kSlack)
        << step << ": registered VRAM offset more than once";
}

std::vector<std::string> testNics() {
    std::vector<std::string> nics;
    if (const char *env = std::getenv("MC_TEST_NICS")) {
        std::stringstream ss(env);
        for (std::string nic; std::getline(ss, nic, ',');)
            if (!nic.empty()) nics.push_back(nic);
    }
    return nics;
}

class RdmaHipKfdPinTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(hipSetDevice(0), hipSuccess);
        if (kfdFreeVram() < kBufferSize + kSlack)
            GTEST_SKIP() << "needs " << ((kBufferSize + kSlack) >> 20)
                         << " MiB of free VRAM";
        engine_ = std::make_unique<TransferEngine>(true, testNics());
        ASSERT_EQ(engine_->init("P2PHANDSHAKE", "kfd_pin_test"), 0);
        ASSERT_EQ(hipMalloc(&buf_, kBufferSize), hipSuccess);
    }

    void TearDown() override {
        engine_.reset();
        if (buf_) EXPECT_EQ(hipFree(buf_), hipSuccess);
    }

    std::unique_ptr<TransferEngine> engine_;
    void *buf_ = nullptr;
};

TEST_F(RdmaHipKfdPinTest, RegistrationKeepsKfdFreeVram) {
    const size_t before = kfdFreeVram();
    ASSERT_EQ(engine_->registerLocalMemory(buf_, kBufferSize), 0);
    expectUnchanged(before, "registered");
    ASSERT_EQ(engine_->unregisterLocalMemory(buf_), 0);
    expectUnchanged(before, "unregistered");
}

// Buffers inside one allocation share its pin until the last one goes.
TEST_F(RdmaHipKfdPinTest, SubBuffersShareAllocationPin) {
    const size_t half = kBufferSize / 2;
    void *hi = static_cast<char *>(buf_) + half;
    const size_t before = kfdFreeVram();
    ASSERT_EQ(engine_->registerLocalMemory(buf_, half), 0);
    ASSERT_EQ(engine_->registerLocalMemory(hi, half), 0);
    expectUnchanged(before, "both registered");
    ASSERT_EQ(engine_->unregisterLocalMemory(buf_), 0);
    expectUnchanged(before, "one unregistered");
    ASSERT_EQ(engine_->unregisterLocalMemory(hi), 0);
    expectUnchanged(before, "both unregistered");
}

}  // namespace
