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
#include <pybind11/embed.h>

#include <array>
#include <stdexcept>
#include <string>

#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transfer_engine.h"
#include "transfer_engine/transfer_engine_py.h"

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
    static size_t aliveBatchCount(TransferEngine& engine) {
        return engine.impl_tent_->impl_->aliveBatchCountForTest();
    }
};

}  // namespace mooncake

namespace {

class TransportHintBatchLeakTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(target_.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""),
                  0);
        ASSERT_EQ(initiator_.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""),
                  0);
        ASSERT_TRUE(initiator_.getEngine()->isUsingTent());
        target_name_ = "127.0.0.1:" + std::to_string(target_.getRpcPort());
    }

    size_t aliveBatchCount() {
        return mooncake::TransferEngineImplTestPeer::aliveBatchCount(
            *initiator_.getEngine());
    }

    TransferEnginePy target_;
    TransferEnginePy initiator_;
    std::string target_name_;
    std::array<char, 128> source_{};
    std::array<char, 128> destination_{};
};

TEST_F(TransportHintBatchLeakTest, TransferSyncInvalidHintDoesNotLeakBatch) {
    pybind11::scoped_interpreter interpreter;
    ASSERT_EQ(aliveBatchCount(), 0u);

    EXPECT_THROW(
        initiator_.transferSyncWrite(
            target_name_.c_str(), reinterpret_cast<uintptr_t>(source_.data()),
            reinterpret_cast<uintptr_t>(destination_.data()), source_.size(),
            "not-a-transport"),
        std::invalid_argument);

    EXPECT_EQ(aliveBatchCount(), 0u);
}

TEST_F(TransportHintBatchLeakTest,
       TransferSubmitWriteInvalidHintDoesNotLeakBatch) {
    pybind11::scoped_interpreter interpreter;
    ASSERT_EQ(aliveBatchCount(), 0u);

    EXPECT_THROW(
        initiator_.transferSubmitWrite(
            target_name_.c_str(), reinterpret_cast<uintptr_t>(source_.data()),
            reinterpret_cast<uintptr_t>(destination_.data()), source_.size(),
            "not-a-transport"),
        std::invalid_argument);

    EXPECT_EQ(aliveBatchCount(), 0u);
}

}  // namespace
