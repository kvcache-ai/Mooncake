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

#include "tent/runtime/admission_queue.h"

#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tent {
namespace {

QueueSubmit makeSubmit(uint64_t batch_token, size_t batch_slots_left,
                       std::vector<QueueOwnerInput> owners) {
    QueueSubmit submit;
    submit.batch_token = batch_token;
    submit.batch_slots_left = batch_slots_left;
    submit.owners = std::move(owners);
    return submit;
}

TEST(AdmissionQueueTest, AllowsEmptySubmitAsNoOp) {
    LocalTransferAdmissionQueue queue({2, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids{99};

    auto status = queue.tryAdmit(makeSubmit(1, 0, {}), admitted_ids);

    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_TRUE(admitted_ids.empty());
    EXPECT_EQ(queue.outstandingOwners(), 0u);
    EXPECT_EQ(queue.outstandingBytes(), 0u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
