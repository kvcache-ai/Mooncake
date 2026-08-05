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

#include "tent/transport/rdma/slice.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace tent {
namespace {

TEST(RdmaCancelTest, PartialCancellationWaitsForPostedSlices) {
    RdmaTask task{};
    task.num_slices = 2;
    task.status_word = PENDING;
    task.transferred_bytes = 0;
    task.success_slices.store(0);
    task.resolved_slices.store(0);
    task.first_error = PENDING;
    // Two slice references plus the batch reference. updateSliceStatus drops
    // one reference per resolved slice; the stack object is never deallocated.
    task.ref_count.store(3);

    RdmaSlice canceled{};
    canceled.task = &task;
    canceled.word = PENDING;
    canceled.length = 4096;
    RdmaSlice posted{};
    posted.task = &task;
    posted.word = PENDING;
    posted.length = 8192;

    updateSliceStatus(&canceled, CANCELED);
    EXPECT_EQ(task.status_word, PENDING);
    EXPECT_EQ(task.transferred_bytes, 0u);

    updateSliceStatus(&posted, COMPLETED);
    EXPECT_EQ(task.status_word, CANCELED);
    EXPECT_EQ(task.transferred_bytes, 8192u);
    EXPECT_EQ(task.resolved_slices.load(), 2);
}

TEST(RdmaCancelTest, FullyPostedTaskMayStillComplete) {
    RdmaTask task{};
    task.num_slices = 1;
    task.status_word = PENDING;
    task.transferred_bytes = 0;
    task.success_slices.store(0);
    task.resolved_slices.store(0);
    task.first_error = PENDING;
    task.cancel_requested.store(true);
    task.ref_count.store(2);

    RdmaSlice posted{};
    posted.task = &task;
    posted.word = PENDING;
    posted.length = 4096;
    updateSliceStatus(&posted, COMPLETED);

    EXPECT_EQ(task.status_word, COMPLETED);
    EXPECT_EQ(task.transferred_bytes, 4096u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
