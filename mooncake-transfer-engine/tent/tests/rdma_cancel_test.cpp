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

#include <atomic>
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

TEST(RdmaCancelTest, LastSlicePublishesFinishedTaskCount) {
    std::atomic<uint64_t> finished{0};
    std::atomic<bool> has_failure{false};
    RdmaTask task{};
    task.num_slices = 2;
    task.status_word = PENDING;
    task.transferred_bytes = 0;
    task.success_slices.store(0);
    task.resolved_slices.store(0);
    task.first_error = PENDING;
    task.finished_counter = &finished;
    task.has_failure_flag = &has_failure;
    task.ref_count.store(3);

    RdmaSlice first{};
    first.task = &task;
    first.word = PENDING;
    first.length = 64;
    RdmaSlice second{};
    second.task = &task;
    second.word = PENDING;
    second.length = 64;

    updateSliceStatus(&first, COMPLETED);
    EXPECT_EQ(task.status_word, PENDING);
    EXPECT_EQ(finished.load(), 0u);
    EXPECT_FALSE(has_failure.load());

    updateSliceStatus(&second, COMPLETED);
    EXPECT_EQ(task.status_word, COMPLETED);
    EXPECT_EQ(finished.load(), 1u);
    EXPECT_FALSE(has_failure.load());

    updateSliceStatus(&second, COMPLETED);
    EXPECT_EQ(finished.load(), 1u);
}

TEST(RdmaCancelTest, FailedLastSliceSetsHasFailure) {
    std::atomic<uint64_t> finished{0};
    std::atomic<bool> has_failure{false};
    RdmaTask task{};
    task.num_slices = 1;
    task.status_word = PENDING;
    task.transferred_bytes = 0;
    task.success_slices.store(0);
    task.resolved_slices.store(0);
    task.first_error = PENDING;
    task.finished_counter = &finished;
    task.has_failure_flag = &has_failure;
    task.ref_count.store(2);

    RdmaSlice slice{};
    slice.task = &task;
    slice.word = PENDING;
    slice.length = 64;
    updateSliceStatus(&slice, FAILED);

    EXPECT_EQ(task.status_word, FAILED);
    EXPECT_EQ(finished.load(), 0u);
    EXPECT_TRUE(has_failure.load());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
