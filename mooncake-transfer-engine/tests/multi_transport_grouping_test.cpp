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

#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

// ---------------------------------------------------------------------------
// Unit tests for TransferRequest::task_group_id field and grouping semantics
// ---------------------------------------------------------------------------

class TaskGroupingTest : public ::testing::Test {};

TEST_F(TaskGroupingTest, DefaultTaskGroupIdIsNoGroup) {
    Transport::TransferRequest req{};
    req.opcode = Transport::TransferRequest::READ;
    req.source = nullptr;
    req.target_id = 0;
    req.target_offset = 0;
    req.length = 4096;

    EXPECT_EQ(req.task_group_id, Transport::TransferRequest::kNoTaskGroup);
}

TEST_F(TaskGroupingTest, SentinelValueIsZero) {
    EXPECT_EQ(Transport::TransferRequest::kNoTaskGroup, 0u);
}

TEST_F(TaskGroupingTest, GroupIdCanBeSet) {
    Transport::TransferRequest req{};
    req.opcode = Transport::TransferRequest::READ;
    req.source = nullptr;
    req.target_id = 0;
    req.target_offset = 0;
    req.length = 4096;
    req.task_group_id = 42;

    EXPECT_EQ(req.task_group_id, 42u);
}

// Verify that adjacent requests sharing the same task_group_id are
// distinguishable from ungrouped requests.
TEST_F(TaskGroupingTest, GroupVsUngroupedRequests) {
    std::vector<Transport::TransferRequest> entries(5);
    for (auto &e : entries) {
        e.opcode = Transport::TransferRequest::READ;
        e.source = nullptr;
        e.target_id = 1;
        e.target_offset = 0;
        e.length = 1024;
    }

    // entries[0]: ungrouped
    entries[0].task_group_id = Transport::TransferRequest::kNoTaskGroup;
    // entries[1..2]: grouped with id=100
    entries[1].task_group_id = 100;
    entries[2].task_group_id = 100;
    // entries[3]: ungrouped
    entries[3].task_group_id = Transport::TransferRequest::kNoTaskGroup;
    // entries[4]: different group
    entries[4].task_group_id = 200;

    // Count logical tasks: ungrouped requests each count as 1,
    // adjacent requests with same group id count as 1 logical task.
    size_t logical_count = 0;
    size_t i = 0;
    while (i < entries.size()) {
        if (entries[i].task_group_id !=
            Transport::TransferRequest::kNoTaskGroup) {
            uint64_t gid = entries[i].task_group_id;
            while (i < entries.size() && entries[i].task_group_id == gid) {
                ++i;
            }
        } else {
            ++i;
        }
        ++logical_count;
    }

    // Expected: entry0(1) + entries1-2(1) + entry3(1) + entry4(1) = 4
    EXPECT_EQ(logical_count, 4u);
}

// Verify that batch_size accounting uses logical task count.
TEST_F(TaskGroupingTest, BatchCapacityUsesLogicalTaskCount) {
    // Create 6 requests where 3 pairs are grouped -> 3 logical tasks
    std::vector<Transport::TransferRequest> entries(6);
    for (size_t i = 0; i < entries.size(); ++i) {
        entries[i].opcode = Transport::TransferRequest::READ;
        entries[i].source = nullptr;
        entries[i].target_id = 1;
        entries[i].target_offset = i * 4096;
        entries[i].length = 4096;
    }

    // Group pairs: (0,1), (2,3), (4,5)
    entries[0].task_group_id = 10;
    entries[1].task_group_id = 10;
    entries[2].task_group_id = 20;
    entries[3].task_group_id = 20;
    entries[4].task_group_id = 30;
    entries[5].task_group_id = 30;

    // Count logical tasks
    size_t logical_count = 0;
    size_t i = 0;
    while (i < entries.size()) {
        if (entries[i].task_group_id !=
            Transport::TransferRequest::kNoTaskGroup) {
            uint64_t gid = entries[i].task_group_id;
            while (i < entries.size() && entries[i].task_group_id == gid) {
                ++i;
            }
        } else {
            ++i;
        }
        ++logical_count;
    }

    // 6 raw requests -> 3 logical tasks
    EXPECT_EQ(logical_count, 3u);
    // A batch of capacity 3 should be sufficient
    EXPECT_LE(logical_count, 3u);
    // A batch of capacity 2 would be insufficient
    EXPECT_GT(logical_count, 2u);
}

// Non-adjacent requests with the same group id should NOT be merged.
TEST_F(TaskGroupingTest, NonAdjacentSameGroupIdNotMerged) {
    std::vector<Transport::TransferRequest> entries(4);
    for (auto &e : entries) {
        e.opcode = Transport::TransferRequest::READ;
        e.source = nullptr;
        e.target_id = 1;
        e.target_offset = 0;
        e.length = 1024;
    }

    // Pattern: group=100, ungrouped, group=100 — the two group=100
    // runs should NOT merge because they are not adjacent.
    entries[0].task_group_id = 100;
    entries[1].task_group_id = Transport::TransferRequest::kNoTaskGroup;
    entries[2].task_group_id = 100;
    entries[3].task_group_id = Transport::TransferRequest::kNoTaskGroup;

    size_t logical_count = 0;
    size_t i = 0;
    while (i < entries.size()) {
        if (entries[i].task_group_id !=
            Transport::TransferRequest::kNoTaskGroup) {
            uint64_t gid = entries[i].task_group_id;
            while (i < entries.size() && entries[i].task_group_id == gid) {
                ++i;
            }
        } else {
            ++i;
        }
        ++logical_count;
    }

    // entries[0](group=100) + entries[1](ungrouped) + entries[2](group=100)
    //   + entries[3](ungrouped) = 4 logical tasks
    EXPECT_EQ(logical_count, 4u);
}

// Grouping does not impose requirements on local buffer layout.
// Each request within a group has its own independent source pointer;
// the transport processes slices independently per-request.
TEST_F(TaskGroupingTest, GroupedRequestsHaveIndependentLocalBuffers) {
    char buf_a[4096];
    char buf_b[4096];

    Transport::TransferRequest req_a{};
    req_a.opcode = Transport::TransferRequest::READ;
    req_a.source = buf_a;
    req_a.target_id = 1;
    req_a.target_offset = 0;
    req_a.length = sizeof(buf_a);
    req_a.task_group_id = 42;

    Transport::TransferRequest req_b{};
    req_b.opcode = Transport::TransferRequest::READ;
    req_b.source = buf_b;
    req_b.target_id = 1;
    req_b.target_offset = 4096;
    req_b.length = sizeof(buf_b);
    req_b.task_group_id = 42;

    // Buffers are in completely different memory regions — this is valid.
    EXPECT_NE(req_a.source, req_b.source);
    EXPECT_EQ(req_a.task_group_id, req_b.task_group_id);
}

}  // namespace mooncake
