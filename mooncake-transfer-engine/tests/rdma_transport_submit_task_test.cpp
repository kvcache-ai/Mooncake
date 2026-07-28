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

// Regression test for RdmaTransport::submitTransferTask()'s
// "memory region not registered" (!found_device) error path: a slice that
// already succeeded device selection and was queued into the function-local
// slices_to_post accumulator must not be deallocated a second time when a
// later slice in the same call fails device selection, since it is already
// owned (and will be freed exactly once) by its TransferTask::slice_list.
//
// This exercises the real, unmodified RdmaTransport::submitTransferTask()
// entry point. It avoids needing a real RDMA device by using a bare
// RdmaContext (construct() never called): submitTransferTask() only checks
// RdmaContext::active(), which defaults to true, for slices that fail device
// selection.
//
// Every request here is a *single* TransferTask whose length spans two
// slice_size blocks: the first offset resolves inside the registered
// buffer (succeeds, queued into slices_to_post), and the second offset
// starts exactly at the buffer's end (fails every retry). This reproduces
// the bug with a single task instead of a task pair, so the only slices
// touching ThreadLocalSliceCache belong to the one task under test -- no
// second task's own slice churns through the shared cache and disturbs the
// recycling queue's ordering.

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "config.h"
#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

namespace {

using SegmentDesc = TransferMetadata::SegmentDesc;
using BufferDesc = TransferMetadata::BufferDesc;

class SubmitTransferTaskTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kBufferAddr = 0x10000;

    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
    std::shared_ptr<RdmaContext> context_;
    uint64_t block_size_ = 0;

    void SetUp() override {
        block_size_ = globalConfig().slice_size;

        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "unit-test-server:1234");

        // construct() is never called: no real device is opened. active()
        // defaults to true, which is all submitTransferTask() checks.
        context_ = std::make_shared<RdmaContext>(*transport_, "mlx5_unit_test");
        RdmaTransportTestPeer::addContext(*transport_, context_);

        auto desc = std::make_shared<SegmentDesc>();
        desc->name = "unit-test-server:1234";
        desc->protocol = "rdma";
        BufferDesc buffer;
        buffer.name = "cpu:0";
        buffer.addr = kBufferAddr;
        // Exactly one slice_size: a request longer than this has its first
        // slice land fully inside the buffer and its second slice start
        // exactly at the (unregistered) byte past the end.
        buffer.length = block_size_;
        buffer.lkey = {1};
        buffer.rkey = {1};
        desc->buffers.push_back(buffer);
        ASSERT_EQ(
            desc->topology.parse(R"({"cpu:0": [["mlx5_unit_test"], []]})"), 0);
        metadata_->addLocalSegment(LOCAL_SEGMENT_ID, desc->name,
                                   std::move(desc));
    }

    // Submits one task whose request spans 2 slice_size blocks starting at
    // kBufferAddr. The first slice (offset 0) resolves inside the
    // registered buffer and succeeds; the second slice (offset
    // block_size_) starts past the buffer's end and fails every retry, so
    // the call always returns early via the !found_device branch -- before
    // ever reaching the end-of-function flush that would otherwise call
    // into the (unconstructed, null) WorkerPool.
    //
    // `req`/`task` are owned by the caller and deliberately left alive on
    // return, so the caller controls exactly when (if ever) the task's
    // slices are returned to ThreadLocalSliceCache.
    void triggerBug(Transport::TransferRequest &req,
                    Transport::TransferTask &task) {
        req.opcode = Transport::TransferRequest::WRITE;
        req.source = reinterpret_cast<void *>(kBufferAddr);
        req.length = 2 * block_size_;
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = 0;
        task.request = &req;

        auto status = transport_->submitTransferTask({&task});
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(status.IsAddressNotRegistered());
        // slice_list[0]: the first slice, in-buffer, succeeded and was
        // queued into slices_to_post -- the victim of the bug.
        // slice_list[1]: the second slice, out-of-buffer, failed and
        // triggered the (buggy) cleanup; never added to slices_to_post,
        // so it is not itself double-freed.
        ASSERT_EQ(task.slice_list.size(), 2u);
    }
};

// task1's first slice is queued for reuse twice in a row, with nothing else
// touching the cache in between: once by the buggy manual deallocate()
// inside submitTransferTask() (while task1 is still alive), and again by
// task1's own ~TransferTask() moments later. The recycling cache now holds
// two entries for that single object.
//
// task2's own call then draws from that corrupted cache twice in a row (its
// offset-0 slice, then its offset-block_size slice): with a correctly
// single-registered cache, task1's two slices (each freed exactly once)
// supply exactly two distinct pool entries, so task2's two independent
// draws come back as two distinct Slice objects -- one reusing task1's
// first slice, the other reusing its second. With the duplicate
// registration, both draws instead return task1's original object --
// proving it was freed more than once, purely by pointer comparison, with
// no forced crash or process teardown required. EXPECT_EQ(task2[0], original)
// is not itself the bug signal (a *single* legitimate free of `original`
// would also make the very next allocation reuse it); it exists to
// document that task2 is
// indeed the one hitting the corrupted state before the discriminating
// check below.
TEST_F(SubmitTransferTaskTest, FoundDeviceFailureFreesSliceTwice) {
    Transport::Slice *original = nullptr;
    {
        Transport::TransferRequest req1;
        Transport::TransferTask task1;
        triggerBug(req1, task1);
        original = task1.slice_list[0];
        // task1 destroyed here: ~TransferTask() deallocate()s both of its
        // slices, including `original` a second time -- immediately after
        // the buggy manual deallocate() inside submitTransferTask() already
        // did so once, with nothing else in between since this task's own
        // second (failed) slice was never added to slices_to_post and so
        // was never touched by the bug.
    }

    Transport::TransferRequest req2;
    Transport::TransferTask task2;
    triggerBug(req2, task2);

    EXPECT_EQ(task2.slice_list[0], original)
        << "sanity check: the cache should have at least one legitimate "
           "reuse of `original` queued up regardless of the bug";
    EXPECT_NE(task2.slice_list[0], task2.slice_list[1])
        << "task2's two slices cover disjoint offsets of the same request "
           "and must never be backed by the same Slice object; "
           "submitTransferTask() double-freed task1's slice on its "
           "!found_device error path, so the recycling cache handed the "
           "exact same, still-conceptually-owned pointer out twice in a "
           "row for what should have been two independent allocations";
}

}  // namespace
