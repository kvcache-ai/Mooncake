// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
//
// The four checks alogfans reproduced against the unregister scanner on
// 2026-11-16: a one-byte probe misses an interior in-flight range, a scan
// must not read the caller's TransferRequest after submit returns, publish
// and scan share the shard lock, and aligned BatchDesc addresses must not
// collapse onto a handful of shards.

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <set>
#include <thread>
#include <vector>

#include "multi_transport.h"

namespace mooncake {

class UnregisterQuiesceTestPeer {
   public:
    static size_t shard(Transport::BatchID batch_id) {
        return MultiTransport::liveBatchShard(batch_id);
    }

    static void track(MultiTransport& mt, Transport::BatchID batch_id) {
        mt.trackLiveBatch(batch_id);
    }

    static void begin(MultiTransport& mt, Transport::BatchID batch_id) {
        mt.beginQuiescePublish(batch_id);
    }

    static void commit(MultiTransport& mt, Transport::BatchID batch_id,
                       const Transport::BatchDesc::QuiesceSpan* spans,
                       size_t count) {
        mt.commitQuiesceSpans(batch_id, spans, count);
    }

    static bool referenced(MultiTransport& mt, Transport::BatchID batch_id,
                           void* addr, size_t length) {
        return mt.memoryReferencedByBatch(batch_id, addr, length);
    }

    static bool busy(const MultiTransport& mt, void* addr, size_t length) {
        return mt.memoryBusy(addr, length);
    }
};

namespace {

Transport::BatchDesc* newBatch() {
    auto* batch = new Transport::BatchDesc;
    batch->id = reinterpret_cast<Transport::BatchID>(batch);
    batch->batch_size = 1;
    batch->task_list.resize(1);
    batch->task_list[0].slice_count = 1;
    return batch;
}

TEST(UnregisterQuiesce, OneByteProbeMissesInteriorInFlightRange) {
    std::string name = "unregister-quiesce";
    MultiTransport mt(nullptr, name);
    auto* batch = newBatch();
    UnregisterQuiesceTestPeer::track(mt, batch->id);

    alignas(4096) unsigned char region[4096];
    const auto base = reinterpret_cast<uintptr_t>(region);
    Transport::BatchDesc::QuiesceSpan span{base + 1024, 128, 0};
    UnregisterQuiesceTestPeer::commit(mt, batch->id, &span, 1);

    EXPECT_FALSE(UnregisterQuiesceTestPeer::referenced(mt, batch->id, region,
                                                       /*length=*/0));
    EXPECT_TRUE(UnregisterQuiesceTestPeer::referenced(mt, batch->id, region,
                                                      /*length=*/4096));
    EXPECT_TRUE(UnregisterQuiesceTestPeer::busy(mt, region, 4096));

    UnregisterQuiesceTestPeer::track(mt, batch->id);
    delete batch;
}

TEST(UnregisterQuiesce, ScanReadsOwnedSpanAfterCallerRequestIsFreed) {
    std::string name = "unregister-quiesce";
    MultiTransport mt(nullptr, name);
    auto* batch = newBatch();
    UnregisterQuiesceTestPeer::track(mt, batch->id);

    alignas(4096) unsigned char region[4096];
    Transport::BatchDesc::QuiesceSpan span{};
    {
        auto requests =
            std::make_unique<std::vector<Transport::TransferRequest>>(1);
        (*requests)[0].source = region + 64;
        (*requests)[0].length = 32;
        span.addr = reinterpret_cast<uintptr_t>((*requests)[0].source);
        span.length = (*requests)[0].length;
        span.task_index = 0;
        batch->task_list[0].request = &(*requests)[0];
        UnregisterQuiesceTestPeer::commit(mt, batch->id, &span, 1);
    }
    batch->task_list[0].request = nullptr;

    EXPECT_TRUE(
        UnregisterQuiesceTestPeer::referenced(mt, batch->id, region, 4096));
    delete batch;
}

TEST(UnregisterQuiesce, PublishAndScanShareTheShardLock) {
    std::string name = "unregister-quiesce";
    MultiTransport mt(nullptr, name);
    auto* batch = newBatch();
    UnregisterQuiesceTestPeer::track(mt, batch->id);

    alignas(4096) unsigned char region[4096];
    const auto base = reinterpret_cast<uintptr_t>(region);
    std::atomic<bool> start{false};
    std::atomic<int> publishes{0};

    std::thread writer([&] {
        while (!start.load()) {
        }
        for (int i = 0; i < 200; ++i) {
            UnregisterQuiesceTestPeer::begin(mt, batch->id);
            Transport::BatchDesc::QuiesceSpan span{base + 32, 64, 0};
            UnregisterQuiesceTestPeer::commit(mt, batch->id, &span, 1);
            publishes.fetch_add(1);
        }
    });
    std::thread reader([&] {
        while (!start.load()) {
        }
        for (int i = 0; i < 200; ++i) {
            (void)UnregisterQuiesceTestPeer::busy(mt, region, 4096);
        }
    });
    start.store(true);
    writer.join();
    reader.join();

    EXPECT_EQ(publishes.load(), 200);
    EXPECT_TRUE(UnregisterQuiesceTestPeer::busy(mt, region, 4096));
    delete batch;
}

TEST(UnregisterQuiesce, AlignedBatchAddressesSpreadAcrossShards) {
    std::set<size_t> buckets;
    std::vector<Transport::BatchDesc*> batches;
    batches.reserve(256);
    for (int i = 0; i < 256; ++i) {
        auto* batch = new Transport::BatchDesc;
        batches.push_back(batch);
        buckets.insert(UnregisterQuiesceTestPeer::shard(
            reinterpret_cast<Transport::BatchID>(batch)));
    }
    for (auto* batch : batches) delete batch;
    EXPECT_GT(buckets.size(), 4u);
}

}  // namespace
}  // namespace mooncake
