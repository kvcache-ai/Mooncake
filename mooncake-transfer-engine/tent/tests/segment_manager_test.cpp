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

// Regression tests for issue #2477: concurrent registerLocalMemory /
// unregisterLocalMemory racing with lock-free readers of the local
// SegmentDesc. The local desc is published as immutable copy-on-write
// snapshots; these tests assert the snapshot semantics (readers never
// observe torn or unsorted buffer lists) and, when built with
// -fsanitize=thread, additionally prove the absence of data races between
// SegmentTracker writers and getLocal() / getLocalDumpedJson() readers.
// A port of ConcurrentWritersVsSnapshotReaders to the pre-fix in-place
// mutation API fails its invariants on stock builds and reports data races
// under TSAN.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "tent/runtime/segment_manager.h"
#include "tent/runtime/segment_registry.h"
#include "tent/runtime/segment_tracker.h"

namespace mooncake {
namespace tent {

namespace {

std::unique_ptr<SegmentManager> makeManager() {
    // No registry: these tests never touch remote segments or
    // synchronizeLocal().
    auto manager = std::make_unique<SegmentManager>(nullptr);
    EXPECT_TRUE(manager
                    ->updateLocal([](SegmentDesc& desc) -> Status {
                        desc.name = "local_test_segment";
                        desc.type = SegmentType::Memory;
                        desc.machine_id = "test_machine";
                        return Status::OK();
                    })
                    .ok());
    return manager;
}

// Location derived from the buffer address; readers use it to detect
// partially-constructed or torn BufferDesc entries.
std::string locationFor(uint64_t addr) {
    return "cpu:" + std::to_string(addr % 4096);
}

BufferDesc makeBuffer(uint64_t addr, uint64_t length) {
    BufferDesc desc;
    desc.addr = addr;
    desc.length = length;
    desc.location = locationFor(addr);
    desc.ref_count = 1;
    return desc;
}

void expectSameBufferDesc(const BufferDesc& actual,
                          const BufferDesc& expected) {
    EXPECT_EQ(actual.addr, expected.addr);
    EXPECT_EQ(actual.length, expected.length);
    EXPECT_EQ(actual.location, expected.location);
    EXPECT_EQ(actual.transports, expected.transports);
    EXPECT_EQ(actual.transport_attrs, expected.transport_attrs);
    EXPECT_EQ(actual.ref_count, expected.ref_count);
    EXPECT_EQ(actual.rkey, expected.rkey);
    EXPECT_EQ(actual.shm_path, expected.shm_path);
    EXPECT_EQ(actual.mnnvl_handle, expected.mnnvl_handle);
    EXPECT_EQ(actual.lkey, expected.lkey);
    EXPECT_EQ(actual.internal, expected.internal);
}

const std::vector<BufferDesc>& buffersOf(const SegmentDescRef& snapshot) {
    return std::get<MemorySegmentDesc>(snapshot->detail).buffers;
}

}  // namespace

TEST(SegmentManagerTest, UpdateLocalPublishesImmutableSnapshots) {
    auto manager = makeManager();
    auto before = manager->getLocal();
    ASSERT_EQ(before->name, "local_test_segment");
    ASSERT_TRUE(buffersOf(before).empty());

    ASSERT_TRUE(manager
                    ->updateLocal([](SegmentDesc& desc) -> Status {
                        auto& detail = std::get<MemorySegmentDesc>(desc.detail);
                        detail.buffers.push_back(makeBuffer(0x1000, 0x1000));
                        return Status::OK();
                    })
                    .ok());

    // The old snapshot is untouched; the new snapshot sees the mutation.
    EXPECT_TRUE(buffersOf(before).empty());
    auto after = manager->getLocal();
    ASSERT_EQ(buffersOf(after).size(), 1u);
    EXPECT_EQ(buffersOf(after)[0].addr, 0x1000u);
    EXPECT_NE(before.get(), after.get());
}

TEST(SegmentManagerTest, UpdateLocalFailureDoesNotPublish) {
    auto manager = makeManager();
    auto before = manager->getLocal();
    auto status = manager->updateLocal([](SegmentDesc& desc) -> Status {
        desc.name = "must_not_be_published";
        return Status::InvalidArgument("injected failure");
    });
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(manager->getLocal().get(), before.get());
    EXPECT_EQ(manager->getLocal()->name, "local_test_segment");
}

TEST(SegmentManagerTest, JsonCacheInvalidatedOnPublication) {
    auto manager = makeManager();
    auto dump1 = manager->getLocalDumpedJson();
    auto dump2 = manager->getLocalDumpedJson();
    EXPECT_EQ(dump1.get(), dump2.get());  // served from cache

    ASSERT_TRUE(manager
                    ->updateLocal([](SegmentDesc& desc) -> Status {
                        auto& detail = std::get<MemorySegmentDesc>(desc.detail);
                        detail.buffers.push_back(makeBuffer(0x2000, 0x1000));
                        return Status::OK();
                    })
                    .ok());

    auto dump3 = manager->getLocalDumpedJson();
    EXPECT_NE(*dump1, *dump3);
    EXPECT_NE(dump3->find("8192"), std::string::npos);  // 0x2000 serialized
}

TEST(SegmentTrackerTest, RefCountedAddRemove) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);

    auto noop = [](std::vector<BufferDesc>&) -> Status { return Status::OK(); };
    std::vector<BufferDesc> first{makeBuffer(0x10000, 0x1000)};
    std::vector<BufferDesc> rollback_removed;
    ASSERT_TRUE(tracker.addInBatch(first, noop, rollback_removed).ok());
    EXPECT_TRUE(rollback_removed.empty());
    std::vector<BufferDesc> second{makeBuffer(0x10000, 0x1000)};
    ASSERT_TRUE(tracker.addInBatch(second, noop, rollback_removed).ok());
    EXPECT_TRUE(rollback_removed.empty());

    auto snapshot = manager->getLocal();
    ASSERT_EQ(buffersOf(snapshot).size(), 1u);
    EXPECT_EQ(buffersOf(snapshot)[0].ref_count, 2);

    int remove_callbacks = 0;
    auto on_remove = [&](BufferDesc&) -> Status {
        remove_callbacks++;
        return Status::OK();
    };
    ASSERT_TRUE(tracker.remove(0x10000, 0x1000, on_remove).ok());
    EXPECT_EQ(remove_callbacks, 0);  // still referenced
    ASSERT_EQ(buffersOf(manager->getLocal()).size(), 1u);

    ASSERT_TRUE(tracker.remove(0x10000, 0x1000, on_remove).ok());
    EXPECT_EQ(remove_callbacks, 1);
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
}

TEST(SegmentTrackerTest, AddInBatchCallbackFailureRollsBackRefCounts) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);

    auto ok = [](std::vector<BufferDesc>&) -> Status { return Status::OK(); };
    std::vector<BufferDesc> first{makeBuffer(0x20000, 0x1000)};
    std::vector<BufferDesc> rollback_removed;
    ASSERT_TRUE(tracker.addInBatch(first, ok, rollback_removed).ok());
    EXPECT_TRUE(rollback_removed.empty());
    ASSERT_EQ(buffersOf(manager->getLocal())[0].ref_count, 1);

    // A duplicate registration whose transport callback fails must not leave
    // the ref-count bumped, or the buffer could never be deregistered.
    auto fail = [](std::vector<BufferDesc>&) -> Status {
        return Status::InternalError("injected transport failure");
    };
    std::vector<BufferDesc> dup{makeBuffer(0x20000, 0x1000)};
    EXPECT_FALSE(tracker.addInBatch(dup, fail, rollback_removed).ok());
    EXPECT_TRUE(rollback_removed.empty());
    ASSERT_EQ(buffersOf(manager->getLocal()).size(), 1u);
    EXPECT_EQ(buffersOf(manager->getLocal())[0].ref_count, 1);

    int remove_callbacks = 0;
    auto on_remove = [&](BufferDesc&) -> Status {
        remove_callbacks++;
        return Status::OK();
    };
    ASSERT_TRUE(tracker.remove(0x20000, 0x1000, on_remove).ok());
    EXPECT_EQ(remove_callbacks, 1);
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
}

TEST(SegmentTrackerTest, FailedBatchHandsOffOnlyRemovedDuplicateDescriptors) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);

    BufferDesc first = makeBuffer(0x30000, 0x1000);
    first.transports = {HP_TCP, TCP};
    first.transport_attrs[HP_TCP] = "hp-first";
    first.rkey = {7};
    first.shm_path = "shm-first";
    first.mnnvl_handle = "mnnvl-first";
    first.lkey = {11};
    first.internal = true;
    BufferDesc second = makeBuffer(0x40000, 0x2000);
    second.transports = {HP_TCP};
    second.transport_attrs[HP_TCP] = "hp-second";
    second.rkey = {13};
    second.lkey = {17};
    second.internal = true;

    auto ok = [](std::vector<BufferDesc>&) -> Status { return Status::OK(); };
    std::vector<BufferDesc> initial{first, second};
    std::vector<BufferDesc> rollback_removed;
    ASSERT_TRUE(tracker.addInBatch(initial, ok, rollback_removed).ok());
    EXPECT_TRUE(rollback_removed.empty());

    auto fail_after_owner_unregister =
        [&](std::vector<BufferDesc>& new_descs) -> Status {
        EXPECT_TRUE(new_descs.empty());
        int remove_callbacks = 0;
        auto on_remove = [&](BufferDesc&) -> Status {
            ++remove_callbacks;
            return Status::OK();
        };
        EXPECT_TRUE(tracker.remove(first.addr, first.length, on_remove).ok());
        EXPECT_TRUE(tracker.remove(second.addr, second.length, on_remove).ok());
        EXPECT_EQ(remove_callbacks, 0);
        return Status::InternalError("injected transport failure");
    };
    std::vector<BufferDesc> duplicates{
        makeBuffer(first.addr, first.length),
        makeBuffer(first.addr, first.length),
        makeBuffer(second.addr, second.length),
    };
    EXPECT_FALSE(tracker
                     .addInBatch(duplicates, fail_after_owner_unregister,
                                 rollback_removed)
                     .ok());
    ASSERT_EQ(rollback_removed.size(), 2u);
    first.ref_count = 0;
    second.ref_count = 0;
    expectSameBufferDesc(rollback_removed[0], first);
    expectSameBufferDesc(rollback_removed[1], second);
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
}

TEST(SegmentTrackerTest, NestedFailedBatchesHandOffOnlyFromLastRelease) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);

    auto ok = [](std::vector<BufferDesc>&) -> Status { return Status::OK(); };
    BufferDesc buffer = makeBuffer(0x50000, 0x1000);
    buffer.transports = {HP_TCP};
    buffer.transport_attrs[HP_TCP] = "hp";
    std::vector<BufferDesc> initial{buffer};
    std::vector<BufferDesc> rollback_removed;
    ASSERT_TRUE(tracker.addInBatch(initial, ok, rollback_removed).ok());

    auto outer_fail = [&](std::vector<BufferDesc>& outer_new_descs) -> Status {
        EXPECT_TRUE(outer_new_descs.empty());
        std::vector<BufferDesc> inner{makeBuffer(buffer.addr, buffer.length)};
        std::vector<BufferDesc> inner_removed;
        auto inner_fail =
            [&](std::vector<BufferDesc>& inner_new_descs) -> Status {
            EXPECT_TRUE(inner_new_descs.empty());
            int remove_callbacks = 0;
            auto on_remove = [&](BufferDesc&) -> Status {
                ++remove_callbacks;
                return Status::OK();
            };
            EXPECT_TRUE(
                tracker.remove(buffer.addr, buffer.length, on_remove).ok());
            EXPECT_EQ(remove_callbacks, 0);
            return Status::InternalError("inner failure");
        };
        EXPECT_FALSE(tracker.addInBatch(inner, inner_fail, inner_removed).ok());
        EXPECT_TRUE(inner_removed.empty());
        return Status::InternalError("outer failure");
    };
    std::vector<BufferDesc> outer{makeBuffer(buffer.addr, buffer.length)};
    EXPECT_FALSE(tracker.addInBatch(outer, outer_fail, rollback_removed).ok());
    ASSERT_EQ(rollback_removed.size(), 1u);
    buffer.ref_count = 0;
    expectSameBufferDesc(rollback_removed[0], buffer);
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
}

TEST(SegmentTrackerTest, AddProbesRealMemoryAndRefCounts) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);

    constexpr size_t kSize = 1 << 20;
    void* mem = malloc(kSize);
    ASSERT_NE(mem, nullptr);
    auto base = reinterpret_cast<uint64_t>(mem);

    int add_callbacks = 0;
    auto on_add = [&](BufferDesc& desc) -> Status {
        add_callbacks++;
        EXPECT_EQ(desc.addr, base);
        EXPECT_FALSE(desc.location.empty());  // NUMA probe ran
        return Status::OK();
    };
    ASSERT_TRUE(tracker.add(base, kSize, on_add).ok());
    EXPECT_EQ(add_callbacks, 1);
    // Re-adding the same range takes the ref-count fast path: no new probe.
    ASSERT_TRUE(tracker.add(base, kSize, on_add).ok());
    EXPECT_EQ(add_callbacks, 1);
    ASSERT_EQ(buffersOf(manager->getLocal()).size(), 1u);
    EXPECT_EQ(buffersOf(manager->getLocal())[0].ref_count, 2);

    auto noop = [](BufferDesc&) -> Status { return Status::OK(); };
    ASSERT_TRUE(tracker.remove(base, kSize, noop).ok());
    ASSERT_TRUE(tracker.remove(base, kSize, noop).ok());
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
    free(mem);
}

// The actual #2477 regression: register/unregister churn concurrent with
// lock-free snapshot readers. With in-place mutation this is a data race on
// MemorySegmentDesc::buffers (vector push_back/erase/sort vs. iteration):
// ThreadSanitizer reports it and the location invariant below can observe
// partially-constructed entries. With copy-on-write snapshots every reader
// observes a fully-consistent (possibly stale) buffer list.
TEST(SegmentTrackerTest, ConcurrentWritersVsSnapshotReaders) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);

    constexpr int kWriters = 4;
    constexpr int kReaders = 4;
    constexpr int kIterations = 300;
    constexpr int kBuffersPerBatch = 8;
    constexpr uint64_t kLength = 0x1000;

    std::atomic<bool> done{false};
    std::atomic<int> failures{0};

    auto noop = [](std::vector<BufferDesc>&) -> Status { return Status::OK(); };

    std::vector<std::thread> writers;
    writers.reserve(kWriters);
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            // Writers 0 and 1 share an address range so ref-count bumps,
            // duplicate-registration races and erase-vs-bump interleavings
            // are exercised concurrently, not just disjoint inserts.
            const uint64_t base = (w < 2 ? 1 : w + 1) * 0x100000000ULL;
            for (int iter = 0; iter < kIterations; ++iter) {
                std::vector<BufferDesc> batch;
                batch.reserve(kBuffersPerBatch);
                for (int i = 0; i < kBuffersPerBatch; ++i) {
                    batch.push_back(
                        makeBuffer(base + i * kLength * 2, kLength));
                }
                std::vector<BufferDesc> rollback_removed;
                if (!tracker.addInBatch(batch, noop, rollback_removed).ok())
                    failures++;
                if (!rollback_removed.empty()) failures++;
                for (int i = 0; i < kBuffersPerBatch; ++i) {
                    auto on_remove = [](BufferDesc&) -> Status {
                        return Status::OK();
                    };
                    if (!tracker
                             .remove(base + i * kLength * 2, kLength, on_remove)
                             .ok())
                        failures++;
                }
            }
        });
    }

    std::vector<std::thread> readers;
    readers.reserve(kReaders);
    for (int r = 0; r < kReaders; ++r) {
        readers.emplace_back([&, r] {
            uint64_t rounds = 0;
            while (!done.load(std::memory_order_acquire)) {
                auto snapshot = manager->getLocal();
                const auto& buffers = buffersOf(snapshot);
                uint64_t prev_addr = 0;
                for (const auto& buf : buffers) {
                    // Entries must be fully constructed and sorted; a torn
                    // read of an in-place mutated vector violates these.
                    if (buf.length != kLength ||
                        buf.location != locationFor(buf.addr) ||
                        buf.addr < prev_addr) {
                        failures++;
                    }
                    prev_addr = buf.addr;
                }
                // Exercise findBuffer() through the snapshot as transports
                // do, and the JSON dump path peers hit via GetSegmentDesc.
                snapshot->findBuffer(0x100000000ULL, kLength);
                if (rounds++ % 64 == 0) {
                    auto dump = manager->getLocalDumpedJson();
                    if (!dump || dump->empty()) failures++;
                }
            }
        });
    }

    for (auto& t : writers) t.join();
    done.store(true, std::memory_order_release);
    for (auto& t : readers) t.join();

    EXPECT_EQ(failures.load(), 0);
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
}

}  // namespace tent
}  // namespace mooncake
