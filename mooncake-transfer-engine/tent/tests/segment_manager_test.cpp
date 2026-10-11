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

#include <atomic>
#include <cstdlib>
#include <string>
#include <thread>
#include <utility>
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
    ASSERT_TRUE(tracker.addInBatch(first, noop).ok());
    std::vector<BufferDesc> second{makeBuffer(0x10000, 0x1000)};
    ASSERT_TRUE(tracker.addInBatch(second, noop).ok());

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
    ASSERT_TRUE(tracker.addInBatch(first, ok).ok());
    ASSERT_EQ(buffersOf(manager->getLocal())[0].ref_count, 1);

    // A duplicate registration whose transport callback fails must not leave
    // the ref-count bumped, or the buffer could never be deregistered.
    auto fail = [](std::vector<BufferDesc>&) -> Status {
        return Status::InternalError("injected transport failure");
    };
    std::vector<BufferDesc> dup{makeBuffer(0x20000, 0x1000)};
    EXPECT_FALSE(tracker.addInBatch(dup, fail).ok());
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

TEST(SegmentTrackerTest, FreshBatchDuplicatesShareOneRegistration) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);
    auto buffer = makeBuffer(0x40000, 0x1000);
    std::vector<BufferDesc> batch{buffer, buffer};
    size_t registrations = 0;
    ASSERT_TRUE(tracker
                    .addInBatch(batch,
                                [&](std::vector<BufferDesc>& descs) {
                                    registrations += descs.size();
                                    return Status::OK();
                                })
                    .ok());
    EXPECT_EQ(registrations, 1u);
    auto snapshot = manager->getLocal();
    EXPECT_EQ(buffersOf(snapshot).size(), 1u);
    EXPECT_EQ(buffersOf(snapshot).front().ref_count, 2);

    int removals = 0;
    auto on_remove = [&](BufferDesc&) {
        ++removals;
        return Status::OK();
    };
    ASSERT_TRUE(tracker.remove(buffer.addr, buffer.length, on_remove).ok());
    EXPECT_EQ(removals, 0);
    EXPECT_EQ(buffersOf(manager->getLocal()).size(), 1u);
    ASSERT_TRUE(tracker.remove(buffer.addr, buffer.length, on_remove).ok());
    EXPECT_EQ(removals, 1);
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
}

TEST(SegmentTrackerTest, MixedBatchDuplicatesRetainAllReferences) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);
    auto old_buffer = makeBuffer(0x40000, 0x1000);
    auto new_buffer = makeBuffer(0x50000, 0x1000);
    std::vector<BufferDesc> first{old_buffer};
    auto ok = [](std::vector<BufferDesc>&) { return Status::OK(); };
    ASSERT_TRUE(tracker.addInBatch(first, ok).ok());

    // Exercise the path that pins old ranges before registering new ranges.
    std::vector<BufferDesc> mixed{new_buffer, old_buffer, new_buffer,
                                  old_buffer};
    ASSERT_TRUE(tracker
                    .addInBatch(mixed,
                                [&](std::vector<BufferDesc>& descs) {
                                    EXPECT_EQ(descs.size(), 1u);
                                    for (const auto& desc : descs)
                                        EXPECT_EQ(desc.addr, new_buffer.addr);
                                    return Status::OK();
                                })
                    .ok());
    auto snapshot = manager->getLocal();
    EXPECT_EQ(buffersOf(snapshot).size(), 2u);
    EXPECT_EQ(buffersOf(snapshot).front().ref_count, 3);
    EXPECT_EQ(buffersOf(snapshot).back().ref_count, 2);

    for (const auto& item : std::vector<std::pair<BufferDesc, int>>{
             {old_buffer, 3}, {new_buffer, 2}}) {
        int removals = 0;
        for (int reference = 0; reference < item.second; ++reference) {
            ASSERT_TRUE(tracker
                            .remove(item.first.addr, item.first.length,
                                    [&](BufferDesc&) {
                                        ++removals;
                                        return Status::OK();
                                    })
                            .ok());
            EXPECT_EQ(removals, reference == item.second - 1 ? 1 : 0);
        }
    }
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
}

TEST(SegmentTrackerTest, FailedMixedDuplicateBatchRollsBackPins) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);
    auto old_buffer = makeBuffer(0x40000, 0x1000);
    auto new_buffer = makeBuffer(0x50000, 0x1000);
    std::vector<BufferDesc> first{old_buffer};
    auto ok = [](std::vector<BufferDesc>&) { return Status::OK(); };
    ASSERT_TRUE(tracker.addInBatch(first, ok).ok());

    std::vector<BufferDesc> mixed{old_buffer, new_buffer, old_buffer,
                                  new_buffer};
    const auto failure = Status::InternalError("injected registration failure");
    auto status =
        tracker.addInBatch(mixed, [&](std::vector<BufferDesc>& descs) {
            EXPECT_EQ(descs.size(), 1u);
            for (const auto& desc : descs)
                EXPECT_EQ(desc.addr, new_buffer.addr);
            return failure;
        });
    EXPECT_EQ(status.code(), failure.code());
    EXPECT_EQ(status.message(), failure.message());
    auto snapshot = manager->getLocal();
    ASSERT_EQ(buffersOf(snapshot).size(), 1u);
    EXPECT_EQ(buffersOf(snapshot).front().addr, old_buffer.addr);
    EXPECT_EQ(buffersOf(snapshot).front().ref_count, 1);

    int removals = 0;
    ASSERT_TRUE(tracker
                    .remove(old_buffer.addr, old_buffer.length,
                            [&](BufferDesc&) {
                                ++removals;
                                return Status::OK();
                            })
                    .ok());
    EXPECT_EQ(removals, 1);
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
}

TEST(SegmentTrackerTest, BatchDuplicatesPreserveDistinctSizesAndMetadata) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);
    auto full = makeBuffer(0x40000, 0x1000);
    full.location = "cpu:7";
    auto shorter = makeBuffer(full.addr, 0x800);
    shorter.location = "cpu:3";
    std::vector<BufferDesc> batch{full, full, shorter};
    ASSERT_TRUE(tracker
                    .addInBatch(batch,
                                [&](std::vector<BufferDesc>& descs) {
                                    EXPECT_EQ(descs.size(), 2u);
                                    for (auto& desc : descs) {
                                        desc.transports = {TCP};
                                        desc.transport_attrs[TCP] =
                                            "registered-" +
                                            std::to_string(desc.length);
                                    }
                                    return Status::OK();
                                })
                    .ok());
    auto snapshot = manager->getLocal();
    const auto& buffers = buffersOf(snapshot);
    ASSERT_EQ(buffers.size(), 2u);
    EXPECT_EQ(buffers[0].addr, full.addr);
    EXPECT_EQ(buffers[0].length, full.length);
    EXPECT_EQ(buffers[0].ref_count, 2);
    EXPECT_EQ(buffers[0].location, full.location);
    EXPECT_EQ(buffers[1].addr, shorter.addr);
    EXPECT_EQ(buffers[1].length, shorter.length);
    EXPECT_EQ(buffers[1].ref_count, 1);
    EXPECT_EQ(buffers[1].location, shorter.location);
    for (const auto& buffer : buffers) {
        EXPECT_EQ(buffer.transports, std::vector<TransportType>{TCP});
        EXPECT_EQ(buffer.transport_attrs.at(TCP),
                  "registered-" + std::to_string(buffer.length));
    }
}

TEST(SegmentTrackerTest, FailedBatchHandsOffLastReferenceOutsideWriterLock) {
    auto manager = makeManager();
    SegmentTracker tracker(*manager);
    auto buffer = makeBuffer(0x30000, 0x1000);
    buffer.transports = {HP_TCP};
    buffer.transport_attrs[HP_TCP] = "old-registration";
    buffer.lkey = {7};
    auto ok = [](std::vector<BufferDesc>&) -> Status { return Status::OK(); };
    std::vector<BufferDesc> first{buffer};
    ASSERT_TRUE(tracker.addInBatch(first, ok).ok());

    int owner_removals = 0;
    int rollback_removals = 0;
    const auto failure = Status::InternalError("injected transport failure");
    // Both duplicate pins must be released; only the last one hands off.
    std::vector<BufferDesc> duplicates{buffer, buffer};
    auto status = tracker.addInBatch(
        duplicates,
        [&](std::vector<BufferDesc>&) -> Status {
            EXPECT_TRUE(tracker
                            .remove(buffer.addr, buffer.length,
                                    [&](BufferDesc&) -> Status {
                                        owner_removals++;
                                        return Status::OK();
                                    })
                            .ok());
            EXPECT_EQ(owner_removals, 0);
            return failure;
        },
        [&](BufferDesc& removed) {
            rollback_removals++;
            EXPECT_EQ(removed.addr, buffer.addr);
            EXPECT_EQ(removed.length, buffer.length);
            EXPECT_EQ(removed.location, buffer.location);
            EXPECT_EQ(removed.transports, buffer.transports);
            EXPECT_EQ(removed.transport_attrs, buffer.transport_attrs);
            EXPECT_EQ(removed.lkey, buffer.lkey);
            EXPECT_EQ(removed.ref_count, 0);
            EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
            // A writer can publish from the callback: the lock is released.
            EXPECT_TRUE(manager
                            ->updateLocal([](SegmentDesc& desc) -> Status {
                                desc.name = "cleanup_callback";
                                return Status::OK();
                            })
                            .ok());
        });
    EXPECT_EQ(status.code(), failure.code());
    EXPECT_EQ(status.message(), failure.message());
    EXPECT_EQ(rollback_removals, 1);
    EXPECT_TRUE(buffersOf(manager->getLocal()).empty());
    EXPECT_EQ(manager->getLocal()->name, "cleanup_callback");
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
                if (!tracker.addInBatch(batch, noop).ok()) failures++;
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
