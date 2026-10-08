// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "gather_read.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include <gtest/gtest.h>
#include <cstring>
#include <numeric>

namespace mooncake::store {
class GatherReadTest : public ::testing::Test {
   protected:
    std::shared_ptr<TransferEngine> owner, reader;
    std::shared_ptr<void> source, destination;
    std::unique_ptr<GatherReadService> service;
    std::unique_ptr<GatherReadClient> client;
    static constexpr size_t size = 4 << 20;
    static std::shared_ptr<TransferEngine> makeEngine() {
        auto engine = std::make_shared<TransferEngine>(false);
        if (engine->init(P2PHANDSHAKE, "127.0.0.1:0") ||
            !engine->installTransport("tcp", nullptr))
            throw std::runtime_error("test TCP engine initialization");
        return engine;
    }
    static std::shared_ptr<void> allocate() {
        void* p = nullptr;
        if (posix_memalign(&p, 4096, size)) throw std::bad_alloc();
        return std::shared_ptr<void>(p, std::free);
    }
    void SetUp() override {
        owner = makeEngine();
        reader = makeEngine();
        source = allocate();
        for (size_t i = 0; i < size; ++i)
            static_cast<uint8_t*>(source.get())[i] = (i * 17 + i / 251) % 253;
        auto memory = allocate();
        ASSERT_EQ(reader->registerLocalMemory(memory.get(), size), 0);
        destination = std::shared_ptr<void>(
            memory.get(), [engine = reader, memory](void* p) {
                EXPECT_EQ(engine->unregisterLocalMemory(p), 0);
            });
        std::memset(destination.get(), 0xa5, size);
        GatherReadOptions options;
        options.chunk_bytes = 4096;
        options.workers = 2;
        service = std::make_unique<GatherReadService>(owner, options);
        service->addSource(source.get(), size);
        service->start("127.0.0.1");
        client = std::make_unique<GatherReadClient>(
            reader, "127.0.0.1:" + std::to_string(service->port()));
    }
    void TearDown() override {
        client.reset();
        service.reset();
        destination.reset();
        source.reset();
        reader.reset();
        owner.reset();
    }
    GatherReadOperation submit(const std::vector<GatherReadRange>& ranges,
                               size_t capacity = size - 64) {
        return client->submitGatherRead(
            addresses(ranges), static_cast<char*>(destination.get()) + 32,
            capacity, destination);
    }
    std::vector<GatherReadRange> addresses(
        std::vector<GatherReadRange> ranges) {
        for (auto& r : ranges) {
            if (r.offset <=
                UINT64_MAX - reinterpret_cast<uintptr_t>(source.get()))
                r.offset += reinterpret_cast<uintptr_t>(source.get());
        }
        return ranges;
    }
    void verify(const std::vector<GatherReadRange>& ranges) {
        size_t offset = 32;
        for (const auto& range : ranges) {
            EXPECT_EQ(memcmp(static_cast<char*>(destination.get()) + offset,
                             static_cast<char*>(source.get()) + range.offset,
                             range.length),
                      0);
            offset += range.length;
        }
        for (size_t i = 0; i < 32; ++i)
            EXPECT_EQ(static_cast<uint8_t*>(destination.get())[i], 0xa5);
        EXPECT_EQ(static_cast<uint8_t*>(destination.get())[offset], 0xa5);
    }
};
TEST_F(GatherReadTest, ConcatenatesUnalignedDuplicateAndCrossChunkRanges) {
    std::vector<GatherReadRange> ranges{
        {91, 3}, {100000, 20000}, {91, 3},    {size - 123, 123},
        {0, 0},  {size, 0},       {300, 8193}};
    auto result = submit(ranges).wait();
    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_TRUE(result.drained());
    verify(ranges);
}
TEST_F(GatherReadTest, ReusesConnectionWithChangingRanges) {
    for (size_t i = 0; i < 20; ++i) {
        std::vector<GatherReadRange> ranges{{i * 997, 264}, {size - 513, 513}};
        auto result = submit(ranges).wait();
        ASSERT_TRUE(result.ok()) << result.error;
        // The destination after this fixed-size output remains untouched.
        verify(ranges);
    }
}
TEST_F(GatherReadTest, RejectsWholeRequestBeforeWritingAnyValidPrefix) {
    auto result = submit({{0, 64}, {size - 1, 2}}).wait();
    EXPECT_EQ(result.completion, GatherReadCompletion::Rejected);
    EXPECT_TRUE(result.drained());
    for (size_t i = 0; i < 128; ++i)
        EXPECT_EQ(static_cast<uint8_t*>(destination.get())[i], 0xa5);
}
TEST_F(GatherReadTest, RejectsDestinationOverflowAndMissingLease) {
    EXPECT_EQ(submit({{0, 128}}, 127).wait().completion,
              GatherReadCompletion::Rejected);
    EXPECT_EQ(client->submitGatherRead({{0, 4}}, destination.get(), size, {})
                  .wait()
                  .completion,
              GatherReadCompletion::Rejected);
    EXPECT_EQ(submit({{UINT64_MAX, 2}}).wait().completion,
              GatherReadCompletion::Rejected);
}
TEST_F(GatherReadTest, RejectsUnregisteredDestination) {
    auto memory = allocate();
    auto result =
        client
            ->submitGatherRead(addresses({{0, 64}}), memory.get(), size, memory)
            .wait();
    EXPECT_EQ(result.completion, GatherReadCompletion::Rejected);
    EXPECT_TRUE(result.drained());
}
TEST_F(GatherReadTest, EmptyReadNeedsNoDestination) {
    auto result = client->submitGatherRead({}, nullptr, 0, {}).wait();
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.bytes, 0);
}
TEST_F(GatherReadTest, SubmissionFailureWithoutPublishedTasksDrains) {
    auto disconnected = std::make_shared<TransferEngine>(false);
    ASSERT_EQ(disconnected->init(P2PHANDSHAKE, "127.0.0.1:0"), 0);
    auto descriptor = std::make_shared<TransferMetadata::SegmentDesc>();
    descriptor->name = disconnected->getLocalIpAndPort();
    descriptor->protocol = "tcp";
    ASSERT_EQ(disconnected->getMetadata()->addLocalSegment(
                  LOCAL_SEGMENT_ID, disconnected->getLocalIpAndPort(),
                  std::move(descriptor)),
              0);
    // Publish a control service without installing any data transport.
    GatherReadService unavailable(disconnected);
    unavailable.addSource(source.get(), size);
    unavailable.start("127.0.0.1");
    GatherReadClient request(reader,
                             "127.0.0.1:" + std::to_string(unavailable.port()));
    auto operation = request.submitGatherRead(
        addresses({{0, 64}}), destination.get(), size, destination);
    auto result = operation.waitFor(std::chrono::seconds(5));
    EXPECT_EQ(result.completion, GatherReadCompletion::FailedDrained)
        << result.error;
    EXPECT_TRUE(result.drained());
    for (size_t i = 0; i < 128; ++i)
        EXPECT_EQ(static_cast<uint8_t*>(destination.get())[i], 0xa5);
}
TEST_F(GatherReadTest, RefreshesPeerAfterNewDestinationRegistration) {
    ASSERT_TRUE(submit({{0, 64}}).wait().ok());
    auto memory = allocate();
    ASSERT_EQ(reader->registerLocalMemory(memory.get(), size), 0);
    auto result = client
                      ->submitGatherRead(addresses({{123, 4096}}), memory.get(),
                                         size, memory)
                      .wait();
    EXPECT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(
        memcmp(memory.get(), static_cast<char*>(source.get()) + 123, 4096), 0);
    EXPECT_EQ(reader->unregisterLocalMemory(memory.get()), 0);
}
TEST_F(GatherReadTest, RejectsRangeCountLimit) {
    std::vector<GatherReadRange> ranges(131073, {0, 1});
    EXPECT_EQ(submit(ranges).wait().completion, GatherReadCompletion::Rejected);
}
TEST_F(GatherReadTest, OperationRetainsClientAndDestinationRegistration) {
    auto operation = submit({{0, 1 << 20}});
    std::weak_ptr<void> weak = destination;
    client.reset();
    destination.reset();
    ASSERT_FALSE(weak.expired());
    EXPECT_TRUE(operation.wait().ok());
    EXPECT_FALSE(weak.expired());
}
TEST_F(GatherReadTest, IndependentClientsRunConcurrently) {
    GatherReadClient other(reader,
                           "127.0.0.1:" + std::to_string(service->port()));
    auto first = submit({{0, 500000}});
    auto second = other.submitGatherRead(
        addresses({{123, 500000}}),
        static_cast<char*>(destination.get()) + 1000000, 500000, destination);
    EXPECT_TRUE(first.wait().ok());
    EXPECT_TRUE(second.wait().ok());
    EXPECT_EQ(memcmp(static_cast<char*>(destination.get()) + 1000000,
                     static_cast<char*>(source.get()) + 123, 500000),
              0);
}
TEST_F(GatherReadTest, InvalidOptionsFailBeforeStartingService) {
    GatherReadOptions options;
    options.pipeline_depth = 0;
    EXPECT_THROW(GatherReadService(owner, options), std::invalid_argument);
}
TEST_F(GatherReadTest, RemovedSourceIsRejectedBeforeWrite) {
    service->removeSource(source.get());
    EXPECT_EQ(submit({{0, 64}}).wait().completion,
              GatherReadCompletion::Rejected);
    EXPECT_EQ(static_cast<uint8_t*>(destination.get())[32], 0xa5);
}
TEST_F(GatherReadTest, ControlTimeoutFencesBeforeReturning) {
    client = std::make_unique<GatherReadClient>(
        reader, "127.0.0.1:" + std::to_string(service->port()),
        std::chrono::milliseconds(1));
    std::vector<GatherReadRange> ranges{{0, 3 << 20}};
    auto result = submit(ranges).wait();
    ASSERT_TRUE(result.drained());
    if (result.ok()) verify(ranges);
}
TEST_F(GatherReadTest, WaitTimeoutDoesNotCancelTransfer) {
    std::vector<GatherReadRange> ranges(65536, {13, 16});
    auto operation = submit(ranges);
    auto result = operation.waitFor(std::chrono::milliseconds(0));
    EXPECT_TRUE(result.completion == GatherReadCompletion::Pending ||
                result.ok());
    EXPECT_TRUE(operation.wait().ok());
    verify(ranges);
}

struct OwnedRanges {
    decltype(TransferRequest::READ) opcode = TransferRequest::READ;
    std::string remote_segment;
    uint64_t remote_base_offset = 0, remote_size = 0;
    void* local_buffer = nullptr;
    size_t local_capacity = 0;
    std::vector<size_t> remote_offsets, local_offsets, lengths;
    operator TransferEngine::ScatterTransferRange() const {
        return {.opcode = opcode,
                .remote_segment = remote_segment,
                .remote_base_offset = remote_base_offset,
                .remote_size = remote_size,
                .local_buffer = local_buffer,
                .local_capacity = local_capacity,
                .local_offsets = local_offsets,
                .remote_offsets = remote_offsets,
                .lengths = lengths,
                .on_fragment_complete = {}};
    }
};
static OwnedRanges ranges(size_t count = 512, size_t length = 264) {
    OwnedRanges t;
    t.opcode = TransferRequest::READ;
    t.remote_segment = "owner";
    t.remote_base_offset = 0x10000000;
    t.remote_size = 1 << 24;
    t.local_buffer = reinterpret_cast<void*>(0x20000000);
    t.local_capacity = 1 << 24;
    for (size_t i = 0; i < count; ++i) {
        t.remote_offsets.push_back(i * 8192);
        t.local_offsets.push_back(i * length);
        t.lengths.push_back(length);
    }
    return t;
}
TEST(GatherReadPlannerTest, ManySmallRangesSelectGather) {
    auto plans = PlanGatherReads({ranges()}, {"owner:1"});
    ASSERT_EQ(plans.size(), 1);
    EXPECT_EQ(plans[0].bytes, 512 * 264);
    EXPECT_EQ(plans[0].ranges.front().offset, 0x10000000);
}
TEST(GatherReadPlannerTest, SmallLargeAndContiguousSourcesUseScatter) {
    EXPECT_TRUE(PlanGatherReads({ranges(24)}, {"owner:1"}).empty());
    EXPECT_TRUE(PlanGatherReads({ranges(512, 4096)}, {"owner:1"}).empty());
    auto t = ranges();
    t.remote_offsets = t.local_offsets;
    EXPECT_TRUE(PlanGatherReads({t}, {"owner:1"}).empty());
}
TEST(GatherReadPlannerTest, GapsOverlapsAndBoundsUseScatter) {
    auto t = ranges();
    t.local_offsets[100]++;
    EXPECT_TRUE(PlanGatherReads({t}, {"owner:1"}).empty());
    t = ranges();
    t.local_offsets[100] = t.local_offsets[99];
    EXPECT_TRUE(PlanGatherReads({t}, {"owner:1"}).empty());
    t = ranges();
    t.remote_offsets[100] = t.remote_size;
    EXPECT_TRUE(PlanGatherReads({t}, {"owner:1"}).empty());
    t = ranges();
    t.local_offsets[100] = t.local_capacity;
    EXPECT_TRUE(PlanGatherReads({t}, {"owner:1"}).empty());
    t = ranges();
    t.lengths.pop_back();
    EXPECT_TRUE(PlanGatherReads({t}, {"owner:1"}).empty());
}
TEST(GatherReadPlannerTest, InterleavedKeysAreGatheredInDestinationOrder) {
    auto a = ranges(256), b = ranges(256);
    for (size_t i = 0; i < 256; ++i) {
        a.local_offsets[i] = i * 528;
        b.local_offsets[i] = i * 528 + 264;
    }
    b.remote_base_offset += 1 << 24;
    auto plans = PlanGatherReads({b, a}, {"owner:1", "owner:1"});
    ASSERT_EQ(plans.size(), 1);
    EXPECT_EQ(plans[0].ranges[0].offset, a.remote_base_offset);
    EXPECT_EQ(plans[0].ranges[1].offset, b.remote_base_offset);
    EXPECT_EQ(plans[0].bytes, 512 * 264);
    // Split owners cannot form this contiguous destination individually.
    EXPECT_TRUE(PlanGatherReads({a, b}, {"owner:1", "owner:2"}).empty());
}
TEST(GatherReadPlannerTest, OldOwnersAndIndependentDestinations) {
    EXPECT_TRUE(PlanGatherReads({ranges()}, {""}).empty());
    auto a = ranges(), b = ranges();
    b.local_buffer = reinterpret_cast<void*>(0x30000000);
    EXPECT_EQ(PlanGatherReads({a, b}, {"owner:1", "owner:1"}).size(), 2);
}
TEST(GatherReadPlannerTest, RespectsDestinationOrdering) {
    auto t = ranges();
    std::reverse(t.local_offsets.begin(), t.local_offsets.end());
    auto plans = PlanGatherReads({t}, {"owner:1"});
    ASSERT_EQ(plans.size(), 1);
    EXPECT_EQ(plans[0].ranges.front().offset,
              t.remote_base_offset + t.remote_offsets.back());
}

TEST(StoreGatherReadTest, InterleavedKeysSnapshotAndScatterFallback) {
    setenv("MC_STORE_MEMCPY", "0", 1);
    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    auto owner = RealClient::create(), reader = RealClient::create();
    ASSERT_EQ(owner->setup_real("127.0.0.1:0", P2PHANDSHAKE, 16 << 20, 8 << 20,
                                "tcp", "", master.master_address()),
              0);
    ASSERT_EQ(reader->setup_real("127.0.0.2:0", P2PHANDSHAKE, 0, 8 << 20, "tcp",
                                 "", master.master_address()),
              0);
    std::vector<char> a(1 << 20), b(1 << 20);
    for (size_t i = 0; i < a.size(); ++i) {
        a[i] = i % 251;
        b[i] = (i * 7) % 253;
    }
    ASSERT_EQ(owner->put("gather-a", a), 0);
    ASSERT_EQ(owner->put("gather-b", b), 0);
    auto snapshot =
        reader->prepare_get_into_ranges_snapshot({"gather-a", "gather-b"});
    ASSERT_TRUE(snapshot.query_result_cache.at("gather-a").has_value());
    MasterClient discovery(generate_uuid());
    ASSERT_EQ(discovery.Connect(master.master_address()), ErrorCode::OK);
    const auto owner_endpoint = snapshot.query_result_cache.at("gather-a")
                                    ->replicas[0]
                                    .get_memory_descriptor()
                                    .buffer_descriptor.transport_endpoint_;
    auto service_endpoint = discovery.ResolveGatherEndpoint(owner_endpoint);
    ASSERT_TRUE(service_endpoint.has_value());
    ASSERT_FALSE(service_endpoint->empty());

    std::vector<char> newer(a.size(), 'q');
    ASSERT_EQ(owner->upsert("gather-a", newer), 0);
    std::vector<char> output(512 * 264 + 128, '\0');
    ASSERT_EQ(reader->register_buffer(output.data(), output.size()), 0);
    std::vector<std::vector<std::vector<size_t>>> dst{{{}, {}}}, src{{{}, {}}},
        sizes{{{}, {}}};
    for (size_t i = 0; i < 256; ++i)
        for (size_t k = 0; k < 2; ++k) {
            dst[0][k].push_back(64 + (i * 2 + k) * 264);
            src[0][k].push_back((i * 3593) % (a.size() - 264));
            sizes[0][k].push_back(264);
        }
    for (const char* mode : {"1", "0"}) {
        setenv("MC_STORE_GATHER_READ", mode, 1);
        for (bool cached : {false, true}) {
            std::fill(output.begin(), output.end(), 'z');
            auto results =
                cached ? reader->get_into_ranges_from_snapshot(
                             {output.data()}, {{"gather-a", "gather-b"}}, dst,
                             src, sizes, snapshot.query_result_cache)
                       : reader->get_into_ranges({output.data()},
                                                 {{"gather-a", "gather-b"}},
                                                 dst, src, sizes);
            ASSERT_EQ(results[0].size(), 2);
            for (size_t k = 0; k < 2; ++k)
                for (size_t i = 0; i < 256; ++i) {
                    ASSERT_EQ(results[0][k][i], 264);
                    EXPECT_EQ(memcmp(output.data() + dst[0][k][i],
                                     (k ? b : (cached ? a : newer)).data() +
                                         src[0][k][i],
                                     264),
                              0);
                }
            EXPECT_EQ(output.front(), 'z');
            EXPECT_EQ(output.back(), 'z');
        }
    }
    setenv("MC_STORE_GATHER_READ", "1", 1);
    // One invalid fragment must retain the existing per-fragment error API.
    src[0][0][7] = a.size();
    auto results = reader->get_into_ranges(
        {output.data()}, {{"gather-a", "gather-b"}}, dst, src, sizes);
    EXPECT_LT(results[0][0][7], 0);
    EXPECT_EQ(results[0][1][0], 264);
    EXPECT_EQ(reader->unregister_buffer(output.data()), 0);
    reader->tearDownAll();
    owner->tearDownAll();
    master.Stop();
}

TEST(GatherReadDirectoryTest, MissingExpiryAndConditionalRemoval) {
    GatherReadDirectory directory;
    ASSERT_TRUE(directory.Resolve("owner").has_value());
    EXPECT_TRUE(directory.Resolve("owner")->empty());
    ASSERT_TRUE(directory.Publish("owner", "host:123", false));
    EXPECT_EQ(*directory.Resolve("owner"), "host:123");
    ASSERT_TRUE(directory.Publish("owner", "host:456", false));
    ASSERT_TRUE(directory.Publish("owner", "host:123", true));
    EXPECT_EQ(*directory.Resolve("owner"), "host:456");
    ASSERT_TRUE(directory.Publish("owner", "host:456", true));
    EXPECT_TRUE(directory.Resolve("owner")->empty());
    GatherReadDirectory expired(std::chrono::seconds(0));
    ASSERT_TRUE(expired.Publish("owner", "host:123", false));
    EXPECT_TRUE(expired.Resolve("owner")->empty());
    EXPECT_FALSE(directory.Publish("", "host:123", false));
}
TEST_F(GatherReadTest, RejectsWrongDiscoveredOwner) {
    EXPECT_THROW(
        GatherReadClient(reader, "127.0.0.1:" + std::to_string(service->port()),
                         std::chrono::seconds(1), "wrong:123"),
        std::runtime_error);
}
}  // namespace mooncake::store
