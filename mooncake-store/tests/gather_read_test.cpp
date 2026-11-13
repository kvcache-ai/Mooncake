// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "gather_read.h"
#include "gather_read_directory.h"
#include "real_client.h"
#include "http_metadata_server.h"
#include "test_server_helpers.h"
#include <gtest/gtest.h>
#include <cstring>
#include <numeric>
#include <atomic>
#include <thread>
#include <poll.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef MOONCAKE_GATHER_TEST_WRAPPED_SUBMIT
namespace {
std::atomic<size_t> test_writes{0}, fail_submission{0}, fail_completion{0};
std::atomic<mooncake::BatchID> failed_batch{mooncake::INVALID_BATCH_ID};
struct ScopedWriteFailure {
    ScopedWriteFailure(size_t submission, size_t completion = 0) {
        test_writes = 0;
        failed_batch = mooncake::INVALID_BATCH_ID;
        fail_submission = submission;
        fail_completion = completion;
    }
    ~ScopedWriteFailure() {
        fail_submission = 0;
        fail_completion = 0;
        failed_batch = mooncake::INVALID_BATCH_ID;
    }
};
}  // namespace
extern "C" mooncake::Status
real_gather_submit(mooncake::TransferEngine*, mooncake::BatchID, const std::vector<mooncake::TransferRequest>&) asm(
    "__real__ZN8mooncake14TransferEngine14submitTransferEmRKSt6vectorINS_"
    "9Transport15TransferRequestESaIS3_EE");
extern "C" mooncake::Status
wrapped_gather_submit(mooncake::TransferEngine*, mooncake::BatchID, const std::vector<mooncake::TransferRequest>&) asm(
    "__wrap__ZN8mooncake14TransferEngine14submitTransferEmRKSt6vectorINS_"
    "9Transport15TransferRequestESaIS3_EE");
extern "C" mooncake::Status wrapped_gather_submit(
    mooncake::TransferEngine* engine, mooncake::BatchID batch,
    const std::vector<mooncake::TransferRequest>& entries) {
    if ((fail_submission || fail_completion) && entries.size() == 1 &&
        entries[0].opcode == mooncake::TransferRequest::WRITE) {
        const auto write = ++test_writes;
        if (write == fail_submission)
            return mooncake::Status::Memory(
                "injected gather submission failure");
        if (write == fail_completion) failed_batch = batch;
    }
    return real_gather_submit(engine, batch, entries);
}
extern "C" mooncake::Status
real_gather_status(mooncake::TransferEngine*, mooncake::BatchID, size_t, mooncake::TransferStatus&) asm(
    "__real__ZN8mooncake14TransferEngine17getTransferStatusEmmRNS_"
    "9Transport14TransferStatusE");
extern "C" mooncake::Status
wrapped_gather_status(mooncake::TransferEngine*, mooncake::BatchID, size_t, mooncake::TransferStatus&) asm(
    "__wrap__ZN8mooncake14TransferEngine17getTransferStatusEmmRNS_"
    "9Transport14TransferStatusE");
extern "C" mooncake::Status wrapped_gather_status(
    mooncake::TransferEngine* engine, mooncake::BatchID batch, size_t index,
    mooncake::TransferStatus& progress) {
    auto status = real_gather_status(engine, batch, index, progress);
    // Report a drained data failure only after real DMA has completed.
    if (status.ok() && batch == failed_batch &&
        progress.s == mooncake::TransferStatusEnum::COMPLETED)
        progress.s = mooncake::TransferStatusEnum::FAILED;
    return status;
}
#endif

namespace mooncake::store {
class GatherReadTest : public ::testing::Test {
   protected:
    std::shared_ptr<TransferEngine> owner, reader;
    std::shared_ptr<void> source, destination;
    std::unique_ptr<GatherReadService> service;
    std::unique_ptr<GatherReadClient> client;
    static constexpr size_t size = 4 << 20;
    std::string protocol = "tcp";
    std::shared_ptr<TransferEngine> makeEngine(
        const std::string& metadata = P2PHANDSHAKE,
        const std::string& name = "127.0.0.1:0") {
        auto engine = std::make_shared<TransferEngine>(false);
        if (engine->init(metadata, name))
            throw std::runtime_error("test engine initialization");
        std::string topology;
        if (protocol == "rdma") {
            const std::string device = std::getenv("MC_GATHER_TEST_DEVICE");
            topology = "{\"cpu:0\": [[\"" + device + "\"], []]}";
        }
        void* args[] = {topology.data()};
        if (!engine->installTransport(protocol,
                                      topology.empty() ? nullptr : args))
            throw std::runtime_error("test transport initialization");
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
class GatherReadRdmaTest : public GatherReadTest {
    void SetUp() override {
        if (!std::getenv("MC_GATHER_TEST_DEVICE"))
            GTEST_SKIP()
                << "Set MC_GATHER_TEST_DEVICE to an RDMA NIC to run data tests";
        protocol = "rdma";
        GatherReadTest::SetUp();
    }
};
TEST_F(GatherReadRdmaTest, ConcatenatesUnalignedDuplicateAndCrossChunkRanges) {
    std::vector<GatherReadRange> ranges{
        {91, 3}, {100000, 20000}, {91, 3},    {size - 123, 123},
        {0, 0},  {size, 0},       {300, 8193}};
    auto result = submit(ranges).wait();
    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_TRUE(result.drained());
    verify(ranges);
}
TEST_F(GatherReadRdmaTest, ReusesConnectionWithChangingRanges) {
    for (size_t i = 0; i < 20; ++i) {
        std::vector<GatherReadRange> ranges{{i * 997, 264}, {size - 513, 513}};
        auto result = submit(ranges).wait();
        ASSERT_TRUE(result.ok()) << result.error;
        // The destination after this fixed-size output remains untouched.
        verify(ranges);
    }
}
#ifdef MOONCAKE_GATHER_TEST_WRAPPED_SUBMIT
TEST_F(GatherReadRdmaTest, LaterSubmissionFailurePreservesCompletedPrefix) {
    ScopedWriteFailure fault(2);
    auto result = submit({{0, 3 * 4096}}).wait();
    ASSERT_EQ(result.completion, GatherReadCompletion::FailedDrained);
    ASSERT_EQ(test_writes, 2);
    EXPECT_EQ(result.bytes, 4096);
    EXPECT_TRUE(result.completedRange(0, 4096));
    EXPECT_FALSE(result.completedRange(4095, 2));
    EXPECT_FALSE(result.completedRange(4096, 1));
    EXPECT_EQ(
        memcmp(static_cast<char*>(destination.get()) + 32, source.get(), 4096),
        0);
    EXPECT_EQ(static_cast<uint8_t*>(destination.get())[32 + 4096], 0xa5);
}
TEST_F(GatherReadRdmaTest, FailedEarlierSlotKeepsLaterSuccessfulChunk) {
    ScopedWriteFailure fault(0, 1);
    auto result = submit({{0, 3 * 4096}}).wait();
    ASSERT_EQ(result.completion, GatherReadCompletion::FailedDrained);
    ASSERT_EQ(test_writes, 2);
    EXPECT_EQ(result.bytes, 4096);
    EXPECT_FALSE(result.completedRange(0, 1));
    EXPECT_TRUE(result.completedRange(4096, 4096));
    EXPECT_FALSE(result.completedRange(4095, 2));
    EXPECT_FALSE(result.completedRange(8192, 1));
    EXPECT_EQ(memcmp(static_cast<char*>(destination.get()) + 32 + 4096,
                     static_cast<char*>(source.get()) + 4096, 4096),
              0);
}
#endif

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
TEST_F(GatherReadTest, MissingRdmaTransportRejectsBeforeWrite) {
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
    EXPECT_EQ(result.completion, GatherReadCompletion::Rejected)
        << result.error;
    EXPECT_TRUE(result.drained());
    for (size_t i = 0; i < 128; ++i)
        EXPECT_EQ(static_cast<uint8_t*>(destination.get())[i], 0xa5);
}
TEST_F(GatherReadRdmaTest, RefreshesPeerAfterNewDestinationRegistration) {
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
TEST_F(GatherReadRdmaTest, OperationRetainsClientAndDestinationRegistration) {
    auto operation = submit({{0, 1 << 20}});
    std::weak_ptr<void> weak = destination;
    client.reset();
    destination.reset();
    ASSERT_FALSE(weak.expired());
    EXPECT_TRUE(operation.wait().ok());
    EXPECT_FALSE(weak.expired());
}
TEST_F(GatherReadRdmaTest, IndependentClientsRunConcurrently) {
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
TEST_F(GatherReadRdmaTest, ControlTimeoutFencesBeforeReturning) {
    client = std::make_unique<GatherReadClient>(
        reader, "127.0.0.1:" + std::to_string(service->port()),
        std::chrono::milliseconds(1));
    std::vector<GatherReadRange> ranges{{0, 3 << 20}};
    auto result = submit(ranges).wait();
    ASSERT_TRUE(result.drained());
    if (result.ok()) verify(ranges);
}
TEST_F(GatherReadRdmaTest, WaitTimeoutDoesNotCancelTransfer) {
    std::vector<GatherReadRange> ranges(65536, {13, 16});
    auto operation = submit(ranges);
    auto result = operation.waitFor(std::chrono::milliseconds(0));
    EXPECT_TRUE(result.completion == GatherReadCompletion::Pending ||
                result.ok());
    EXPECT_TRUE(operation.wait().ok());
    verify(ranges);
}

TEST_F(GatherReadTest, TcpIsRejectedBeforeWriting) {
    auto result = submit({{0, 64}}).wait();
    EXPECT_EQ(result.completion, GatherReadCompletion::Rejected);
    EXPECT_EQ(result.error,
              "gather requires an RDMA destination and transport");
    for (size_t i = 0; i < size; ++i)
        EXPECT_EQ(static_cast<uint8_t*>(destination.get())[i], 0xa5);
}
#ifdef USE_HTTP
TEST_F(GatherReadTest, NonP2PUsesRegisteredSegmentName) {
    HttpMetadataServer metadata(0, "127.0.0.1");
    ASSERT_TRUE(metadata.start());
    const auto url =
        "http://127.0.0.1:" + std::to_string(metadata.port()) + "/metadata";
    auto http_owner = makeEngine(url, "gather-owner");
    auto http_reader = makeEngine(url, "gather-reader");
    auto output = allocate();
    ASSERT_EQ(http_reader->registerLocalMemory(output.get(), size), 0);
    GatherReadService http_service(http_owner);
    http_service.addSource(source.get(), size);
    http_service.start("127.0.0.1");
    GatherReadClient http_client(
        http_reader, "127.0.0.1:" + std::to_string(http_service.port()),
        std::chrono::seconds(5), "gather-owner");
    auto result =
        http_client
            .submitGatherRead(addresses({{0, 64}}), output.get(), size, output)
            .wait();
    // Reaching the transport gate proves the registered destination was found.
    EXPECT_EQ(result.error,
              "gather requires an RDMA destination and transport");
    EXPECT_TRUE(result.rejected());
    EXPECT_EQ(http_reader->unregisterLocalMemory(output.get()), 0);
}
#endif
TEST_F(GatherReadTest, RetiredSessionsDoNotExhaustCapacity) {
    std::vector<std::unique_ptr<GatherReadClient>> clients;
    const auto endpoint = "127.0.0.1:" + std::to_string(service->port());
    for (size_t i = 0; i < 4096; ++i)
        clients.push_back(std::make_unique<GatherReadClient>(reader, endpoint));
    auto result = submit({{0, 64}}).wait();
    EXPECT_EQ(result.completion, GatherReadCompletion::SessionExpired);
    EXPECT_TRUE(result.rejected());
    EXPECT_TRUE(client->retired());
    EXPECT_FALSE(client->available());
    auto fresh = clients.back()
                     ->submitGatherRead(addresses({{0, 64}}), destination.get(),
                                        size, destination)
                     .wait();
    EXPECT_EQ(fresh.completion, GatherReadCompletion::Rejected);
    EXPECT_FALSE(clients.back()->retired());
}

struct LegacyGatherReadResult {
    GatherReadCompletion completion;
    std::string error;
    uint64_t bytes;
};
TEST(GatherReadResultTest,
     PartialCompletionIsWireCompatibleAndRequiresWholeSpan) {
    LegacyGatherReadResult legacy{GatherReadCompletion::FailedDrained, "failed",
                                  0};
    auto upgraded = struct_pack::deserialize<GatherReadResult>(
        struct_pack::serialize(legacy));
    ASSERT_TRUE(upgraded.has_value());
    EXPECT_FALSE(upgraded->completed_ranges.has_value());
    EXPECT_FALSE(upgraded->completedRange(0, 1));
    GatherReadResult partial{GatherReadCompletion::FailedDrained, "failed",
                             8192};
    partial.completed_ranges =
        std::vector<GatherReadRange>{{0, 4096}, {8192, 4096}};
    auto encoded = struct_pack::serialize(partial);
    auto restored = struct_pack::deserialize<GatherReadResult>(encoded);
    ASSERT_TRUE(restored.has_value());
    EXPECT_TRUE(restored->completedRange(0, 4096));
    EXPECT_TRUE(restored->completedRange(8192, 4096));
    EXPECT_FALSE(restored->completedRange(4095, 2));
    EXPECT_FALSE(restored->completedRange(4096, 1));
    EXPECT_FALSE(restored->completedRange(12287, 2));
    auto downgraded = struct_pack::deserialize<LegacyGatherReadResult>(encoded);
    ASSERT_TRUE(downgraded.has_value());
    EXPECT_EQ(downgraded->completion, GatherReadCompletion::FailedDrained);
    EXPECT_EQ(downgraded->bytes, 8192);
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

class StoreGatherReadTest : public ::testing::TestWithParam<std::string> {};
TEST_P(StoreGatherReadTest, InterleavedKeysSnapshotAndScatterFallback) {
    const auto& protocol = GetParam();
    const char* device = std::getenv("MC_GATHER_TEST_DEVICE");
    if (protocol == "rdma" && !device)
        GTEST_SKIP()
            << "Set MC_GATHER_TEST_DEVICE to run the RDMA Store integration";
    setenv("MC_STORE_MEMCPY", "0", 1);
    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    auto owner = RealClient::create(), reader = RealClient::create();
    ASSERT_EQ(owner->setup_real("127.0.0.1:0", P2PHANDSHAKE, 16 << 20, 8 << 20,
                                protocol, device ? device : "",
                                master.master_address()),
              0);
    ASSERT_EQ(
        reader->setup_real("127.0.0.2:0", P2PHANDSHAKE, 0, 8 << 20, protocol,
                           device ? device : "", master.master_address()),
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
    EXPECT_EQ(service_endpoint->empty(), protocol == "tcp");

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

// Accept the open request, hold the connection beyond the Store lease TTL,
// then abandon the reply. No data write is ever accepted on this endpoint.
class DelayedGatherEndpoint {
   public:
    DelayedGatherEndpoint() {
        listener_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listener_ < 0) throw std::runtime_error("test socket");
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (bind(listener_, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) ||
            listen(listener_, 1))
            throw std::runtime_error("test listen");
        socklen_t length = sizeof(address);
        if (getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                        &length))
            throw std::runtime_error("test port");
        endpoint = "127.0.0.1:" + std::to_string(ntohs(address.sin_port));
        worker_ = std::jthread([this](std::stop_token stop) {
            pollfd pending{listener_, POLLIN, 0};
            while (!stop.stop_requested()) {
                if (poll(&pending, 1, 20) <= 0) continue;
                const int peer = accept(listener_, nullptr, nullptr);
                if (peer < 0) return;
                pollfd request{peer, POLLIN, 0};
                while (!stop.stop_requested()) {
                    if (poll(&request, 1, 20) <= 0) continue;
                    char buffer[4096];
                    received = recv(peer, buffer, sizeof(buffer), 0) > 0;
                    if (received)
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(500));
                    break;
                }
                close(peer);
                return;
            }
        });
    }
    ~DelayedGatherEndpoint() {
        worker_.request_stop();
        worker_.join();
        close(listener_);
    }
    std::string endpoint;
    std::atomic<bool> received{false};

   private:
    int listener_;
    std::jthread worker_;
};

TEST(StoreGatherLeaseTest, RenewsAllOwnersDuringDelayedSessionOpen) {
    const char* device = std::getenv("MC_GATHER_TEST_DEVICE");
    if (!device) GTEST_SKIP() << "Set MC_GATHER_TEST_DEVICE for RDMA";
    setenv("MC_STORE_MEMCPY", "0", 1);
    setenv("MC_STORE_GATHER_READ", "1", 1);
    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(
        InProcMasterConfigBuilder().set_default_kv_lease_ttl(100).build()));
    auto a = RealClient::create(), b = RealClient::create(),
         reader = RealClient::create();
    std::vector<char> value(1 << 20);
    for (size_t i = 0; i < value.size(); ++i) value[i] = i % 251;
    ASSERT_EQ(a->setup_real("127.0.0.1:0", P2PHANDSHAKE, 16 << 20, 8 << 20,
                            "rdma", device, master.master_address()),
              0);
    ASSERT_EQ(a->put("lease-a", value), 0);
    auto first = a->get_replica_desc("lease-a");
    ASSERT_EQ(first.size(), 1);
    const auto first_owner =
        first[0].get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    ASSERT_EQ(b->setup_real("127.0.0.2:0", P2PHANDSHAKE, 16 << 20, 8 << 20,
                            "rdma", device, master.master_address()),
              0);
    ReplicateConfig config;
    config.preferred_segment = b->get_hostname();
    ASSERT_EQ(b->put("lease-b", value, config), 0);
    auto second = b->get_replica_desc("lease-b");
    ASSERT_EQ(second.size(), 1);
    const auto second_owner =
        second[0].get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    ASSERT_NE(first_owner, second_owner);
    ASSERT_EQ(reader->setup_real("127.0.0.3:0", P2PHANDSHAKE, 0, 8 << 20,
                                 "rdma", device, master.master_address()),
              0);
    MasterClient discovery(generate_uuid());
    ASSERT_EQ(discovery.Connect(master.master_address()), ErrorCode::OK);
    DelayedGatherEndpoint delayed;
    // Plans are ordered by owner: stall the second after the first is
    // submitted.
    ASSERT_TRUE(discovery.PublishGatherEndpoint(
        std::max(first_owner, second_owner), delayed.endpoint, false));
    std::vector<char> out_a(256 * 264), out_b(out_a.size());
    ASSERT_EQ(reader->register_buffer(out_a.data(), out_a.size()), 0);
    ASSERT_EQ(reader->register_buffer(out_b.data(), out_b.size()), 0);
    std::vector<std::vector<std::vector<size_t>>> dst(2, {{}}), src(2, {{}}),
        sizes(2, {{}});
    for (size_t owner = 0; owner < 2; ++owner)
        for (size_t i = 0; i < 256; ++i) {
            dst[owner][0].push_back(i * 264);
            src[owner][0].push_back(i * 997);
            sizes[owner][0].push_back(264);
        }
    auto results =
        reader->get_into_ranges({out_a.data(), out_b.data()},
                                {{"lease-a"}, {"lease-b"}}, dst, src, sizes);
    ASSERT_TRUE(delayed.received);
    for (size_t owner = 0; owner < 2; ++owner)
        for (size_t i = 0; i < 256; ++i) {
            EXPECT_EQ(results[owner][0][i], 264) << owner << ":" << i;
            EXPECT_EQ(memcmp((owner ? out_b : out_a).data() + i * 264,
                             value.data() + i * 997, 264),
                      0);
        }
    EXPECT_EQ(reader->unregister_buffer(out_a.data()), 0);
    EXPECT_EQ(reader->unregister_buffer(out_b.data()), 0);
    reader->tearDownAll();
    b->tearDownAll();
    a->tearDownAll();
}

#ifdef MOONCAKE_GATHER_TEST_WRAPPED_SUBMIT
TEST(StoreGatherFailureTest, CompletedChunksKeepPerFragmentResults) {
    const char* device = std::getenv("MC_GATHER_TEST_DEVICE");
    if (!device) GTEST_SKIP() << "Set MC_GATHER_TEST_DEVICE for RDMA";
    setenv("MC_STORE_MEMCPY", "0", 1);
    setenv("MC_STORE_GATHER_READ", "1", 1);
    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    auto owner = RealClient::create(), reader = RealClient::create();
    ASSERT_EQ(owner->setup_real("127.0.0.1:0", P2PHANDSHAKE, 16 << 20, 8 << 20,
                                "rdma", device, master.master_address()),
              0);
    ASSERT_EQ(reader->setup_real("127.0.0.2:0", P2PHANDSHAKE, 0, 8 << 20,
                                 "rdma", device, master.master_address()),
              0);
    std::vector<char> value(4 << 20);
    for (size_t i = 0; i < value.size(); ++i) value[i] = i % 251;
    ASSERT_EQ(owner->put("partial-a", value), 0);
    ASSERT_EQ(owner->put("partial-b", value), 0);
    // Interleave two keys and reverse API fragment order. Fragment 512
    // straddles the failed chunk boundary and must not report success.
    std::vector<char> output(1024 * 1024 + 1, 'z');
    ASSERT_EQ(reader->register_buffer(output.data(), output.size()), 0);
    std::vector<std::vector<std::vector<size_t>>> dst{{{}, {}}}, src{{{}, {}}},
        sizes{{{}, {}}};
    for (size_t n = 1024; n > 0; --n) {
        const size_t i = n - 1, key = i % 2;
        dst[0][key].push_back(1 + i * 1024 - (i > 511 ? 1 : 0));
        src[0][key].push_back(i * 2048);
        sizes[0][key].push_back(i == 511 ? 1023 : 1024);
    }
    ScopedWriteFailure fault(2);
    auto results = reader->get_into_ranges(
        {output.data()}, {{"partial-a", "partial-b"}}, dst, src, sizes);
    ASSERT_EQ(test_writes, 2);
    for (size_t key = 0; key < 2; ++key)
        for (size_t k = 0; k < sizes[0][key].size(); ++k) {
            const bool completed =
                dst[0][key][k] - 1 + sizes[0][key][k] <= 512 * 1024;
            EXPECT_EQ(results[0][key][k],
                      completed ? static_cast<int64_t>(sizes[0][key][k])
                                : toInt(ErrorCode::TRANSFER_FAIL));
            if (completed)
                EXPECT_EQ(
                    memcmp(output.data() + dst[0][key][k],
                           value.data() + src[0][key][k], sizes[0][key][k]),
                    0);
        }
    EXPECT_EQ(output.front(), 'z');
    EXPECT_EQ(output.back(), 'z');
    EXPECT_EQ(reader->unregister_buffer(output.data()), 0);
    reader->tearDownAll();
    owner->tearDownAll();
}
#endif

INSTANTIATE_TEST_SUITE_P(Transports, StoreGatherReadTest,
                         ::testing::Values("tcp", "rdma"));

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
