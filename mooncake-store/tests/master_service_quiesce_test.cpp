// MasterService::QuiesceBackgroundWork: after it returns, no background
// thread changes metadata, segments or clients any more.

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

#include "master_service.h"
#include "master_service/master_service_test_peer.h"

namespace mooncake::test {
namespace {

constexpr uint64_t kSegmentBase = 0x300000000;
constexpr uint64_t kSegmentSize = 16 * 1024 * 1024;
constexpr uint64_t kObjectSize = 1024 * 1024;
constexpr uint64_t kLeaseTtlMs = 200;

MasterServiceConfig ShortTtlConfig() {
    // Clients that stop pinging are offboarded within about two seconds.
    return MasterServiceConfig::builder()
        .set_default_kv_lease_ttl(kLeaseTtlMs)
        .set_eviction_ratio(0.0)
        .set_eviction_high_watermark_ratio(1.0)
        .set_client_live_ttl_sec(1)
        .set_client_suspicion_ttl_sec(1)
        .build();
}

Segment MakeSegment() {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "quiesce_segment";
    segment.base = kSegmentBase;
    segment.size = kSegmentSize;
    segment.te_endpoint = segment.name;
    return segment;
}

void PutObject(MasterService& service, const UUID& client,
               const std::string& key) {
    ReplicateConfig config;
    config.replica_num = 1;
    auto started =
        service.PutStart(client, key, TenantId::Default(), kObjectSize, config);
    ASSERT_TRUE(started.has_value()) << toString(started.error());
    ASSERT_TRUE(
        service.PutEnd(client, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
}

size_t MountedSegments(MasterService& service) {
    auto segments = service.GetAllSegments();
    return segments ? segments->size() : 0;
}

TEST(MasterServiceQuiesceTest, NoBackgroundChangeAfterQuiesce) {
    MasterService service(ShortTtlConfig());
    const UUID client = generate_uuid();
    ASSERT_TRUE(service.MountSegment(MakeSegment(), client).has_value());
    PutObject(service, client, "quiesce_key");
    EXPECT_FALSE(service.BackgroundWorkQuiesced());

    service.QuiesceBackgroundWork();
    EXPECT_TRUE(service.BackgroundWorkQuiesced());
    // The client never pings: without the quiesce the client monitor would
    // offboard it and unmount its segment (see the next test).
    std::this_thread::sleep_for(std::chrono::seconds(4));
    EXPECT_EQ(MountedSegments(service), 1u);
    EXPECT_EQ(service.GetKeyCount(), 1u);

    // Idempotent; the destructor quiesces again.
    service.QuiesceBackgroundWork();
    EXPECT_TRUE(service.BackgroundWorkQuiesced());
}

TEST(MasterServiceQuiesceTest, WithoutQuiesceTheMonitorOffboards) {
    // Control for the test above.
    MasterService service(ShortTtlConfig());
    ASSERT_TRUE(
        service.MountSegment(MakeSegment(), generate_uuid()).has_value());
    bool offboarded = false;
    for (int i = 0; i < 100 && !offboarded; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        offboarded = MountedSegments(service) == 0;
    }
    EXPECT_TRUE(offboarded);
}

TEST(MasterServiceQuiesceTest, EvictionPassAfterQuiesceFreesNothing) {
    for (const bool quiesce : {false, true}) {
        MasterService service(ShortTtlConfig());
        const UUID client = generate_uuid();
        ASSERT_TRUE(service.MountSegment(MakeSegment(), client).has_value());
        for (int i = 0; i < 8; ++i) {
            PutObject(service, client, "evict_" + std::to_string(i));
        }
        // Leases expire, so every object is evictable.
        std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseTtlMs * 2));
        if (quiesce) {
            service.QuiesceBackgroundWork();
        }
        MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
        if (quiesce) {
            // A pass that was already running when the eviction thread
            // stopped ends without evicting anything.
            EXPECT_EQ(service.GetKeyCount(), 8u);
        } else {
            EXPECT_LT(service.GetKeyCount(), 8u);
        }
    }
}

TEST(MasterServiceQuiesceTest, QuiesceDoesNotWaitOutThreadSleeps) {
    MasterService service(ShortTtlConfig());
    // Let the client monitor (1 s) and job dispatch (0.5 s) threads reach
    // their sleeps.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto start = std::chrono::steady_clock::now();
    service.QuiesceBackgroundWork();
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::milliseconds(400));
}

}  // namespace
}  // namespace mooncake::test
