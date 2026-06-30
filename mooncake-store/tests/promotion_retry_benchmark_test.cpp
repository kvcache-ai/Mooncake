// Deterministic public-API benchmark for promotion retry admission.
//
// The scenario keeps promotion_queue_limit=1, rejects a second hot
// LOCAL_DISK-only key on the cap gate, then releases the first task slot.
// A retry-capable master should re-enqueue the second key without another
// foreground Get.

#include "master_service.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "types.h"

namespace mooncake::test {
namespace {

size_t CountPromotionTask(const std::vector<PromotionTaskItem>& tasks,
                          const std::string& key) {
    return std::count_if(
        tasks.begin(), tasks.end(),
        [&key](const PromotionTaskItem& task) { return task.key == key; });
}

Segment MakeSegment(std::string name, size_t base, size_t size) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = std::move(name);
    segment.base = base;
    segment.size = size;
    segment.te_endpoint = segment.name;
    return segment;
}

struct MountedSegmentContext {
    UUID segment_id;
    UUID client_id;
    std::string segment_name;
};

MountedSegmentContext PrepareSegment(MasterService& service, std::string name,
                                     size_t base, size_t size) {
    Segment segment = MakeSegment(std::move(name), base, size);
    UUID client_id = generate_uuid();
    auto mount_result = service.MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result.has_value());
    auto mount_ld = service.MountLocalDiskSegment(client_id, true);
    EXPECT_TRUE(mount_ld.has_value());
    return {.segment_id = segment.id,
            .client_id = client_id,
            .segment_name = segment.name};
}

bool InjectLocalDiskReplica(MasterService& service, const UUID& client_id,
                            const std::string& key, int64_t size,
                            const std::string& transport_endpoint) {
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "default", .key = key, .size = size}};
    StorageObjectMetadata sm;
    sm.bucket_id = 0;
    sm.offset = 0;
    sm.key_size = static_cast<int64_t>(key.size());
    sm.data_size = size;
    sm.transport_endpoint = transport_endpoint;
    std::vector<StorageObjectMetadata> metas{sm};
    auto res = service.NotifyOffloadSuccess(client_id, tasks, metas);
    return res.has_value();
}

}  // namespace

TEST(PromotionRetryBenchmark, QueueLimitRetryPublicPath) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.promotion_on_hit = true;
    config.promotion_admission_threshold = 1;
    config.promotion_queue_limit = 1;
    config.default_kv_lease_ttl = 2000;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t kSegmentBase = 0x330000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;
    auto seg =
        PrepareSegment(*service, "bench_seg", kSegmentBase, kSegmentSize);
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "bench_first",
                                       1024, seg.segment_name));
    ASSERT_TRUE(InjectLocalDiskReplica(*service, seg.client_id, "bench_retry",
                                       1024, seg.segment_name));

    auto first = service->GetReplicaList("bench_first", "default");
    ASSERT_TRUE(first.has_value());
    auto retry = service->GetReplicaList("bench_retry", "default");
    ASSERT_TRUE(retry.has_value());

    auto initial = service->PromotionObjectHeartbeat(seg.client_id);
    ASSERT_TRUE(initial.has_value());
    ASSERT_EQ(CountPromotionTask(*initial, "bench_first"), 1u);
    ASSERT_EQ(CountPromotionTask(*initial, "bench_retry"), 0u);

    auto failure = service->NotifyPromotionFailure(seg.client_id, "bench_first",
                                                   "default");
    ASSERT_TRUE(failure.has_value());

    bool retry_visible = false;
    int retry_poll_ms = -1;
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 100; ++i) {
        auto heartbeat = service->PromotionObjectHeartbeat(seg.client_id);
        ASSERT_TRUE(heartbeat.has_value());
        if (CountPromotionTask(*heartbeat, "bench_retry") == 1u) {
            retry_visible = true;
            retry_poll_ms = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count());
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    int fallback_reads_needed = 0;
    if (!retry_visible) {
        fallback_reads_needed = 1;
        auto fallback = service->GetReplicaList("bench_retry", "default");
        ASSERT_TRUE(fallback.has_value());
        auto heartbeat = service->PromotionObjectHeartbeat(seg.client_id);
        ASSERT_TRUE(heartbeat.has_value());
        retry_visible = CountPromotionTask(*heartbeat, "bench_retry") == 1u;
    }

    std::cout << "PROMOTION_RETRY_BENCHMARK_JSON={"
              << "\"retry_visible\":" << (retry_visible ? "true" : "false")
              << ",\"fallback_reads_needed\":" << fallback_reads_needed
              << ",\"retry_poll_ms\":" << retry_poll_ms << ",\"queue_limit\":1"
              << ",\"workload\":\"two_hot_local_disk_only_keys\""
              << "}" << std::endl;

    EXPECT_TRUE(retry_visible);
    service->RemoveAll();
}

}  // namespace mooncake::test
