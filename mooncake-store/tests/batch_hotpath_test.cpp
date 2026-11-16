#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "master_metric_manager.h"
#include "rpc_service.h"

namespace mooncake {
namespace {

Segment MakeSegment(uint64_t id_lo, const std::string& name, uintptr_t base,
                    size_t size = 64 * 1024 * 1024) {
    Segment seg;
    seg.id = UUID{0, id_lo};
    seg.name = name;
    seg.base = base;
    seg.size = size;
    seg.te_endpoint = name;
    return seg;
}

WrappedMasterServiceConfig MakeTestConfig() {
    WrappedMasterServiceConfig cfg;
    cfg.default_kv_lease_ttl = 500;
    cfg.default_kv_soft_pin_ttl = 1000;
    cfg.memory_allocator = BufferAllocatorType::OFFSET;
    return cfg;
}

TEST(BatchHotpathTest, EndToEndLifecycleChecksumsAndMetrics) {
    auto& metrics = MasterMetricManager::instance();
    const int64_t base_keys = metrics.get_key_count();
    const int64_t base_mem_cache = metrics.get_mem_cache_nums();
    const int64_t base_get_items = metrics.get_batch_get_replica_list_items();
    const int64_t base_get_failed =
        metrics.get_batch_get_replica_list_failed_items();
    const double base_mem_hits =
        metrics.calculate_cache_stats()
            [MasterMetricManager::CacheHitStat::MEMORY_HITS];
    const int64_t base_mem_hit_bytes = metrics.get_mem_cache_hit_bytes();

    WrappedMasterService service(MakeTestConfig());
    const UUID client{1, 100};
    ASSERT_TRUE(
        service
            .MountSegment(MakeSegment(1, "127.0.0.1:15001", 0x100000000ULL),
                          client)
            .has_value());

    constexpr size_t kBatchSize = 128;
    std::vector<std::string> keys;
    std::vector<uint64_t> lengths;
    std::vector<ObjectMeta> metas;
    keys.reserve(kBatchSize);
    lengths.reserve(kBatchSize);
    metas.reserve(kBatchSize);
    int64_t expected_bytes = 0;
    for (size_t i = 0; i < kBatchSize; ++i) {
        keys.push_back("batch_key_" + std::to_string(i));
        const uint64_t len = 1024 + static_cast<uint64_t>(i) * 16;
        lengths.push_back(len);
        expected_bytes += static_cast<int64_t>(len);
        metas.push_back(
            ObjectMeta{keys.back(), static_cast<uint64_t>(0xABC000 + i)});
    }

    ReplicateConfig rep_cfg;
    rep_cfg.replica_num = 1;
    auto start_res = service.BatchPutStart(client, keys, lengths, rep_cfg);
    ASSERT_EQ(start_res.size(), kBatchSize);
    for (const auto& r : start_res) {
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->size(), 1u);
    }
    EXPECT_EQ(metrics.get_key_count() - base_keys,
              static_cast<int64_t>(kBatchSize));

    // Processing replicas are not yet readable.
    auto pre_exist = service.BatchExistKey(keys);
    for (const auto& r : pre_exist) {
        ASSERT_TRUE(r.has_value());
        EXPECT_FALSE(*r);
    }

    auto end_res = service.BatchPutEnd(client, metas, ReplicaType::ALL);
    ASSERT_EQ(end_res.size(), kBatchSize);
    for (const auto& r : end_res) {
        EXPECT_TRUE(r.has_value());
    }
    EXPECT_EQ(metrics.get_mem_cache_nums() - base_mem_cache,
              static_cast<int64_t>(kBatchSize));

    // Idempotent retry of BatchPutEnd succeeds without double-counting metrics.
    auto retry_res = service.BatchPutEnd(client, metas, ReplicaType::ALL);
    for (const auto& r : retry_res) {
        EXPECT_TRUE(r.has_value());
    }
    EXPECT_EQ(metrics.get_mem_cache_nums() - base_mem_cache,
              static_cast<int64_t>(kBatchSize));

    auto exist_res = service.BatchExistKey(keys);
    for (const auto& r : exist_res) {
        ASSERT_TRUE(r.has_value());
        EXPECT_TRUE(*r);
    }

    std::vector<std::string> query_keys = keys;
    query_keys.push_back("missing_key");
    auto get_res = service.BatchGetReplicaList(query_keys);
    ASSERT_EQ(get_res.size(), kBatchSize + 1);
    for (size_t i = 0; i < kBatchSize; ++i) {
        ASSERT_TRUE(get_res[i].has_value());
        EXPECT_EQ(get_res[i]->replicas.size(), 1u);
        EXPECT_EQ(get_res[i]->object_checksum,
                  std::optional<uint64_t>(0xABC000 + i));
    }
    ASSERT_FALSE(get_res[kBatchSize].has_value());
    EXPECT_EQ(get_res[kBatchSize].error(), ErrorCode::OBJECT_NOT_FOUND);

    EXPECT_EQ(metrics.get_batch_get_replica_list_items() - base_get_items,
              static_cast<int64_t>(kBatchSize + 1));
    EXPECT_EQ(
        metrics.get_batch_get_replica_list_failed_items() - base_get_failed, 1);
    const double cur_mem_hits =
        metrics.calculate_cache_stats()
            [MasterMetricManager::CacheHitStat::MEMORY_HITS];
    EXPECT_DOUBLE_EQ(cur_mem_hits - base_mem_hits,
                     static_cast<double>(kBatchSize));
    EXPECT_EQ(metrics.get_mem_cache_hit_bytes() - base_mem_hit_bytes,
              expected_bytes);

    EXPECT_EQ(service.RemoveAll(true), static_cast<long>(kBatchSize));
    EXPECT_EQ(metrics.get_key_count(), base_keys);
    EXPECT_EQ(metrics.get_mem_cache_nums(), base_mem_cache);
}

TEST(BatchHotpathTest, MixedErrorsAuthorizationAndPerKeyOptions) {
    auto& metrics = MasterMetricManager::instance();
    const int64_t base_soft_pin = metrics.get_soft_pin_key_count();
    WrappedMasterService service(MakeTestConfig());
    const UUID owner{2, 200};
    const UUID other{2, 201};
    Segment owner_seg = MakeSegment(2, "127.0.0.1:15002", 0x200000000ULL);
    owner_seg.host_id = "host_owner";
    Segment other_seg = MakeSegment(3, "127.0.0.1:15003", 0x300000000ULL);
    other_seg.host_id = "host_other";
    ASSERT_TRUE(service.MountSegment(owner_seg, owner).has_value());
    ASSERT_TRUE(service.MountSegment(other_seg, other).has_value());

    ReplicateConfig rep_cfg;
    rep_cfg.replica_num = 1;

    // Mixed valid + invalid items in a single BatchPutStart.
    std::vector<std::string> mixed_keys = {"valid_a", "", "valid_b"};
    std::vector<uint64_t> mixed_lens = {512, 512, 0};
    auto start_res =
        service.BatchPutStart(owner, mixed_keys, mixed_lens, rep_cfg);
    ASSERT_EQ(start_res.size(), 3u);
    EXPECT_TRUE(start_res[0].has_value());
    ASSERT_FALSE(start_res[1].has_value());
    EXPECT_EQ(start_res[1].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_FALSE(start_res[2].has_value());
    EXPECT_EQ(start_res[2].error(), ErrorCode::INVALID_PARAMS);

    // Wrong client cannot PutEnd an in-flight write.
    auto bad_end = service.BatchPutEnd(
        other, {ObjectMeta{"valid_a", std::nullopt}}, ReplicaType::ALL);
    ASSERT_EQ(bad_end.size(), 1u);
    ASSERT_FALSE(bad_end[0].has_value());
    EXPECT_EQ(bad_end[0].error(), ErrorCode::ILLEGAL_CLIENT);

    auto good_end = service.BatchPutEnd(
        owner, {ObjectMeta{"valid_a", std::nullopt}}, ReplicaType::ALL);
    ASSERT_TRUE(good_end[0].has_value());

    // Duplicate BatchPutStart returns OBJECT_ALREADY_EXISTS for existing key.
    auto dup_res = service.BatchPutStart(owner, {"valid_a", "valid_c"},
                                         {512, 512}, rep_cfg);
    ASSERT_EQ(dup_res.size(), 2u);
    ASSERT_FALSE(dup_res[0].has_value());
    EXPECT_EQ(dup_res[0].error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_TRUE(dup_res[1].has_value());
    ASSERT_TRUE(service
                    .BatchPutEnd(owner, {ObjectMeta{"valid_c", std::nullopt}},
                                 ReplicaType::ALL)[0]
                    .has_value());

    // Unified per-key options: soft-pin + session_ids + group_ids + same-node.
    ReplicateConfig opt_cfg;
    opt_cfg.replica_num = 1;
    opt_cfg.soft_pin_action = SoftPinAction::ENABLE;
    opt_cfg.prefer_alloc_in_same_node = true;
    opt_cfg.host_id = "host_owner";
    opt_cfg.session_ids = std::vector<std::string>{"sess_0", "sess_1"};
    opt_cfg.group_ids = std::vector<std::string>{"grp_a", "grp_a"};
    std::vector<std::string> opt_keys = {"opt_0", "opt_1"};
    auto opt_start =
        service.BatchPutStart(owner, opt_keys, {256, 256}, opt_cfg);
    ASSERT_EQ(opt_start.size(), 2u);
    ASSERT_TRUE(opt_start[0].has_value());
    ASSERT_TRUE(opt_start[1].has_value());
    EXPECT_EQ(opt_start[0]
                  ->at(0)
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "127.0.0.1:15002");
    EXPECT_EQ(opt_start[1]
                  ->at(0)
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "127.0.0.1:15002");

    auto opt_end = service.BatchPutEnd(
        owner,
        {ObjectMeta{"opt_0", std::nullopt}, ObjectMeta{"opt_1", std::nullopt}},
        ReplicaType::ALL);
    EXPECT_TRUE(opt_end[0].has_value());
    EXPECT_TRUE(opt_end[1].has_value());
    EXPECT_EQ(metrics.get_soft_pin_key_count() - base_soft_pin, 2);
}

TEST(BatchHotpathTest, ConcurrentBatchReadWriteStress) {
    WrappedMasterService service(MakeTestConfig());
    const UUID setup_client{3, 300};
    ASSERT_TRUE(
        service
            .MountSegment(MakeSegment(4, "127.0.0.1:15004", 0x400000000ULL,
                                      256 * 1024 * 1024),
                          setup_client)
            .has_value());

    constexpr int kThreads = 8;
    constexpr int kBatchesPerThread = 16;
    constexpr int kBatchSize = 32;

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int tid = 0; tid < kThreads; ++tid) {
        workers.emplace_back([&, tid]() {
            const UUID client{4, static_cast<uint64_t>(tid + 1)};
            ReplicateConfig cfg;
            cfg.replica_num = 1;
            for (int b = 0; b < kBatchesPerThread; ++b) {
                std::vector<std::string> keys;
                std::vector<uint64_t> lens(kBatchSize, 512);
                std::vector<ObjectMeta> metas;
                keys.reserve(kBatchSize);
                metas.reserve(kBatchSize);
                for (int i = 0; i < kBatchSize; ++i) {
                    std::string k = "t" + std::to_string(tid) + "_b" +
                                    std::to_string(b) + "_" + std::to_string(i);
                    keys.push_back(k);
                    metas.push_back(ObjectMeta{k, std::nullopt});
                }
                auto starts = service.BatchPutStart(client, keys, lens, cfg);
                for (const auto& s : starts) {
                    EXPECT_TRUE(s.has_value());
                }
                auto ends =
                    service.BatchPutEnd(client, metas, ReplicaType::ALL);
                for (const auto& e : ends) {
                    EXPECT_TRUE(e.has_value());
                }
                auto exists = service.BatchExistKey(keys);
                for (const auto& ex : exists) {
                    ASSERT_TRUE(ex.has_value());
                    EXPECT_TRUE(*ex);
                }
                auto gets = service.BatchGetReplicaList(keys);
                for (const auto& g : gets) {
                    EXPECT_TRUE(g.has_value());
                }
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }
    EXPECT_EQ(service.RemoveAll(true),
              static_cast<long>(kThreads * kBatchesPerThread * kBatchSize));
}

}  // namespace
}  // namespace mooncake
