// Measures the master-side cost of the read-only batch replica lookup the
// holder reclaim pass issues (BatchGetReplicaListForAdmin), per key, on a
// master holding as many objects as a holder with 10k+ full buckets.
// Disabled by default; run with --gtest_also_run_disabled_tests.
#include <time.h>

#include <chrono>
#include <cstdlib>

#include "master_service_test_fixture.h"

namespace mooncake::test {

namespace {
double ThreadCpuSeconds() {
    timespec ts{};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}
}  // namespace

TEST_F(MasterServiceTest, DISABLED_ReclaimQueryCostPerKey) {
    const size_t num_keys = std::getenv("RECLAIM_BENCH_KEYS")
                                ? std::stoul(std::getenv("RECLAIM_BENCH_KEYS"))
                                : 5'000'000;
    constexpr size_t kSlice = 1024;
    MasterServiceConfig config;
    config.client_active_ttl_sec = 3600;
    config.client_suspicion_ttl_sec = 3600;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(
        service, "bench_segment", kDefaultSegmentBase, num_keys * kSlice * 2);

    std::vector<std::string> keys;
    keys.reserve(num_keys);
    ReplicateConfig replicate;
    replicate.replica_num = 1;
    for (size_t i = 0; i < num_keys; ++i) {
        keys.push_back("aux/session/" + std::to_string(i / 500) + "/" +
                       std::to_string(i));
        ASSERT_TRUE(service.PutStart(context.client_id, keys.back(),
                                     TenantId::Default(), kSlice, replicate));
        ASSERT_TRUE(service.PutEnd(context.client_id, keys.back(),
                                   TenantId::Default(), ReplicaType::MEMORY));
    }

    for (size_t chunk : {500ul, 10'000ul}) {
        const double cpu0 = ThreadCpuSeconds();
        const auto t0 = std::chrono::steady_clock::now();
        size_t found = 0;
        for (size_t i = 0; i < num_keys; i += chunk) {
            std::vector<std::string> batch(
                keys.begin() + i, keys.begin() + std::min(num_keys, i + chunk));
            for (const auto& r : service.BatchGetReplicaListForAdmin(
                     batch, TenantId::Default())) {
                found += r.has_value();
            }
        }
        const double wall =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
                .count();
        const double cpu = ThreadCpuSeconds() - cpu0;
        EXPECT_EQ(found, num_keys);
        LOG(INFO) << "RECLAIM_BENCH master keys=" << num_keys
                  << " chunk=" << chunk << " wall_s=" << wall
                  << " cpu_s=" << cpu << " ns_per_key=" << cpu * 1e9 / num_keys;
    }
}

}  // namespace mooncake::test
