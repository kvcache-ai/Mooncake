#include <gtest/gtest.h>

#include <atomic>
#include <cmath>
#include <cstdint>
#include <latch>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "metrics/registry.h"
#include "metrics/sharded.h"
#include "prometheus/counter.h"
#include "prometheus/family.h"

namespace mooncake::metrics {
namespace {

#ifdef MOONCAKE_METRICS_NOOP
TEST(MetricsNoopTest, RecordingLeavesMetricsAtZero) {
    ShardedCounterFamily counter_family("noop_counter_total", "noop counter",
                                        std::vector<std::string>{});
    auto& counter = counter_family.Add({});
    counter.Inc(123);
    EXPECT_EQ(counter.Value(), 0U);

    auto counter_families = counter_family.Collect();
    ASSERT_EQ(counter_families.size(), 1U);
    ASSERT_EQ(counter_families[0].metric.size(), 1U);
    EXPECT_DOUBLE_EQ(counter_families[0].metric[0].counter.value, 0.0);

    ShardedHistogramFamily histogram_family(
        "noop_request_seconds", "noop histogram", std::vector<std::string>{},
        std::vector<double>{1.0, 2.0});
    auto& histogram = histogram_family.Add({});
    histogram.Observe(1.5);

    auto histogram_families = histogram_family.Collect();
    ASSERT_EQ(histogram_families.size(), 1U);
    ASSERT_EQ(histogram_families[0].metric.size(), 1U);
    const auto& collected = histogram_families[0].metric[0].histogram;
    EXPECT_EQ(collected.sample_count, 0U);
    EXPECT_DOUBLE_EQ(collected.sample_sum, 0.0);
    ASSERT_EQ(collected.bucket.size(), 3U);
    for (const auto& bucket : collected.bucket) {
        EXPECT_EQ(bucket.cumulative_count, 0U);
    }
}
#else
TEST(MetricsRegistryTest, CombinesDirectAndShardedFamiliesInNameOrder) {
    Registry registry;
    auto& direct = prometheus::BuildCounter()
                       .Name("z_direct_total")
                       .Help("direct counter")
                       .Register(registry.prom());
    direct.Add({}).Increment(3);

    auto sharded = std::make_shared<ShardedCounterFamily>(
        "a_sharded_total", "sharded counter", std::vector<std::string>{});
    sharded->Add({}).Inc(7);
    registry.Register(sharded);

    const auto collected = registry.Collect();
    ASSERT_EQ(collected.size(), 2U);
    EXPECT_EQ(collected[0].name, "a_sharded_total");
    EXPECT_EQ(collected[1].name, "z_direct_total");

    const auto text = registry.SerializeText();
    const auto sharded_pos = text.find("# HELP a_sharded_total");
    const auto direct_pos = text.find("# HELP z_direct_total");
    ASSERT_NE(sharded_pos, std::string::npos);
    ASSERT_NE(direct_pos, std::string::npos);
    EXPECT_LT(sharded_pos, direct_pos);
    EXPECT_NE(text.find("a_sharded_total 7"), std::string::npos);
    EXPECT_NE(text.find("z_direct_total 3"), std::string::npos);

    EXPECT_TRUE(registry.Remove(sharded));
    const auto after_remove = registry.SerializeText();
    EXPECT_EQ(after_remove.find("a_sharded_total"), std::string::npos);
    EXPECT_NE(after_remove.find("z_direct_total"), std::string::npos);
    EXPECT_FALSE(registry.Remove(sharded));
}

TEST(ShardedFamilyTest, AddReturnsStableSeriesAndRemoveDropsIt) {
    ShardedCounterFamily counters("ops_total", "counter",
                                  std::vector<std::string>{"op"});
    auto& get = counters.Add({{"op", "get"}});
    EXPECT_EQ(&get, &counters.Add({{"op", "get"}}));
    counters.Add({{"op", "put"}});
    EXPECT_EQ(counters.Collect()[0].metric.size(), 2U);
    EXPECT_TRUE(counters.Remove(get));
    EXPECT_FALSE(counters.Remove(get));
    EXPECT_EQ(counters.Collect()[0].metric.size(), 1U);

    ShardedHistogramFamily histograms("op_seconds", "histogram",
                                      std::vector<std::string>{"op"},
                                      std::vector<double>{1.0});
    auto& put = histograms.Add({{"op", "put"}});
    EXPECT_EQ(&put, &histograms.Add({{"op", "put"}}));
    EXPECT_TRUE(histograms.Remove(put));
    EXPECT_TRUE(histograms.Collect()[0].metric.empty());
}

TEST(ShardedHistogramTest, ManyBucketsSpanCacheLines) {
    std::vector<double> bounds;
    for (int i = 1; i <= 20; ++i) bounds.push_back(i);
    ShardedHistogramFamily family("wide_seconds", "histogram",
                                  std::vector<std::string>{}, bounds);
    auto& histogram = family.Add({});
    for (int i = 1; i <= 21; ++i) histogram.Observe(i);
    const auto collected = family.Collect();
    const auto& metric = collected[0].metric[0].histogram;
    ASSERT_EQ(metric.bucket.size(), 21U);
    for (size_t i = 0; i < metric.bucket.size(); ++i) {
        EXPECT_EQ(metric.bucket[i].cumulative_count, i + 1);
    }
    EXPECT_DOUBLE_EQ(metric.sample_sum, 231.0);
}

TEST(ShardedCounterTest, IncUpdatesValue) {
    ShardedCounterFamily family("single_counter_total", "counter",
                                std::vector<std::string>{});
    auto& counter = family.Add({});
    EXPECT_EQ(counter.Value(), 0U);
    counter.Inc(3);
    counter.Inc();
    EXPECT_EQ(counter.Value(), 4U);
}

TEST(ShardedCounterTest, ConcurrentIncIsExactAndSerializes) {
    auto family = std::make_shared<ShardedCounterFamily>(
        "metric_counter_total", "counter", std::vector<std::string>{"label"});
    auto& counter = family->Add({{"label", "x"}});

    constexpr size_t kThreads = 8;
    constexpr uint64_t kIterations = 100000;
    std::latch start(kThreads);
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (size_t i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            start.arrive_and_wait();
            for (uint64_t j = 0; j < kIterations; ++j) {
                counter.Inc();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(counter.Value(), kThreads * kIterations);

    Registry registry;
    registry.Register(family);
    const auto text = registry.SerializeText();
    EXPECT_NE(text.find("metric_counter_total{label=\"x\"} 800000"),
              std::string::npos);
}

TEST(ShardedHistogramTest, UsesPrometheusBoundarySemantics) {
    auto family = std::make_shared<ShardedHistogramFamily>(
        "request_seconds", "histogram", std::vector<std::string>{},
        std::vector<double>{1.0, 2.0, 4.0});
    auto& histogram = family->Add({});
    histogram.Observe(1.0);
    histogram.Observe(2.0);
    histogram.Observe(3.0);
    histogram.Observe(5.0);

    const auto collected = family->Collect();
    ASSERT_EQ(collected.size(), 1U);
    ASSERT_EQ(collected[0].metric.size(), 1U);
    const auto& metric = collected[0].metric[0].histogram;
    ASSERT_EQ(metric.bucket.size(), 4U);
    EXPECT_DOUBLE_EQ(metric.bucket[0].upper_bound, 1.0);
    EXPECT_EQ(metric.bucket[0].cumulative_count, 1U);
    EXPECT_DOUBLE_EQ(metric.bucket[1].upper_bound, 2.0);
    EXPECT_EQ(metric.bucket[1].cumulative_count, 2U);
    EXPECT_DOUBLE_EQ(metric.bucket[2].upper_bound, 4.0);
    EXPECT_EQ(metric.bucket[2].cumulative_count, 3U);
    EXPECT_TRUE(std::isinf(metric.bucket[3].upper_bound));
    EXPECT_EQ(metric.bucket[3].cumulative_count, 4U);
    EXPECT_EQ(metric.sample_count, 4U);
    EXPECT_DOUBLE_EQ(metric.sample_sum, 11.0);

    Registry registry;
    registry.Register(family);
    const auto text = registry.SerializeText();
    EXPECT_NE(text.find("request_seconds_bucket{le=\"1\"} 1"),
              std::string::npos);
    EXPECT_NE(text.find("request_seconds_bucket{le=\"2\"} 2"),
              std::string::npos);
    EXPECT_NE(text.find("request_seconds_bucket{le=\"4\"} 3"),
              std::string::npos);
    EXPECT_NE(text.find("request_seconds_bucket{le=\"+Inf\"} 4"),
              std::string::npos);
    EXPECT_NE(text.find("request_seconds_count 4"), std::string::npos);
    EXPECT_NE(text.find("request_seconds_sum 11"), std::string::npos);
}

TEST(ShardedHistogramTest, NanCountsOnlyTowardInf) {
    ShardedHistogramFamily family("nan_seconds", "histogram",
                                  std::vector<std::string>{},
                                  std::vector<double>{1.0, 2.0});
    family.Add({}).Observe(std::nan(""));
    const auto collected = family.Collect();
    const auto& metric = collected[0].metric[0].histogram;
    EXPECT_EQ(metric.bucket[0].cumulative_count, 0U);
    EXPECT_EQ(metric.bucket[1].cumulative_count, 0U);
    EXPECT_EQ(metric.bucket[2].cumulative_count, 1U);
    EXPECT_EQ(metric.sample_count, 1U);
}

TEST(ShardedHistogramTest, ConcurrentScrapesRemainConsistent) {
    ShardedHistogramFamily family("event_seconds", "histogram",
                                  std::vector<std::string>{},
                                  std::vector<double>{1.0, 2.0, 4.0});
    auto& histogram = family.Add({});

    constexpr size_t kWriterCount = 4;
    std::atomic<bool> stop{false};
    std::latch started(kWriterCount);
    std::vector<std::thread> writers;
    writers.reserve(kWriterCount);
    for (size_t i = 0; i < kWriterCount; ++i) {
        writers.emplace_back([&] {
            bool first = true;
            while (!stop.load(std::memory_order_relaxed)) {
                histogram.Observe(0.5);
                histogram.Observe(1.0);
                histogram.Observe(3.0);
                histogram.Observe(8.0);
                if (first) {
                    started.count_down();
                    first = false;
                }
            }
        });
    }
    started.wait();

    bool consistent = true;
    uint64_t last_count = 0;
    std::vector<uint64_t> previous;
    for (size_t scrape = 0; scrape < 1000 && consistent; ++scrape) {
        const auto collected = family.Collect();
        if (collected.size() != 1U || collected[0].metric.size() != 1U) {
            consistent = false;
            break;
        }
        const auto& metric = collected[0].metric[0].histogram;
        if (metric.bucket.empty() ||
            metric.sample_count != metric.bucket.back().cumulative_count) {
            consistent = false;
            break;
        }
        if (previous.empty()) {
            previous.resize(metric.bucket.size(), 0);
        }
        if (previous.size() != metric.bucket.size()) {
            consistent = false;
            break;
        }
        for (size_t i = 0; i < metric.bucket.size(); ++i) {
            if (metric.bucket[i].cumulative_count < previous[i]) {
                consistent = false;
                break;
            }
            previous[i] = metric.bucket[i].cumulative_count;
        }
        last_count = metric.sample_count;
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto& writer : writers) {
        writer.join();
    }

    EXPECT_TRUE(consistent);
    EXPECT_GT(last_count, 0U);
}

TEST(MetricsValidationTest, RejectsInvalidLabelsBoundsAndShardCounts) {
    ShardedCounterFamily counter_family("bad_counter_total", "counter",
                                        std::vector<std::string>{"label"});
    EXPECT_THROW(counter_family.Add({{"other", "x"}}), std::invalid_argument);
    EXPECT_THROW(counter_family.Add({}), std::invalid_argument);
    EXPECT_THROW(ShardedCounterFamily("bad_shards_total", "counter",
                                      std::vector<std::string>{}, 0),
                 std::invalid_argument);

    EXPECT_THROW(ShardedHistogramFamily("bad_histogram_seconds", "histogram",
                                        std::vector<std::string>{},
                                        std::vector<double>{}),
                 std::invalid_argument);
    EXPECT_THROW(ShardedHistogramFamily("bad_histogram_seconds", "histogram",
                                        std::vector<std::string>{},
                                        std::vector<double>{1.0, 1.0}),
                 std::invalid_argument);
    EXPECT_THROW(ShardedHistogramFamily("bad_histogram_seconds", "histogram",
                                        std::vector<std::string>{},
                                        std::vector<double>{2.0, 1.0}),
                 std::invalid_argument);

    ShardedHistogramFamily histogram_family(
        "labeled_histogram_seconds", "histogram",
        std::vector<std::string>{"label"}, std::vector<double>{1.0});
    EXPECT_THROW(histogram_family.Add({{"other", "x"}}), std::invalid_argument);
}
#endif

}  // namespace
}  // namespace mooncake::metrics
