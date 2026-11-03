#include "allocator_metric.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

namespace mooncake::test {

namespace {

JemallocSnapshot g_snapshot;
bool g_collector_succeeds = true;
int g_collector_calls = 0;

bool FakeCollector(JemallocSnapshot& snapshot) {
    ++g_collector_calls;
    if (!g_collector_succeeds) {
        return false;
    }
    snapshot = g_snapshot;
    return true;
}

// A ylt series is emitted as "<name>[{labels}] <value>", so an exact-value
// assertion has to anchor on the leading newline to avoid matching a longer
// metric name that ends with the one under test.
bool HasSeries(const std::string& text, const std::string& name,
               const std::string& value) {
    return text.find("\n" + name + " " + value + "\n") != std::string::npos;
}

bool HasBinSeries(const std::string& text, const std::string& name,
                  const std::string& size_class, const std::string& value) {
    return text.find("\n" + name + "{size=\"" + size_class + "\"} " + value +
                     "\n") != std::string::npos;
}

}  // namespace

class AllocatorMetricTest : public ::testing::Test {
   protected:
    void SetUp() override {
        g_snapshot = JemallocSnapshot{};
        g_collector_succeeds = true;
        g_collector_calls = 0;
        SetJemallocStatsCollector(nullptr);
    }

    void TearDown() override { SetJemallocStatsCollector(nullptr); }

    // Bypasses the refresh throttle so that a test can sample repeatedly.
    static void ForceRefresh(AllocatorMetric& metric) {
        metric.RefreshLocked();
    }
};

TEST_F(AllocatorMetricTest, ProcessSeriesReportedWithoutCollector) {
    AllocatorMetric metric;
    metric.Refresh();

    std::string text;
    metric.serialize(text);

    EXPECT_NE(text.find("mooncake_process_rss_bytes"), std::string::npos);
    EXPECT_NE(text.find("mooncake_process_rss_peak_bytes"), std::string::npos);
    EXPECT_NE(text.find("mooncake_process_vsize_bytes"), std::string::npos);
    // The process is alive, so its resident size cannot be zero.
    EXPECT_FALSE(HasSeries(text, "mooncake_process_rss_bytes", "0"));
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_enabled", "0"));
}

TEST_F(AllocatorMetricTest, ZeroValuedSeriesAreStillEmitted) {
    AllocatorMetric metric;

    std::string text;
    metric.serialize(text);

    // ylt omits a metric that is still at its untouched zero, so every series
    // has to be marked in the constructor or it disappears from a fresh
    // process until the first non-zero sample.
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_allocated_bytes", "0"));
    EXPECT_TRUE(
        HasSeries(text, "mooncake_jemalloc_dirty_purge_runs_total", "0"));
}

TEST_F(AllocatorMetricTest, JemallocSeriesReportedWhenCollectorInstalled) {
    g_snapshot.allocated = 1000;
    g_snapshot.active = 2000;
    g_snapshot.resident = 3000;
    g_snapshot.retained = 4000;
    g_snapshot.dirty_bytes = 5000;
    g_snapshot.arenas = 7;
    g_snapshot.opt_dirty_decay_ms = 10000;
    g_snapshot.opt_muzzy_decay_ms = -1;
    SetJemallocStatsCollector(&FakeCollector);

    AllocatorMetric metric;
    metric.Refresh();

    std::string text;
    metric.serialize(text);

    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_enabled", "1"));
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_allocated_bytes", "1000"));
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_active_bytes", "2000"));
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_resident_bytes", "3000"));
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_retained_bytes", "4000"));
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_dirty_bytes", "5000"));
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_arenas", "7"));
    // A negative decay means the arena never purges, so the sign has to
    // survive into the gauge rather than be clamped.
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_opt_muzzy_decay_ms", "-1"));
}

TEST_F(AllocatorMetricTest, CollectorFailureLeavesJemallocDisabled) {
    g_snapshot.allocated = 1000;
    g_collector_succeeds = false;
    SetJemallocStatsCollector(&FakeCollector);

    AllocatorMetric metric;
    metric.Refresh();

    std::string text;
    metric.serialize(text);

    EXPECT_EQ(g_collector_calls, 1);
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_enabled", "0"));
    EXPECT_TRUE(HasSeries(text, "mooncake_jemalloc_allocated_bytes", "0"));
}

TEST_F(AllocatorMetricTest, CumulativeTotalsAdvanceByDelta) {
    SetJemallocStatsCollector(&FakeCollector);
    AllocatorMetric metric;

    g_snapshot.dirty_purge_runs = 10;
    g_snapshot.small_allocations = 100;
    ForceRefresh(metric);

    g_snapshot.dirty_purge_runs = 25;
    g_snapshot.small_allocations = 260;
    ForceRefresh(metric);

    std::string text;
    metric.serialize(text);

    // The counter must track jemalloc's cumulative total, not the sum of the
    // samples: adding the absolute value each time would report 35 here.
    EXPECT_TRUE(
        HasSeries(text, "mooncake_jemalloc_dirty_purge_runs_total", "25"));
    EXPECT_TRUE(
        HasSeries(text, "mooncake_jemalloc_small_allocations_total", "260"));
}

TEST_F(AllocatorMetricTest, CounterIgnoresTotalGoingBackwards) {
    SetJemallocStatsCollector(&FakeCollector);
    AllocatorMetric metric;

    g_snapshot.dirty_purge_runs = 25;
    ForceRefresh(metric);
    g_snapshot.dirty_purge_runs = 5;
    ForceRefresh(metric);

    std::string text;
    metric.serialize(text);

    // A counter can only move forward, so a total that regressed must hold the
    // series rather than push it below its previous value.
    EXPECT_TRUE(
        HasSeries(text, "mooncake_jemalloc_dirty_purge_runs_total", "25"));
}

TEST_F(AllocatorMetricTest, RefreshIsThrottled) {
    SetJemallocStatsCollector(&FakeCollector);
    AllocatorMetric metric;

    metric.Refresh();
    metric.Refresh();
    metric.Refresh();

    EXPECT_EQ(g_collector_calls, 1);
}

TEST_F(AllocatorMetricTest, BinSeriesReportedPerSizeClass) {
    g_snapshot.bins = {{160, 128, 266843, 3125}, {112, 256, 266779, 1563}};
    SetJemallocStatsCollector(&FakeCollector);

    AllocatorMetric metric;
    metric.Refresh();

    std::string text;
    metric.serialize(text);

    EXPECT_TRUE(
        HasBinSeries(text, "mooncake_jemalloc_bin_regs", "160", "266843"));
    EXPECT_TRUE(
        HasBinSeries(text, "mooncake_jemalloc_bin_slabs", "160", "3125"));
    // Live bytes are the region count times the size class.
    EXPECT_TRUE(HasBinSeries(text, "mooncake_jemalloc_bin_used_bytes", "160",
                             std::to_string(266843LL * 160)));
    // Slab bytes are what the class holds from the OS: every slab is charged
    // in full, which is the whole point of watching this series.
    EXPECT_TRUE(HasBinSeries(text, "mooncake_jemalloc_bin_slab_bytes", "160",
                             std::to_string(3125LL * 128 * 160)));
    EXPECT_TRUE(HasBinSeries(text, "mooncake_jemalloc_bin_used_bytes", "112",
                             std::to_string(266779LL * 112)));
}

TEST_F(AllocatorMetricTest, EmptyBinKeepsItsSeries) {
    g_snapshot.bins = {{160, 128, 0, 0}};
    SetJemallocStatsCollector(&FakeCollector);

    AllocatorMetric metric;
    metric.Refresh();

    std::string text;
    metric.serialize(text);

    // A size class that drains must report zero rather than vanish, or the
    // series goes stale and Grafana keeps drawing its last value.
    EXPECT_TRUE(HasBinSeries(text, "mooncake_jemalloc_bin_slabs", "160", "0"));
    EXPECT_TRUE(
        HasBinSeries(text, "mooncake_jemalloc_bin_slab_bytes", "160", "0"));
}

TEST_F(AllocatorMetricTest, ResampledBinsReplaceRatherThanAccumulate) {
    g_snapshot.bins = {{160, 128, 10, 1}};
    SetJemallocStatsCollector(&FakeCollector);
    AllocatorMetric metric;

    ForceRefresh(metric);
    g_snapshot.bins = {{160, 128, 20, 2}};
    ForceRefresh(metric);

    std::string text;
    metric.serialize(text);

    // One series per size class, holding the newest sample. A collector that
    // appended to the caller's vector would grow it without bound and leave
    // the first sample in place here.
    EXPECT_TRUE(HasBinSeries(text, "mooncake_jemalloc_bin_regs", "160", "20"));
    EXPECT_EQ(text.find("mooncake_jemalloc_bin_regs{size=\"160\"}"),
              text.rfind("mooncake_jemalloc_bin_regs{size=\"160\"}"));
}

}  // namespace mooncake::test
