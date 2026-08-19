#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

#include "client_metric.h"

namespace mooncake::test {

class SsdMetricsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("SsdMetricsTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(SsdMetricsTest, InitialValuesTest) {
    SsdMetric metrics;

    ASSERT_EQ(metrics.ssd_read_bytes.value(), 0);
    ASSERT_EQ(metrics.ssd_write_bytes.value(), 0);
    ASSERT_EQ(metrics.ssd_read_ops.value(), 0);
    ASSERT_EQ(metrics.ssd_write_ops.value(), 0);

    // Histogram bucket counts should all be 0
    auto read_buckets = metrics.ssd_read_latency_us.get_bucket_counts();
    for (auto& bucket : read_buckets) {
        ASSERT_EQ(bucket->value(), 0);
    }
    auto write_buckets = metrics.ssd_write_latency_us.get_bucket_counts();
    for (auto& bucket : write_buckets) {
        ASSERT_EQ(bucket->value(), 0);
    }

    // Total metrics
    ASSERT_EQ(metrics.ssd_total_bytes.value(), 0);
    ASSERT_EQ(metrics.ssd_total_ops.value(), 0);
}

TEST_F(SsdMetricsTest, ReadMetricsTest) {
    SsdMetric metrics;

    // Simulate a successful BatchLoad of 3 keys totaling 1MB
    metrics.ssd_read_ops.inc(3);
    metrics.ssd_read_bytes.inc(1024 * 1024);
    metrics.ssd_read_latency_us.observe(1500);  // 1.5ms

    ASSERT_EQ(metrics.ssd_read_ops.value(), 3);
    ASSERT_EQ(metrics.ssd_read_bytes.value(), 1024 * 1024);

    // Simulate another BatchLoad of 5 keys totaling 2MB
    metrics.ssd_read_ops.inc(5);
    metrics.ssd_read_bytes.inc(2 * 1024 * 1024);
    metrics.ssd_read_latency_us.observe(3000);  // 3ms

    ASSERT_EQ(metrics.ssd_read_ops.value(), 8);
    ASSERT_EQ(metrics.ssd_read_bytes.value(), 3 * 1024 * 1024);

    // Write metrics should remain 0
    ASSERT_EQ(metrics.ssd_write_ops.value(), 0);
    ASSERT_EQ(metrics.ssd_write_bytes.value(), 0);
}

TEST_F(SsdMetricsTest, WriteMetricsTest) {
    SsdMetric metrics;

    // Simulate a successful BatchOffload of 10 keys totaling 5MB
    metrics.ssd_write_ops.inc(10);
    metrics.ssd_write_bytes.inc(5 * 1024 * 1024);
    metrics.ssd_write_latency_us.observe(50000);  // 50ms

    ASSERT_EQ(metrics.ssd_write_ops.value(), 10);
    ASSERT_EQ(metrics.ssd_write_bytes.value(), 5 * 1024 * 1024);

    // Read metrics should remain 0
    ASSERT_EQ(metrics.ssd_read_ops.value(), 0);
    ASSERT_EQ(metrics.ssd_read_bytes.value(), 0);
}

TEST_F(SsdMetricsTest, TotalMetricsTest) {
    SsdMetric metrics;

    // Simulate read: 3 keys, 1MB
    metrics.ssd_read_ops.inc(3);
    metrics.ssd_read_bytes.inc(1024 * 1024);
    metrics.ssd_read_latency_us.observe(1500);
    metrics.ssd_read_latency_summary.observe(1500);
    metrics.ssd_total_ops.inc(3);
    metrics.ssd_total_bytes.inc(1024 * 1024);
    metrics.ssd_total_latency_us.observe(1500);
    metrics.ssd_total_latency_summary.observe(1500);

    // Simulate write: 5 keys, 2MB
    metrics.ssd_write_ops.inc(5);
    metrics.ssd_write_bytes.inc(2 * 1024 * 1024);
    metrics.ssd_write_latency_us.observe(3000);
    metrics.ssd_write_latency_summary.observe(3000);
    metrics.ssd_total_ops.inc(5);
    metrics.ssd_total_bytes.inc(2 * 1024 * 1024);
    metrics.ssd_total_latency_us.observe(3000);
    metrics.ssd_total_latency_summary.observe(3000);

    // Verify totals = read + write
    ASSERT_EQ(metrics.ssd_total_ops.value(), 3 + 5);
    ASSERT_EQ(metrics.ssd_total_bytes.value(), 1024 * 1024 + 2 * 1024 * 1024);

    // Total latency histogram should have 2 observations
    auto buckets = metrics.ssd_total_latency_us.get_bucket_counts();
    int64_t total_count = 0;
    for (auto& b : buckets) total_count += b->value();
    ASSERT_EQ(total_count, 2);

    // Verify summary shows Total line
    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("SSD Total:") != std::string::npos);
    EXPECT_TRUE(summary.find("Total:") != std::string::npos);

    std::cout << "Total Metrics Summary:\n" << summary << std::endl;
}

TEST_F(SsdMetricsTest, FailureNotCountedTest) {
    SsdMetric metrics;

    // Simulate: nothing recorded (as if BatchLoad failed and we skipped
    // metrics) All metrics should remain 0
    ASSERT_EQ(metrics.ssd_read_ops.value(), 0);
    ASSERT_EQ(metrics.ssd_read_bytes.value(), 0);
    ASSERT_EQ(metrics.ssd_write_ops.value(), 0);
    ASSERT_EQ(metrics.ssd_write_bytes.value(), 0);

    // Now simulate a success after a failure
    metrics.ssd_read_ops.inc(1);
    metrics.ssd_read_bytes.inc(4096);
    metrics.ssd_read_latency_us.observe(100);

    // Only the successful operation should be counted
    ASSERT_EQ(metrics.ssd_read_ops.value(), 1);
    ASSERT_EQ(metrics.ssd_read_bytes.value(), 4096);
}

TEST_F(SsdMetricsTest, ConcurrentTest) {
    SsdMetric metrics;

    const int num_threads = 8;
    const int ops_per_thread = 1000;

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&metrics, ops_per_thread]() {
            for (int j = 0; j < ops_per_thread; ++j) {
                metrics.ssd_read_ops.inc(1);
                metrics.ssd_read_bytes.inc(4096);
                metrics.ssd_read_latency_us.observe(500);
                metrics.ssd_write_ops.inc(1);
                metrics.ssd_write_bytes.inc(8192);
                metrics.ssd_write_latency_us.observe(1000);
                metrics.ssd_total_ops.inc(2);
                metrics.ssd_total_bytes.inc(4096 + 8192);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    int64_t expected_ops = num_threads * ops_per_thread;
    ASSERT_EQ(metrics.ssd_read_ops.value(), expected_ops);
    ASSERT_EQ(metrics.ssd_read_bytes.value(), expected_ops * 4096);
    ASSERT_EQ(metrics.ssd_write_ops.value(), expected_ops);
    ASSERT_EQ(metrics.ssd_write_bytes.value(), expected_ops * 8192);
    ASSERT_EQ(metrics.ssd_total_ops.value(), expected_ops * 2);
    ASSERT_EQ(metrics.ssd_total_bytes.value(), expected_ops * (4096 + 8192));
}

TEST_F(SsdMetricsTest, SerializeTest) {
    SsdMetric metrics;

    // Add some data so serialization produces output
    metrics.ssd_read_ops.inc(5);
    metrics.ssd_read_bytes.inc(1024);
    metrics.ssd_read_latency_us.observe(200);
    metrics.ssd_write_ops.inc(3);
    metrics.ssd_write_bytes.inc(2048);
    metrics.ssd_write_latency_us.observe(1000);
    metrics.ssd_total_ops.inc(8);
    metrics.ssd_total_bytes.inc(3072);
    metrics.ssd_total_latency_us.observe(200);
    metrics.ssd_total_latency_us.observe(1000);
    metrics.ssd_read_latency_summary.observe(200);
    metrics.ssd_write_latency_summary.observe(1000);
    metrics.ssd_total_latency_summary.observe(200);
    metrics.ssd_total_latency_summary.observe(1000);

    std::string serialized;
    metrics.serialize(serialized);

    // Verify all metric names appear in serialized output
    EXPECT_TRUE(serialized.find("mooncake_ssd_read_bytes_total") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_ssd_write_bytes_total") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_ssd_read_ops_total") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_ssd_write_ops_total") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_ssd_read_latency_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_ssd_write_latency_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_ssd_total_bytes_total") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_ssd_total_ops_total") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_ssd_total_latency_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_ssd_read_latency_summary_us") !=
                std::string::npos);

    std::cout << "Serialized SSD Metrics:\n" << serialized << std::endl;
}

TEST_F(SsdMetricsTest, SummaryMetricsTest) {
    SsdMetric metrics;

    // Test empty metrics - no throughput shown when bytes=0
    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("SSD Metrics Summary") != std::string::npos);
    EXPECT_TRUE(summary.find("SSD Read: 0 B") != std::string::npos);
    EXPECT_TRUE(summary.find("SSD Write: 0 B") != std::string::npos);
    EXPECT_TRUE(summary.find("Read: No data") != std::string::npos);
    EXPECT_TRUE(summary.find("Write: No data") != std::string::npos);
    // No throughput/IOPS when no data
    EXPECT_TRUE(summary.find("throughput=") == std::string::npos);
    EXPECT_TRUE(summary.find("IOPS=") == std::string::npos);

    // Add some data
    metrics.ssd_read_bytes.inc(5 * 1024 * 1024);    // 5MB
    metrics.ssd_write_bytes.inc(10 * 1024 * 1024);  // 10MB
    metrics.ssd_read_ops.inc(100);
    metrics.ssd_write_ops.inc(50);

    // Add latency observations (both histogram and summary)
    for (int i = 0; i < 50; ++i) {
        metrics.ssd_read_latency_us.observe(500);
        metrics.ssd_read_latency_summary.observe(500);
    }
    for (int i = 0; i < 40; ++i) {
        metrics.ssd_read_latency_us.observe(2000);
        metrics.ssd_read_latency_summary.observe(2000);
    }
    for (int i = 0; i < 10; ++i) {
        metrics.ssd_read_latency_us.observe(50000);
        metrics.ssd_read_latency_summary.observe(50000);
    }

    summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("SSD Read: 5.00 MB") != std::string::npos);
    EXPECT_TRUE(summary.find("SSD Write: 10.00 MB") != std::string::npos);
    EXPECT_TRUE(summary.find("ops=100") != std::string::npos);
    EXPECT_TRUE(summary.find("ops=50") != std::string::npos);
    // Throughput and IOPS should now appear
    EXPECT_TRUE(summary.find("throughput=") != std::string::npos);
    EXPECT_TRUE(summary.find("/s") != std::string::npos);
    EXPECT_TRUE(summary.find("IOPS=") != std::string::npos);
    // Latency percentiles
    EXPECT_TRUE(summary.find("p50=") != std::string::npos);
    EXPECT_TRUE(summary.find("p90=") != std::string::npos);
    EXPECT_TRUE(summary.find("p99=") != std::string::npos);

    std::cout << "SSD Metrics Summary:\n" << summary << std::endl;
}

TEST_F(SsdMetricsTest, ThroughputCalculationTest) {
    SsdMetric metrics;

    // Record 10MB of reads and 20MB of writes
    int64_t read_bytes = 10 * 1024 * 1024;
    int64_t write_bytes = 20 * 1024 * 1024;
    metrics.ssd_read_bytes.inc(read_bytes);
    metrics.ssd_write_bytes.inc(write_bytes);
    metrics.ssd_read_ops.inc(1000);
    metrics.ssd_write_ops.inc(2000);
    metrics.ssd_read_latency_us.observe(1000);
    metrics.ssd_read_latency_summary.observe(1000);
    metrics.ssd_write_latency_us.observe(2000);
    metrics.ssd_write_latency_summary.observe(2000);
    metrics.ssd_total_bytes.inc(read_bytes + write_bytes);
    metrics.ssd_total_ops.inc(3000);
    metrics.ssd_total_latency_us.observe(1000);
    metrics.ssd_total_latency_us.observe(2000);
    metrics.ssd_total_latency_summary.observe(1000);
    metrics.ssd_total_latency_summary.observe(2000);

    // Wait a small amount to get non-zero elapsed time
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::string summary = metrics.summary_metrics();

    // Verify throughput is displayed for both read and write
    // Read line should have: throughput=<value>/s, IOPS=<value>
    EXPECT_TRUE(summary.find("throughput=") != std::string::npos)
        << "Missing throughput in summary:\n"
        << summary;
    EXPECT_TRUE(summary.find("IOPS=") != std::string::npos)
        << "Missing IOPS in summary:\n"
        << summary;

    // Count occurrences of "throughput=" - should be 3 (read + write + total)
    size_t count = 0;
    size_t pos = 0;
    while ((pos = summary.find("throughput=", pos)) != std::string::npos) {
        ++count;
        pos += 11;
    }
    EXPECT_EQ(count, 3) << "Expected throughput for read, write, and total";

    // Count occurrences of "IOPS=" - should be 3 (read + write + total)
    count = 0;
    pos = 0;
    while ((pos = summary.find("IOPS=", pos)) != std::string::npos) {
        ++count;
        pos += 5;
    }
    EXPECT_EQ(count, 3) << "Expected IOPS for read, write, and total";

    std::cout << "Throughput Test Summary:\n" << summary << std::endl;
}

TEST_F(SsdMetricsTest, IntegrationWithClientMetric) {
    ClientMetric client_metrics;

    // Add SSD data via client_metrics.ssd_metric
    client_metrics.ssd_metric.ssd_read_ops.inc(10);
    client_metrics.ssd_metric.ssd_read_bytes.inc(1024 * 1024);
    client_metrics.ssd_metric.ssd_read_latency_us.observe(1500);

    // Verify it appears in ClientMetric serialize
    std::string serialized;
    client_metrics.serialize(serialized);
    EXPECT_TRUE(serialized.find("mooncake_ssd_read_ops_total") !=
                std::string::npos);

    // Verify it appears in ClientMetric summary
    std::string summary = client_metrics.summary_metrics();
    EXPECT_TRUE(summary.find("SSD Metrics Summary") != std::string::npos);

    std::cout << "Client Metrics with SSD:\n" << summary << std::endl;
}

TEST_F(SsdMetricsTest, SerializeWithDynamicLabels) {
    std::map<std::string, std::string> labels = {{"instance_id", "test123"},
                                                 {"cluster_id", "cluster_abc"}};
    SsdMetric metrics(labels);

    metrics.ssd_read_ops.inc(1);
    metrics.ssd_read_latency_us.observe(100);

    std::string serialized;
    metrics.serialize(serialized);

    EXPECT_TRUE(serialized.find("instance_id=\"test123\"") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("cluster_id=\"cluster_abc\"") !=
                std::string::npos);

    std::cout << "SSD Metrics with labels:\n" << serialized << std::endl;
}

TEST_F(SsdMetricsTest, LatencyBucketBoundaryTest) {
    SsdMetric metrics;

    // kSsdLatencyBucket = {50, 100, 200, 500, 1000, ... 10000000, 30000000}

    // Value below the smallest bucket (50us)
    metrics.ssd_read_latency_us.observe(25);
    // Value exactly at bucket boundary (50us)
    metrics.ssd_read_latency_us.observe(50);
    // Value at the largest bucket boundary (30s)
    metrics.ssd_read_latency_us.observe(30000000);
    // Value exceeding all buckets -> falls into +Inf
    metrics.ssd_read_latency_us.observe(60000000);

    auto buckets = metrics.ssd_read_latency_us.get_bucket_counts();
    ASSERT_FALSE(buckets.empty());

    // First bucket (le=50): should contain 2 observations (25 and 50)
    EXPECT_EQ(buckets[0]->value(), 2);

    // Last real bucket (le=30000000): should contain 1 observation (30000000)
    // (buckets[0..N-2] are real buckets, buckets[N-1] is +Inf)
    size_t last_real_idx = kSsdLatencyBucket.size() - 1;
    EXPECT_EQ(buckets[last_real_idx]->value(), 1);

    // +Inf bucket: should contain 1 observation (60000000)
    size_t inf_idx = buckets.size() - 1;
    EXPECT_EQ(buckets[inf_idx]->value(), 1);

    // Verify total count via serialization
    std::string serialized;
    metrics.serialize(serialized);
    EXPECT_TRUE(serialized.find("mooncake_ssd_read_latency_us_count 4") !=
                std::string::npos);

    std::cout << "Bucket boundary test serialization:\n"
              << serialized << std::endl;
}

TEST_F(SsdMetricsTest, EmptyBatchMetricsTest) {
    SsdMetric metrics;

    // Simulate recording metrics for an empty batch (0 keys, 0 bytes)
    // This mirrors what happens if FileStorage::BatchLoad is called with
    // an empty map and succeeds - the instrumentation code would compute
    // total_bytes=0 and batch_object.size()=0
    metrics.ssd_read_ops.inc(0);
    metrics.ssd_read_bytes.inc(0);
    metrics.ssd_read_latency_us.observe(10);  // Still takes some time

    // Counters should remain 0 (inc(0) is a no-op for counters)
    ASSERT_EQ(metrics.ssd_read_ops.value(), 0);
    ASSERT_EQ(metrics.ssd_read_bytes.value(), 0);

    // But latency histogram should record the observation
    auto buckets = metrics.ssd_read_latency_us.get_bucket_counts();
    int64_t total = 0;
    for (auto& b : buckets) total += b->value();
    EXPECT_EQ(total, 1);
}

}  // namespace mooncake::test
