// Copyright 2026 KVCache.AI
//
// Two-sided managed-buffer path: alloc → WRITE/READ → COMPLETED → release.
// Requires RDMA. Self-skips when none is present.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <infiniband/verbs.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "config.h"
#include "transfer_engine.h"

using namespace mooncake;

namespace {

std::string pickRdmaDevice() {
    const char *override_name = std::getenv("MC_TEST_DEVICE_NAME");
    if (override_name && *override_name) return override_name;
    int num_devices = 0;
    ibv_device **list = ibv_get_device_list(&num_devices);
    if (!list || num_devices == 0) {
        if (list) ibv_free_device_list(list);
        return "";
    }
    std::string name = ibv_get_device_name(list[0]);
    ibv_free_device_list(list);
    return name;
}

// Busy-wait for COMPLETED. Sleeping here dwarfs real RDMA latency.
bool waitCompleted(TransferEngine &engine, BatchID batch,
                   int timeout_ms = 10000) {
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        TransferStatus st;
        Status s = engine.getTransferStatus(batch, 0, st);
        if (!s.ok()) return false;
        if (st.s == TransferStatusEnum::COMPLETED) return true;
        if (st.s == TransferStatusEnum::FAILED) return false;
    }
    return false;
}

struct LatencyStats {
    double min_us = 0;
    double mean_us = 0;
    double p50_us = 0;
    double p99_us = 0;
    double max_us = 0;
};

LatencyStats summarize(std::vector<double> samples_us) {
    LatencyStats s;
    if (samples_us.empty()) return s;
    std::sort(samples_us.begin(), samples_us.end());
    s.min_us = samples_us.front();
    s.max_us = samples_us.back();
    s.mean_us = std::accumulate(samples_us.begin(), samples_us.end(), 0.0) /
                samples_us.size();
    auto pct = [&](double p) {
        size_t idx = static_cast<size_t>(
            std::llround(p * (samples_us.size() - 1)));
        return samples_us[idx];
    };
    s.p50_us = pct(0.50);
    s.p99_us = pct(0.99);
    return s;
}

void logStats(const char *tag, size_t len, const LatencyStats &s) {
    LOG(INFO) << tag << " len=" << len << " min_us=" << s.min_us
              << " mean_us=" << s.mean_us << " p50_us=" << s.p50_us
              << " p99_us=" << s.p99_us << " max_us=" << s.max_us;
}

// Measure submit→COMPLETED RTT samples (µs), synchronous one-inflight.
bool measureWriteLatency(TransferEngine &engine, SegmentID remote, void *src,
                         uint64_t dst_addr, size_t len, int warmup, int iters,
                         std::vector<double> &out_us) {
    out_us.clear();
    out_us.reserve(iters);
    TransferRequest req;
    req.opcode = TransferRequest::WRITE;
    req.source = src;
    req.target_id = remote;
    req.target_offset = dst_addr;
    req.length = len;

    for (int i = 0; i < warmup; ++i) {
        auto batch = engine.allocateBatchID(1);
        if (!engine.submitTransfer(batch, {req}).ok()) return false;
        if (!waitCompleted(engine, batch)) return false;
        if (!engine.freeBatchID(batch).ok()) return false;
    }
    for (int i = 0; i < iters; ++i) {
        auto batch = engine.allocateBatchID(1);
        auto t0 = std::chrono::steady_clock::now();
        if (!engine.submitTransfer(batch, {req}).ok()) return false;
        if (!waitCompleted(engine, batch)) return false;
        auto t1 = std::chrono::steady_clock::now();
        if (!engine.freeBatchID(batch).ok()) return false;
        out_us.push_back(
            std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    return true;
}

}  // namespace

class RdmaTwoSidedTest : public ::testing::Test {
   protected:
    void SetUp() override {
        device_ = pickRdmaDevice();
        if (device_.empty()) GTEST_SKIP() << "no RDMA device";
        ASSERT_EQ(setenv("MC_TE_FILTERS", device_.c_str(), 1), 0);
        ASSERT_EQ(setenv("MC_RDMA_NOTIFY_ENABLED", "1", 1), 0);
        ASSERT_EQ(setenv("MC_RDMA_MSG_ENABLED", "1", 1), 0);
        ASSERT_EQ(setenv("MC_RDMA_MSG_DEFAULT", "1", 1), 0);
        ASSERT_EQ(setenv("MC_RDMA_CREDIT_ENABLED", "1", 1), 0);
        loadGlobalConfig(globalConfig());

        std::vector<std::string> filter{device_};
        sender_ = std::make_unique<TransferEngine>(true, filter);
        receiver_ = std::make_unique<TransferEngine>(true, filter);
        ASSERT_EQ(sender_->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
        ASSERT_EQ(receiver_->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
        sender_name_ = sender_->getLocalIpAndPort();
        receiver_name_ = receiver_->getLocalIpAndPort();
    }

    void TearDown() override {
        sender_.reset();
        receiver_.reset();
        unsetenv("MC_TE_FILTERS");
        unsetenv("MC_RDMA_NOTIFY_ENABLED");
        unsetenv("MC_RDMA_MSG_ENABLED");
        unsetenv("MC_RDMA_MSG_DEFAULT");
        unsetenv("MC_RDMA_CREDIT_ENABLED");
        loadGlobalConfig(globalConfig());
    }

    std::string device_;
    std::unique_ptr<TransferEngine> sender_;
    std::unique_ptr<TransferEngine> receiver_;
    std::string sender_name_;
    std::string receiver_name_;
};

TEST_F(RdmaTwoSidedTest, ManagedWriteCorrectness) {
    constexpr size_t kLen = 16 * 1024;
    void *src = sender_->allocateManagedBuffer(kLen);
    void *dst = receiver_->allocateManagedBuffer(kLen);
    ASSERT_NE(src, nullptr);
    ASSERT_NE(dst, nullptr);

    for (size_t i = 0; i < kLen; ++i)
        static_cast<char *>(src)[i] = static_cast<char>('A' + (i % 26));
    std::memset(dst, 0, kLen);

    // Refresh peer segment view so two_sided buffers are visible.
    auto remote = sender_->openSegment(receiver_name_);
    ASSERT_NE(remote, (SegmentID)0);

    auto batch = sender_->allocateBatchID(1);
    TransferRequest req;
    req.opcode = TransferRequest::WRITE;
    req.source = src;
    req.target_id = remote;
    req.target_offset = reinterpret_cast<uint64_t>(dst);
    req.length = kLen;
    ASSERT_TRUE(sender_->submitTransfer(batch, {req}).ok());
    ASSERT_TRUE(waitCompleted(*sender_, batch)) << "WRITE did not complete";
    ASSERT_EQ(std::memcmp(src, dst, kLen), 0);
    ASSERT_TRUE(sender_->freeBatchID(batch).ok());

    ASSERT_EQ(sender_->releaseManagedBuffer(src), 0);
    ASSERT_EQ(receiver_->releaseManagedBuffer(dst), 0);
}

TEST_F(RdmaTwoSidedTest, ManagedReadCorrectness) {
    constexpr size_t kLen = 8 * 1024;
    void *local = sender_->allocateManagedBuffer(kLen);
    void *remote = receiver_->allocateManagedBuffer(kLen);
    ASSERT_NE(local, nullptr);
    ASSERT_NE(remote, nullptr);

    for (size_t i = 0; i < kLen; ++i)
        static_cast<char *>(remote)[i] = static_cast<char>('a' + (i % 26));
    std::memset(local, 0, kLen);

    auto seg = sender_->openSegment(receiver_name_);
    auto batch = sender_->allocateBatchID(1);
    TransferRequest req;
    req.opcode = TransferRequest::READ;
    req.source = local;
    req.target_id = seg;
    req.target_offset = reinterpret_cast<uint64_t>(remote);
    req.length = kLen;
    ASSERT_TRUE(sender_->submitTransfer(batch, {req}).ok());
    ASSERT_TRUE(waitCompleted(*sender_, batch)) << "READ did not complete";
    ASSERT_EQ(std::memcmp(local, remote, kLen), 0);
    ASSERT_TRUE(sender_->freeBatchID(batch).ok());

    ASSERT_EQ(sender_->releaseManagedBuffer(local), 0);
    ASSERT_EQ(receiver_->releaseManagedBuffer(remote), 0);
}

TEST_F(RdmaTwoSidedTest, ManagedWritePerfSmoke) {
    constexpr size_t kLen = 64 * 1024;
    // Keep below default bounce credit (pool_base=64) until ACK returns slots.
    constexpr int kWarmup = 8;
    constexpr int kIters = 40;
    void *src = sender_->allocateManagedBuffer(kLen);
    void *dst = receiver_->allocateManagedBuffer(kLen);
    ASSERT_NE(src, nullptr);
    ASSERT_NE(dst, nullptr);
    auto remote = sender_->openSegment(receiver_name_);

    std::vector<double> samples;
    ASSERT_TRUE(measureWriteLatency(*sender_, remote, src,
                                    reinterpret_cast<uint64_t>(dst), kLen,
                                    kWarmup, kIters, samples));
    auto stats = summarize(samples);
    logStats("two-sided WRITE latency", kLen, stats);
    double gbps = (kLen * 8.0) / (stats.mean_us * 1e-6) / 1e9;
    LOG(INFO) << "two-sided WRITE equiv_gbps_from_mean=" << gbps;
    EXPECT_GT(stats.p50_us, 0.0);
    EXPECT_LT(stats.p50_us, 50000.0);  // sanity: < 50 ms

    ASSERT_EQ(sender_->releaseManagedBuffer(src), 0);
    ASSERT_EQ(receiver_->releaseManagedBuffer(dst), 0);
}

// Busy-wait submit→COMPLETED latency: one-sided WRITE vs two-sided WRITE.
// One-sided completes on local RDMA WRITE CQE; two-sided waits for DATA_ACK.
// Credit admission is disabled for this microbench: BounceSlots are reserved
// but not yet returned on DATA_ACK, so long sync runs would stall in WAITING.
TEST_F(RdmaTwoSidedTest, LatencyCompareOneSidedVsTwoSided) {
    sender_.reset();
    receiver_.reset();
    ASSERT_EQ(setenv("MC_RDMA_CREDIT_ENABLED", "0", 1), 0);
    loadGlobalConfig(globalConfig());
    std::vector<std::string> filter{device_};
    sender_ = std::make_unique<TransferEngine>(true, filter);
    receiver_ = std::make_unique<TransferEngine>(true, filter);
    ASSERT_EQ(sender_->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
    ASSERT_EQ(receiver_->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
    sender_name_ = sender_->getLocalIpAndPort();
    receiver_name_ = receiver_->getLocalIpAndPort();

    constexpr int kWarmup = 32;
    constexpr int kIters = 200;
    const std::vector<size_t> sizes = {64, 1024, 4096, 16 * 1024, 64 * 1024};

    auto remote = sender_->openSegment(receiver_name_);
    ASSERT_NE(remote, (SegmentID)0);

    for (size_t len : sizes) {
        // ---- two-sided (managed) ----
        void *ts_src = sender_->allocateManagedBuffer(len);
        void *ts_dst = receiver_->allocateManagedBuffer(len);
        ASSERT_NE(ts_src, nullptr);
        ASSERT_NE(ts_dst, nullptr);
        std::memset(ts_src, 0xab, len);

        // Refresh remote view after new managed buffers.
        remote = sender_->openSegment(receiver_name_);
        std::vector<double> two_us;
        ASSERT_TRUE(measureWriteLatency(*sender_, remote, ts_src,
                                        reinterpret_cast<uint64_t>(ts_dst),
                                        len, kWarmup, kIters, two_us))
            << "two-sided measure failed len=" << len;
        auto two = summarize(two_us);
        logStats("TWO-SIDED", len, two);

        ASSERT_EQ(sender_->releaseManagedBuffer(ts_src), 0);
        ASSERT_EQ(receiver_->releaseManagedBuffer(ts_dst), 0);

        // ---- one-sided (user register) ----
        void *os_src = nullptr;
        void *os_dst = nullptr;
        ASSERT_EQ(posix_memalign(&os_src, 64, len), 0);
        ASSERT_EQ(posix_memalign(&os_dst, 64, len), 0);
        std::memset(os_src, 0xcd, len);
        std::memset(os_dst, 0, len);
        ASSERT_EQ(sender_->registerLocalMemory(os_src, len, "cpu:0"), 0);
        ASSERT_EQ(receiver_->registerLocalMemory(os_dst, len, "cpu:0"), 0);
        remote = sender_->openSegment(receiver_name_);

        std::vector<double> one_us;
        ASSERT_TRUE(measureWriteLatency(*sender_, remote, os_src,
                                        reinterpret_cast<uint64_t>(os_dst),
                                        len, kWarmup, kIters, one_us))
            << "one-sided measure failed len=" << len;
        auto one = summarize(one_us);
        logStats("ONE-SIDED", len, one);

        ASSERT_EQ(sender_->unregisterLocalMemory(os_src), 0);
        ASSERT_EQ(receiver_->unregisterLocalMemory(os_dst), 0);
        free(os_src);
        free(os_dst);

        const double delta_p50 = two.p50_us - one.p50_us;
        const double ratio =
            one.p50_us > 0.0 ? (two.p50_us / one.p50_us) : 0.0;
        LOG(INFO) << "COMPARE len=" << len << " two_p50_us=" << two.p50_us
                  << " one_p50_us=" << one.p50_us
                  << " delta_p50_us=" << delta_p50 << " ratio_p50=" << ratio
                  << " two_p99_us=" << two.p99_us
                  << " one_p99_us=" << one.p99_us;

        // Both paths must complete; two-sided is expected slower or similar.
        EXPECT_GT(one.p50_us, 0.0);
        EXPECT_GT(two.p50_us, 0.0);
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
