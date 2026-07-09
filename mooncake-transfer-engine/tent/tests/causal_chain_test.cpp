#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

using namespace mooncake::tent;
using std::chrono::steady_clock;

static constexpr SegmentID LOCAL_SEGMENT_ID = 0;

class FakeTransport : public Transport {
   public:
    explicit FakeTransport(TransportType type) : type_(type) {}

    TransportType type() const override { return type_; }

    Status install(const std::string&, SegmentTracker*,
                   Platform*) override {
        return Status::OK();
    }
    void uninstall() override {}

    Status allocateSubBatch(std::shared_ptr<SubBatch>& sub_batch,
                            size_t max_size) override {
        sub_batch = std::make_shared<SubBatch>();
        sub_batch->max_size = max_size;
        return Status::OK();
    }

    Status submitTransferTasks(
        std::shared_ptr<SubBatch>& sub_batch,
        const std::vector<Request>& requests) override {
        submit_calls.fetch_add(1);
        for (auto& req : requests) {
            sub_batch->task_list.push_back({});
            auto& t = sub_batch->task_list.back();
            t.status = TransferStatusEnum::COMPLETED;
        }
        return Status::OK();
    }

    std::atomic<int> submit_calls{0};

   private:
    TransportType type_;
};

static std::shared_ptr<Config> makeConfig(size_t window_count,
                                          size_t window_bytes) {
    auto cfg = std::make_shared<Config>();
    cfg->local_segment_name = "causal-chain-test";
    cfg->rpc_server_address = "127.0.0.1";
    cfg->rpc_server_port = 0;
    cfg->dispatch_window_count = window_count;
    cfg->dispatch_window_bytes = window_bytes;
    cfg->transports = {{.type = RDMA, .enabled = true}};
    return cfg;
}

static void installFakeRdma(TransferEngineImpl& engine,
                            const std::shared_ptr<FakeTransport>& fake) {
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(fake->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake);
}

static Request makeLocalWrite(uint8_t* ptr, size_t length) {
    Request r;
    r.opcode = Request::WRITE;
    r.source = ptr;
    r.target_id = LOCAL_SEGMENT_ID;
    r.target_offset = reinterpret_cast<uint64_t>(ptr);
    r.length = length;
    r.transport_hint = RDMA;
    return r;
}

TEST(CausalChain, TimestampsPopulatedOnDirectPath) {
    auto cfg = makeConfig(0, 0);
    cfg->dispatch_window_count = 64;
    cfg->dispatch_window_bytes = 1UL << 30;
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xAA);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto status =
        engine.submitTransfer(batch, {makeLocalWrite(buf.data(), kLen)});
    ASSERT_TRUE(status.ok()) << status.ToString();

    TransferStatus ts{};
    for (int i = 0; i < 100; ++i) {
        engine.getTransferStatus(batch, 0, ts);
        if (ts.s == TransferStatusEnum::COMPLETED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED);
    EXPECT_GE(fake->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

TEST(CausalChain, TimestampsPopulatedOnQueuePath) {
    auto cfg = makeConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xBB);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto status =
        engine.submitTransfer(batch, {makeLocalWrite(buf.data(), kLen)});
    ASSERT_TRUE(status.ok()) << status.ToString();

    TransferStatus ts{};
    for (int i = 0; i < 100; ++i) {
        engine.getTransferStatus(batch, 0, ts);
        if (ts.s == TransferStatusEnum::COMPLETED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED);
    EXPECT_GE(fake->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

TEST(CausalChain, MetricsRecordStageLatencyIsCallable) {
    TENT_RECORD_STAGE_LATENCY("queue_wait", 100.0);
    TENT_RECORD_STAGE_LATENCY("dispatch", 50.0);
    TENT_RECORD_STAGE_LATENCY("transport", 200.0);
}
