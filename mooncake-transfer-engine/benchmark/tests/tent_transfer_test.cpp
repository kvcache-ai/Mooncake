// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent_backend.h"
#include "split_output.h"
#include "workload_config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/progress_worker.h"
#include "tent/runtime/transport.h"

#include <gtest/gtest.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <unistd.h>

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
    static void install(tent::TransferEngine& engine,
                        std::shared_ptr<tent::Transport> transport) {
        auto& impl = *engine.impl_;
        // Keep the dispatch pass deterministic: polling in the runner drives
        // progress, and every request has exactly one second of queue slack.
        impl.progress_worker_->stop();
        impl.swapTransportForTest(tent::RDMA, std::move(transport));
        impl.runtime_queue_->now_provider_ = [] { return 0; };
    }
};

namespace tent {

class CopyingRdmaTransport : public Transport {
   public:
    CopyingRdmaTransport() { caps.dram_to_dram = true; }

    struct Batch : SubBatch {
        std::vector<Request> requests;
        size_t size() const override { return requests.size(); }
    };

    double bandwidth = 8192;
    TransferStatusEnum terminal_status = TransferStatusEnum::COMPLETED;
    bool poll_error = false;

    double getEstimatedBandwidth() const override { return bandwidth; }

    Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new Batch;
        return Status::OK();
    }
    Status freeSubBatch(SubBatchRef& batch) override {
        delete batch;
        batch = nullptr;
        return Status::OK();
    }
    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        auto& stored = static_cast<Batch*>(batch)->requests;
        for (const auto& request : requests) {
            auto* remote = reinterpret_cast<void*>(request.target_offset);
            if (request.opcode == Request::READ)
                std::memcpy(request.source, remote, request.length);
            else
                std::memcpy(remote, request.source, request.length);
            stored.push_back(request);
        }
        return Status::OK();
    }
    Status getTransferStatus(SubBatchRef batch, int task,
                             TransferStatus& status) override {
        if (poll_error) return Status::InternalError("injected poll failure");
        status = {terminal_status,
                  terminal_status == TransferStatusEnum::COMPLETED
                      ? static_cast<Batch*>(batch)->requests.at(task).length
                      : 0};
        return Status::OK();
    }
    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(RDMA);
        return Status::OK();
    }
    Status removeMemoryBuffer(BufferDesc&) override { return Status::OK(); }
};

// Uses the real TENT submission/polling implementation. Only buffer ownership
// and the number of measured iterations are test-controlled.
class TENTTransferTestRunner : public TENTBenchRunner {
   public:
    static constexpr size_t kBlock = 4096, kBatch = 4;
    std::vector<uint8_t> local = std::vector<uint8_t>(kBlock * kBatch);
    std::vector<uint8_t> remote = std::vector<uint8_t>(kBlock * kBatch);
    std::shared_ptr<CopyingRdmaTransport> transport =
        std::make_shared<CopyingRdmaTransport>();
    bool drop_writes = false;
    bool drop_all = false;
    std::vector<TransferResult> results;

    TENTTransferTestRunner() : TENTBenchRunner(makeEngine(), 1.25) {
        TransferEngineImplTestPeer::install(*engine_, transport);
        transport_hint_ = RDMA;
        EXPECT_TRUE(engine_->available());
        EXPECT_TRUE(
            engine_->registerLocalMemory(local.data(), local.size()).ok());
        EXPECT_TRUE(
            engine_->registerLocalMemory(remote.data(), remote.size()).ok());
        fillData(remote.data(), remote.size(), stableDataSeed(remoteAddress()));
        target_handles_ = {LOCAL_SEGMENT_ID};
        startWorkers(1);
    }
    ~TENTTransferTestRunner() override {
        stopInitiator();
        EXPECT_TRUE(
            engine_->unregisterLocalMemory(local.data(), local.size()).ok());
        EXPECT_TRUE(
            engine_->unregisterLocalMemory(remote.data(), remote.size()).ok());
    }

    void pinThread(int) override {}
    bool isRunning() const { return running(); }
    uint64_t getLocalBufferBase(int, uint64_t, uint64_t) const override {
        return reinterpret_cast<uint64_t>(local.data());
    }
    uint64_t getTargetBufferBase(int, uint64_t, uint64_t) const override {
        return remoteAddress();
    }
    double runSingleTransfer(uint64_t, uint64_t, uint64_t, uint64_t, uint64_t,
                             OpCode, uint64_t, IntentType) override {
        // Skip unmeasured traffic without spinning for the warmup interval.
        std::this_thread::sleep_for(std::chrono::seconds(1));
        return 0;
    }
    TransferResult runTransfer(uint64_t local_addr, uint64_t target_id,
                               uint64_t target_addr, uint64_t block_size,
                               uint64_t batch_size, OpCode opcode, uint64_t,
                               IntentType intent) override {
        transport->bandwidth =
            drop_all ? 1.0 : (opcode == READ || drop_writes ? 8192.0 : 1e12);
        if (opcode == READ) {
            // Canceled reads leave deliberately incorrect bytes, so verifying
            // the whole batch instead of completed_indices fails this test.
            std::memset(local.data(), static_cast<uint8_t>(~remote.front()),
                        local.size());
        }
        auto result = TENTBenchRunner::runTransfer(
            local_addr, target_id, target_addr, block_size, batch_size, opcode,
            1'000'000'000ULL, intent);
        results.push_back(result);
        if (opcode == READ || drop_writes) XferBenchConfig::duration = 0;
        return result;
    }

   private:
    uint64_t remoteAddress() const {
        return reinterpret_cast<uint64_t>(remote.data());
    }
    static std::unique_ptr<TransferEngine> makeEngine() {
        auto config = std::make_shared<Config>();
        config->set("metadata_type", "p2p");
        config->set("rpc_server_hostname", "127.0.0.1");
        config->set("rpc_server_port", 0);
        config->set("log_level", "warning");
        config->set("merge_requests", false);
        config->set("enable_runtime_queue", true);
        config->set("runtime_queue/deadline_aware", true);
        config->set("runtime_queue/mlu_local_threshold", 1.25);
        for (const auto* name : {"rdma", "tcp", "hp_tcp", "shm", "io_uring"})
            config->set(std::string("transports/") + name + "/enable", false);
        return std::make_unique<TransferEngine>(config);
    }
};

}  // namespace tent
}  // namespace mooncake

int processBatchSizes(mooncake::tent::BenchRunner&, size_t, size_t, int,
                      const std::vector<mooncake::tent::QosClassConfig>&,
                      const std::vector<mooncake::tent::WorkloadClassConfig>&);

namespace mooncake::tent {
namespace {

class TENTTransferTest : public testing::TestWithParam<bool> {
   protected:
    void SetUp() override {
        flag_saver_ = std::make_unique<gflags::FlagSaver>();
        if (const char* value = std::getenv("MC_TENT_CONF")) saved_env_ = value;
        unsetenv("MC_TENT_CONF");
        XferBenchConfig::loadFromFlags();
        XferBenchConfig::backend = "tent";
        XferBenchConfig::target_seg_name = "fake-target";
        XferBenchConfig::op_type = "read_verify";
        XferBenchConfig::target_access = "fixed";
        XferBenchConfig::max_block_size = TENTTransferTestRunner::kBlock;
        XferBenchConfig::max_batch_size = TENTTransferTestRunner::kBatch;
        XferBenchConfig::duration = 1;
        XferBenchConfig::deadline_us = 1'000'000;
        XferBenchConfig::deadline_tight_threads = 1;
        dir_ = testing::TempDir() + "tebench-transfer-XXXXXX";
        ASSERT_NE(mkdtemp(dir_.data()), nullptr);
        XferBenchConfig::result_output_jsonl = dir_ + "/target.jsonl";
        XferBenchConfig::qos_output_jsonl = dir_ + "/qos.jsonl";
        if (GetParam()) {
            std::string error;
            ASSERT_TRUE(openSplitOutput(dir_ + "/split.jsonl", &error))
                << error;
        }
    }
    void TearDown() override {
        closeSplitOutput();
        for (const auto* name : {"target.jsonl", "qos.jsonl", "split.jsonl"})
            std::remove((dir_ + "/" + name).c_str());
        rmdir(dir_.c_str());
        if (saved_env_)
            setenv("MC_TENT_CONF", saved_env_->c_str(), 1);
        else
            unsetenv("MC_TENT_CONF");
        flag_saver_.reset();
        XferBenchConfig::loadFromFlags();
    }
    void checkReports(TENTTransferTestRunner& runner, uint64_t bytes,
                      size_t samples, uint64_t completed, uint64_t dropped) {
        testing::internal::CaptureStdout();
        const int rc =
            processBatchSizes(runner, runner.kBlock, runner.kBatch, 1,
                              {{"foreground", 1, 100, 1.0, std::nullopt}}, {});
        const auto output = testing::internal::GetCapturedStdout();
        ASSERT_EQ(rc, 0) << output;
        EXPECT_NE(output.find("completed=" + std::to_string(completed) +
                              " dropped=" + std::to_string(dropped)),
                  std::string::npos);
        json target, qos;
        std::ifstream(dir_ + "/target.jsonl") >> target;
        std::ifstream(dir_ + "/qos.jsonl") >> qos;
        std::istringstream row(output);
        size_t block, batch;
        double bandwidth, instant, avg_latency, avg_tx, p99, p999;
        ASSERT_TRUE(static_cast<bool>(row >> block >> batch >> bandwidth >>
                                      instant >> avg_latency >> avg_tx >> p99 >>
                                      p999));
        EXPECT_EQ(block, runner.kBlock);
        EXPECT_EQ(batch, runner.kBatch);
        EXPECT_NEAR(bandwidth,
                    target["aggregate_throughput_gbps"].get<double>(),
                    0.00000051);
        double duration_sum = 0;
        for (const auto& result : runner.results)
            if (result.completed != 0) duration_sum += result.duration_us;
        EXPECT_NEAR(avg_tx, samples == 0 ? 0 : duration_sum / samples, 0.051);
        if (samples == 0) {
            EXPECT_DOUBLE_EQ(avg_latency, 0);
            EXPECT_DOUBLE_EQ(p99, 0);
            EXPECT_DOUBLE_EQ(p999, 0);
        }
        EXPECT_EQ(target["aggregate_operations"], samples);
        EXPECT_EQ(target["aggregate_transferred_bytes"], bytes);
        EXPECT_EQ(target["targets"][0]["transferred_bytes"], bytes);
        EXPECT_EQ(qos["classes"][0]["operations"], samples);
        EXPECT_EQ(qos["classes"][0]["transferred_bytes"], bytes);
        EXPECT_NEAR(target["aggregate_throughput_gbps"].get<double>(),
                    qos["aggregate_throughput_gbps"].get<double>(), 1e-12);
        if (GetParam()) {
            closeSplitOutput();
            std::ifstream split(dir_ + "/split.jsonl");
            std::string line;
            size_t count = 0;
            while (std::getline(split, line)) {
                const auto record = json::parse(line);
                ASSERT_LT(count, runner.results.size());
                const auto& result = runner.results[count++];
                if (result.dropped != 0) {
                    EXPECT_EQ(record["completed"], result.completed);
                    EXPECT_EQ(record["dropped"], result.dropped);
                }
            }
            EXPECT_EQ(count, runner.results.size());
        }
    }
    std::unique_ptr<gflags::FlagSaver> flag_saver_;
    std::optional<std::string> saved_env_;
    std::string dir_;
};

TEST_P(TENTTransferTest, PartialReadRetainsBytesAndVerifiesOnlyCompletedTasks) {
    TENTTransferTestRunner runner;
    checkReports(runner, 8192, 1, 2, 2);
    ASSERT_EQ(runner.results.size(), 1u);
    EXPECT_EQ(runner.results[0].completed_indices, (std::vector<size_t>{0, 1}));
}

TEST_P(TENTTransferTest, FullyCanceledBatchContributesNoBytesOrLatency) {
    TENTTransferTestRunner runner;
    runner.drop_all = true;
    checkReports(runner, 0, 0, 0, 4);
    ASSERT_EQ(runner.results.size(), 1u);
    EXPECT_DOUBLE_EQ(runner.results[0].duration_us, kTransferDropped);
}

TEST_P(TENTTransferTest, ConsistencyReadbackSkipsCanceledBuffers) {
    XferBenchConfig::op_type = "mix";
    XferBenchConfig::check_consistency = true;
    TENTTransferTestRunner runner;
    checkReports(runner, 24576, 2, 6, 2);
}

TEST_P(TENTTransferTest, PartialWriteIsCountedWithoutFullReadback) {
    XferBenchConfig::op_type = "mix";
    XferBenchConfig::check_consistency = true;
    TENTTransferTestRunner runner;
    runner.drop_writes = true;
    checkReports(runner, 8192, 1, 2, 2);
    EXPECT_EQ(runner.results.size(), 1u);
}

TEST_P(TENTTransferTest, TaskAndPollFailuresStillAbort) {
    for (auto status :
         {TransferStatusEnum::FAILED, TransferStatusEnum::TIMEOUT,
          TransferStatusEnum::INVALID, TransferStatusEnum::COMPLETED}) {
        SCOPED_TRACE(static_cast<int>(status));
        XferBenchConfig::duration = 1;
        TENTTransferTestRunner runner;
        runner.transport->terminal_status = status;
        runner.transport->poll_error = status == TransferStatusEnum::COMPLETED;
        EXPECT_EQ(
            processBatchSizes(runner, runner.kBlock, runner.kBatch, 1, {}, {}),
            -1);
        EXPECT_FALSE(runner.isRunning());
    }
}

INSTANTIATE_TEST_SUITE_P(OutputModes, TENTTransferTest, testing::Bool());

}  // namespace
}  // namespace mooncake::tent
