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
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

#include <gtest/gtest.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <unistd.h>

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
    static void install(tent::TransferEngine& engine,
                        std::shared_ptr<tent::Transport> transport) {
        engine.impl_->swapTransportForTest(tent::RDMA, std::move(transport));
    }
    static std::weak_ptr<tent::ControlService> metadata(
        const tent::TransferEngine& engine) {
        return engine.impl_->metadata_;
    }
};

namespace tent {

extern std::atomic<bool> g_tent_running;

class TENTBenchRunnerTestPeer {
   public:
    static std::unique_ptr<TENTBenchRunner> make(
        std::unique_ptr<TransferEngine> engine, void* buffer) {
        auto runner = std::unique_ptr<TENTBenchRunner>(
            new TENTBenchRunner(std::move(engine)));
        runner->pinned_buffer_list_.push_back(buffer);
        runner->transport_hint_ = RDMA;
        return runner;
    }
    static uint64_t abandoned(const TENTBenchRunner& runner) {
        return runner.abandoned_batches_.load();
    }
};

namespace {

// Deliberately inherits Transport::quiesce(), which does not drain anything.
// Pending requests retain both their local-buffer and sub-batch references.
class NoQuiesceTransport : public Transport {
   public:
    enum class Mode { Completed, CompleteAfterStop, Pending, PollError };
    struct Batch : SubBatch {
        std::vector<Request> requests;
        size_t size() const override { return requests.size(); }
    };

    NoQuiesceTransport() { caps.dram_to_dram = true; }
    Mode mode = Mode::Completed;
    int polls = 0, freed_batches = 0, removed_buffers = 0, freed_buffers = 0;
    Batch* pending = nullptr;

    Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new Batch;
        return Status::OK();
    }
    Status freeSubBatch(SubBatchRef& batch) override {
        ++freed_batches;
        if (pending == batch) pending = nullptr;
        delete batch;
        batch = nullptr;
        return Status::OK();
    }
    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        pending = static_cast<Batch*>(batch);
        pending->requests.insert(pending->requests.end(), requests.begin(),
                                 requests.end());
        return Status::OK();
    }
    void complete() {
        if (pending)
            for (const auto& request : pending->requests)
                std::memset(request.source, 0x5a, request.length);
        mode = Mode::Completed;
    }
    Status getTransferStatus(SubBatchRef batch, int task,
                             TransferStatus& status) override {
        ++polls;
        if (mode == Mode::PollError)
            return Status::InternalError("injected poll error");
        if (mode == Mode::CompleteAfterStop && polls > 1) complete();
        if (mode == Mode::Completed) {
            complete();
            status = {TransferStatusEnum::COMPLETED,
                      static_cast<Batch*>(batch)->requests.at(task).length};
        } else {
            g_tent_running.store(false);
            status = {TransferStatusEnum::PENDING, 0};
        }
        return Status::OK();
    }
    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions&) override {
        *addr = std::malloc(size);
        return *addr ? Status::OK() : Status::InternalError("malloc failed");
    }
    Status freeLocalMemory(void* addr, size_t) override {
        ++freed_buffers;
        std::free(addr);
        return Status::OK();
    }
    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(RDMA);
        return Status::OK();
    }
    Status removeMemoryBuffer(BufferDesc&) override {
        ++removed_buffers;
        return Status::OK();
    }
};

class TENTTeardownTest : public testing::TestWithParam<bool> {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv("MC_TENT_CONF")) saved_env_ = value;
        unsetenv("MC_TENT_CONF");
        saved_size_ = XferBenchConfig::total_buffer_size;
        XferBenchConfig::total_buffer_size = kSize;
        auto config = std::make_shared<Config>();
        config->set("metadata_type", "p2p");
        config->set("rpc_server_hostname", "127.0.0.1");
        config->set("rpc_server_port", 0);
        config->set("log_level", "warning");
        for (const auto* name : {"rdma", "tcp", "hp_tcp", "shm", "io_uring"})
            config->set(std::string("transports/") + name + "/enable", false);
        auto engine = std::make_unique<TransferEngine>(config);
        ASSERT_TRUE(engine->available());
        engine_ = engine.get();
        metadata_ = TransferEngineImplTestPeer::metadata(*engine);
        transport_ = std::make_shared<NoQuiesceTransport>();
        TransferEngineImplTestPeer::install(*engine, transport_);
        MemoryOptions options;
        options.type = RDMA;
        ASSERT_TRUE(engine->allocateLocalMemory(&buffer_, kSize, options).ok());
        ASSERT_TRUE(engine->registerLocalMemory(buffer_, kSize, options).ok());
        runner_ = TENTBenchRunnerTestPeer::make(std::move(engine), buffer_);
        if (GetParam()) {
            path_ = testing::TempDir() + "tebench-teardown-XXXXXX";
            const int fd = mkstemp(path_.data());
            ASSERT_GE(fd, 0);
            close(fd);
            std::string error;
            ASSERT_TRUE(openSplitOutput(path_, &error)) << error;
        }
        g_tent_running.store(true);
    }
    void TearDown() override {
        runner_.reset();
        if (!metadata_.expired()) {
            // Production intentionally retains this engine until process exit.
            // Tests complete the simulated I/O before reclaiming it explicitly.
            transport_->complete();
            EXPECT_TRUE(engine_->unregisterLocalMemory(buffer_, kSize).ok());
            EXPECT_TRUE(engine_->freeLocalMemory(buffer_).ok());
            delete engine_;
        } else if (transport_ && transport_->freed_buffers == 0) {
            // Keep the regression test leak-free even against the old code,
            // which destroys the engine but leaves its allocation behind.
            std::free(buffer_);
        }
        closeSplitOutput();
        if (!path_.empty()) std::remove(path_.c_str());
        XferBenchConfig::total_buffer_size = saved_size_;
        if (saved_env_)
            setenv("MC_TENT_CONF", saved_env_->c_str(), 1);
        else
            unsetenv("MC_TENT_CONF");
        g_tent_running.store(true);
    }
    double transfer() {
        return runner_->runSingleTransfer(
            reinterpret_cast<uint64_t>(buffer_), LOCAL_SEGMENT_ID,
            reinterpret_cast<uint64_t>(buffer_), kSize, 1, READ, 0,
            IntentType::INTENT_UNSPEC);
    }
    void expectRetained() {
        EXPECT_EQ(TENTBenchRunnerTestPeer::abandoned(*runner_), 1u);
        runner_.reset();
        EXPECT_FALSE(metadata_.expired());
        EXPECT_EQ(transport_->freed_batches, 0);
        EXPECT_EQ(transport_->removed_buffers, 0);
        EXPECT_EQ(transport_->freed_buffers, 0);
        if (!metadata_.expired()) {
            transport_->complete();
            EXPECT_EQ(static_cast<uint8_t*>(buffer_)[kSize - 1], 0x5a);
        }
    }
    static constexpr size_t kSize = 4096;
    size_t saved_size_ = 0;
    std::optional<std::string> saved_env_;
    std::string path_;
    void* buffer_ = nullptr;
    TransferEngine* engine_ = nullptr;
    std::weak_ptr<ControlService> metadata_;
    std::shared_ptr<NoQuiesceTransport> transport_;
    std::unique_ptr<TENTBenchRunner> runner_;
};

TEST_P(TENTTeardownTest, PollErrorRetainsEngineAndBuffer) {
    transport_->mode = NoQuiesceTransport::Mode::PollError;
    EXPECT_LT(transfer(), 0);
    expectRetained();
}

TEST_P(TENTTeardownTest, DrainTimeoutRetainsEngineAndBuffer) {
    transport_->mode = NoQuiesceTransport::Mode::Pending;
    EXPECT_LT(transfer(), 0);
    expectRetained();
}

TEST_P(TENTTeardownTest, CompletedBatchReleasesEngineAndBuffer) {
    EXPECT_GE(transfer(), 0);
    EXPECT_EQ(TENTBenchRunnerTestPeer::abandoned(*runner_), 0u);
    runner_.reset();
    EXPECT_TRUE(metadata_.expired());
    EXPECT_EQ(transport_->freed_batches, 1);
    EXPECT_EQ(transport_->removed_buffers, 1);
    EXPECT_EQ(transport_->freed_buffers, 1);
}

TEST_P(TENTTeardownTest, BatchDrainedAfterStopReleasesEngineAndBuffer) {
    transport_->mode = NoQuiesceTransport::Mode::CompleteAfterStop;
    EXPECT_GE(transfer(), 0);
    EXPECT_FALSE(g_tent_running.load());
    EXPECT_EQ(TENTBenchRunnerTestPeer::abandoned(*runner_), 0u);
    runner_.reset();
    EXPECT_TRUE(metadata_.expired());
    EXPECT_EQ(transport_->freed_batches, 1);
    EXPECT_EQ(transport_->removed_buffers, 1);
    EXPECT_EQ(transport_->freed_buffers, 1);
}

INSTANTIATE_TEST_SUITE_P(OutputModes, TENTTeardownTest, testing::Bool());

}  // namespace
}  // namespace tent
}  // namespace mooncake
