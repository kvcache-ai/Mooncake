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

#include <gtest/gtest.h>
#include <pybind11/embed.h>
#include <pybind11/stl.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "config.h"
#include "multi_transport.h"
#include "transfer_engine/transfer_engine_py.h"
#include "transfer_engine_impl.h"

extern "C" PyObject* PyInit_engine();

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static void replaceTransports(TransferEngine& engine,
                                  std::shared_ptr<Transport> transport) {
        auto& multi = *engine.impl_->multi_transports_;
        multi.transport_map_.clear();
        multi.transport_map_.emplace("sync-test", std::move(transport));
    }

    static bool isDeferred(TransferEngine& engine, BatchID batch) {
        auto& multi = *engine.impl_->multi_transports_;
        std::lock_guard<std::mutex> lock(multi.deferred_cleanup_mutex_);
        return multi.deferred_cleanup_batches_.count(batch) != 0;
    }
};
}  // namespace mooncake

namespace {
namespace py = pybind11;
using namespace mooncake;
using namespace std::chrono_literals;

class ScopedEnv {
   public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        if (const char* old = std::getenv(name)) old_ = old;
        setenv(name, value, 1);
    }
    ~ScopedEnv() {
        if (old_)
            setenv(name_.c_str(), old_->c_str(), 1);
        else
            unsetenv(name_.c_str());
    }

   private:
    std::string name_;
    std::optional<std::string> old_;
};

struct Probe {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::vector<TransferRequest>> attempts;
    Transport::Slice* pending = nullptr;
    BatchID old_batch = 0;
    size_t issued = 0;
    size_t reclaimed = 0;
    bool aborted = false;
    bool old_completed = false;
    bool reclaimed_before_completion = false;

    void completePending(bool abort = false) {
        Transport::Slice* slice;
        {
            std::lock_guard<std::mutex> lock(mutex);
            aborted = aborted || abort;
            slice = std::exchange(pending, nullptr);
            if (slice) old_completed = true;
        }
        // A terminal publish may let the cleanup worker delete the slice.
        if (slice) slice->markFailed();
    }

    bool waitForReclaim() {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, 10s, [this] { return reclaimed == issued; });
    }
};

class GatedTransport : public Transport {
   public:
    explicit GatedTransport(std::shared_ptr<Probe> probe)
        : probe_(std::move(probe)) {}

    void failFirstAttempt() { fail_first_ = true; }
    void failEveryAttempt() { fail_every_ = true; }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::InvalidArgument("use task submission");
    }

    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        std::vector<Slice*> finish;
        bool fail;
        {
            std::lock_guard<std::mutex> lock(probe_->mutex);
            const bool first = probe_->attempts.empty();
            fail = probe_->aborted || fail_every_ || (first && fail_first_);
            std::vector<TransferRequest> requests;
            for (size_t i = 0; i < tasks.size(); ++i) {
                auto* task = tasks[i];
                requests.push_back(*task->request);
                auto* slice = new Slice{};
                slice->task = task;
                slice->source_addr = probe_.get();
                slice->length = task->request->length;
                slice->status = Slice::PENDING;
                slice->cleanup_callback = [](Slice* released) {
                    auto* probe = static_cast<Probe*>(released->source_addr);
                    std::lock_guard<std::mutex> lock(probe->mutex);
                    ++probe->reclaimed;
                    if (probe->pending == released) {
                        probe->reclaimed_before_completion = true;
                        probe->pending = nullptr;
                    }
                    probe->cv.notify_all();
                };
                task->slice_list.push_back(slice);
                task->slice_count = 1;
                task->total_bytes = slice->length;
                ++probe_->issued;
                if (first && fail_first_ && i == 1 && !probe_->aborted) {
                    probe_->pending = slice;
                    probe_->old_batch = task->batch_id;
                } else {
                    finish.push_back(slice);
                }
            }
            probe_->attempts.push_back(std::move(requests));
        }
        for (auto* slice : finish) {
            if (fail)
                slice->markFailed();
            else
                slice->markSuccess();
        }
        return Status::OK();
    }

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override {
        auto& batch = toBatchDesc(batch_id);
        if (task_id >= batch.task_list.size())
            return Status::InvalidArgument("task index out of range");
        auto& task = batch.task_list[task_id];
        auto success =
            __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
        auto failed =
            __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
        status.transferred_bytes =
            __atomic_load_n(&task.transferred_bytes, __ATOMIC_RELAXED);
        if (success + failed == task.slice_count) {
            status.s = failed ? TransferStatusEnum::FAILED
                              : TransferStatusEnum::COMPLETED;
            __atomic_store_n(&task.is_finished, true, __ATOMIC_RELEASE);
        } else {
            status.s = TransferStatusEnum::WAITING;
        }
        return Status::OK();
    }

   private:
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void*, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }
    const char* getName() const override { return "sync-test"; }

    std::shared_ptr<Probe> probe_;
    bool fail_first_ = false;
    bool fail_every_ = false;
};

// These characterize the current wrapper, not a safe buffer-release contract.
// The fake models completion ownership, not RDMA or actual memory writes.
class ClassicBatchSyncCharacterization : public ::testing::Test {
   protected:
    void SetUp() override {
        old_slice_timeout_ = globalConfig().slice_timeout;
        old_metacache_ = globalConfig().metacache;
        globalConfig().slice_timeout = -1;
        globalConfig().metacache = true;
        object_ = py::module_::import("engine").attr("TransferEngine")();
        wrapper_ = object_.cast<TransferEnginePy*>();
        ASSERT_EQ(
            wrapper_->initialize("127.0.0.1:0", "P2PHANDSHAKE", "tcp", ""), 0);
        engine_ = wrapper_->getEngine();
        ASSERT_NE(engine_, nullptr);
        transport_ = std::make_shared<GatedTransport>(probe_);
        TransferEngineImplTestPeer::replaceTransports(*engine_, transport_);
        ASSERT_EQ(engine_->getLocalTopology()->parse(
                      R"({"cpu:0":[["test-hca-0"],[]]})"),
                  0);
        ASSERT_EQ(engine_->numContexts(), 1);
        auto descriptor = std::make_shared<TransferMetadata::SegmentDesc>();
        descriptor->name = kRemote;
        descriptor->protocol = "sync-test";
        engine_->getMetadata()->addLocalSegment(14, kRemote,
                                                std::move(descriptor));
    }

    void TearDown() override {
        {
            py::gil_scoped_release release;
            // Also terminates tasks submitted after an assertion aborted a
            // test.
            probe_->completePending(true);
            if (worker_.joinable()) worker_.join();
            EXPECT_TRUE(probe_->waitForReclaim());
        }
        object_ = py::none();
        wrapper_ = nullptr;
        engine_.reset();
        transport_.reset();
        globalConfig().slice_timeout = old_slice_timeout_;
        globalConfig().metacache = old_metacache_;
    }

    void start(bool through_python = false) {
        worker_ = std::thread([this, through_python] {
            try {
                py::gil_scoped_acquire acquire;
                if (through_python) {
                    auto result = object_.attr("batch_transfer_sync_write")(
                        kRemote, sources(), destinations(), lengths());
                    if (!py::isinstance<py::int_>(result))
                        throw std::runtime_error("expected Python int result");
                    result_.set_value(result.cast<int>());
                } else {
                    result_.set_value(wrapper_->batchTransferSyncWrite(
                        kRemote, sources(), destinations(), lengths()));
                }
            } catch (...) {
                result_.set_exception(std::current_exception());
            }
        });
    }

    std::optional<int> result() {
        py::gil_scoped_release release;
        if (future_.wait_for(10s) != std::future_status::ready)
            return std::nullopt;
        return future_.get();
    }

    void expectRequests(size_t attempts) {
        std::lock_guard<std::mutex> lock(probe_->mutex);
        ASSERT_EQ(probe_->attempts.size(), attempts);
        for (const auto& batch : probe_->attempts) {
            ASSERT_EQ(batch.size(), 2u);
            for (size_t i = 0; i < batch.size(); ++i) {
                EXPECT_EQ(batch[i].opcode, TransferRequest::WRITE);
                EXPECT_EQ(batch[i].source,
                          reinterpret_cast<void*>(sources()[i]));
                EXPECT_EQ(batch[i].target_offset, destinations()[i]);
                EXPECT_EQ(batch[i].length, lengths()[i]);
            }
        }
    }

    void expectPendingOldBatch() {
        BatchID batch;
        {
            std::lock_guard<std::mutex> lock(probe_->mutex);
            ASSERT_NE(probe_->pending, nullptr);
            EXPECT_FALSE(probe_->old_completed);
            EXPECT_FALSE(probe_->reclaimed_before_completion);
            batch = probe_->old_batch;
        }
        EXPECT_TRUE(TransferEngineImplTestPeer::isDeferred(*engine_, batch));
        probe_->completePending();
        {
            py::gil_scoped_release release;
            ASSERT_TRUE(probe_->waitForReclaim());
        }
        EXPECT_FALSE(TransferEngineImplTestPeer::isDeferred(*engine_, batch));
        std::lock_guard<std::mutex> lock(probe_->mutex);
        EXPECT_TRUE(probe_->old_completed);
        EXPECT_FALSE(probe_->reclaimed_before_completion);
    }

    std::vector<uintptr_t> sources() {
        return {reinterpret_cast<uintptr_t>(buffers_[0].data()),
                reinterpret_cast<uintptr_t>(buffers_[1].data())};
    }
    std::vector<uintptr_t> destinations() { return {0x1000, 0x2000}; }
    std::vector<size_t> lengths() { return {8, 8}; }

    static constexpr const char* kRemote = "sync-test-remote";
    ScopedEnv force_tcp_{"MC_FORCE_TCP", "1"};
    std::shared_ptr<Probe> probe_ = std::make_shared<Probe>();
    std::shared_ptr<GatedTransport> transport_;
    std::shared_ptr<TransferEngine> engine_;
    py::object object_;
    TransferEnginePy* wrapper_ = nullptr;
    std::array<std::array<char, 8>, 2> buffers_{};
    std::promise<int> result_;
    std::future<int> future_ = result_.get_future();
    std::thread worker_;
    int64_t old_slice_timeout_ = -1;
    bool old_metacache_ = true;
};

TEST_F(ClassicBatchSyncCharacterization, AllCompletedReturnsZero) {
    start();
    ASSERT_EQ(result(), 0);
    expectRequests(1);
}

TEST_F(ClassicBatchSyncCharacterization,
       SuccessfulRetryCanReturnBeforeOldAttemptQuiesces) {
    transport_->failFirstAttempt();
    start();
    ASSERT_EQ(result(), 0);
    expectRequests(2);
    expectPendingOldBatch();
}

TEST_F(ClassicBatchSyncCharacterization,
       ExhaustionCanReturnMinusOneWithPendingTask) {
    engine_->getLocalTopology()->clear();
    ASSERT_EQ(engine_->numContexts(), 0);
    transport_->failFirstAttempt();
    start();
    ASSERT_EQ(result(), -1);
    expectRequests(1);
    expectPendingOldBatch();
}

TEST_F(ClassicBatchSyncCharacterization, PythonBoundSuccessReturnsIntegerZero) {
    start(true);
    ASSERT_EQ(result(), 0);
    expectRequests(1);
}

TEST_F(ClassicBatchSyncCharacterization,
       PythonBoundFailureReturnsIntegerMinusOne) {
    engine_->getLocalTopology()->clear();
    transport_->failEveryAttempt();
    start(true);
    ASSERT_EQ(result(), -1);
    expectRequests(1);
}
}  // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    if (PyImport_AppendInittab("engine", &PyInit_engine) != 0) return 1;
    py::scoped_interpreter interpreter;
    return RUN_ALL_TESTS();
}
