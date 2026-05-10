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

#ifndef TENT_FAULT_PROXY_TRANSPORT_H
#define TENT_FAULT_PROXY_TRANSPORT_H

#include <atomic>
#include <cassert>
#include <chrono>
#include <memory>
#include <random>
#include <thread>

#include "tent/common/status.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

// Configurable fault injection policy.  All fields default to "no fault".
struct FaultPolicy {
    // Probability [0.0, 1.0] that submitTransferTasks() returns an error.
    double submit_fail_rate = 0.0;

    // Probability [0.0, 1.0] that getTransferStatus() flips COMPLETED→FAILED.
    double status_corrupt_rate = 0.0;

    // Artificial latency (microseconds) added before each submit.
    uint64_t submit_delay_us = 0;

    // Deterministic mode: succeed for the first N submits, then always fail.
    // -1 disables this mode (rate-based mode used instead).
    int fail_after_n_submits = -1;

    // If true, install() returns an error immediately.
    bool fail_install = false;
};

// Decorator that wraps any Transport and injects faults according to a
// FaultPolicy.  The engine sees a normal Transport; failures trigger the
// real failover / retry machinery without requiring hardware.
class FaultProxyTransport : public Transport {
   public:
    FaultProxyTransport(std::shared_ptr<Transport> real, FaultPolicy policy)
        : real_(std::move(real)), policy_(policy), submit_count_(0) {
        assert(real_ && "FaultProxyTransport: real transport must not be null");
    }

    // -- Lifecycle -----------------------------------------------------------

    Status install(std::string& local_segment_name,
                   std::shared_ptr<ControlService> metadata,
                   std::shared_ptr<Topology> local_topology,
                   std::shared_ptr<Config> conf = nullptr) override {
        if (policy_.fail_install) {
            return Status::InternalError(
                "fault injected: install failure" LOC_MARK);
        }
        return real_->install(local_segment_name, metadata, local_topology,
                              conf);
    }

    Status uninstall() override { return real_->uninstall(); }

    // -- Capabilities --------------------------------------------------------

    const Capabilities capabilities() const override {
        return real_->capabilities();
    }

    // -- Batch management ----------------------------------------------------

    Status allocateSubBatch(SubBatchRef& batch, size_t max_size) override {
        return real_->allocateSubBatch(batch, max_size);
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        return real_->freeSubBatch(batch);
    }

    // -- Transfer (primary injection points) ---------------------------------

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override {
        // Artificial delay
        if (policy_.submit_delay_us > 0) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(policy_.submit_delay_us));
        }

        int count = submit_count_.fetch_add(1, std::memory_order_relaxed);

        // Deterministic count-based failure (takes precedence over rate-based)
        if (policy_.fail_after_n_submits >= 0 &&
            count >= policy_.fail_after_n_submits) {
            return Status::InternalError(
                "fault injected: submit failure (count)" LOC_MARK);
        }

        // Rate-based probabilistic failure
        if (policy_.submit_fail_rate > 0.0 &&
            randomDouble() < policy_.submit_fail_rate) {
            return Status::InternalError(
                "fault injected: submit failure (rate)" LOC_MARK);
        }

        return real_->submitTransferTasks(batch, request_list);
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto s = real_->getTransferStatus(batch, task_id, status);
        if (!s.ok()) return s;

        // Status corruption: flip COMPLETED → FAILED
        if (status.s == TransferStatusEnum::COMPLETED &&
            policy_.status_corrupt_rate > 0.0 &&
            randomDouble() < policy_.status_corrupt_rate) {
            status.s = TransferStatusEnum::FAILED;
        }
        return s;
    }

    // -- Memory management (pass-through) ------------------------------------

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override {
        return real_->addMemoryBuffer(desc, options);
    }

    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override {
        return real_->addMemoryBuffer(desc_list, options);
    }

    Status removeMemoryBuffer(BufferDesc& desc) override {
        return real_->removeMemoryBuffer(desc);
    }

    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions& options) override {
        return real_->allocateLocalMemory(addr, size, options);
    }

    Status freeLocalMemory(void* addr, size_t size) override {
        return real_->freeLocalMemory(addr, size);
    }

    bool warmupMemory(void* addr, size_t length) override {
        return real_->warmupMemory(addr, length);
    }

    // -- Notifications (pass-through) ----------------------------------------

    bool supportNotification() const override {
        return real_->supportNotification();
    }

    Status sendNotification(SegmentID target_id,
                            const Notification& notify) override {
        return real_->sendNotification(target_id, notify);
    }

    Status receiveNotification(
        std::vector<Notification>& notify_list) override {
        return real_->receiveNotification(notify_list);
    }

    // -- Identity ------------------------------------------------------------

    const char* getName() const override { return "<fault-proxy>"; }

    // -- Test helpers --------------------------------------------------------
    // Note: resetPolicy() is intended for single-threaded test scenarios.
    // It is NOT safe to call concurrently with submitTransferTasks().

    void resetPolicy(FaultPolicy new_policy) {
        policy_ = new_policy;
        submit_count_.store(0, std::memory_order_relaxed);
    }

    int submitCount() const {
        return submit_count_.load(std::memory_order_relaxed);
    }

   private:
    // Thread-safe random double in [0.0, 1.0).
    static double randomDouble() {
        thread_local std::mt19937 rng(std::random_device{}());
        thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);
        return dist(rng);
    }

    std::shared_ptr<Transport> real_;
    FaultPolicy policy_;
    std::atomic<int> submit_count_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_FAULT_PROXY_TRANSPORT_H
