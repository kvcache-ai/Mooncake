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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string_view>
#include <vector>

#include "transfer_engine.h"
#include "transfer_engine_impl.h"

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
    static void replaceTransport(TransferEngine& engine,
                                 std::shared_ptr<Transport> transport) {
        auto& impl = *engine.impl_;
        impl.multi_transports_->transport_map_.clear();
        impl.multi_transports_->transport_map_.emplace("scripted",
                                                       std::move(transport));
    }

    static TransferEngineImpl& implementation(TransferEngine& engine) {
        return *engine.impl_;
    }
};

class ScriptedBenchmarkTransport : public Transport {
   public:
    void setStatus(TransferStatusEnum status) { status_.store(status); }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    Status submitTransferTask(const std::vector<TransferTask*>&) override {
        return Status::OK();
    }

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override {
        const auto current = status_.load();
        status.s = current;
        status.transferred_bytes = 0;
        if (current != TransferStatusEnum::WAITING &&
            current != TransferStatusEnum::PENDING) {
            auto* batch = reinterpret_cast<BatchDesc*>(batch_id);
            if (task_id >= batch->task_list.size())
                return Status::InvalidArgument("scripted task out of range");
            batch->task_list[task_id].is_finished = true;
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

    const char* getName() const override { return "scripted-benchmark"; }

    std::atomic<TransferStatusEnum> status_{TransferStatusEnum::COMPLETED};
};

struct Summary {
    int64_t median_ns;
    int64_t p95_ns;
};

Summary summarize(std::vector<int64_t> samples) {
    std::sort(samples.begin(), samples.end());
    const size_t median = samples.size() / 2;
    const size_t p95 = (samples.size() - 1) * 95 / 100;
    return {.median_ns = samples[median], .p95_ns = samples[p95]};
}

size_t parseSize(int argc, char** argv, std::string_view prefix,
                 size_t default_value) {
    for (int i = 1; i < argc; ++i) {
        std::string_view argument(argv[i]);
        if (argument.starts_with(prefix))
            return std::strtoull(argv[i] + prefix.size(), nullptr, 10);
    }
    return default_value;
}

}  // namespace mooncake

int main(int argc, char** argv) {
    using Clock = std::chrono::steady_clock;
    using namespace mooncake;

    const size_t iterations = parseSize(argc, argv, "--iterations=", 1000);
    const size_t fragments = parseSize(argc, argv, "--fragments=", 16);
    if (iterations == 0 || fragments == 0) {
        std::cerr << "iterations and fragments must be non-zero\n";
        return 2;
    }

    TransferEngine engine(false);
    if (engine.init(P2PHANDSHAKE, "127.0.0.1:12345") != 0) return 1;
    auto transport = std::make_shared<ScriptedBenchmarkTransport>();
    TransferEngineImplTestPeer::replaceTransport(engine, transport);
    auto& impl = TransferEngineImplTestPeer::implementation(engine);

    constexpr SegmentID kSegmentId = 91;
    auto descriptor = std::make_shared<TransferMetadata::SegmentDesc>();
    descriptor->name = "benchmark-remote";
    descriptor->protocol = "scripted";
    impl.getMetadata()->addLocalSegment(kSegmentId, "benchmark-remote",
                                        std::move(descriptor));

    auto buffer = std::make_shared<std::vector<char>>(fragments);
    std::vector<size_t> offsets(fragments);
    std::vector<size_t> lengths(fragments, 1);
    for (size_t i = 0; i < fragments; ++i) offsets[i] = i;
    TransferEngine::ScatterTransferRange range{
        .opcode = TransferRequest::READ,
        .remote_segment = "benchmark-remote",
        .remote_base_offset = 0,
        .remote_size = buffer->size(),
        .local_buffer = buffer->data(),
        .local_capacity = buffer->size(),
        .local_offsets = offsets,
        .remote_offsets = offsets,
        .lengths = lengths,
        .on_fragment_complete = {},
    };
    TransferEngine::ScatterTransferOptions options{
        .local_lifetimes = {buffer},
    };

    std::vector<int64_t> baseline_samples;
    std::vector<int64_t> lifecycle_samples;
    baseline_samples.reserve(iterations);
    lifecycle_samples.reserve(iterations);

    auto run_baseline = [&]() {
        const auto started = Clock::now();
        auto operation = engine.submitScatter({range});
        if (!operation.wait().ok()) return int64_t{-1};
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   Clock::now() - started)
            .count();
    };
    auto run_lifecycle = [&]() {
        const auto started = Clock::now();
        auto operation = engine.submitScatter({range}, options);
        if (!operation.wait().ok()) return int64_t{-1};
        if (!operation.snapshot().buffer_reusable) return int64_t{-1};
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   Clock::now() - started)
            .count();
    };

    const size_t warmups = std::min<size_t>(iterations, 100);
    for (size_t i = 0; i < warmups; ++i) {
        if (run_baseline() < 0 || run_lifecycle() < 0) return 1;
    }
    for (size_t i = 0; i < iterations; ++i) {
        int64_t baseline_ns;
        int64_t lifecycle_ns;
        if (i % 2 == 0) {
            baseline_ns = run_baseline();
            lifecycle_ns = run_lifecycle();
        } else {
            lifecycle_ns = run_lifecycle();
            baseline_ns = run_baseline();
        }
        if (baseline_ns < 0 || lifecycle_ns < 0) return 1;
        baseline_samples.push_back(baseline_ns);
        lifecycle_samples.push_back(lifecycle_ns);
    }

    transport->setStatus(TransferStatusEnum::WAITING);
    std::vector<int64_t> quarantine_samples;
    std::vector<int64_t> late_drain_samples;
    quarantine_samples.reserve(iterations);
    late_drain_samples.reserve(iterations);
    for (size_t i = 0; i < iterations; ++i) {
        size_t callback_count = 0;
        range.on_fragment_complete = [&](size_t, const Status&) {
            ++callback_count;
        };
        auto operation = engine.submitScatter({range}, options);
        const auto started = Clock::now();
        if (!operation.waitUntil(started).IsClock()) return 1;
        if (!operation.cancelAndDrainUntil(started).IsClock()) return 1;
        const auto quarantined = Clock::now();
        if (operation.snapshot().state !=
            TransferEngine::ScatterTransferState::QUARANTINED)
            return 1;
        quarantine_samples.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(quarantined -
                                                                 started)
                .count());

        transport->setStatus(TransferStatusEnum::COMPLETED);
        if (!operation.wait().IsClock()) return 1;
        if (callback_count != fragments ||
            operation.snapshot().late_completions != fragments)
            return 1;
        late_drain_samples.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                 quarantined)
                .count());
        transport->setStatus(TransferStatusEnum::WAITING);
    }

    const auto baseline = summarize(std::move(baseline_samples));
    const auto lifecycle = summarize(std::move(lifecycle_samples));
    const auto quarantine = summarize(std::move(quarantine_samples));
    const auto late_drain = summarize(std::move(late_drain_samples));
    std::cout << "{\"iterations\":" << iterations
              << ",\"fragments\":" << fragments
              << ",\"baseline_median_ns\":" << baseline.median_ns
              << ",\"baseline_p95_ns\":" << baseline.p95_ns
              << ",\"lifecycle_median_ns\":" << lifecycle.median_ns
              << ",\"lifecycle_p95_ns\":" << lifecycle.p95_ns
              << ",\"quarantine_median_ns\":" << quarantine.median_ns
              << ",\"quarantine_p95_ns\":" << quarantine.p95_ns
              << ",\"late_drain_median_ns\":" << late_drain.median_ns
              << ",\"late_drain_p95_ns\":" << late_drain.p95_ns << "}\n";
    return 0;
}
