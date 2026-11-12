// Copyright 2025 KVCache.AI
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

#ifndef TEV1_BACKEND_H
#define TEV1_BACKEND_H

#include "bench_runner.h"
#include "utils.h"

#include <optional>
#include <string>
#include <vector>
#include <functional>
#include <utility>
#include <glog/logging.h>
#include <memory>
#include <atomic>

#include "tent/transfer_engine.h"
#include "tent/common/utils/random.h"
#include "tent/common/utils/os.h"
#include "tent/runtime/topology.h"  // LocationParser

namespace mooncake {
namespace tent {
class TENTBenchRunner : public BenchRunner {
   public:
    TENTBenchRunner();
    ~TENTBenchRunner();

    TENTBenchRunner(const TENTBenchRunner&) = delete;
    TENTBenchRunner& operator=(const TENTBenchRunner&) = delete;

    // Mixed mode spreads workers over the targets first and the seg_types
    // second: worker w uses target w % T and seg_type (w / T) % M on both the
    // local and the target side, so the memory types always match and every
    // target sees every type. Public so the rule can be unit-tested.
    static size_t mixTypeIndex(int thread_id, size_t targets, size_t types) {
        return static_cast<size_t>(thread_id) / targets % types;
    }

    // Index of the worker among the local workers with the same seg_type.
    static size_t mixLocalOrdinal(int thread_id, size_t targets, size_t types) {
        const size_t w = static_cast<size_t>(thread_id);
        return w / (targets * types) * targets + w % targets;
    }

    // Index of this worker's target buffer among the target's buffers of its
    // seg_type: the first same-type worker starts at --target_gpu_id, so the
    // flag only shifts which buffer is used, not the type.
    static size_t mixTargetOrdinal(int thread_id, size_t targets,
                                   size_t types) {
        return static_cast<size_t>(
                   std::max(0, XferBenchConfig::target_gpu_id)) +
               static_cast<size_t>(thread_id) / targets / types;
    }

    void pinThread(int thread_id);

    int runTarget();

    int startInitiator(int num_threads);

    std::string getSegmentName() const { return engine_->getSegmentName(); }

    std::optional<uint64_t> getLocalBufferBase(int thread_id,
                                               uint64_t stride) const {
        const uint64_t length = XferBenchConfig::total_buffer_size;
        if (seg_type_mix_.empty()) {
            // Single seg_type mode: original behavior.
            const size_t num_buffers = pinned_buffer_list_.size();
            return slotAddress(
                (uint64_t)pinned_buffer_list_[thread_id % num_buffers], length,
                stride, thread_id / num_buffers);
        }
        // Mixed mode: use this worker's seg_type (the same as its target
        // buffer's, see mixTypeIndex), then index into the subset of
        // pinned_buffer_list_ matching that seg_type.
        const std::string& seg_type = seg_type_mix_[mixTypeIndex(
            thread_id, getTargetCount(), seg_type_mix_.size())];
        size_t base = 0, count = 0;
        for (size_t i = 0; i < pinned_buffer_seg_type_.size(); ++i) {
            if (pinned_buffer_seg_type_[i] == seg_type) {
                if (count == 0) base = i;
                ++count;
            }
        }
        if (count == 0) {
            LOG(FATAL) << "No local buffer of seg_type " << seg_type
                       << " for thread " << thread_id;
        }
        const size_t ordinal =
            mixLocalOrdinal(thread_id, getTargetCount(), seg_type_mix_.size());
        return slotAddress(
            (uint64_t)pinned_buffer_list_[base + ordinal % count], length,
            stride, ordinal / count);
    }

    std::optional<uint64_t> getTargetBufferBase(int thread_id,
                                                uint64_t window) const {
        const auto& info = target_infos_[getTargetIndex(thread_id)];
        if (seg_type_mix_.empty()) {
            // Single seg_type mode: original behavior.
            const int local_thread_id = localTargetThreadId(thread_id);
            const auto& buffer =
                info.buffers[local_thread_id % info.buffers.size()];
            return targetSlotAddress(buffer.base, buffer.length, window,
                                     local_thread_id / info.buffers.size());
        }
        // Mixed mode: pick this worker's seg_type, then index into the
        // subset of target segment buffers matching that seg_type. Use
        // LocationParser to classify each buffer as host (cpu) vs device
        // (cuda/rocm/supa/...) rather than hard-coding a vendor prefix.
        const std::string& seg_type = seg_type_mix_[mixTypeIndex(
            thread_id, getTargetCount(), seg_type_mix_.size())];
        bool want_device = (seg_type == "vram");
        std::vector<size_t> matches;
        for (size_t i = 0; i < info.buffers.size(); ++i) {
            const auto& loc = info.buffers[i].location;
            bool is_device =
                LocationParser(loc).type() != "cpu" && loc != kWildcardLocation;
            if (is_device == want_device) {
                matches.push_back(i);
            }
        }
        if (matches.empty()) {
            LOG(FATAL) << "No target buffer of seg_type " << seg_type
                       << " for thread " << thread_id;
        }
        // Index among this target's workers of this seg_type, starting at
        // --target_gpu_id.
        const size_t ordinal =
            mixTargetOrdinal(thread_id, getTargetCount(), seg_type_mix_.size());
        const auto& buffer = info.buffers[matches[ordinal % matches.size()]];
        return targetSlotAddress(buffer.base, buffer.length, window,
                                 ordinal / matches.size());
    }

    double runSingleTransfer(uint64_t local_addr, uint64_t target_id,
                             uint64_t target_addr, uint64_t block_size,
                             uint64_t batch_size, OpCode opcode,
                             uint64_t deadline_ns, IntentType intent_type);

   private:
    // Tests register their own buffers on an engine with a fake transport.
    friend class TENTBenchRunnerTestPeer;
    explicit TENTBenchRunner(std::unique_ptr<TransferEngine> engine)
        : engine_(std::move(engine)) {}

    int allocateBuffers();

    int freeBuffers();

    // Polls batch_id until it is terminal. A stop (failed worker, SIGINT,
    // SIGTERM) does not end the wait early: the transports may still be
    // writing into the local buffers. Returns with status PENDING only if the
    // batch did not finish within the drain timeout after the stop; the batch
    // is then counted in abandoned_batches_.
    Status waitForBatch(BatchID batch_id, TransferStatus& status,
                        uint64_t& polls);

   private:
    std::unique_ptr<TransferEngine> engine_;
    std::vector<void*> pinned_buffer_list_;
    // Parallel to pinned_buffer_list_: the seg_type each buffer belongs to
    // ("dram" or "vram"). Populated by allocateBuffers; used by
    // getLocalBufferBase to pick the right buffer per thread.
    std::vector<std::string> pinned_buffer_seg_type_;
    // Parsed --seg_type_mix (e.g. ["dram", "vram"]). Empty when --seg_type_mix
    // is not set → single-seg_type mode (existing behavior).
    std::vector<std::string> seg_type_mix_;
    std::vector<SegmentInfo> target_infos_;
    TransportType transport_hint_{UNSPEC};
    IntentType intent_type_{IntentType::INTENT_UNSPEC};
    // Batches that may still be in flight (see waitForBatch); while non-zero
    // teardown retains both the engine and its local buffers until process
    // exit.
    std::atomic<uint64_t> abandoned_batches_{0};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TEV1_BACKEND_H
