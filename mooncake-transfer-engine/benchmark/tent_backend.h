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

#include <string>
#include <vector>
#include <functional>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/time.h>
#include <memory>

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

    void pinThread(int thread_id);

    int runTarget();

    int startInitiator(int num_threads);

    std::string getSegmentName() const { return engine_->getSegmentName(); }

    uint64_t getLocalBufferBase(int thread_id, uint64_t block_size,
                                uint64_t batch_size) const {
        if (seg_type_mix_.empty()) {
            // Single seg_type mode: original behavior.
            const size_t num_buffers = pinned_buffer_list_.size();
            return (uint64_t)pinned_buffer_list_[thread_id % num_buffers] +
                   block_size * batch_size * (thread_id / num_buffers);
        }
        // Mixed mode: pick this thread's seg_type by round-robin, then index
        // into the subset of pinned_buffer_list_ matching that seg_type.
        const std::string& seg_type =
            seg_type_mix_[thread_id % seg_type_mix_.size()];
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
        size_t slot = (thread_id / seg_type_mix_.size()) % count;
        return (uint64_t)pinned_buffer_list_[base + slot] +
               block_size * batch_size *
                   ((thread_id / seg_type_mix_.size()) / count);
    }

    uint64_t getTargetBufferBase(int thread_id, uint64_t block_size,
                                 uint64_t batch_size) const {
        const int local_thread_id = localTargetThreadId(thread_id);
        const auto& info = target_infos_[getTargetIndex(thread_id)];
        const uint64_t bytes =
            checkedMul(block_size, batch_size, "target operation size");
        if (seg_type_mix_.empty()) {
            // Single seg_type mode: original behavior.
            const auto& buffer =
                info.buffers[local_thread_id % info.buffers.size()];
            return targetSlotAddress(thread_id, buffer.base, buffer.length,
                                     bytes,
                                     local_thread_id / info.buffers.size());
        }
        // Mixed mode: pick this thread's seg_type, then index into the
        // subset of target segment buffers matching that seg_type. Use
        // LocationParser to classify each buffer as host (cpu) vs device
        // (cuda/rocm/supa/...) rather than hard-coding a vendor prefix.
        const std::string& seg_type =
            seg_type_mix_[local_thread_id % seg_type_mix_.size()];
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
        const size_t type_slot = local_thread_id / seg_type_mix_.size();
        const auto& buffer = info.buffers[matches[type_slot % matches.size()]];
        return targetSlotAddress(thread_id, buffer.base, buffer.length, bytes,
                                 type_slot / matches.size());
    }

    double runSingleTransfer(uint64_t local_addr, uint64_t target_id,
                             uint64_t target_addr, uint64_t block_size,
                             uint64_t batch_size, OpCode opcode,
                             uint64_t deadline_ns, IntentType intent_type);

   private:
    int allocateBuffers();

    int freeBuffers();

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
};

}  // namespace tent
}  // namespace mooncake

#endif  // TEV1_BACKEND_H
