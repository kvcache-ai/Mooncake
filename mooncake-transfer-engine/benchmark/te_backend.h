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

#ifndef TEV0_BACKEND_H
#define TEV0_BACKEND_H

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
#include <atomic>

#include "transfer_engine.h"

namespace mooncake {
namespace tent {
class TEBenchRunner : public BenchRunner {
   public:
    TEBenchRunner();
    ~TEBenchRunner();

    TEBenchRunner(const TEBenchRunner&) = delete;
    TEBenchRunner& operator=(const TEBenchRunner&) = delete;

    void pinThread(int thread_id);

    int runTarget();

    int startInitiator(int num_threads);

    std::string getSegmentName() const { return engine_->getLocalIpAndPort(); }

    uint64_t getLocalBufferBase(int thread_id, uint64_t block_size,
                                uint64_t batch_size) const {
        const size_t num_buffers = pinned_buffer_list_.size();
        return (uint64_t)pinned_buffer_list_[thread_id % num_buffers] +
               block_size * batch_size * (thread_id / num_buffers);
    }

    uint64_t getTargetBufferBase(int thread_id, uint64_t block_size,
                                 uint64_t batch_size) const {
        const int local_thread_id = localTargetThreadId(thread_id);
        const auto& info = target_infos_[getTargetIndex(thread_id)];
        const auto& buffer =
            info->buffers[local_thread_id % info->buffers.size()];
        return targetSlotAddress(
            thread_id, buffer.addr, buffer.length,
            checkedMul(block_size, batch_size, "target operation size"),
            local_thread_id / info->buffers.size());
    }

    double runSingleTransfer(uint64_t local_addr, uint64_t target_id,
                             uint64_t target_addr, uint64_t block_size,
                             uint64_t batch_size, OpCode opcode,
                             uint64_t deadline_ns, IntentType intent_type);

   private:
    int allocateBuffers();

    int freeBuffers();

    // Polls batch_id until it is terminal. A stop does not end the wait
    // early: the transports may still be writing into the local buffers.
    // Returns with status PENDING only if the batch did not finish within the
    // drain timeout after the stop; the batch is then counted in
    // abandoned_batches_.
    mooncake::Status waitForBatch(BatchID batch_id,
                                  mooncake::TransferStatus& status,
                                  uint64_t& polls);

   private:
    enum class DramBacking {
        kNone,
        kNuma,
        kSharedMemory,
        kMmap,
        kIsolatedMmap,
    };

    std::unique_ptr<mooncake::TransferEngine> engine_;
    std::vector<void*> pinned_buffer_list_;
    DramBacking dram_backing_ = DramBacking::kNone;
    // Isolated anonymous mmap: leading PROT_NONE size and full mapping length
    // saved at allocation so munmap does not recompute (and overflow).
    size_t mmap_guard_ = 0;
    size_t mmap_span_ = 0;
    std::vector<std::shared_ptr<TransferMetadata::SegmentDesc>> target_infos_;
    bool init_ok_ = true;
    // Batches that may still be in flight (see waitForBatch); while non-zero
    // freeBuffers() leaves the local buffers allocated.
    std::atomic<uint64_t> abandoned_batches_{0};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TEV0_BACKEND_H
