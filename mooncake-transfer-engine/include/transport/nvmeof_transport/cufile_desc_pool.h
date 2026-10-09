// Copyright 2024 KVCache.AI
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

#ifndef CUFILE_DESC_POOL_H_
#define CUFILE_DESC_POOL_H_

#include <cufile.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include "transfer_engine.h"

namespace mooncake {

class CUFileDescPoolTestPeer;

// Wrapper for reusable CUfileBatchHandle_t
// cuFileBatchIOSetUp is expensive, so we reuse handles (similar to GDS
// transport)
struct BatchHandle {
    CUfileBatchHandle_t handle;
    int max_nr;  // max number of batch entries
};

// Per-batch descriptor with independent io_params and io_events
// Each allocation gets a fresh descriptor to avoid parameter confusion
struct CUFileBatchDesc {
    BatchHandle* batch_handle;  // Pointer to reusable handle from pool
    std::vector<CUfileIOParams_t> io_params;
    // Completion events returned by cuFile are correlated by cookie and cached
    // by submission index. cuFileBatchIOGetStatus only returns completed I/Os,
    // so its output cannot be treated as a positional status snapshot.
    std::vector<CUfileIOEvents_t> io_events;
    std::vector<CUfileIOEvents_t> polled_events;
    bool reusable = true;
};

class CUFileDescPool {
    friend class CUFileDescPoolTestPeer;

   public:
    explicit CUFileDescPool(size_t max_batch_size = 128);
    ~CUFileDescPool();

    CUFileDescPool(const CUFileDescPool&) = delete;
    CUFileDescPool& operator=(const CUFileDescPool&) = delete;
    CUFileDescPool(CUFileDescPool&&) = delete;

    // Allocate a new batch descriptor with independent io_params/io_events
    // Returns descriptor index, or -1 on failure
    int allocCUfileDesc(size_t batch_size);

    // Add params to the descriptor
    int pushParams(int idx, const CUfileIOParams_t& io_params);

    // Submit the batch
    int submitBatch(int idx);

    // Get transfer status for a specific slice
    CUfileIOEvents_t getTransferStatus(int idx, int slice_id);

    // Poll cuFile once for the batch and update cached completion events.
    bool updateBatchStatus(int idx);

    // Get cached transfer status for a specific slice.
    CUfileIOEvents_t getCachedTransferStatus(int idx, int slice_id);

    // Best-effort cancellation for a submitted batch.
    bool cancelBatch(int idx);

    // Prevent an unsafe batch handle from being returned to the reusable pool.
    void markUnreusable(int idx);

    // Get current number of slices in the descriptor
    int getSliceNum(int idx);

    // Free the descriptor and return handle to pool
    int freeCUfileDesc(int idx);

    // Get descriptor by index
    CUFileBatchDesc* getDesc(int idx);

   private:
    static bool cachePolledEvent(std::vector<CUfileIOEvents_t>& io_events,
                                 const CUfileIOEvents_t& event);
    static bool isTerminalStatus(CUfileStatus_t status);
    static CUfileIOEvents_t failedEvent();
    static void destroyDesc(CUFileBatchDesc* desc);
    static bool updateBatchStatus(CUFileBatchDesc* desc, int idx);

    static const size_t MAX_NR_DESC = 256;  // Max number of descriptors
    void cleanupQuarantinedDescs();

    size_t max_batch_size_;

    // Object pool for BatchHandle to avoid frequent cuFileBatchIOSetUp/Destroy
    std::vector<BatchHandle*> handle_pool_;
    std::mutex handle_pool_lock_;

    std::vector<CUFileBatchDesc*> quarantined_descs_;

    // Array of descriptors (nullptr = free slot)
    CUFileBatchDesc* descs_[MAX_NR_DESC];
    RWSpinlock mutex_;
};

}  // namespace mooncake

#endif
