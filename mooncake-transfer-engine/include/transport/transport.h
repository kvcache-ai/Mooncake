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

#ifndef TRANSPORT_H_
#define TRANSPORT_H_

#include <bits/stdint-uintn.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <atomic>
#include <functional>
#include <mutex>
#include <condition_variable>

#include "common/base/status.h"
#include "transfer_metadata.h"

namespace mooncake {
class TransferMetadata;
/// By default, these functions return 0 (or non-null pointer) on success and
/// return -1 (or null pointer) on failure. The errno is set accordingly on
/// failure.
class Transport {
    friend class TransferEngine;
    friend class TransferEngineImpl;
    friend class MultiTransport;

   public:
    using SegmentID = uint64_t;
    using SegmentHandle = SegmentID;

    using BatchID = uint64_t;

    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;
    using NotifyDesc = TransferMetadata::NotifyDesc;

    struct TransferRequest {
        enum OpCode { READ, WRITE };

        OpCode opcode;
        void *source;
        SegmentID target_id;
        uint64_t target_offset;
        size_t length;
        int advise_retry_cnt = 0;
    };

    enum TransferStatusEnum {
        WAITING,
        PENDING,
        INVALID,
        CANCELED,
        COMPLETED,
        TIMEOUT,
        FAILED
    };

    struct TransferStatus {
        TransferStatusEnum s;
        size_t transferred_bytes;
    };

    struct BatchDesc;
    struct TransferTask;

    // NOTE ABOUT BatchID → BatchDesc conversion:
    //
    // BatchID is an opaque 64‑bit unsigned integer that carries a
    // BatchDesc pointer value. For performance reasons, this helper
    // reinterprets the integral handle directly as a BatchDesc
    // reference.
    //
    // The conversion intentionally bypasses any map or lookup to
    // minimize overhead on hot paths. The caller must ensure that
    // the underlying BatchDesc object remains alive and valid for
    // as long as the handle is in use.
    static inline BatchDesc &toBatchDesc(BatchID id) {
        return *reinterpret_cast<BatchDesc *>(id);
    }

    // Slice must be allocated on heap, as it will delete self on markSuccess
    // or markFailed.
    struct Slice {
        enum SliceStatus { PENDING, POSTED, SUCCESS, TIMEOUT, FAILED };

        void *source_addr;
        size_t length;
        TransferRequest::OpCode opcode;
        SegmentID target_id;
        std::string peer_nic_path;
        SliceStatus status;
        TransferTask *task;
        std::vector<uint32_t> dest_rkeys;
        bool from_cache;

        union {
            struct {
                uint64_t dest_addr;
                uint32_t source_lkey;
                uint32_t dest_rkey;
                int lkey_index;
                int rkey_index;
                volatile int *qp_depth;
                uint32_t retry_cnt;
                uint32_t max_retry_cnt;
            } rdma;
            struct {
                void *dest_addr;
            } local;
            struct {
                uint64_t dest_addr;
            } tcp;
            struct {
                uint64_t offset;
                int cufile_desc;
                uint64_t start;
                const char *file_path;
            } nvmeof;
            struct {
                void *dest_addr;
            } cxl;
            struct {
                uint64_t dest_addr;
            } hccl;
            struct {
                uint64_t dest_addr;
                void *handle;
                int64_t start_time;
            } ascend_direct;
            struct {
                uint64_t dest_addr;
            } ubshmem;
        };

       public:
        void markSuccess() {
            status = Slice::SUCCESS;
            __atomic_fetch_add(&task->transferred_bytes, length,
                               __ATOMIC_RELAXED);
            __atomic_fetch_add(&task->success_slice_count, 1, __ATOMIC_RELAXED);

            check_batch_completion(false);
        }

        void markFailed() {
            status = Slice::FAILED;
            __atomic_fetch_add(&task->failed_slice_count, 1, __ATOMIC_RELAXED);

            check_batch_completion(true);
        }

        volatile int64_t ts;

       private:
        inline void check_batch_completion(bool is_failed) {
#ifdef USE_EVENT_DRIVEN_COMPLETION
            auto &batch_desc = toBatchDesc(task->batch_id);
            if (is_failed) {
                batch_desc.has_failure.store(true, std::memory_order_relaxed);
            }

            // When the last slice of a task completes, check if the entire task
            // is done using a single atomic counter to avoid reading
            // inconsistent results.
            uint64_t prev_completed = __atomic_fetch_add(
                &task->completed_slice_count, 1, __ATOMIC_RELAXED);

            // Only the thread completing the final slice will see prev+1 ==
            // slice_count.
            if (prev_completed + 1 == task->slice_count) {
                __atomic_store_n(&task->is_finished, true, __ATOMIC_RELAXED);

                // Increment the number of finished tasks in the batch
                // (relaxed). This counter does not itself publish data; only
                // the thread that observes the last task completion performs
                // the release-store on batch_desc.is_finished below. The waiter
                // pairs this with an acquire load, which makes all prior writes
                // (including relaxed increments) visible.
                //
                // check if this is the last task in the batch
                auto prev = batch_desc.finished_task_count.fetch_add(
                    1, std::memory_order_relaxed);

                // Last task in the batch: wake up waiting thread directly
                if (prev + 1 == batch_desc.batch_size) {
                    // Publish completion of the entire batch under the same
                    // mutex used by the waiter to avoid lost notifications.
                    //
                    // Keep a release-store because the reader has a fast path
                    // that may observe completion without taking the mutex. The
                    // acquire load in that fast path pairs with this release to
                    // make all prior updates visible. For the predicate checked
                    // under the mutex, relaxed would suffice since the mutex
                    // acquire provides the necessary visibility.
                    {
                        std::lock_guard<std::mutex> lock(
                            batch_desc.completion_mutex);
                        batch_desc.is_finished.store(true,
                                                     std::memory_order_release);
                    }
                    // Notify after releasing the lock to avoid waking threads
                    // only to block again on the mutex.
                    batch_desc.completion_cv.notify_all();
                }
            }
#endif
        }
    };

    struct ThreadLocalSliceCache {
        ThreadLocalSliceCache() : head_(0), tail_(0) {
            lazy_delete_slices_.resize(kLazyDeleteSliceCapacity);
        }

        ~ThreadLocalSliceCache() {
            for (uint64_t i = tail_; i != head_; i++) {
                auto slice = lazy_delete_slices_[i % kLazyDeleteSliceCapacity];
                delete slice;
                freed_++;
            }
            if (allocated_ != freed_) {
                LOG(WARNING) << "detected slice leak: allocated " << allocated_
                             << " freed " << freed_;
            }
        }

        Slice *allocate() {
            Slice *slice;

            if (head_ - tail_ == 0) {
                allocated_++;
                slice = new Slice();
                slice->from_cache = false;
            } else {
                slice = lazy_delete_slices_[tail_ % kLazyDeleteSliceCapacity];
                tail_++;
                slice->from_cache = true;
            }

            return slice;
        }

        void deallocate(Slice *slice) {
            if (head_ - tail_ == kLazyDeleteSliceCapacity) {
                delete slice;
                freed_++;
                return;
            }
            lazy_delete_slices_[head_ % kLazyDeleteSliceCapacity] = slice;
            head_++;
        }

        const static size_t kLazyDeleteSliceCapacity = 4096;
        std::vector<Slice *> lazy_delete_slices_;
        uint64_t head_, tail_;
        uint64_t allocated_ = 0, freed_ = 0;
    };

    struct TransferTask {
        volatile uint64_t slice_count = 0;
        volatile uint64_t success_slice_count = 0;
        volatile uint64_t failed_slice_count = 0;
        volatile uint64_t transferred_bytes = 0;
        volatile bool is_finished = false;
        uint64_t total_bytes = 0;
        BatchID batch_id = 0;

#ifdef WITH_METRICS
        std::chrono::steady_clock::time_point start_time;
#endif

#ifdef USE_EVENT_DRIVEN_COMPLETION
        volatile uint64_t completed_slice_count = 0;
#endif

        // record the origin request
#ifdef USE_ASCEND_HETEROGENEOUS
        // need to modify the request's source address, changing it from an NPU
        // address to a CPU address.
        TransferRequest *request = nullptr;
#else
        const TransferRequest *request = nullptr;
#endif
        // record the slice list for freeing objects
        std::vector<Slice *> slice_list;
        ~TransferTask() {
            for (auto &slice : slice_list)
                Transport::getSliceCache().deallocate(slice);
        }
    };

    struct BatchDesc {
        BatchID id;
        size_t batch_size;
        std::vector<TransferTask> task_list;
        void *context;  // for transport implementers.
        int64_t start_timestamp;

        // Track batch progress and notifies waiters
        std::atomic<bool> has_failure{false};
        std::atomic<bool> is_finished{
            false};  // Completion flag for wait predicate
        std::atomic<uint64_t> finished_transfer_bytes{0};

#ifdef USE_EVENT_DRIVEN_COMPLETION
        // Event-driven completion: tracks batch progress and notifies waiters
        std::atomic<uint64_t> finished_task_count{0};

        // Synchronization primitives for direct notification
        std::mutex completion_mutex;
        std::condition_variable completion_cv;
#endif
    };

   public:
    virtual ~Transport() {}

    /// @brief Create a batch with specified maximum outstanding transfers.
    virtual BatchID allocateBatchID(size_t batch_size);

    /// @brief Free an allocated batch.
    virtual Status freeBatchID(BatchID batch_id);

    /// @brief Submit a batch of transfer requests to the batch.
    /// @return The number of successfully submitted transfers on success. If
    /// that number is less than nr, errno is set.
    virtual Status submitTransfer(
        BatchID batch_id, const std::vector<TransferRequest> &entries) = 0;

    virtual Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) {
        return Status::NotImplemented(
            "Transport::submitTransferTask is not implemented");
    }

    /// @brief Get the status of a submitted transfer. This function shall not
    /// be called again after completion.
    /// @return Return 1 on completed (either success or failure); 0 if still in
    /// progress.
    virtual Status getTransferStatus(BatchID batch_id, size_t task_id,
                                     TransferStatus &status) = 0;

    std::shared_ptr<TransferMetadata> &meta() { return metadata_; }

    struct BufferEntry {
        void *addr;
        size_t length;
    };

    virtual Status OpenChannel(const std::string &segment_name, SegmentID sid) {
        return Status::OK();
    }
    virtual Status CheckStatus(SegmentID sid) { return Status::OK(); }

   protected:
    virtual int install(std::string &local_server_name,
                        std::shared_ptr<TransferMetadata> meta,
                        std::shared_ptr<Topology> topo);

    std::string local_server_name_;
    std::shared_ptr<TransferMetadata> metadata_;

    RWSpinlock batch_desc_lock_;
    std::unordered_map<BatchID, std::shared_ptr<BatchDesc>> batch_desc_set_;

    static ThreadLocalSliceCache &getSliceCache();

   private:
    virtual int registerLocalMemory(void *addr, size_t length,
                                    const std::string &location,
                                    bool remote_accessible,
                                    bool update_metadata = true) = 0;

    virtual int unregisterLocalMemory(void *addr,
                                      bool update_metadata = true) = 0;

    virtual int registerLocalMemoryBatch(
        const std::vector<BufferEntry> &buffer_list,
        const std::string &location) = 0;

    virtual int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) = 0;

    virtual const char *getName() const = 0;
};
}  // namespace mooncake

#endif  // TRANSPORT_H_
