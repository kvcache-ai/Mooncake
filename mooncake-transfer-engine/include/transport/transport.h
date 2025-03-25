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
#include <string>

#include "common/base/status.h"
#include "transfer_metadata.h"

namespace mooncake {
class TransferMetadata;
/// By default, these functions return 0 (or non-null pointer) on success and
/// return -1 (or null pointer) on failure. The errno is set accordingly on
/// failure.
class Transport {
    friend class TransferEngine;
    friend class MultiTransport;

   public:
    using SegmentID = uint64_t;
    using SegmentHandle = SegmentID;

    using BatchID = uint64_t;
    const static BatchID INVALID_BATCH_ID = UINT64_MAX;

    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

    struct TransferRequest {
        enum OpCode { READ, WRITE };

        OpCode opcode;
        void *source;
        SegmentID target_id;
        uint64_t target_offset;
        size_t length;
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

    struct TransferTask;

    struct Slice {
        enum SliceStatus { PENDING, POSTED, SUCCESS, TIMEOUT, FAILED };

        void *source_addr;
        size_t length;
        TransferRequest::OpCode opcode;
        SegmentID target_id;
        std::string peer_nic_path;
        SliceStatus status;
        TransferTask *task;

        union {
            struct {
                uint64_t dest_addr;
                uint32_t source_lkey;
                uint32_t dest_rkey;
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
                const char *file_path;
                uint64_t start;
                uint64_t length;
                uint64_t buffer_id;
            } nvmeof;
            struct {
                void *remote_filename;
                void *remote_addr;
                size_t remote_offset;
            } cxl;
        };

       public:
        void markSuccess() {
            status = Slice::SUCCESS;
            __sync_fetch_and_add(&task->transferred_bytes, length);
            __sync_fetch_and_add(&task->success_slice_count, 1);
        }

        void markFailed() {
            status = Slice::FAILED;
            __sync_fetch_and_add(&task->failed_slice_count, 1);
        }
    };

    struct TransferTask {


        volatile uint64_t slice_count   = 0;
        volatile uint64_t success_slice_count = 0;
        volatile uint64_t failed_slice_count = 0;
        volatile uint64_t transferred_bytes = 0;
        volatile bool is_finished = false;
        uint64_t total_bytes = 0;
    };

    struct BatchDesc {
        BatchID id;
        size_t batch_size;
        std::vector<TransferTask> task_list;
        void *context;  // for transport implementers.
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
    virtual Status submitTransfer(BatchID batch_id,
                               const std::vector<TransferRequest> &entries) = 0;

    virtual Status submitTransferTask(
        const std::vector<TransferRequest *> &request_list,
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

   protected:
    virtual int install(std::string &local_server_name,
                        std::shared_ptr<TransferMetadata> meta,
                        std::shared_ptr<Topology> topo);

    std::string local_server_name_;
    std::shared_ptr<TransferMetadata> metadata_;

    RWSpinlock batch_desc_lock_;
    std::unordered_map<BatchID, std::shared_ptr<BatchDesc>> batch_desc_set_;

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