// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef FLAGCX_TRANSPORT_H_
#define FLAGCX_TRANSPORT_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <flagcx.h>

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

// Minimal FlagCX-backed transport (proof of concept).
//
// Model:
//   - One static global FlagCX comm built at install() time.
//     Bootstrap is via a shared filesystem file (MC_FLAGCX_UID_PATH).
//   - One pre-registered staging window per node (MC_FLAGCX_WINDOW_SIZE_MB,
//     default 64). registerLocalMemory() bump-allocates an offset inside
//     the window for each user buffer.
//   - Data path: WRITE -> flagcxPut, READ -> flagcxGet, then
//     flagcxWaitCounter for completion. submitTransfer blocks per slice.
//
// Required runtime env vars:
//   MC_FLAGCX_NRANKS   (default 2)
//   MC_FLAGCX_RANK     (no default; install() fails if unset/out-of-range)
//   MC_FLAGCX_UID_PATH (default /home/$USER/.mc_flagcx_uid; must be on a
//                       filesystem shared by all ranks)
//   MC_FLAGCX_WINDOW_SIZE_MB (default 64)
class FlagCxTransport : public Transport {
   public:
    FlagCxTransport();
    ~FlagCxTransport() override;

    Status submitTransfer(
        BatchID batch_id,
        const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    // PoC helper: expose staging window so demos can verify incoming data.
    void *getWindow() const { return window_; }
    size_t getWindowSize() const { return window_size_; }

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location,
                            bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr,
                              bool update_metadata = false) override;

    int registerLocalMemoryBatch(
        const std::vector<BufferEntry> &buffer_list,
        const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    const char *getName() const override { return "flagcx"; }

    int bootstrapUniqueId(flagcxUniqueId_t *id);
    int allocateLocalSegment();
    int doSlice(Slice *slice);

    // Translate a (user_addr, length) into a window offset.
    bool resolveOffset(void *addr, size_t length, size_t &offset_out);

    // PoC: SegmentID -> peer rank. With nranks=2, peer = 1 - rank.
    int peerRankForSegment(SegmentID target_id);

    flagcxComm_t comm_ = nullptr;
    int rank_ = -1;
    int nranks_ = 0;

    void *window_ = nullptr;
    size_t window_size_ = 0;
    std::atomic<size_t> window_used_{0};

    struct Reg {
        void *addr;
        size_t length;
        size_t offset;
    };
    std::mutex reg_mu_;
    // Single-threaded I/O worker: all FlagCX ops happen in io_thread_.
    // submitTransfer/Task pushes Slice* into io_queue_ and returns immediately.
    void ioWorker();
    std::thread io_thread_;
    std::atomic<bool> running_{false};
    std::mutex queue_mu_;
    std::condition_variable queue_cv_;
    std::deque<Slice *> io_queue_;
    std::vector<Reg> regs_;
};

}  // namespace mooncake

#endif  // FLAGCX_TRANSPORT_H_
