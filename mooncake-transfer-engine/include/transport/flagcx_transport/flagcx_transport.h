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

#include <flagcx_p2p.h>

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

// FlagCX-backed transport built on the FlagCX P2P engine API
// (flagcx_p2p.h).  Unlike the earlier OneSide/collective proof of
// concept, this version maps one-to-one onto Mooncake's per-SegmentID
// point-to-point model:
//
//   - install() creates a single FlagcxP2pEngine and starts its RPC
//     accept daemon.  The engine's "ip:rpc_port" handshake endpoint is
//     advertised to peers via the segment descriptor (rdma_server_name).
//   - registerLocalMemory() registers the *user* buffer directly with
//     the engine (flagcxP2pEngineReg) -- no staging window, no copy.
//     Incoming RDMA lands straight in the registered buffer.
//   - The data path resolves a cached connection per target segment
//     (flagcxP2pEngineGetConn), looks up the remote rkey by absolute
//     virtual address (flagcxP2pEngineMakeDesc -- the FlagCX equivalent
//     of Mooncake's "rkey by dst_ptr"), then issues a one-sided
//     WriteVectorSync (WRITE) or ReadVector + XferStatus poll (READ).
//
// A single I/O worker thread drains the slice queue so submitTransfer /
// submitTransferTask stay non-blocking; completion is reported through
// the usual TransferTask slice counters.
//
// Required runtime env: the FlagCX engine honours the standard FlagCX
// knobs (FLAGCX_SOCKET_IFNAME selects the NIC the RPC endpoint binds /
// advertises).  No MC_FLAGCX_RANK / NRANKS / UID file / window size are
// needed anymore.
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

    int allocateLocalSegment();
    int doSlice(Slice *slice);

    // Find the engine MR handle whose registered region fully contains
    // [addr, addr+length).  Returns false if the source is unregistered.
    bool resolveLocalMr(void *addr, size_t length, FlagcxP2pMr &mr_out);

    // Resolve (and lazily establish + cache) a connection to the engine
    // serving `target_id`, using the flagcx endpoint published in that
    // segment's descriptor.
    FlagcxP2pConn *connForSegment(SegmentID target_id);

    FlagcxP2pEngine *engine_ = nullptr;
    // "ip:rpc_port" handshake endpoint advertised to peers.
    std::string flagcx_endpoint_;

    struct Reg {
        void *addr;
        size_t length;
        FlagcxP2pMr mr;
    };
    std::mutex reg_mu_;
    std::vector<Reg> regs_;

    // Single-threaded I/O worker: all FlagCX ops happen in io_thread_.
    // submitTransfer/Task pushes Slice* into io_queue_ and returns
    // immediately.
    void ioWorker();
    std::thread io_thread_;
    std::atomic<bool> running_{false};
    std::mutex queue_mu_;
    std::condition_variable queue_cv_;
    std::deque<Slice *> io_queue_;
};

}  // namespace mooncake

#endif  // FLAGCX_TRANSPORT_H_
