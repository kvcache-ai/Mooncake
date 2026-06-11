// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "transport/flagcx_transport/flagcx_transport.h"

#include <glog/logging.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"

namespace mooncake {

FlagCxTransport::FlagCxTransport() {}

FlagCxTransport::~FlagCxTransport() {
    // Stop and join the worker first; any in-flight slices fail.
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        running_.store(false);
    }
    queue_cv_.notify_all();
    if (io_thread_.joinable()) io_thread_.join();
    for (auto *s : io_queue_) s->markFailed();
    io_queue_.clear();

    if (engine_) {
        // Deregister everything still registered, then tear the engine down.
        {
            std::lock_guard<std::mutex> lk(reg_mu_);
            for (auto &r : regs_) flagcxP2pEngineMrDestroy(engine_, r.mr);
            regs_.clear();
        }
        flagcxP2pEngineStopAccept(engine_);
        flagcxP2pEngineDestroy(engine_);
        engine_ = nullptr;
    }
    if (metadata_) metadata_->removeSegmentDesc(local_server_name_);
}

int FlagCxTransport::install(std::string &local_server_name,
                             std::shared_ptr<TransferMetadata> meta,
                             std::shared_ptr<Topology> topo) {
    (void)topo;
    local_server_name_ = local_server_name;
    metadata_ = meta;

    engine_ = flagcxP2pEngineCreate();
    if (!engine_) {
        LOG(ERROR) << "FlagCxTransport: flagcxP2pEngineCreate failed";
        return -1;
    }

    // Bring up the accept daemon so peers can RDMA into our registered
    // regions.  Idempotent; safe even though buffers register later.
    if (flagcxP2pEngineStartRpcServer(engine_) != 0) {
        LOG(ERROR) << "FlagCxTransport: flagcxP2pEngineStartRpcServer failed";
        return -1;
    }

    // The engine metadata string is "ip:rdma_port?gpu_index?notif_port";
    // the leading "ip:rdma_port" doubles as the GetConn session id, and
    // rdma_port == the RPC/handshake port (flagcxP2pEngineGetRpcPort).
    char *meta_str = nullptr;
    if (flagcxP2pEngineGetMetadata(engine_, &meta_str) != 0 || !meta_str) {
        LOG(ERROR) << "FlagCxTransport: flagcxP2pEngineGetMetadata failed";
        return -1;
    }
    std::string md(meta_str);
    std::free(meta_str);
    auto q = md.find('?');
    flagcx_endpoint_ = (q == std::string::npos) ? md : md.substr(0, q);
    if (flagcx_endpoint_.empty()) {
        LOG(ERROR) << "FlagCxTransport: empty flagcx endpoint from metadata '"
                   << md << "'";
        return -1;
    }

    LOG(INFO) << "FlagCxTransport: engine up, endpoint=" << flagcx_endpoint_
              << " (rpc_port=" << flagcxP2pEngineGetRpcPort(engine_) << ")";

    if (allocateLocalSegment() != 0) return -1;

    running_.store(true);
    io_thread_ = std::thread(&FlagCxTransport::ioWorker, this);
    LOG(INFO) << "FlagCxTransport: install OK (io_thread spawned)";
    return 0;
}

void FlagCxTransport::ioWorker() {
    while (true) {
        Slice *slice = nullptr;
        {
            std::unique_lock<std::mutex> lk(queue_mu_);
            queue_cv_.wait(lk, [this] {
                return !io_queue_.empty() || !running_.load();
            });
            if (io_queue_.empty()) return;  // running_ false and queue drained
            slice = io_queue_.front();
            io_queue_.pop_front();
        }
        if (doSlice(slice) == 0)
            slice->markSuccess();
        else
            slice->markFailed();
    }
}

int FlagCxTransport::allocateLocalSegment() {
    auto desc = metadata_->getSegmentDesc(local_server_name_);
    if (!desc) desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (!desc->protocol.empty()) desc->protocol += ",";
    desc->protocol += "flagcx";
#else
    desc->protocol = "flagcx";
#endif
    // Advertise the flagcx RPC/handshake endpoint ("ip:rpc_port"); peers
    // read it back from the segment descriptor to open a connection.  The
    // RDMA-reachable endpoint legitimately differs from the TCP-routable
    // segment name, which is exactly what rdma_server_name is meant for.
    desc->rdma_server_name = flagcx_endpoint_;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return metadata_->updateLocalSegmentDesc();
}

int FlagCxTransport::registerLocalMemory(void *addr, size_t length,
                                         const std::string &location,
                                         bool remote_accessible,
                                         bool update_metadata) {
    (void)location;
    (void)remote_accessible;
    FlagcxP2pMr mr = 0;
    if (flagcxP2pEngineReg(engine_, reinterpret_cast<uintptr_t>(addr), length,
                           mr) != 0) {
        LOG(ERROR) << "FlagCxTransport: flagcxP2pEngineReg failed addr=" << addr
                   << " len=" << length;
        return -1;
    }
    {
        std::lock_guard<std::mutex> lk(reg_mu_);
        regs_.push_back({addr, length, mr});
    }
    BufferDesc buf_desc;
    buf_desc.name = local_server_name_;
    buf_desc.addr = reinterpret_cast<uint64_t>(addr);
    buf_desc.length = length;
#ifdef ENABLE_MULTI_PROTOCOL
    buf_desc.protocol = "flagcx";
#endif
    return metadata_->addLocalMemoryBuffer(buf_desc, update_metadata);
}

int FlagCxTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    {
        std::lock_guard<std::mutex> lk(reg_mu_);
        for (auto it = regs_.begin(); it != regs_.end(); ++it) {
            if (it->addr == addr) {
                if (engine_) flagcxP2pEngineMrDestroy(engine_, it->mr);
                regs_.erase(it);
                break;
            }
        }
    }
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int FlagCxTransport::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list, const std::string &location) {
    for (const auto &b : buffer_list)
        registerLocalMemory(b.addr, b.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int FlagCxTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto a : addr_list) unregisterLocalMemory(a, false);
    return metadata_->updateLocalSegmentDesc();
}

bool FlagCxTransport::resolveLocalMr(void *addr, size_t length,
                                     FlagcxP2pMr &mr_out) {
    std::lock_guard<std::mutex> lk(reg_mu_);
    for (const auto &r : regs_) {
        char *base = reinterpret_cast<char *>(r.addr);
        char *hit = reinterpret_cast<char *>(addr);
        if (hit >= base && hit + length <= base + r.length) {
            mr_out = r.mr;
            return true;
        }
    }
    return false;
}

FlagcxP2pConn *FlagCxTransport::connForSegment(SegmentID target_id) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    if (!desc) {
        LOG(ERROR) << "FlagCxTransport: no segment desc for id " << target_id;
        return nullptr;
    }
    if (desc->rdma_server_name.empty()) {
        LOG(ERROR) << "FlagCxTransport: segment " << desc->name
                   << " has no flagcx endpoint (rdma_server_name empty)";
        return nullptr;
    }
    // The engine caches connections by session string, so calling this on
    // every slice is cheap after the first handshake.
    FlagcxP2pConn *conn =
        flagcxP2pEngineGetConn(engine_, desc->rdma_server_name.c_str());
    if (!conn) {
        LOG(ERROR) << "FlagCxTransport: GetConn failed for "
                   << desc->rdma_server_name;
    }
    return conn;
}

int FlagCxTransport::doSlice(Slice *slice) {
    // Runs only on the ioWorker thread; no extra locking required here.
    FlagcxP2pMr local_mr = 0;
    if (!resolveLocalMr(slice->source_addr, slice->length, local_mr)) {
        LOG(ERROR) << "FlagCxTransport: source not registered "
                   << slice->source_addr;
        return -1;
    }

    FlagcxP2pConn *conn = connForSegment(slice->target_id);
    if (!conn) return -1;

    // Resolve the remote rkey by absolute virtual address using the region
    // table exchanged at handshake -- the FlagCX analogue of Mooncake's
    // "look up rkey by dst_ptr".  dest_offset already carries the remote
    // absolute VA (request.target_offset).
    FlagcxP2pRdmaDesc desc;
    if (flagcxP2pEngineMakeDesc(conn, slice->flagcx.dest_offset,
                                static_cast<uint32_t>(slice->length),
                                &desc) != 0) {
        LOG(ERROR) << "FlagCxTransport: MakeDesc failed for remote VA 0x"
                   << std::hex << slice->flagcx.dest_offset << std::dec
                   << " len=" << slice->length;
        return -1;
    }

    std::vector<FlagcxP2pMr> mrs{local_mr};
    std::vector<void *> bufs{slice->source_addr};
    std::vector<size_t> sizes{slice->length};
    std::vector<FlagcxP2pRdmaDesc> descs{desc};

    if (slice->opcode == TransferRequest::WRITE) {
        // Blocking one-sided write; on return the data has landed in the
        // peer's registered buffer (no separate signal/counter needed).
        if (flagcxP2pEngineWriteVectorSync(conn, mrs, bufs, sizes, descs) !=
            0) {
            LOG(ERROR) << "FlagCxTransport: WriteVectorSync failed";
            return -1;
        }
    } else {
        uint64_t transfer_id = 0;
        if (flagcxP2pEngineReadVector(conn, mrs, bufs, sizes, descs,
                                      /*numIovs=*/1, &transfer_id) != 0) {
            LOG(ERROR) << "FlagCxTransport: ReadVector failed";
            return -1;
        }
        // Poll to completion with a generous deadline.
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (!flagcxP2pEngineXferStatus(conn, transfer_id)) {
            if (std::chrono::steady_clock::now() > deadline) {
                LOG(ERROR) << "FlagCxTransport: ReadVector timed out, tid="
                           << transfer_id;
                return -1;
            }
            std::this_thread::yield();
        }
    }
    return 0;
}

Status FlagCxTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *reinterpret_cast<BatchDesc *>(batch_id);
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::InvalidArgument(
            "FlagCxTransport: exceeds batch capacity");
    }
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    std::vector<Slice *> to_post;
    to_post.reserve(entries.size());
    for (const auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id++];
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->target_id = request.target_id;
        slice->flagcx.dest_offset = request.target_offset;
        slice->task = &task;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        to_post.push_back(slice);
    }
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        for (auto *s : to_post) io_queue_.push_back(s);
    }
    queue_cv_.notify_all();
    return Status::OK();
}

Status FlagCxTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::vector<Slice *> to_post;
    to_post.reserve(task_list.size());
    for (auto *tp : task_list) {
        assert(tp && tp->request);
        TransferTask &task = *tp;
        const auto &request = *task.request;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->target_id = request.target_id;
        slice->flagcx.dest_offset = request.target_offset;
        slice->task = &task;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        to_post.push_back(slice);
    }
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        for (auto *s : to_post) io_queue_.push_back(s);
    }
    queue_cv_.notify_all();
    return Status::OK();
}

Status FlagCxTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus &status) {
    auto &batch_desc = *reinterpret_cast<BatchDesc *>(batch_id);
    if (task_id >= batch_desc.task_list.size()) {
        return Status::InvalidArgument(
            "FlagCxTransport::getTransferStatus task_id out of range");
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    if (task.success_slice_count + task.failed_slice_count ==
        task.slice_count) {
        status.s = task.failed_slice_count ? TransferStatusEnum::FAILED
                                           : TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

}  // namespace mooncake
