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
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"

namespace mooncake {

FlagCxTransport::FlagCxTransport() {}

FlagCxTransport::~FlagCxTransport() {
    {
        std::lock_guard<std::mutex> lk(pending_mu_);
        for (auto &p : pending_) {
            for (auto *s : p.slices) s->markFailed();
        }
        pending_.clear();
    }

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

    LOG(INFO) << "FlagCxTransport: install OK (direct submit)";
    return 0;
}

// Issue one batch of slices that all target the same segment and share an
// opcode as a SINGLE multi-iov transfer. The engine fans the iovs out across
// the conn's rails (NICs), so a deep batch keeps every NIC busy instead of one
// block-at-a-time. Completion is marked by getTransferStatus().
void FlagCxTransport::runSliceGroup(const std::vector<Slice *> &group) {
    if (group.empty()) return;
    bool ok = true;
    FlagcxP2pConn *conn = nullptr;
    {
        std::lock_guard<std::mutex> lk(flagcx_mu_);
        conn = connForSegment(group.front()->target_id);
    }
    const bool isWrite = group.front()->opcode == TransferRequest::WRITE;

    std::vector<FlagcxP2pMr> mrs;
    std::vector<void *> bufs;
    std::vector<size_t> sizes;
    std::vector<FlagcxP2pRdmaDesc> descs;
    mrs.reserve(group.size());
    bufs.reserve(group.size());
    sizes.reserve(group.size());
    descs.reserve(group.size());

    if (conn == nullptr) ok = false;
    for (Slice *s : group) {
        if (!ok) break;
        FlagcxP2pMr local_mr = 0;
        if (!resolveLocalMr(s->source_addr, s->length, local_mr)) {
            LOG(ERROR) << "FlagCxTransport: source not registered "
                       << s->source_addr;
            ok = false;
            break;
        }
        FlagcxP2pRdmaDesc desc;
        if (flagcxP2pEngineMakeDesc(conn, s->flagcx.dest_offset,
                                    static_cast<uint32_t>(s->length),
                                    &desc) != 0) {
            LOG(ERROR) << "FlagCxTransport: MakeDesc failed for remote VA 0x"
                       << std::hex << s->flagcx.dest_offset << std::dec;
            ok = false;
            break;
        }
        mrs.push_back(local_mr);
        bufs.push_back(s->source_addr);
        sizes.push_back(s->length);
        descs.push_back(desc);
    }

    if (ok) {
        uint64_t transfer_id = 0;
        if (isWrite) {
            std::lock_guard<std::mutex> lk(flagcx_mu_);
            ok = flagcxP2pEngineWriteVector(conn, mrs, bufs, sizes, descs,
                                            static_cast<int>(group.size()),
                                            &transfer_id) == 0;
        } else {
            std::lock_guard<std::mutex> lk(flagcx_mu_);
            ok = flagcxP2pEngineReadVector(conn, mrs, bufs, sizes, descs,
                                           static_cast<int>(group.size()),
                                           &transfer_id) == 0;
        }
        if (ok) {
            const auto now = std::chrono::steady_clock::now();
            for (Slice *s : group) {
                s->status = Slice::POSTED;
                s->ts = getCurrentTimeInNano();
            }
            std::lock_guard<std::mutex> lk(pending_mu_);
            pending_.push_back(
                {conn, transfer_id, group, now + std::chrono::seconds(30)});
        }
    }

    if (!ok) {
        for (Slice *s : group) s->markFailed();
    }
}

void FlagCxTransport::submitSlices(const std::vector<Slice *> &slices) {
    std::lock_guard<std::mutex> lk(submit_mu_);
    std::unordered_map<uint64_t, std::vector<Slice *>> groups;
    for (Slice *s : slices) {
        const uint64_t key =
            (static_cast<uint64_t>(s->target_id) << 1) |
            (s->opcode == TransferRequest::WRITE ? 0u : 1u);
        groups[key].push_back(s);
    }
    for (auto &kv : groups) runSliceGroup(kv.second);
}

void FlagCxTransport::pollPendingTransfers() {
    std::vector<PendingTransfer> local;
    {
        std::lock_guard<std::mutex> lk(pending_mu_);
        local.swap(pending_);
    }

    if (local.empty()) return;

    std::vector<PendingTransfer> still_pending;
    const auto now = std::chrono::steady_clock::now();
    for (auto &p : local) {
        bool done = false;
        {
            std::lock_guard<std::mutex> lk(flagcx_mu_);
            done = flagcxP2pEngineXferStatus(p.conn, p.transfer_id);
        }
        if (done) {
            for (auto *s : p.slices) s->markSuccess();
        } else if (now > p.deadline) {
            LOG(ERROR) << "FlagCxTransport: transfer timeout, tid="
                       << p.transfer_id;
            for (auto *s : p.slices) s->markFailed();
        } else {
            still_pending.push_back(std::move(p));
        }
    }

    if (!still_pending.empty()) {
        std::lock_guard<std::mutex> lk(pending_mu_);
        for (auto &p : still_pending) pending_.push_back(std::move(p));
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
    submitSlices(to_post);
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
    submitSlices(to_post);
    return Status::OK();
}

Status FlagCxTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                          TransferStatus &status) {
    pollPendingTransfers();

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
