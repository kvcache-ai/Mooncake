// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "transport/flagcx_transport/flagcx_transport.h"

#include <glog/logging.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <thread>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"

namespace mooncake {

namespace {

int getEnvInt(const char *name, int def) {
    const char *v = std::getenv(name);
    return v ? std::atoi(v) : def;
}

const char *getEnvStr(const char *name, const char *def) {
    const char *v = std::getenv(name);
    return v ? v : def;
}

}  // namespace

FlagCxTransport::FlagCxTransport() {}

FlagCxTransport::~FlagCxTransport() {
    // Stop and join worker first; any in-flight slices fail.
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        running_.store(false);
    }
    queue_cv_.notify_all();
    if (io_thread_.joinable()) io_thread_.join();
    for (auto *s : io_queue_) s->markFailed();
    io_queue_.clear();

    if (comm_) {
        flagcxCommDestroy(comm_);
        comm_ = nullptr;
    }
    if (window_) {
        std::free(window_);
        window_ = nullptr;
    }
    if (metadata_) metadata_->removeSegmentDesc(local_server_name_);
}

int FlagCxTransport::install(std::string &local_server_name,
                             std::shared_ptr<TransferMetadata> meta,
                             std::shared_ptr<Topology> topo) {
    (void)topo;
    local_server_name_ = local_server_name;
    metadata_ = meta;

    nranks_ = getEnvInt("MC_FLAGCX_NRANKS", 2);
    rank_ = getEnvInt("MC_FLAGCX_RANK", -1);
    if (rank_ < 0 || rank_ >= nranks_) {
        LOG(ERROR) << "FlagCxTransport: invalid MC_FLAGCX_RANK=" << rank_
                   << " (MC_FLAGCX_NRANKS=" << nranks_ << ")";
        return -1;
    }
    size_t window_mb = static_cast<size_t>(
        getEnvInt("MC_FLAGCX_WINDOW_SIZE_MB", 64));
    window_size_ = window_mb * 1024ULL * 1024ULL;

    LOG(INFO) << "FlagCxTransport: rank=" << rank_ << "/" << nranks_
              << " window=" << window_size_ << "B";

    flagcxUniqueId id_storage;
    flagcxUniqueId_t id = &id_storage;
    if (flagcxGetUniqueId(&id) != flagcxSuccess) {
        LOG(ERROR) << "flagcxGetUniqueId failed";
        return -1;
    }
    if (bootstrapUniqueId(&id) != 0) return -1;

    if (flagcxCommInitRank(&comm_, nranks_, id, rank_) != flagcxSuccess) {
        LOG(ERROR) << "flagcxCommInitRank failed";
        return -1;
    }

    window_ = std::aligned_alloc(4096, window_size_);
    if (!window_) {
        LOG(ERROR) << "FlagCxTransport: window aligned_alloc failed";
        return -1;
    }
    std::memset(window_, 0, window_size_);

    auto reg = flagcxOneSideRegister(comm_, window_, window_size_);
    if (reg != flagcxSuccess) {
        LOG(ERROR) << "flagcxOneSideRegister rc=" << reg
                   << " (net adaptor may not be RDMA-capable; "
                   << "set FLAGCX_NET=IBRC or FLAGCX_NET=UCX)";
        return -1;
    }

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
            if (io_queue_.empty()) return;  // running_ is false and queue drained
            slice = io_queue_.front();
            io_queue_.pop_front();
        }
        if (doSlice(slice) == 0)
            slice->markSuccess();
        else
            slice->markFailed();
    }
}

int FlagCxTransport::bootstrapUniqueId(flagcxUniqueId_t *id) {
    static const char kDefault[] = "/home/zhangzuoyuan/.mc_flagcx_uid";
    const char *path = getEnvStr("MC_FLAGCX_UID_PATH", kDefault);
    if (rank_ == 0) {
        // Stale leftover from a previous run will confuse later ranks.
        std::remove(path);
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out) {
            LOG(ERROR) << "FlagCxTransport: cannot write uid file " << path;
            return -1;
        }
        out.write(reinterpret_cast<const char *>(*id),
                  sizeof(flagcxUniqueId));
        out.close();
        LOG(INFO) << "FlagCxTransport: rank 0 published uid to " << path;
        return 0;
    }
    // Non-zero ranks: poll for the file (~10 min budget).
    for (int i = 0; i < 600; ++i) {
        std::ifstream in(path, std::ios::binary);
        if (in) {
            in.read(reinterpret_cast<char *>(*id), sizeof(flagcxUniqueId));
            if (in.gcount() ==
                static_cast<std::streamsize>(sizeof(flagcxUniqueId))) {
                LOG(INFO) << "FlagCxTransport: rank " << rank_
                          << " loaded uid from " << path;
                return 0;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    LOG(ERROR) << "FlagCxTransport: timed out waiting for uid file " << path;
    return -1;
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
    size_t offset = window_used_.fetch_add(length);
    if (offset + length > window_size_) {
        LOG(ERROR) << "FlagCxTransport: registerLocalMemory exceeds window "
                   << (offset + length) << " > " << window_size_;
        return -1;
    }
    {
        std::lock_guard<std::mutex> lk(reg_mu_);
        regs_.push_back({addr, length, offset});
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
                regs_.erase(it);
                break;
            }
        }
    }
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int FlagCxTransport::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list,
    const std::string &location) {
    for (const auto &b : buffer_list)
        registerLocalMemory(b.addr, b.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int FlagCxTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto a : addr_list) unregisterLocalMemory(a, false);
    return metadata_->updateLocalSegmentDesc();
}

bool FlagCxTransport::resolveOffset(void *addr, size_t length,
                                    size_t &offset_out) {
    std::lock_guard<std::mutex> lk(reg_mu_);
    for (const auto &r : regs_) {
        char *base = reinterpret_cast<char *>(r.addr);
        char *hit = reinterpret_cast<char *>(addr);
        if (hit >= base && hit + length <= base + r.length) {
            offset_out = r.offset + (hit - base);
            return true;
        }
    }
    return false;
}

int FlagCxTransport::peerRankForSegment(SegmentID target_id) {
    (void)target_id;
    return 1 - rank_;
}

int FlagCxTransport::doSlice(Slice *slice) {
    // Called only from ioWorker thread, no locking needed.
    size_t src_offset = 0;
    if (!resolveOffset(slice->source_addr, slice->length, src_offset)) {
        LOG(ERROR) << "FlagCxTransport: source not registered "
                   << slice->source_addr;
        return -1;
    }
    if (slice->opcode == TransferRequest::WRITE) {
        std::memcpy(reinterpret_cast<char *>(window_) + src_offset,
                    slice->source_addr, slice->length);
    }

    // Translate remote absolute address -> window offset.
    // Assumes remote registered its buffers contiguously inside the
    // staging window in segment desc order.
    size_t dst_offset = slice->flagcx.dest_offset;
    auto remote_desc = metadata_->getSegmentDescByID(slice->target_id);
    if (remote_desc) {
        size_t cursor = 0;
        for (const auto &b : remote_desc->buffers) {
            if (slice->flagcx.dest_offset >= b.addr &&
                slice->flagcx.dest_offset + slice->length <=
                    b.addr + b.length) {
                dst_offset = cursor +
                             (slice->flagcx.dest_offset - b.addr);
                break;
            }
            cursor += b.length;
        }
    }

    uint64_t before = 0;
    flagcxReadCounter(comm_, &before);

    int peer = peerRankForSegment(slice->target_id);
    flagcxResult_t rc;
    if (slice->opcode == TransferRequest::WRITE) {
        rc = flagcxPut(comm_, peer, src_offset, dst_offset,
                       slice->length, 0, 0);
    } else {
        rc = flagcxGet(comm_, peer, dst_offset, src_offset,
                       slice->length, 0, 0);
    }
    if (rc != flagcxSuccess) {
        LOG(ERROR) << "FlagCxTransport: Put/Get rc=" << rc;
        return -1;
    }

    rc = flagcxWaitCounter(comm_, before + 1);
    if (rc != flagcxSuccess) {
        LOG(ERROR) << "FlagCxTransport: WaitCounter rc=" << rc;
        return -1;
    }

    if (slice->opcode == TransferRequest::READ) {
        std::memcpy(slice->source_addr,
                    reinterpret_cast<char *>(window_) + src_offset,
                    slice->length);
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
    if (task.success_slice_count + task.failed_slice_count
            == task.slice_count) {
        status.s = task.failed_slice_count
                       ? TransferStatusEnum::FAILED
                       : TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

}  // namespace mooncake
