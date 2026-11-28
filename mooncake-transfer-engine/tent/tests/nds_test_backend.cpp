// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
// Test-only UBSIO facade implementation, using its public header and contracts.
#include "nds_test_backend.h"
#include <ascend_nds.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <deque>
#include <map>
#include <set>
#include <vector>

namespace nds_test {
State state;
}
namespace {
using nds_test::state;
struct FileHandle {
    int fd;
};
struct PendingIo {
    NDSfileHandle_t file;
    NDSfileOpcode_t opcode;
    off_t offset;
    void* cookie;
    std::vector<NDSfileIOVec_t> iovs;
};
struct Batch {
    std::deque<PendingIo> pending;
};
std::map<uint64_t, size_t> registrations;
std::set<int> topology_fds;

int perform(const PendingIo& io) {
    int fd = static_cast<FileHandle*>(io.file)->fd;
    off_t offset = io.offset;
    for (const auto& iov : io.iovs) {
        ssize_t rc = io.opcode == NDS_FILE_READ
                         ? pread(fd, iov.buf, iov.length, offset)
                         : pwrite(fd, iov.buf, iov.length, offset);
        if (rc != static_cast<ssize_t>(iov.length)) return -EIO;
        offset += iov.length;
    }
    return 0;
}
}  // namespace
extern "C" {
int ndsFileDriverOpen() {
    if (state.open_error) return -ENODEV;
    ++state.opens;
    return 0;
}
int ndsFileDriverClose() {
    for (int fd : topology_fds) {
        if (fcntl(fd, F_GETFD) < 0) return -EBADF;
    }
    topology_fds.clear();
    state.live_topologies = 0;
    ++state.closes;
    return 0;
}
int ndsFileBufRegister(void* addr, size_t length, int) {
    auto base = reinterpret_cast<uint64_t>(addr);
    if (registrations.count(base)) return -EBUSY;
    registrations.emplace(base, length);
    ++state.pins;
    return 0;
}
int ndsFileBufDeregister(void* addr) {
    if (state.unpin_error) return -EIO;
    auto it = registrations.find(reinterpret_cast<uint64_t>(addr));
    if (it == registrations.end()) return -EINVAL;
    registrations.erase(it);
    ++state.unpins;
    return 0;
}
int ndsFileRegisterFs(const NDSfileFsDesc_t* desc) {
    if (!desc || !desc->fs_fd || !desc->fs_fd_cnt || desc->reserved)
        return -EINVAL;
    for (unsigned i = 0; i < desc->fs_fd_cnt; ++i) {
        int fd = desc->fs_fd[i];
        if (fcntl(fd, F_GETFD) < 0) return -EBADF;
        topology_fds.insert(fd);
        state.topology_fd = fd;
    }
    state.live_topologies = topology_fds.size();
    return 0;
}
int ndsFileHandleRegister(NDSfileHandle_t* handle, const NDSfileDescr_t* desc) {
    if (state.handle_error) return -EIO;
    if (desc->type != NDS_FILE_HANDLE_TYPE_OPAQUE_FD) return -EINVAL;
    *handle = new FileHandle{desc->handle.fd};
    ++state.live_files;
    return 0;
}
void ndsFileHandleDeregister(NDSfileHandle_t handle) {
    delete static_cast<FileHandle*>(handle);
    --state.live_files;
}
int ndsFileBatchIOSetUp(NDSfileBatchHandle_t* context, unsigned depth) {
    if (!depth || depth > NDS_FILE_MAX_BATCH_IO) return -EINVAL;
    *context = new Batch;
    ++state.live_contexts;
    ++state.context_setups;
    return 0;
}
int ndsFileBatchIODestroy(NDSfileBatchHandle_t context) {
    auto* batch = static_cast<Batch*>(context);
    for (const auto& io : batch->pending) {
        perform(io);
        ++state.drains;
    }
    delete batch;
    --state.live_contexts;
    return state.destroy_error ? -EIO : 0;
}
int ndsFileBatchIOSubmit(NDSfileBatchHandle_t context, unsigned nr,
                         const NDSfileIOParams_t* params, unsigned flags) {
    if (!context || !nr || nr > NDS_FILE_MAX_BATCH_IO || !params ||
        (flags & ~NDS_FILE_IO_REGISTERED_MEM))
        return -EINVAL;
    auto* batch = static_cast<Batch*>(context);
    ++state.submits;
    if (state.submit_error) return state.submit_error;
    unsigned accepted = state.partial_submit ? std::min(1u, nr) : nr;
    std::vector<PendingIo> prepared;
    prepared.reserve(accepted);
    for (unsigned i = 0; i < accepted; ++i) {
        const auto& param = params[i];
        if (!param.fh || param.file_offset < 0 || param.bufPtr_offset < 0 ||
            !param.bufPtr_bases || !param.iov_count ||
            param.iov_count > NDS_FILE_MAX_IOV)
            return -EINVAL;
        const auto* iovs =
            static_cast<const NDSfileIOVec_t*>(param.bufPtr_bases);
        PendingIo io{
            param.fh, param.opcode, param.file_offset, param.cookie, {}};
        for (size_t j = 0; j < param.iov_count; ++j) {
            const auto& iov = iovs[j];
            if (!iov.buf || !iov.length || iov.length > UINT32_MAX)
                return -EINVAL;
            auto* addr = static_cast<char*>(iov.buf) + param.bufPtr_offset;
            if (flags & NDS_FILE_IO_REGISTERED_MEM) {
                auto address = reinterpret_cast<uint64_t>(addr);
                auto it = registrations.upper_bound(address);
                if (it == registrations.begin()) return -ERANGE;
                --it;
                size_t offset = address - it->first;
                if (offset > it->second || iov.length > it->second - offset)
                    return -ERANGE;
            }
            io.iovs.push_back({addr, iov.length});
        }
        prepared.push_back(std::move(io));
    }
    for (auto& io : prepared) {
        auto& lengths = state.submitted_iov_lengths.emplace_back();
        lengths.reserve(io.iovs.size());
        for (const auto& iov : io.iovs) lengths.push_back(iov.length);
        batch->pending.push_back(std::move(io));
    }
    state.submitted_ios += accepted;
    if (flags & NDS_FILE_IO_REGISTERED_MEM) state.registered_ios += accepted;
    state.max_inflight =
        std::max(state.max_inflight, int(batch->pending.size()));
    return accepted;
}
int ndsFileBatchIOGetStatus(NDSfileBatchHandle_t context, unsigned min_nr,
                            unsigned* nr, NDSfileIOEvents_t* events,
                            timespec* timeout) {
    if (min_nr || !nr || !*nr || !timeout || timeout->tv_sec ||
        timeout->tv_nsec)
        return -EINVAL;
    if (state.poll_error) return -EIO;
    auto* batch = static_cast<Batch*>(context);
    *nr = 0;
    if (state.pause || batch->pending.empty()) return 0;
    // Reverse completions and one event per poll exercise cookie/refill logic.
    auto io = std::move(batch->pending.back());
    batch->pending.pop_back();
    events[0].cookie =
        state.bad_cookie ? reinterpret_cast<void*>(UINTPTR_MAX) : io.cookie;
    events[0].ret = perform(io);
    events[0].status = events[0].ret ? NDS_FILE_FAILED : NDS_FILE_COMPLETE;
    *nr = 1;
    return 0;
}
}
