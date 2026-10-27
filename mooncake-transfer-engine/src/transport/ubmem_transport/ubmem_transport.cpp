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

#include "transport/ubmem_transport/ubmem_transport.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstring>

#include <glog/logging.h>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"
#include "ubmem_peer_map.h"

namespace mooncake {

UbmemTransport::UbmemTransport() {
    const char* env_path = std::getenv("MC_UBMEM_DEV_PATH");
    if (!env_path || !*env_path) {
        // hard error deferred to install() (cxl constructor semantics)
        return;
    }
    own_path_ = env_path;
}

UbmemTransport::~UbmemTransport() {
    copy_pool_.stop();
    {
        std::lock_guard<std::mutex> lock(mappings_mu_);
        for (auto& kv : mappings_) {
            if (kv.second.base && kv.second.size)
                munmap(kv.second.base, kv.second.size);
        }
        mappings_.clear();
    }
    for (auto& slot : resolve_cache_) {
        Resolved* r = slot.exchange(nullptr, std::memory_order_acq_rel);
        delete r;
    }
    own_base_ = nullptr;
    metadata_->removeSegmentDesc(local_server_name_);
}

UbmemTransport::CopyPool::~CopyPool() { stop(); }

void UbmemTransport::CopyPool::start(int workers) {
    if (workers <= 0 || !threads_.empty()) return;
    workers_ = workers;
    for (int i = 0; i < workers; i++)
        threads_.emplace_back([this, i]() { workerMain(i); });
}

void UbmemTransport::CopyPool::stop() {
    if (threads_.empty()) return;
    {
        std::lock_guard<std::mutex> lock(mu_);
        stop_flag_ = true;
    }
    wake_cv_.notify_all();
    for (auto& t : threads_)
        if (t.joinable()) t.join();
    threads_.clear();
    workers_ = 0;
    stop_flag_ = false;
}

void UbmemTransport::CopyPool::workerMain(int index) {
    uint64_t my_gen = 0;
    for (;;) {
        std::unique_lock<std::mutex> lock(mu_);
        wake_cv_.wait(lock, [&] { return gen_ != my_gen || stop_flag_; });
        if (stop_flag_) return;
        const uint8_t* src = task_src_;
        uint8_t* dst = task_dst_;
        size_t bytes = task_bytes_;
        my_gen = gen_;
        lock.unlock();

        size_t parts = static_cast<size_t>(workers_) + 1;
        size_t chunk = (bytes + parts - 1) / parts;
        chunk = (chunk + 63) & ~static_cast<size_t>(63);
        size_t beg = static_cast<size_t>(index) * chunk;
        size_t end = std::min(beg + chunk, bytes);
        if (beg < end)
            UbmemTransport::ubmemCopy(dst + beg, src + beg, end - beg);

        lock.lock();
        if (++done_count_ == static_cast<size_t>(workers_))
            done_cv_.notify_all();
    }
}

void UbmemTransport::CopyPool::copy(void* dest, const void* src, size_t bytes) {
    std::lock_guard<std::mutex> dispatch_lock(dispatch_mu_);
    {
        std::lock_guard<std::mutex> lock(mu_);
        task_src_ = static_cast<const uint8_t*>(src);
        task_dst_ = static_cast<uint8_t*>(dest);
        task_bytes_ = bytes;
        done_count_ = 0;
        ++gen_;
    }
    wake_cv_.notify_all();

    // The submitting thread takes the last chunk, in parallel with workers
    size_t parts = static_cast<size_t>(workers_) + 1;
    size_t chunk = (bytes + parts - 1) / parts;
    chunk = (chunk + 63) & ~static_cast<size_t>(63);
    size_t beg = static_cast<size_t>(workers_) * chunk;
    if (beg < bytes)
        ubmemCopy(static_cast<uint8_t*>(dest) + beg,
                  static_cast<const uint8_t*>(src) + beg, bytes - beg);

    std::unique_lock<std::mutex> lock(mu_);
    done_cv_.wait(lock,
                  [&] { return done_count_ == static_cast<size_t>(workers_); });
}

uint64_t UbmemTransport::probeDevSize(const char* path) {
    const char* env_size = std::getenv("MC_UBMEM_DEV_SIZE");
    if (env_size && *env_size) {
        char* end = nullptr;
        unsigned long long val = strtoull(env_size, &end, 10);
        if (end != env_size && *end == '\0') return static_cast<uint64_t>(val);
    }
    int fd = open(path, O_RDWR);
    if (fd < 0) return 0;
    off_t end = lseek(fd, 0, SEEK_END);
    close(fd);
    return end < 0 ? 0 : static_cast<uint64_t>(end);
}

void* UbmemTransport::mapDevice(const char* path, uint64_t bytes) {
    if (!path || !*path || bytes == 0) return nullptr;
    int open_flags[2] = {O_RDWR, O_RDWR | O_SYNC};
    int nflags = 2;
    if (std::getenv("MC_UBMEM_FORCE_O_SYNC") != nullptr) {
        open_flags[0] = O_RDWR | O_SYNC;
        open_flags[1] = O_RDWR;
    }
    int fd = -1;
    for (int i = 0; i < nflags; i++) {
        fd = open(path, open_flags[i]);
        if (fd >= 0) break;
    }
    if (fd < 0) {
        LOG(ERROR) << "UbmemTransport: cannot open device " << path << " ("
                   << strerror(errno) << ")";
        return nullptr;
    }
    void* ptr = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "UbmemTransport: mmap failed on " << path
                   << " bytes=" << bytes << " (" << strerror(errno) << ")";
        return nullptr;
    }
    return ptr;
}

void UbmemTransport::ubmemCopy(void* dest, const void* source, size_t size) {
    const uint64_t* s = static_cast<const uint64_t*>(source);
    uint64_t* d = static_cast<uint64_t*>(dest);
    while (size >= 64) {
        // 8 independent loads: pipelines on nc memory where memcpy does not
        uint64_t a0 = s[0], a1 = s[1], a2 = s[2], a3 = s[3];
        uint64_t a4 = s[4], a5 = s[5], a6 = s[6], a7 = s[7];
        d[0] = a0;
        d[1] = a1;
        d[2] = a2;
        d[3] = a3;
        d[4] = a4;
        d[5] = a5;
        d[6] = a6;
        d[7] = a7;
        s += 8;
        d += 8;
        size -= 64;
    }
    while (size >= 8) {
        *d++ = *s++;
        size -= 8;
    }
    if (size > 0) {
        const unsigned char* b = reinterpret_cast<const unsigned char*>(s);
        unsigned char* e = reinterpret_cast<unsigned char*>(d);
        while (size--) *e++ = *b++;
    }
}

int UbmemTransport::ubmemDevInit() {
    if (own_path_.empty()) {
        LOG(ERROR) << "UbmemTransport: MC_UBMEM_DEV_PATH is not set";
        return -1;
    }
    own_size_ = probeDevSize(own_path_.c_str());
    if (own_size_ == 0) {
        LOG(ERROR) << "UbmemTransport: cannot determine device size for "
                   << own_path_ << " (set MC_UBMEM_DEV_SIZE)";
        return -1;
    }
    own_base_ = mapDevice(own_path_.c_str(), own_size_);
    if (!own_base_) return -1;
    {
        std::lock_guard<std::mutex> lock(mappings_mu_);
        Mapping& m = mappings_[own_path_];
        m.base = own_base_;
        m.size = own_size_;
    }
    // memcpy beats the software pipeline at high concurrency;
    // MC_UBMEM_INLINE_MEMCPY=0 restores the nc load pipeline
    {
        const char* env = std::getenv("MC_UBMEM_INLINE_MEMCPY");
        inline_memcpy_ = !(env && *env == '0');
    }
    LOG(INFO) << "UbmemTransport: pool device " << own_path_
              << " mapped base=" << own_base_ << " size=" << own_size_
              << " inline_memcpy=" << inline_memcpy_;
    return 0;
}

void UbmemTransport::loadPeerDevMap() {
    const char* spec = std::getenv("MC_UBMEM_PEER_DEV_MAP");
    if (spec && *spec) {
        peer_dev_map_ = parsePeerDevMap(spec);
    } else {
        const char* file = std::getenv("MC_UBMEM_PEER_DEV_MAP_FILE");
        if (file && *file) {
            // identity: host part of the server name, else system hostname
            char hostname[64] = {};
            std::vector<std::string> self_hosts;
            std::string server_host = hostOf(local_server_name_);
            if (!server_host.empty()) self_hosts.push_back(server_host);
            if (gethostname(hostname, sizeof(hostname)) == 0 && *hostname)
                self_hosts.push_back(hostname);
            peer_dev_map_ = parsePeerDevMapFile(file, self_hosts);
        }
    }
    if (!peer_dev_map_.empty())
        LOG(INFO) << "UbmemTransport: " << peer_dev_map_.size()
                  << " peer mirror mapping(s) configured";
}

int UbmemTransport::install(std::string& local_server_name,
                            std::shared_ptr<TransferMetadata> meta,
                            std::shared_ptr<Topology> topo) {
    metadata_ = meta;
    local_server_name_ = local_server_name;

    int ret = ubmemDevInit();
    if (ret) return ret;

    loadPeerDevMap();

    {
        int copy_threads = 8;
        const char* env_threads = std::getenv("MC_UBMEM_COPY_THREADS");
        if (env_threads && *env_threads) {
            char* end = nullptr;
            long val = strtol(env_threads, &end, 10);
            if (end != env_threads && *end == '\0' && val >= 0 && val <= 64)
                copy_threads = static_cast<int>(val);
        }
        copy_pool_.start(copy_threads);
        if (copy_pool_.ready())
            LOG(INFO) << "UbmemTransport: parallel copy pool started with "
                      << copy_threads << " worker(s)";
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "UbmemTransport: cannot allocate local segment";
        return ret;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "UbmemTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        return ret;
    }

    return 0;
}

int UbmemTransport::allocateLocalSegmentID() {
    auto desc = metadata_->getSegmentDesc(local_server_name_);
    if (!desc) desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name_;
    desc->protocol = "ubmem";
    desc->ubmem_name = own_path_;
    // local mapping address: only for local unregistration matching;
    // peers resolve their own mirror + offset
    desc->ubmem_base_addr = (uint64_t)own_base_;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int UbmemTransport::registerLocalMemory(void* addr, size_t length,
                                        const std::string& location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    (void)remote_accessible;
    BufferDesc ubmem_buffer_desc;
    ubmem_buffer_desc.name = local_server_name_;

    uintptr_t base = reinterpret_cast<uintptr_t>(own_base_);
    uintptr_t end = base + own_size_;
    uintptr_t ptr = reinterpret_cast<uintptr_t>(addr);
    uintptr_t ptr_end = ptr + length;
    // check addr legal
    if (ptr < base || ptr >= end) {
        errno = EFAULT;
        return -1;
    }
    // check overflow
    if (ptr_end > end || ptr_end < ptr) {
        errno = EOVERFLOW;
        return -1;
    }

    ubmem_buffer_desc.offset = (uint64_t)addr - (uint64_t)own_base_;
    ubmem_buffer_desc.length = length;
    return metadata_->addLocalMemoryBuffer(ubmem_buffer_desc, update_metadata);
}

int UbmemTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int UbmemTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    for (auto& buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int UbmemTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    int first_error = 0;
    for (auto& addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

UbmemTransport::Mapping* UbmemTransport::getOrCreateMapping(
    const std::string& path, uint64_t bytes) {
    std::lock_guard<std::mutex> lock(mappings_mu_);
    auto it = mappings_.find(path);
    if (it != mappings_.end()) return &it->second;
    // mirror devices are symmetric with the local pool device
    uint64_t size = bytes ? bytes : own_size_;
    void* base = mapDevice(path.c_str(), size);
    if (!base) return nullptr;
    Mapping& m = mappings_[path];
    m.base = base;
    m.size = size;
    LOG(INFO) << "UbmemTransport: mirror device " << path
              << " mapped base=" << base << " size=" << size;
    return &m;
}

UbmemTransport::Mapping* UbmemTransport::resolveTargetMapping(
    const TransferRequest& request, std::string& error, uint64_t* target_base,
    bool* nc) {
    // fast path: lock-free cache hit
    const size_t sid = static_cast<size_t>(request.target_id);
    if (sid < kResolveCacheSize) {
        Resolved* r = resolve_cache_[sid].load(std::memory_order_acquire);
        if (r) {
            if (target_base) *target_base = r->target_base;
            if (nc) *nc = r->nc;
            return r->mapping;
        }
    }

    auto segment_desc = metadata_->getSegmentDescByID(request.target_id);
    if (!segment_desc) {
        error =
            "invalid target segment id " + std::to_string(request.target_id);
        return nullptr;
    }
    if (segment_desc->protocol != "ubmem") {
        error = "target segment protocol is not ubmem (" +
                segment_desc->protocol + ")";
        return nullptr;
    }
    if (segment_desc->ubmem_name.empty()) {
        error = "target segment has no ubmem_name";
        return nullptr;
    }
    if (target_base) *target_base = segment_desc->ubmem_base_addr;

    Mapping* mapping = nullptr;
    bool is_nc = std::getenv("MC_UBMEM_FORCE_O_SYNC") != nullptr;
    // compare host parts only: names carry their own ports, and a
    // full-string compare misclassifies same-host peers as cross-node
    if (segment_desc->name == local_server_name_ ||
        hostOf(segment_desc->name) == hostOf(local_server_name_)) {
        // same-host: map the target device directly
        mapping = getOrCreateMapping(segment_desc->ubmem_name, 0);
        if (!mapping)
            error = "cannot map local device " + segment_desc->ubmem_name;
    } else {
        // cross-node: access peer memory through the local mirror; the
        // lookup key uses the host part because PEER_DEV_MAP keys carry
        // no port while segment names do
        auto it = peer_dev_map_.find(hostOf(segment_desc->name) + "\x1f" +
                                     segment_desc->ubmem_name);
        if (it == peer_dev_map_.end()) {
            error =
                "no mirror configured for peer " + segment_desc->name +
                " device " + segment_desc->ubmem_name +
                " (set MC_UBMEM_PEER_DEV_MAP or MC_UBMEM_PEER_DEV_MAP_FILE)";
            return nullptr;
        }
        mapping = getOrCreateMapping(it->second, 0);
        if (!mapping)
            error = "cannot map mirror device " + it->second;
        else
            is_nc = true;
    }
    if (mapping) {
        if (nc) *nc = is_nc;
        if (sid < kResolveCacheSize) {
            Resolved* r =
                new Resolved{mapping, segment_desc->ubmem_base_addr, is_nc};
            Resolved* expected = nullptr;
            if (!resolve_cache_[sid].compare_exchange_strong(expected, r))
                delete r;
        }
    }
    return mapping;
}

Status UbmemTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "UbmemTransport::getTransferStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

// Synchronous execution of one request; on failure writes error and the
// slice is marked FAILED (the task is not aborted).
int UbmemTransport::ubmemExecuteOne(Mapping* mapping,
                                    const TransferRequest& request,
                                    std::string& error, uint64_t target_base,
                                    bool nc) {
    if (!mapping) {
        error = "no mapping";
        return -1;
    }
    if (request.target_offset < target_base) {
        error = "target offset below owner device base: offset=" +
                std::to_string(request.target_offset) +
                " base=" + std::to_string(target_base);
        return -1;
    }
    uint64_t offset = request.target_offset - target_base;
    uint64_t length = request.length;
    if (offset + length > mapping->size || offset + length < offset) {
        error = "target offset/length out of device bounds: offset=" +
                std::to_string(offset) + " length=" + std::to_string(length) +
                " device=" + std::to_string(mapping->size);
        return -1;
    }
    char* device_side = static_cast<char*>(mapping->base) + offset;
    // WRITE: local memory -> device; READ: device -> local memory
    const void* copy_src = request.opcode == TransferRequest::WRITE
                               ? static_cast<const void*>(request.source)
                               : static_cast<const void*>(device_side);
    void* copy_dst = request.opcode == TransferRequest::WRITE
                         ? static_cast<void*>(device_side)
                         : static_cast<void*>(request.source);
    if (length >= kParallelThreshold && copy_pool_.ready()) {
        copy_pool_.copy(copy_dst, copy_src, length);
    } else if (!nc || inline_memcpy_) {
        memcpy(copy_dst, copy_src, length);
    } else {
        ubmemCopy(copy_dst, copy_src, length);
    }
    // memory ordering for device accesses (cxl does the same)
    __sync_synchronize();
    return 0;
}

Status UbmemTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "UbmemTransport: Exceed the limitation of current "
                      "batch's capacity";
        return Status::InvalidArgument(
            "UbmemTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto& request : entries) {
        TransferTask& task = batch_desc.task_list[task_id];
        ++task_id;
        task.total_bytes = request.length;
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        __sync_fetch_and_add(&task.slice_count, 1);

        std::string error;
        uint64_t target_base = 0;
        bool nc = false;
        Mapping* mapping =
            resolveTargetMapping(request, error, &target_base, &nc);
        if (mapping && request.target_offset >= target_base)
            slice->ubmem.dest_addr =
                (uint64_t)((char*)mapping->base +
                           (request.target_offset - target_base));
        if (!mapping ||
            ubmemExecuteOne(mapping, request, error, target_base, nc) != 0) {
            LOG(ERROR) << "UbmemTransport: " << error;
            slice->markFailed();
        } else {
            slice->markSuccess();
        }
    }

    return Status::OK();
}

Status UbmemTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        assert(task.request);
        auto& request = *task.request;
        task.total_bytes = request.length;

        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);

        std::string error;
        uint64_t target_base = 0;
        bool nc = false;
        Mapping* mapping =
            resolveTargetMapping(request, error, &target_base, &nc);
        if (mapping && request.target_offset >= target_base)
            slice->ubmem.dest_addr =
                (uint64_t)((char*)mapping->base +
                           (request.target_offset - target_base));
        if (!mapping ||
            ubmemExecuteOne(mapping, request, error, target_base, nc) != 0) {
            LOG(ERROR) << "UbmemTransport: " << error;
            slice->markFailed();
        } else {
            slice->markSuccess();
        }
    }
    return Status::OK();
}

}  // namespace mooncake
