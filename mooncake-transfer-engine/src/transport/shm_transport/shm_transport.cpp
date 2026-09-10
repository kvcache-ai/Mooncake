// Copyright 2026 KVCache.AI
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

#include "transport/shm_transport/shm_transport.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <glog/logging.h>
#include <limits>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "error.h"

namespace mooncake {
namespace {

std::string randomShmName() {
    std::string result = std::string(kPosixShmNamePrefix) +
                         std::to_string(static_cast<long>(getpid())) + "_";
    for (int i = 0; i < 8; ++i) result += 'a' + SimpleRandom::Get().next(26);
    return result;
}

bool rangeContains(uint64_t start, uint64_t span, uint64_t addr,
                   uint64_t length) {
    if (addr < start) return false;
    const uint64_t offset = addr - start;
    return offset <= span && span - offset >= length;
}

bool posixShmObjectExists(const std::string& shm_name) {
    int fd = shm_open(shm_name.c_str(), O_RDWR, 0);
    if (fd < 0) return false;
    close(fd);
    return true;
}

uint64_t rangeEnd(uint64_t start, uint64_t span) {
    if (span == 0) return start;
    if (start > std::numeric_limits<uint64_t>::max() - span) {
        return std::numeric_limits<uint64_t>::max();
    }
    return start + span;
}

bool rangesOverlap(uint64_t a, uint64_t a_len, uint64_t b, uint64_t b_len) {
    return a < rangeEnd(b, b_len) && b < rangeEnd(a, a_len);
}

bool protocolListContains(const std::string& protocol,
                          const std::string& token) {
    std::stringstream ss(protocol);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (item == token) return true;
    }
    return false;
}

Status mapPosixShm(const std::string& shm_name, uint64_t length, void** out) {
    int shm_fd = shm_open(shm_name.c_str(), O_RDWR, 0600);
    if (shm_fd < 0) {
        return Status::Memory(std::string("Failed to open shared memory ") +
                              shm_name);
    }
    struct stat st;
    if (fstat(shm_fd, &st) != 0) {
        close(shm_fd);
        return Status::Memory(std::string("Failed to fstat shared memory ") +
                              shm_name);
    }
    if (st.st_size < 0 || static_cast<uint64_t>(st.st_size) < length) {
        close(shm_fd);
        return Status::Memory(
            std::string("Shared memory file shorter than registered "
                        "buffer length: ") +
            shm_name);
    }
    void* mapped =
        mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    if (mapped == MAP_FAILED) {
        return Status::Memory("Failed to map shared memory");
    }
    *out = mapped;
    return Status::OK();
}

size_t posixBufferCount(const TransferMetadata::SegmentDesc& desc) {
    size_t count = 0;
    for (const auto& entry : desc.buffers) {
        if (isPosixShmName(entry.shm_name)) ++count;
    }
    return count;
}

bool posixBufferAddrListed(const TransferMetadata::SegmentDesc& desc,
                           uint64_t addr) {
    for (const auto& entry : desc.buffers) {
        if (isPosixShmName(entry.shm_name) && entry.addr == addr) return true;
    }
    return false;
}

}  // namespace

ShmTransport::ShmTransport() = default;

ShmTransport::~ShmTransport() {
    PendingUnmap pending;
    {
        RWSpinlock::WriteGuard guard(relocate_lock_);
        for (auto& relocate_map : relocate_map_) {
            for (auto& entry : relocate_map.second) {
                if (!entry.second || !entry.second->shm_addr ||
                    !entry.second->length) {
                    continue;
                }
                pending.emplace_back(entry.second->shm_addr,
                                     entry.second->length);
                entry.second->shm_addr = nullptr;
                entry.second->length = 0;
            }
        }
        relocate_map_.clear();
    }
    flushUnmaps(pending);
    {
        std::lock_guard<std::mutex> lock(shm_path_mutex_);
        for (auto& entry : shm_path_map_) {
            if (entry.first && entry.second.length) {
                munmap(entry.first, entry.second.length);
            }
            if (!entry.second.name.empty()) {
                shm_unlink(entry.second.name.c_str());
            }
        }
        shm_path_map_.clear();
    }
    if (metadata_ && !local_server_name_.empty()) {
        metadata_->removeSegmentDesc(local_server_name_);
    }
}

int ShmTransport::install(std::string& local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    (void)topo;
    metadata_ = meta;
    local_server_name_ = local_server_name;

    auto old_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    if (old_desc) *desc = *old_desc;

    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (desc->protocol.empty()) {
        desc->protocol = "shm";
    } else if (!protocolListContains(desc->protocol, "shm")) {
        desc->protocol += ",shm";
    }
#else
    if (!desc->protocol.empty() && desc->protocol != "shm") {
        LOG(WARNING) << "ShmTransport::install replaces segment protocol '"
                     << desc->protocol
                     << "' with 'shm'. Rebuild with "
                        "-DENABLE_MULTI_PROTOCOL=ON to keep RDMA/TCP "
                        "alongside SHM";
    }
    desc->protocol = "shm";
#endif

    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

void* ShmTransport::createSharedMemory(const std::string& path, size_t size,
                                       int* error) {
    auto fail = [&](int err) -> void* {
        if (error) *error = err;
        return nullptr;
    };

    // O_EXCL prevents silently opening and truncating an existing object.
    int shm_fd = shm_open(path.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (shm_fd == -1) {
        const int err = errno;
        if (err != EEXIST) {
            PLOG(ERROR) << "Failed to open shared memory file " << path;
        }
        return fail(err);
    }

    if (ftruncate(shm_fd, static_cast<off_t>(size)) == -1) {
        const int err = errno;
        PLOG(ERROR) << "Failed to truncate shared memory file " << path;
        close(shm_fd);
        shm_unlink(path.c_str());
        return fail(err);
    }

    void* mapped_addr =
        mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (mapped_addr == MAP_FAILED) {
        const int err = errno;
        PLOG(ERROR) << "Failed to map shared memory file " << path;
        close(shm_fd);
        shm_unlink(path.c_str());
        return fail(err);
    }

    close(shm_fd);
    std::lock_guard<std::mutex> lock(shm_path_mutex_);
    shm_path_map_[mapped_addr] = AllocatedShmEntry{path, size};
    if (error) *error = 0;
    return mapped_addr;
}

void* ShmTransport::allocateSharedMemory(size_t length) {
    if (length == 0) {
        LOG(ERROR) << "ShmTransport does not support zero-length allocation";
        return nullptr;
    }
    for (int attempt = 0; attempt < kShmCreateMaxRetries; ++attempt) {
        const std::string name = randomShmName();
        int error = 0;
        void* addr = createSharedMemory(name, length, &error);
        if (addr) return addr;
        if (error != EEXIST) break;
    }
    LOG(ERROR) << "Failed to allocate shared memory of size " << length;
    return nullptr;
}

int ShmTransport::freeSharedMemory(void* addr) {
    std::lock_guard<std::mutex> lock(shm_path_mutex_);
    auto it = shm_path_map_.find(addr);
    if (it == shm_path_map_.end()) {
        return ERR_INVALID_ARGUMENT;
    }
    munmap(addr, it->second.length);
    shm_unlink(it->second.name.c_str());
    shm_path_map_.erase(it);
    return 0;
}

bool ShmTransport::getShmName(void* addr, std::string* name) const {
    if (!name) return false;
    std::lock_guard<std::mutex> lock(shm_path_mutex_);
    auto it = shm_path_map_.find(addr);
    if (it == shm_path_map_.end()) return false;
    *name = it->second.name;
    return true;
}

int ShmTransport::registerLocalMemory(void* addr, size_t length,
                                      const std::string& location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    (void)remote_accessible;
    std::string shm_name;
    size_t alloc_length = 0;
    bool exact_base = false;
    bool inside_or_overlapping = false;
    {
        std::lock_guard<std::mutex> lock(shm_path_mutex_);
        auto it = shm_path_map_.find(addr);
        if (it != shm_path_map_.end()) {
            exact_base = true;
            shm_name = it->second.name;
            alloc_length = it->second.length;
        } else {
            const uint64_t range_addr = reinterpret_cast<uint64_t>(addr);
            for (const auto& entry : shm_path_map_) {
                const uint64_t base = reinterpret_cast<uint64_t>(entry.first);
                if (rangeContains(base, entry.second.length, range_addr,
                                  length) ||
                    rangesOverlap(range_addr, length, base,
                                  entry.second.length)) {
                    inside_or_overlapping = true;
                    break;
                }
            }
        }
    }
    if (inside_or_overlapping) {
        LOG(ERROR) << "registerLocalMemory must use the pointer returned by "
                      "allocateSharedMemory, not a sub-range inside it: addr="
                   << addr << " length=" << length;
        return ERR_INVALID_ARGUMENT;
    }
    if (!exact_base) return 0;
    if (length > alloc_length) {
        LOG(ERROR) << "registerLocalMemory length " << length
                   << " exceeds POSIX shm allocation " << alloc_length << " at "
                   << addr;
        return ERR_INVALID_ARGUMENT;
    }

    TransferMetadata::BufferDesc desc;
    desc.addr = reinterpret_cast<uint64_t>(addr);
    desc.length = length;
    desc.name = location.empty() ? local_server_name_ : location;
    desc.shm_name = shm_name;
#ifdef ENABLE_MULTI_PROTOCOL
    desc.protocol = "shm";
#endif
    return metadata_->addLocalMemoryBuffer(desc, update_metadata);
}

int ShmTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    std::string shm_name;
    if (!getShmName(addr, &shm_name)) return 0;
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int ShmTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    bool exported = false;
    for (const auto& buffer : buffer_list) {
        std::string name;
        const bool owned = getShmName(buffer.addr, &name);
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
        if (owned) exported = true;
    }
    if (!exported) return 0;
    return metadata_->updateLocalSegmentDesc();
}

int ShmTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    int first_error = 0;
    bool owned_any = false;
    for (auto* addr : addr_list) {
        std::string name;
        if (getShmName(addr, &name)) owned_any = true;
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    if (!owned_any) return first_error;
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

bool ShmTransport::tryResolve(const RelocateMap& relocate_map,
                              uint64_t& dest_addr, uint64_t length,
                              const std::string& shm_name,
                              std::shared_ptr<OpenedShmEntry>* out_entry) {
    for (const auto& entry : relocate_map) {
        if (!entry.second ||
            entry.second->stale.load(std::memory_order_acquire))
            continue;
        if (entry.second->shm_name != shm_name) continue;
        if (!entry.second->shm_addr) continue;
        if (rangeContains(entry.first, entry.second->length, dest_addr,
                          length)) {
            dest_addr = dest_addr - entry.first +
                        reinterpret_cast<uint64_t>(entry.second->shm_addr);
            if (out_entry) *out_entry = entry.second;
            return true;
        }
    }
    return false;
}

void ShmTransport::queueUnmap(OpenedShmEntry& entry, PendingUnmap* pending) {
    if (entry.pin_count.load(std::memory_order_acquire) > 0) {
        entry.stale.store(true, std::memory_order_release);
        return;
    }
    if (!pending) return;
    if (entry.shm_addr && entry.length) {
        pending->emplace_back(entry.shm_addr, entry.length);
    }
    entry.shm_addr = nullptr;
    entry.length = 0;
    entry.stale.store(true, std::memory_order_release);
}

void ShmTransport::unpinLocked(OpenedShmEntry& entry, PendingUnmap* pending) {
    const uint32_t prev =
        entry.pin_count.fetch_sub(1, std::memory_order_acq_rel);
    if (prev == 1 && entry.stale.load(std::memory_order_acquire) &&
        entry.shm_addr && entry.length) {
        if (pending) pending->emplace_back(entry.shm_addr, entry.length);
        entry.shm_addr = nullptr;
        entry.length = 0;
    }
}

void ShmTransport::flushUnmaps(PendingUnmap& pending) {
    for (auto& entry : pending) {
        if (entry.first && entry.second) munmap(entry.first, entry.second);
    }
    pending.clear();
}

void ShmTransport::pruneAndCapLocked(RelocateMap& mappings,
                                     const TransferMetadata::SegmentDesc& desc,
                                     uint64_t keep_addr,
                                     PendingUnmap* pending) {
    for (auto it = mappings.begin(); it != mappings.end();) {
        if (it->first != keep_addr &&
            (!it->second || !posixBufferAddrListed(desc, it->first))) {
            if (it->second) queueUnmap(*it->second, pending);
            it = mappings.erase(it);
        } else {
            ++it;
        }
    }
    while (mappings.size() > kMaxMappingsPerTarget) {
        auto victim = mappings.end();
        for (auto it = mappings.begin(); it != mappings.end(); ++it) {
            if (it->first == keep_addr || !it->second) continue;
            if (it->second->pin_count.load(std::memory_order_acquire) == 0) {
                victim = it;
                break;
            }
        }
        if (victim == mappings.end()) {
            for (auto it = mappings.begin(); it != mappings.end(); ++it) {
                if (it->first == keep_addr) continue;
                victim = it;
                break;
            }
        }
        if (victim == mappings.end()) break;
        if (victim->second) queueUnmap(*victim->second, pending);
        mappings.erase(victim);
    }
}

void ShmTransport::pruneIfNeeded(SegmentID target_id,
                                 const TransferMetadata::SegmentDesc& desc,
                                 uint64_t keep_addr) {
    const size_t live = posixBufferCount(desc);
    bool need = false;
    {
        RWSpinlock::ReadGuard guard(relocate_lock_);
        auto target = relocate_map_.find(target_id);
        if (target == relocate_map_.end()) return;
        need = target->second.size() > live ||
               target->second.size() > kMaxMappingsPerTarget;
    }
    if (!need) return;
    PendingUnmap pending;
    {
        RWSpinlock::WriteGuard guard(relocate_lock_);
        auto target = relocate_map_.find(target_id);
        if (target != relocate_map_.end()) {
            pruneAndCapLocked(target->second, desc, keep_addr, &pending);
        }
    }
    flushUnmaps(pending);
}

ShmTransport::MappingPin::MappingPin(ShmTransport* transport,
                                     std::shared_ptr<OpenedShmEntry> entry)
    : transport_(transport), entry_(std::move(entry)) {}

ShmTransport::MappingPin::MappingPin(MappingPin&& other) noexcept
    : transport_(other.transport_), entry_(std::move(other.entry_)) {
    other.transport_ = nullptr;
}

ShmTransport::MappingPin& ShmTransport::MappingPin::operator=(
    MappingPin&& other) noexcept {
    if (this != &other) {
        reset();
        transport_ = other.transport_;
        entry_ = std::move(other.entry_);
        other.transport_ = nullptr;
    }
    return *this;
}

ShmTransport::MappingPin::~MappingPin() { reset(); }

void ShmTransport::MappingPin::reset() {
    if (!transport_ || !entry_) {
        transport_ = nullptr;
        entry_.reset();
        return;
    }
    PendingUnmap pending;
    {
        RWSpinlock::WriteGuard guard(transport_->relocate_lock_);
        unpinLocked(*entry_, &pending);
    }
    flushUnmaps(pending);
    transport_ = nullptr;
    entry_.reset();
}

void ShmTransport::adoptPin(std::shared_ptr<OpenedShmEntry> entry,
                            MappingPin* pin) {
    if (!pin || !entry) return;
    *pin = MappingPin(this, std::move(entry));
}

Status ShmTransport::relocateSharedMemoryAddress(uint64_t& dest_addr,
                                                 uint64_t length,
                                                 uint64_t target_id,
                                                 MappingPin* pin) {
    if (pin) pin->reset();
    if (!metadata_) {
        return Status::InvalidArgument("SHM transport is not installed");
    }
    const uint64_t requested_addr = dest_addr;
    auto attempt = [&](bool force_update) -> Status {
        dest_addr = requested_addr;
        auto desc = metadata_->getSegmentDescByID(target_id, force_update);
        if (!desc) {
            return Status::AddressNotRegistered("Invalid target segment ID");
        }

        const BufferDesc* buffer = nullptr;
        for (const auto& entry : desc->buffers) {
            if (!isPosixShmName(entry.shm_name)) continue;
            if (rangeContains(entry.addr, entry.length, dest_addr, length)) {
                buffer = &entry;
                break;
            }
        }
        if (!buffer) {
            return Status::AddressNotRegistered(
                "Requested address is not in a POSIX shm buffer");
        }

        std::shared_ptr<OpenedShmEntry> hit;
        {
            RWSpinlock::ReadGuard guard(relocate_lock_);
            auto target = relocate_map_.find(target_id);
            uint64_t candidate = requested_addr;
            if (target == relocate_map_.end() ||
                !tryResolve(target->second, candidate, length, buffer->shm_name,
                            &hit) ||
                !hit) {
                hit.reset();
            }
        }
        if (hit && pin) {
            bool pinned = false;
            {
                RWSpinlock::WriteGuard guard(relocate_lock_);
                if (!hit->stale.load(std::memory_order_acquire) &&
                    hit->shm_addr) {
                    hit->pin_count.fetch_add(1, std::memory_order_acq_rel);
                    pinned = true;
                }
            }
            if (!pinned) hit.reset();
        }
        if (hit) {
            // Cached mmap stays valid after shm_unlink. Probe the name so a
            // peer free+realloc cannot silently write into the orphaned object.
            if (posixShmObjectExists(buffer->shm_name)) {
                dest_addr = requested_addr - buffer->addr +
                            reinterpret_cast<uint64_t>(hit->shm_addr);
                adoptPin(hit, pin);
                pruneIfNeeded(target_id, *desc, buffer->addr);
                return Status::OK();
            }
            if (pin) {
                PendingUnmap drop;
                {
                    RWSpinlock::WriteGuard guard(relocate_lock_);
                    unpinLocked(*hit, &drop);
                }
                flushUnmaps(drop);
            }
        }

        PendingUnmap pending;
        std::shared_ptr<OpenedShmEntry> reused;
        {
            RWSpinlock::WriteGuard guard(relocate_lock_);
            RelocateMap& mappings = relocate_map_[target_id];
            pruneAndCapLocked(mappings, *desc, buffer->addr, &pending);
            auto mapping = mappings.find(buffer->addr);
            if (mapping != mappings.end() && mapping->second &&
                mapping->second->shm_name == buffer->shm_name &&
                mapping->second->shm_addr) {
                reused = mapping->second;
                if (pin)
                    reused->pin_count.fetch_add(1, std::memory_order_acq_rel);
            } else if (mapping != mappings.end()) {
                if (mapping->second) queueUnmap(*mapping->second, &pending);
                mappings.erase(mapping);
            }
        }
        flushUnmaps(pending);

        if (reused && posixShmObjectExists(buffer->shm_name)) {
            dest_addr = requested_addr - buffer->addr +
                        reinterpret_cast<uint64_t>(reused->shm_addr);
            adoptPin(reused, pin);
            return Status::OK();
        }
        if (reused) {
            {
                RWSpinlock::WriteGuard guard(relocate_lock_);
                if (pin) unpinLocked(*reused, &pending);
                auto target = relocate_map_.find(target_id);
                if (target != relocate_map_.end()) {
                    auto mapping = target->second.find(buffer->addr);
                    if (mapping != target->second.end() && mapping->second &&
                        mapping->second->shm_name == buffer->shm_name) {
                        queueUnmap(*mapping->second, &pending);
                        target->second.erase(mapping);
                    }
                }
            }
            flushUnmaps(pending);
        }

        void* shm_addr = nullptr;
        Status mapped =
            mapPosixShm(buffer->shm_name, buffer->length, &shm_addr);
        if (!mapped.ok()) return mapped;
        LOG(INFO) << "Original shared memory: " << (void*)buffer->addr << "--"
                  << (void*)(buffer->addr + buffer->length);
        LOG(INFO) << "Remapped shared memory: " << shm_addr << "--"
                  << (void*)((uintptr_t)shm_addr + buffer->length);

        std::shared_ptr<OpenedShmEntry> installed;
        {
            RWSpinlock::WriteGuard guard(relocate_lock_);
            RelocateMap& mappings = relocate_map_[target_id];
            auto mapping = mappings.find(buffer->addr);
            if (mapping != mappings.end() && mapping->second &&
                mapping->second->shm_name == buffer->shm_name &&
                mapping->second->shm_addr) {
                pending.emplace_back(shm_addr, buffer->length);
                shm_addr = mapping->second->shm_addr;
                installed = mapping->second;
            } else {
                if (mapping != mappings.end()) {
                    if (mapping->second) queueUnmap(*mapping->second, &pending);
                    mappings.erase(mapping);
                }
                installed = std::make_shared<OpenedShmEntry>();
                installed->shm_addr = shm_addr;
                installed->length = buffer->length;
                installed->shm_name = buffer->shm_name;
                mappings[buffer->addr] = installed;
            }
            if (pin && installed)
                installed->pin_count.fetch_add(1, std::memory_order_acq_rel);
            pruneAndCapLocked(mappings, *desc, buffer->addr, &pending);
        }
        flushUnmaps(pending);

        dest_addr = requested_addr - buffer->addr +
                    reinterpret_cast<uint64_t>(shm_addr);
        adoptPin(installed, pin);
        return Status::OK();
    };

    Status status = attempt(false);
    if (status.ok() || !status.IsMemory()) return status;
    if (pin) pin->reset();
    // The cached segment desc still named an object that is gone (peer
    // free/realloc). Refetch once so the same virtual address can remap.
    return attempt(true);
}

Status ShmTransport::copySlice(TransferTask& task,
                               const TransferRequest& request,
                               uint64_t dest_addr) {
    task.total_bytes = request.length;
    Slice* slice = getSliceCache().allocate();
    slice->source_addr = (char*)request.source;
    slice->local.dest_addr = (char*)dest_addr;
    slice->length = request.length;
    slice->opcode = request.opcode;
    slice->task = &task;
    slice->target_id = request.target_id;
    slice->status = Slice::PENDING;
    task.slice_list.push_back(slice);
    __sync_fetch_and_add(&task.slice_count, 1);

    if (!slice->source_addr || !slice->local.dest_addr) {
        slice->markFailed();
        return Status::InvalidArgument("SHM copy with null pointer");
    }

    void* src = (request.opcode == TransferRequest::READ)
                    ? (void*)slice->local.dest_addr
                    : slice->source_addr;
    void* dst = (request.opcode == TransferRequest::READ)
                    ? slice->source_addr
                    : (void*)slice->local.dest_addr;
    std::memcpy(dst, src, slice->length);
    slice->markSuccess();
    return Status::OK();
}

void ShmTransport::failSlice(TransferTask& task,
                             const TransferRequest& request) {
    Slice* slice = getSliceCache().allocate();
    slice->source_addr = (char*)request.source;
    slice->local.dest_addr = nullptr;
    slice->length = request.length;
    slice->opcode = request.opcode;
    slice->task = &task;
    slice->target_id = request.target_id;
    slice->status = Slice::PENDING;
    task.slice_list.push_back(slice);
    __sync_fetch_and_add(&task.slice_count, 1);
    slice->markFailed();
}

Status ShmTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::InvalidArgument(
            "ShmTransport: Exceed the limitation of capacity");
    }

    std::vector<uint64_t> dest_addrs;
    dest_addrs.reserve(entries.size());
    std::vector<MappingPin> pins(entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& request = entries[i];
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            Status status = relocateSharedMemoryAddress(
                dest_addr, request.length, request.target_id, &pins[i]);
            if (!status.ok()) return status;
        }
        dest_addrs.push_back(dest_addr);
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        TransferTask& task = batch_desc.task_list[task_id++];
        task.batch_id = batch_id;
        Status status = copySlice(task, entries[i], dest_addrs[i]);
        if (!status.ok()) return status;
    }
    return Status::OK();
}

Status ShmTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    std::vector<uint64_t> dest_addrs;
    dest_addrs.reserve(task_list.size());
    std::vector<MappingPin> pins(task_list.size());
    Status first_error = Status::OK();
    for (size_t i = 0; i < task_list.size(); ++i) {
        auto* task = task_list[i];
        assert(task);
        assert(task->request);
        const auto& request = *task->request;
        uint64_t dest_addr = request.target_offset;
        if (request.target_id != LOCAL_SEGMENT_ID) {
            Status status = relocateSharedMemoryAddress(
                dest_addr, request.length, request.target_id, &pins[i]);
            if (!status.ok()) {
                if (first_error.ok()) first_error = status;
                dest_addrs.push_back(0);
                continue;
            }
        }
        dest_addrs.push_back(dest_addr);
    }

    if (!first_error.ok()) {
        for (auto* task : task_list) failSlice(*task, *task->request);
        return first_error;
    }

    for (size_t i = 0; i < task_list.size(); ++i) {
        Status status =
            copySlice(*task_list[i], *task_list[i]->request, dest_addrs[i]);
        if (!status.ok()) return status;
    }
    return Status::OK();
}

Status ShmTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "ShmTransport::getTransferStatus invalid argument, batch id: " +
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

}  // namespace mooncake
