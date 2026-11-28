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

#include "shared_segment/shared_segment.h"

#include <cstring>
#include <glog/logging.h>
#include <unordered_map>
#include <unordered_set>

#include "shared_segment_internal.h"

namespace mooncake {

// Backend-specific creators are implementation details of the generic factory
// below and are intentionally not exposed through the internal header.
std::unique_ptr<SharedSegmentBackend> CreateMmapSharedSegmentBackend();

#ifdef USE_ASCEND_DIRECT
std::unique_ptr<SharedSegmentBackend> CreateAscendSharedSegmentBackend();
#endif

namespace {
constexpr uint32_t kSegmentBlobMagic = 0x4D435353;  // "MCSS"
constexpr uint16_t kSegmentBlobVersion = 1;
constexpr uint64_t kFnvOffsetBasis = 1469598103934665603ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;
// A segment that does not fit in the virtual address space several times over
// is a declaration bug.
constexpr uint64_t kMaxSegmentBytes = 1ULL << 46;

std::string SelectSharedSegmentBackend(bool mmap) {
    if (mmap) {
        return "mmap";
    }
#if defined(USE_ASCEND_DIRECT)
    return "ascend";
#else
    return {};
#endif
}

uint64_t HashBytes(uint64_t seed, const void* data, size_t length) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint64_t hash = seed;
    for (size_t i = 0; i < length; ++i) {
        hash = (hash ^ bytes[i]) * kFnvPrime;
    }
    return hash;
}

uint64_t HashValue(uint64_t seed, uint64_t value) {
    return HashBytes(seed, &value, sizeof(value));
}

Status ValidateOptions(const SharedSegmentOptions& options,
                       const std::vector<uint32_t>& owner_ranks) {
    if (options.world_size == 0) {
        return Status::InvalidArgument(
            "Shared segment world_size must not be zero");
    }
    if (options.rank_id >= options.world_size) {
        return Status::InvalidArgument(
            "Shared segment rank_id must be below world_size");
    }
    if (owner_ranks.empty()) {
        return Status::InvalidArgument(
            "Shared segment owner_ranks must not be empty");
    }
    std::unordered_set<uint32_t> seen;
    seen.reserve(owner_ranks.size());
    for (uint32_t owner : owner_ranks) {
        if (owner >= options.world_size) {
            return Status::InvalidArgument("Shared segment owner rank " +
                                           std::to_string(owner) +
                                           " must be below world_size");
        }
        if (!seen.insert(owner).second) {
            return Status::InvalidArgument(
                "Shared segment owner_ranks contains duplicate rank " +
                std::to_string(owner));
        }
    }
    if (options.host_register && !options.mmap) {
        return Status::InvalidArgument(
            "Shared segment host_register requires mmap=true");
    }
    return Status::OK();
}

Status CheckPeerBlob(uint32_t rank, uint16_t backend_id, uint64_t fingerprint,
                     uint64_t alloc_size, const std::string& blob,
                     SegmentBlobHeader& header, std::vector<uint8_t>& handle) {
    auto status = DecodeSegmentBlob(blob, header, handle);
    if (!status.ok()) {
        return status;
    }
    if (header.rank_id != rank) {
        return Status::InvalidArgument("Shared segment blob of rank " +
                                       std::to_string(rank) +
                                       " carries a different rank id");
    }
    if (header.backend_id != backend_id) {
        return Status::InvalidArgument("Shared segment backend of rank " +
                                       std::to_string(rank) + " differs");
    }
    if (header.fingerprint != fingerprint) {
        return Status::InvalidArgument("Shared segment declaration of rank " +
                                       std::to_string(rank) +
                                       " differs from the local one");
    }
    if (header.alloc_size != alloc_size) {
        return Status::InvalidArgument(
            "Shared segment allocation size of rank " + std::to_string(rank) +
            " differs from the local one");
    }
    return Status::OK();
}

Status ExtractOwnerHandles(const SharedSegmentOptions& options,
                           const std::vector<uint32_t>& owner_ranks,
                           uint16_t backend_id, uint64_t fingerprint,
                           uint64_t alloc_size,
                           const std::vector<std::string>& blobs,
                           std::vector<std::vector<uint8_t>>& owner_handles) {
    if (blobs.size() != options.world_size) {
        return Status::InvalidArgument(
            "Shared segment expects one blob per rank, got " +
            std::to_string(blobs.size()));
    }
    std::unordered_map<uint32_t, size_t> rank_to_owner_index;
    rank_to_owner_index.reserve(owner_ranks.size());
    for (size_t i = 0; i < owner_ranks.size(); ++i) {
        rank_to_owner_index[owner_ranks[i]] = i;
    }

    owner_handles.assign(owner_ranks.size(), {});
    for (uint32_t rank = 0; rank < blobs.size(); ++rank) {
        SegmentBlobHeader header{};
        std::vector<uint8_t> handle;
        auto status = CheckPeerBlob(rank, backend_id, fingerprint, alloc_size,
                                    blobs[rank], header, handle);
        if (!status.ok()) {
            return status;
        }
        const auto it = rank_to_owner_index.find(rank);
        if (it != rank_to_owner_index.end()) {
            if (handle.empty()) {
                return Status::InvalidArgument("Shared segment owner rank " +
                                               std::to_string(rank) +
                                               " did not export a handle");
            }
            owner_handles[it->second] = std::move(handle);
        } else if (!handle.empty()) {
            return Status::InvalidArgument("Shared segment non-owner rank " +
                                           std::to_string(rank) +
                                           " unexpectedly exported a handle");
        }
    }
    return Status::OK();
}
}  // namespace

uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    return (value + alignment - 1) / alignment * alignment;
}

std::vector<uint32_t> ResolveOwnerRanks(const SharedSegmentOptions& options) {
    if (!options.owner_ranks.empty()) {
        return options.owner_ranks;
    }
    return {options.owner_rank};
}

std::vector<OwnerChunk> ComputeOwnerChunks(
    uint64_t size, uint64_t granularity,
    const std::vector<uint32_t>& owner_ranks, uint64_t& total_alloc_size) {
    std::vector<OwnerChunk> chunks;
    chunks.reserve(owner_ranks.size());
    uint64_t total_granules = AlignUp(size, granularity) / granularity;
    if (total_granules < owner_ranks.size()) {
        total_granules = owner_ranks.size();
    }
    const uint64_t base_granules = total_granules / owner_ranks.size();
    const uint64_t extra_granules = total_granules % owner_ranks.size();
    uint64_t offset = 0;
    for (size_t i = 0; i < owner_ranks.size(); ++i) {
        const uint64_t granules = base_granules + (i < extra_granules ? 1 : 0);
        const uint64_t chunk_size = granules * granularity;
        chunks.push_back(OwnerChunk{owner_ranks[i], offset, chunk_size});
        offset += chunk_size;
    }
    total_alloc_size = offset;
    return chunks;
}

uint64_t ComputeSegmentFingerprint(const std::string& name, uint64_t size,
                                   const SharedSegmentOptions& options) {
    const auto owners = ResolveOwnerRanks(options);
    uint64_t hash = HashBytes(kFnvOffsetBasis, name.data(), name.size());
    hash = HashValue(hash, size);
    hash = HashValue(hash, options.world_size);
    hash = HashValue(hash, owners.size());
    for (uint32_t owner : owners) {
        hash = HashValue(hash, owner);
    }
    hash = HashValue(hash, options.mmap ? 1ULL : 0ULL);
    hash = HashValue(hash, options.host_register ? 1ULL : 0ULL);
    return hash;
}

Status EncodeSegmentBlob(const SegmentBlobHeader& header,
                         const std::vector<uint8_t>& handle,
                         std::string& blob) {
    if (handle.size() > kMaxHandleBytes) {
        return Status::InvalidArgument("Shared segment handle of " +
                                       std::to_string(handle.size()) +
                                       " bytes does not fit in a blob");
    }
    blob.assign(kSegmentBlobBytes, '\0');
    SegmentBlobHeader stamped = header;
    stamped.magic = kSegmentBlobMagic;
    stamped.version = kSegmentBlobVersion;
    stamped.handle_bytes = static_cast<uint32_t>(handle.size());
    memcpy(blob.data(), &stamped, sizeof(stamped));
    if (!handle.empty()) {
        memcpy(blob.data() + sizeof(stamped), handle.data(), handle.size());
    }
    return Status::OK();
}

Status DecodeSegmentBlob(const std::string& blob, SegmentBlobHeader& header,
                         std::vector<uint8_t>& handle) {
    if (blob.size() != kSegmentBlobBytes) {
        return Status::InvalidArgument(
            "Shared segment blob has an unexpected length");
    }
    memcpy(&header, blob.data(), sizeof(header));
    if (header.magic != kSegmentBlobMagic) {
        return Status::InvalidArgument("Shared segment blob magic mismatch");
    }
    if (header.version != kSegmentBlobVersion) {
        return Status::InvalidArgument(
            "Shared segment blob version mismatch, peers run different builds");
    }
    if (header.handle_bytes > kMaxHandleBytes) {
        return Status::InvalidArgument(
            "Shared segment blob declares an oversized handle");
    }
    const auto* payload =
        reinterpret_cast<const uint8_t*>(blob.data()) + sizeof(header);
    handle.assign(payload, payload + header.handle_bytes);
    return Status::OK();
}

std::unique_ptr<SharedSegmentBackend> CreateSharedSegmentBackend(
    const std::string& impl) {
    if (impl == "mmap") {
        return CreateMmapSharedSegmentBackend();
    }
#if defined(USE_ASCEND_DIRECT)
    if (impl == "ascend") {
        return CreateAscendSharedSegmentBackend();
    }
#endif
    return nullptr;
}

SharedSegment::SharedSegment(std::string name, uint64_t size,
                             SharedSegmentOptions options)
    : name_(std::move(name)), size_(size), options_(std::move(options)) {}

SharedSegment::~SharedSegment() = default;

uintptr_t SharedSegment::device_addr() const {
    return backend_ == nullptr ? 0 : backend_->DeviceAddr();
}

bool SharedSegment::Supported(bool mmap, bool host_register) {
    if (host_register && !mmap) {
        return false;
    }
    if (!mmap) {
        return CreateSharedSegmentBackend(SelectSharedSegmentBackend(false)) !=
               nullptr;
    }
    if (host_register) {
#if defined(USE_ASCEND_DIRECT)
        return true;
#else
        return false;
#endif
    }
    // mmap without HostRegister only needs an anonymous memfd.
    return true;
}

Status SharedSegment::Create(const std::string& name, uint64_t size,
                             const SharedSegmentOptions& options,
                             std::shared_ptr<SharedSegment>& segment,
                             std::string& out_blob) {
    auto owner_ranks = ResolveOwnerRanks(options);
    auto status = ValidateOptions(options, owner_ranks);
    if (!status.ok()) {
        return status;
    }
    if (size == 0 || size > kMaxSegmentBytes) {
        return Status::InvalidArgument("Shared segment size must be in (0, " +
                                       std::to_string(kMaxSegmentBytes) + "]");
    }

    auto candidate =
        std::shared_ptr<SharedSegment>(new SharedSegment(name, size, options));
    candidate->owner_ranks_ = std::move(owner_ranks);
    candidate->backend_ =
        CreateSharedSegmentBackend(SelectSharedSegmentBackend(options.mmap));
    if (candidate->backend_ == nullptr) {
        return Status::NotImplemented(
            options.mmap ? "This build has no mmap shared-segment backend"
                         : "This build has no VMM backend for shared segments");
    }

    candidate->fingerprint_ = ComputeSegmentFingerprint(name, size, options);
    candidate->granularity_ = candidate->backend_->Granularity(options);
    if (candidate->granularity_ == 0) {
        return Status::InvalidArgument(
            "Shared segment backend reported zero granularity");
    }

    const auto chunks =
        ComputeOwnerChunks(size, candidate->granularity_,
                           candidate->owner_ranks_, candidate->alloc_size_);
    if (candidate->alloc_size_ > kMaxSegmentBytes) {
        return Status::InvalidArgument(
            "Shared segment aligned allocation size exceeds limit");
    }

    const OwnerChunk* local_chunk = nullptr;
    for (const auto& chunk : chunks) {
        if (chunk.owner_rank == options.rank_id) {
            local_chunk = &chunk;
            break;
        }
    }

    std::vector<uint8_t> handle;
    status =
        candidate->backend_->Prepare(candidate->alloc_size_, local_chunk,
                                     options, candidate->base_addr_, handle);
    if (!status.ok()) {
        return status;
    }

    SegmentBlobHeader header{};
    header.backend_id = candidate->backend_->BackendId();
    header.rank_id = options.rank_id;
    header.fingerprint = candidate->fingerprint_;
    header.alloc_size = candidate->alloc_size_;

    status = EncodeSegmentBlob(header, handle, out_blob);
    if (!status.ok()) {
        return status;
    }
    // Python may fold a layout tag after '|'; log the bare name only.
    const auto bare_name = name.substr(0, name.find('|'));
    LOG(INFO) << "Shared segment " << bare_name << " create: rank "
              << options.rank_id << " of " << options.world_size << ", owners "
              << candidate->owner_ranks_.size() << ", base 0x" << std::hex
              << candidate->base_addr_ << std::dec << ", "
              << candidate->alloc_size_ << " bytes";
    segment = std::move(candidate);
    return Status::OK();
}

Status SharedSegment::Complete(const std::vector<std::string>& blobs) {
    if (ready_) {
        return Status::InvalidArgument(
            "SharedSegment::Complete has already been called");
    }
    if (backend_ == nullptr) {
        return Status::InvalidArgument(
            "SharedSegment::Complete needs a segment returned by Create");
    }
    uint64_t total_alloc_size = 0;
    const auto chunks =
        ComputeOwnerChunks(size_, granularity_, owner_ranks_, total_alloc_size);
    std::vector<std::vector<uint8_t>> owner_handles;
    auto status =
        ExtractOwnerHandles(options_, owner_ranks_, backend_->BackendId(),
                            fingerprint_, alloc_size_, blobs, owner_handles);
    if (!status.ok()) {
        return status;
    }

    status = backend_->CompleteMapping(chunks, owner_handles, options_);
    if (!status.ok()) {
        return status;
    }
    ready_ = true;
    return Status::OK();
}

}  // namespace mooncake
