// Copyright 2026 Huawei Technologies Co., Ltd
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

#include "shared_segment_internal.h"

namespace mooncake {
namespace {
constexpr uint32_t kSegmentBlobMagic = 0x4D435353;  // "MCSS"
constexpr uint16_t kSegmentBlobVersion = 1;
constexpr uint64_t kFnvOffsetBasis = 1469598103934665603ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;
// A segment that does not fit in the virtual address space several times over
// is a declaration bug.
constexpr uint64_t kMaxSegmentBytes = 1ULL << 46;

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

Status ValidateOptions(const SharedSegmentOptions& options) {
    if (options.world_size == 0) {
        return Status::InvalidArgument(
            "Shared segment world_size must not be zero");
    }
    if (options.rank_id >= options.world_size ||
        options.owner_rank >= options.world_size) {
        return Status::InvalidArgument(
            "Shared segment rank_id and owner_rank must be below world_size");
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

Status ExtractOwnerHandle(const SharedSegmentOptions& options,
                          uint16_t backend_id, uint64_t fingerprint,
                          uint64_t alloc_size,
                          const std::vector<std::string>& blobs,
                          std::vector<uint8_t>& owner_handle) {
    if (blobs.size() != options.world_size) {
        return Status::InvalidArgument(
            "Shared segment expects one blob per rank, got " +
            std::to_string(blobs.size()));
    }
    for (uint32_t rank = 0; rank < blobs.size(); ++rank) {
        SegmentBlobHeader header{};
        std::vector<uint8_t> handle;
        auto status = CheckPeerBlob(rank, backend_id, fingerprint, alloc_size,
                                    blobs[rank], header, handle);
        if (!status.ok()) {
            return status;
        }
        if (rank == options.owner_rank) {
            owner_handle = std::move(handle);
        }
    }
    if (owner_handle.empty()) {
        return Status::InvalidArgument(
            "Shared segment owner rank did not export a handle");
    }
    return Status::OK();
}
}  // namespace

uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    return (value + alignment - 1) / alignment * alignment;
}

uint64_t ComputeSegmentFingerprint(const std::string& name, uint64_t size,
                                   const SharedSegmentOptions& options) {
    uint64_t hash = HashBytes(kFnvOffsetBasis, name.data(), name.size());
    hash = HashValue(hash, size);
    hash = HashValue(hash, options.world_size);
    hash = HashValue(hash, options.owner_rank);
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
    const SharedSegmentOptions& options) {
    if (options.mmap) {
        return CreateMmapSharedSegmentBackend();
    }
#if defined(USE_ASCEND_DIRECT)
    return CreateAscendSharedSegmentBackend();
#elif defined(USE_CUDA)
    return CreateCudaSharedSegmentBackend();
#else
    return nullptr;
#endif
}

SharedSegment::SharedSegment(std::string name, uint64_t size,
                             SharedSegmentOptions options)
    : name_(std::move(name)), size_(size), options_(options) {}

SharedSegment::~SharedSegment() = default;

uintptr_t SharedSegment::device_addr() const {
    return backend_ == nullptr ? 0 : backend_->DeviceAddr();
}

bool SharedSegment::Supported(bool mmap) {
    SharedSegmentOptions options;
    options.mmap = mmap;
    return CreateSharedSegmentBackend(options) != nullptr;
}

Status SharedSegment::Create(const std::string& name, uint64_t size,
                             const SharedSegmentOptions& options,
                             std::shared_ptr<SharedSegment>& segment,
                             std::string& out_blob) {
    auto status = ValidateOptions(options);
    if (!status.ok()) {
        return status;
    }
    if (size == 0 || size > kMaxSegmentBytes) {
        return Status::InvalidArgument("Shared segment size must be in (0, " +
                                       std::to_string(kMaxSegmentBytes) + "]");
    }

    auto candidate =
        std::shared_ptr<SharedSegment>(new SharedSegment(name, size, options));
    candidate->backend_ = CreateSharedSegmentBackend(options);
    if (candidate->backend_ == nullptr) {
        return Status::NotImplemented(
            options.mmap
                ? "This build has no mmap shared-segment backend (needs CUDA "
                  "or Ascend for HostRegister)"
                : "This build has no VMM backend for shared segments");
    }

    candidate->fingerprint_ = ComputeSegmentFingerprint(name, size, options);
    const uint64_t granularity = candidate->backend_->Granularity(options);
    candidate->alloc_size_ = AlignUp(size, granularity);

    SegmentBlobHeader header{};
    header.backend_id = candidate->backend_->BackendId();
    header.rank_id = options.rank_id;
    header.fingerprint = candidate->fingerprint_;
    header.alloc_size = candidate->alloc_size_;

    std::vector<uint8_t> handle;
    if (options.rank_id == options.owner_rank) {
        status = candidate->backend_->CreateOwner(
            candidate->alloc_size_, options, candidate->base_addr_, handle);
    } else {
        status = candidate->backend_->ReserveLocal(
            candidate->alloc_size_, options, candidate->base_addr_);
    }
    if (!status.ok()) {
        return status;
    }

    status = EncodeSegmentBlob(header, handle, out_blob);
    if (!status.ok()) {
        return status;
    }
    LOG(INFO) << "Shared segment " << name << " create: rank "
              << options.rank_id << " of " << options.world_size << ", base 0x"
              << std::hex << candidate->base_addr_ << std::dec << ", "
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
    std::vector<uint8_t> owner_handle;
    auto status =
        ExtractOwnerHandle(options_, backend_->BackendId(), fingerprint_,
                           alloc_size_, blobs, owner_handle);
    if (!status.ok()) {
        return status;
    }

    if (options_.rank_id != options_.owner_rank) {
        status = backend_->ImportAndMap(alloc_size_, options_, owner_handle);
        if (!status.ok()) {
            return status;
        }
    }
    ready_ = true;
    return Status::OK();
}

}  // namespace mooncake
