#include "p2p/client/v2/copier.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include <glog/logging.h>

#include "p2p/client/v2/transfer_coordinator.h"

namespace mooncake::v2 {
namespace {

// Bounds one Read/Write call against a slow medium so a large object cannot
// monopolise a device queue in a single request.
constexpr size_t kDefaultChunkBytes = 4ULL * 1024 * 1024;

std::span<const std::byte> AsBytes(const void* ptr, size_t size) {
    return {static_cast<const std::byte*>(ptr), size};
}

std::span<std::byte> AsWritableBytes(void* ptr, size_t size) {
    return {static_cast<std::byte*>(ptr), size};
}

/** Common precondition check: both sides must actually hold `length` bytes. */
ErrorCode ValidateRequest(const CopyRequest& request) {
    if (request.length == 0) return ErrorCode::OK;
    const CopyEndpoint& src = request.source;
    const CopyEndpoint& dst = request.destination;
    if ((src.handle == nullptr) == (src.host_buffer == nullptr) ||
        (dst.handle == nullptr) == (dst.host_buffer == nullptr)) {
        LOG(ERROR) << "A copy endpoint must be either a block or a buffer";
        return ErrorCode::INVALID_PARAMS;
    }
    if (src.capacity < request.length || dst.capacity < request.length) {
        LOG(ERROR) << "Copy of " << request.length
                   << " bytes does not fit, source capacity=" << src.capacity
                   << ", destination capacity=" << dst.capacity;
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

bool DeadlinePassed(const std::optional<Clock::time_point>& deadline) {
    return deadline.has_value() &&
           std::chrono::steady_clock::now() >= *deadline;
}

/**
 * @class DramMemcpyCopier
 */
class DramMemcpyCopier final : public Copier {
   public:
    bool CanCopy(const CopyEndpoint& source,
                 const CopyEndpoint& destination) const override {
        return source.HostAddress() != nullptr &&
               destination.HostAddress() != nullptr;
    }

    CopyResult Copy(const CopyRequest& request) override {
        const ErrorCode invalid = ValidateRequest(request);
        if (invalid != ErrorCode::OK) return CopyResult::Failure(invalid);
        if (request.length == 0) return CopyResult::Success(0);
        if (IsCancelled(request.cancellation)) {
            return CopyResult::Failure(ErrorCode::SHUTTING_DOWN);
        }
        void* src = request.source.HostAddress();
        void* dst = request.destination.HostAddress();
        if (src == nullptr || dst == nullptr) {
            return CopyResult::Failure(ErrorCode::INVALID_PARAMS);
        }
        std::memcpy(dst, src, request.length);
        return CopyResult::Success(request.length);
    }

    CopierCapabilities Capabilities() const override {
        CopierCapabilities capabilities;
        capabilities.name = "dram_memcpy";
        return capabilities;
    }
};

/**
 * @class StagedReadWriteCopier
 */
class StagedReadWriteCopier final : public Copier {
   public:
    explicit StagedReadWriteCopier(size_t default_chunk_bytes)
        : default_chunk_bytes_(default_chunk_bytes == 0 ? kDefaultChunkBytes
                                                        : default_chunk_bytes) {
    }

    /** The fallback: it only needs the generic handle interface. */
    bool CanCopy(const CopyEndpoint&, const CopyEndpoint&) const override {
        return true;
    }

    CopyResult Copy(const CopyRequest& request) override {
        const ErrorCode invalid = ValidateRequest(request);
        if (invalid != ErrorCode::OK) return CopyResult::Failure(invalid);
        if (request.length == 0) return CopyResult::Success(0);

        const size_t chunk_bytes = ChunkBytesFor(request);
        std::vector<std::byte>& scratch = Scratch();
        if (scratch.size() < chunk_bytes) scratch.resize(chunk_bytes);

        size_t done = 0;
        while (done < request.length) {
            if (IsCancelled(request.cancellation)) {
                return CopyResult{ErrorCode::SHUTTING_DOWN, done, false};
            }
            if (DeadlinePassed(request.deadline)) {
                // Retryable: the bytes are unchanged and a later attempt with
                // a fresh budget can simply start again.
                return CopyResult{ErrorCode::TRANSFER_FAIL, done, true};
            }
            const size_t chunk = std::min(chunk_bytes, request.length - done);
            auto read = ReadChunk(request.source, done, scratch.data(), chunk);
            if (!read) return CopyResult{read.error(), done, false};
            auto written =
                WriteChunk(request.destination, done, scratch.data(), chunk);
            if (!written) return CopyResult{written.error(), done, false};
            done += chunk;
        }
        return CopyResult::Success(done);
    }

    CopierCapabilities Capabilities() const override {
        CopierCapabilities capabilities;
        capabilities.name = "staged_read_write";
        capabilities.requires_staging = true;
        capabilities.preferred_chunk_bytes = default_chunk_bytes_;
        return capabilities;
    }

   private:
    /**
     * @brief Reused across calls, per thread.
     *
     * A migration worker copies block after block; allocating and freeing a
     * multi-megabyte buffer for each one is pure overhead, and a shared pool
     * would need a lock on the copy path.
     */
    static std::vector<std::byte>& Scratch() {
        static thread_local std::vector<std::byte> scratch;
        return scratch;
    }

    size_t ChunkBytesFor(const CopyRequest& request) const {
        // The slower side sets the pace: a 1 MiB SSD preferred IO beats a
        // 4 KiB DRAM page hint, because the SSD is what the copy waits on.
        const size_t hint = std::max(request.source.preferred_io_size,
                                     request.destination.preferred_io_size);
        const size_t chunk = hint > 1 ? hint : default_chunk_bytes_;
        return std::min(std::max(chunk, size_t{1}), request.length);
    }

    static tl::expected<void, ErrorCode> ReadChunk(const CopyEndpoint& endpoint,
                                                   size_t offset, void* into,
                                                   size_t size) {
        if (endpoint.handle != nullptr) {
            return endpoint.handle->Read(endpoint.offset + offset,
                                         AsWritableBytes(into, size));
        }
        std::memcpy(into,
                    static_cast<const std::byte*>(endpoint.host_buffer) +
                        endpoint.offset + offset,
                    size);
        return {};
    }

    static tl::expected<void, ErrorCode> WriteChunk(
        const CopyEndpoint& endpoint, size_t offset, const void* from,
        size_t size) {
        if (endpoint.handle != nullptr) {
            return endpoint.handle->Write(endpoint.offset + offset,
                                          AsBytes(from, size));
        }
        std::memcpy(static_cast<std::byte*>(endpoint.host_buffer) +
                        endpoint.offset + offset,
                    from, size);
        return {};
    }

    size_t default_chunk_bytes_;
};

/**
 * @class TransferEngineCopier
 */
class TransferEngineCopier final : public Copier {
   public:
    TransferEngineCopier(TransferCoordinator* coordinator,
                         std::string local_endpoint)
        : coordinator_(coordinator),
          local_endpoint_(std::move(local_endpoint)) {}

    bool CanCopy(const CopyEndpoint& source,
                 const CopyEndpoint& destination) const override {
        // The engine writes from a local host address into a registered
        // destination. A source it cannot dereference, or a destination it
        // does not know about, is not its job.
        return coordinator_ != nullptr && !local_endpoint_.empty() &&
               source.HostAddress() != nullptr && destination.te_addressable &&
               destination.address.has_value();
    }

    CopyResult Copy(const CopyRequest& request) override {
        const ErrorCode invalid = ValidateRequest(request);
        if (invalid != ErrorCode::OK) return CopyResult::Failure(invalid);
        if (request.length == 0) return CopyResult::Success(0);
        if (IsCancelled(request.cancellation)) {
            return CopyResult::Failure(ErrorCode::SHUTTING_DOWN);
        }
        void* source = request.source.HostAddress();
        if (source == nullptr || !request.destination.address.has_value()) {
            return CopyResult::Failure(ErrorCode::INVALID_PARAMS);
        }

        // A local copy is a transfer whose peer is this node.
        RemoteBufferDesc peer;
        peer.segment_endpoint = local_endpoint_;
        peer.addr =
            request.destination.address->addr + request.destination.offset;
        peer.size = request.length;

        auto transferred =
            coordinator_->Transfer(source, request.length, {peer},
                                   Transport::TransferRequest::OpCode::WRITE);
        if (!transferred) {
            // Transport failures are transient far more often than not; the
            // executor's bounded retry decides whether it is worth another go.
            return CopyResult::Failure(transferred.error(), /*retryable=*/true);
        }
        return CopyResult::Success(request.length);
    }

    CopierCapabilities Capabilities() const override {
        CopierCapabilities capabilities;
        capabilities.name = "transfer_engine";
        return capabilities;
    }

   private:
    TransferCoordinator* coordinator_ = nullptr;
    std::string local_endpoint_;
};

}  // namespace

const char* ToString(CopyDomain domain) {
    switch (domain) {
        case CopyDomain::kHostMemory:
            return "host";
        case CopyDomain::kFileOrBlock:
            return "file";
        case CopyDomain::kDevice:
            return "device";
        case CopyDomain::kOpaque:
            return "opaque";
    }
    return "unknown";
}

tl::expected<void, ErrorCode> ValidateCopierConfig(const CopierConfig& config) {
    if (config.copy_timeout < std::chrono::milliseconds::zero()) {
        LOG(ERROR) << "copier.copy_timeout_ms must not be negative, got "
                   << config.copy_timeout.count();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // No upper bound on staging_buffer_bytes: a deployment with large objects
    // and plenty of memory is entitled to a big buffer, and the cost is paid
    // per copying thread, which the movement worker count already bounds.
    return {};
}

bool IsCancelled(const CancellationToken& token) {
    return token != nullptr && token->load(std::memory_order_acquire);
}

CopyEndpoint CopyEndpoint::FromBlock(BlockDataHandle& handle, size_t offset,
                                     size_t size,
                                     const BlockPoolCapabilities& capabilities,
                                     const UUID& tiler_id) {
    CopyEndpoint endpoint;
    endpoint.handle = &handle;
    endpoint.offset = offset;
    endpoint.capacity = size;
    // Derived from what the pool can do, never from a MemoryType: two pools
    // with the same medium can differ here, and one of them being reachable by
    // the CPU is exactly the fact routing needs.
    endpoint.domain = capabilities.direct_cpu_access ? CopyDomain::kHostMemory
                                                     : CopyDomain::kFileOrBlock;
    endpoint.direct_cpu_access = capabilities.direct_cpu_access;
    endpoint.te_addressable = capabilities.te_addressable;
    endpoint.preferred_io_size = capabilities.preferred_io_size;
    endpoint.minimum_alignment = capabilities.minimum_alignment;
    endpoint.tiler_id = tiler_id;
    if (capabilities.direct_cpu_access || capabilities.te_addressable) {
        endpoint.address = handle.GetTransferAddress();
    }
    return endpoint;
}

CopyEndpoint CopyEndpoint::FromHandle(BlockDataHandle& handle, size_t offset,
                                      size_t size, const UUID& tiler_id) {
    CopyEndpoint endpoint;
    endpoint.handle = &handle;
    endpoint.offset = offset;
    endpoint.capacity = size;
    endpoint.address = handle.GetTransferAddress();
    endpoint.direct_cpu_access = handle.DirectCpuAccess();
    // Registration, not addressability. A DRAM block hands out a usable
    // pointer whether or not the arena was registered with an engine, so
    // taking the address as proof would submit unregistered memory to RDMA.
    endpoint.te_addressable =
        handle.TeRegistered() && endpoint.address.has_value();
    endpoint.domain = endpoint.direct_cpu_access ? CopyDomain::kHostMemory
                                                 : CopyDomain::kFileOrBlock;
    endpoint.tiler_id = tiler_id;
    return endpoint;
}

CopyEndpoint CopyEndpoint::FromHost(void* buffer, size_t size) {
    CopyEndpoint endpoint;
    endpoint.host_buffer = buffer;
    endpoint.capacity = size;
    endpoint.domain = CopyDomain::kHostMemory;
    endpoint.direct_cpu_access = true;
    return endpoint;
}

void* CopyEndpoint::HostAddress() const {
    if (host_buffer != nullptr) {
        return static_cast<std::byte*>(host_buffer) + offset;
    }
    if (handle != nullptr && direct_cpu_access && address.has_value()) {
        return reinterpret_cast<std::byte*>(address->addr) + offset;
    }
    return nullptr;
}

std::vector<CopyResult> Copier::BatchCopy(
    std::span<const CopyRequest> requests) {
    std::vector<CopyResult> results;
    results.reserve(requests.size());
    for (const CopyRequest& request : requests) {
        // Each item independently: one failure must not re-run or roll back
        // the ones that already succeeded.
        results.push_back(Copy(request));
    }
    return results;
}

tl::expected<void, ErrorCode> CopierRegistry::Register(
    std::unique_ptr<Copier> copier) {
    if (frozen_) {
        LOG(ERROR) << "CopierRegistry is frozen; register during Init only";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (copier == nullptr) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    copiers_.push_back(std::move(copier));
    uses_.push_back(std::make_unique<std::atomic<uint64_t>>(0));
    return {};
}

void CopierRegistry::Freeze() { frozen_ = true; }

Copier* CopierRegistry::Route(const CopyEndpoint& source,
                              const CopyEndpoint& destination) const {
    // Registration order is priority order, so the generic fallback goes last
    // and a specialised copier registered before it always wins.
    for (size_t i = 0; i < copiers_.size(); ++i) {
        if (copiers_[i]->CanCopy(source, destination)) {
            uses_[i]->fetch_add(1, std::memory_order_relaxed);
            return copiers_[i].get();
        }
    }
    return nullptr;
}

std::vector<uint64_t> CopierRegistry::Uses() const {
    std::vector<uint64_t> counts;
    counts.reserve(uses_.size());
    for (const auto& used : uses_) {
        counts.push_back(used->load(std::memory_order_relaxed));
    }
    return counts;
}

std::vector<CopierCapabilities> CopierRegistry::Describe() const {
    std::vector<CopierCapabilities> described;
    described.reserve(copiers_.size());
    for (const auto& copier : copiers_) {
        described.push_back(copier->Capabilities());
    }
    return described;
}

std::unique_ptr<Copier> CreateDramMemcpyCopier() {
    return std::make_unique<DramMemcpyCopier>();
}

std::unique_ptr<Copier> CreateStagedReadWriteCopier(
    size_t default_chunk_bytes) {
    return std::make_unique<StagedReadWriteCopier>(default_chunk_bytes);
}

std::unique_ptr<Copier> CreateTransferEngineCopier(
    TransferCoordinator* coordinator, std::string local_endpoint) {
    return std::make_unique<TransferEngineCopier>(coordinator,
                                                  std::move(local_endpoint));
}

}  // namespace mooncake::v2
