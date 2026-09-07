#include "p2p/client/v2/local_copy_engine.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

#include "p2p/client/v2/transfer_coordinator.h"

namespace mooncake::v2 {
namespace {

constexpr size_t kDefaultChunkBytes = 4ULL * 1024 * 1024;

}  // namespace

LocalCopyEngine::LocalCopyEngine(const LocalTransferConfig& config,
                                 TransferCoordinator* coordinator,
                                 const CopierConfig& copier_config,
                                 std::shared_ptr<Clock> clock)
    : config_(config),
      copier_config_(copier_config),
      clock_(clock != nullptr ? std::move(clock)
                              : std::make_shared<SteadyClock>()) {
    // Registration order is priority order.
    //
    // In TE mode the TransferEngine copier goes first because that is what the
    // deployment asked for; it only accepts pairs it can actually serve (a
    // host-addressable source and a registered destination), so everything
    // else still falls through to the paths below. A local DRAM-to-DRAM copy
    // is faster as a memcpy than as an engine round trip, and that ordering is
    // the price of honouring the configured mode rather than second-guessing
    // it.
    if (config.mode == LocalTransferMode::TE) {
        if (coordinator != nullptr && !config.te_endpoint.empty()) {
            (void)registry_.Register(
                CreateTransferEngineCopier(coordinator, config.te_endpoint));
        } else {
            LOG(WARNING)
                << "local_transfer_mode=TE but no "
                << (coordinator == nullptr ? "transfer coordinator"
                                           : "te_endpoint")
                << " is available; local copies will use the memory paths";
        }
    }
    (void)registry_.Register(CreateDramMemcpyCopier());
    // Last: it accepts everything, so anything registered after it would be
    // unreachable.
    (void)registry_.Register(
        CreateStagedReadWriteCopier(copier_config_.staging_buffer_bytes > 0
                                        ? copier_config_.staging_buffer_bytes
                                        : kDefaultChunkBytes));
}

tl::expected<void, ErrorCode> LocalCopyEngine::RegisterCopier(
    std::unique_ptr<Copier> copier) {
    return registry_.Register(std::move(copier));
}

tl::expected<void, ErrorCode> LocalCopyEngine::Run(
    const CopyRequest& request) const {
    if (request.length == 0) return {};
    // The configured backstop, applied only when the caller has not set one of
    // its own: a migration that already carries a deadline must keep it, and a
    // request-path copy with no deadline should still not be able to hang on a
    // wedged device forever.
    CopyRequest bounded = request;
    if (!bounded.deadline.has_value() &&
        copier_config_.copy_timeout > std::chrono::milliseconds::zero()) {
        bounded.deadline = clock_->Now() + copier_config_.copy_timeout;
    }
    const CopyRequest& effective = bounded;
    Copier* copier = registry_.Route(effective.source, effective.destination);
    if (copier == nullptr) {
        // The staged copier accepts every pair, so reaching this means the
        // registry was built wrong rather than that the pair is exotic.
        unroutable_.fetch_add(1, std::memory_order_relaxed);
        LOG(ERROR) << "No copier accepted a "
                   << ToString(effective.source.domain) << " -> "
                   << ToString(effective.destination.domain) << " copy";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const CopyResult result = copier->Copy(effective);
    copies_.fetch_add(1, std::memory_order_relaxed);
    bytes_.fetch_add(result.copied_bytes, std::memory_order_relaxed);
    if (!result.Ok()) {
        failures_.fetch_add(1, std::memory_order_relaxed);
        return tl::make_unexpected(result.status);
    }
    return {};
}

tl::expected<void, ErrorCode> LocalCopyEngine::Copy(
    const ImmutableBlock& source, MutableBlock& destination) const {
    if (!source || !destination) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const size_t total = source.Size();
    if (destination.Size() < total) {
        LOG(ERROR) << "Copy destination is too small, required=" << total
                   << ", provided=" << destination.Size();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    BlockDataHandle* from = source.DataHandleForCopy();
    BlockDataHandle* into = destination.DataHandleForCopy();
    if (from == nullptr || into == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    CopyRequest request;
    request.source = CopyEndpoint::FromHandle(*from, 0, total, UUID{0, 0});
    request.destination =
        CopyEndpoint::FromHandle(*into, 0, destination.Size(), UUID{0, 0});
    request.length = total;
    return Run(request);
}

tl::expected<void, ErrorCode> LocalCopyEngine::ReadToSlices(
    const ImmutableBlock& source, const std::vector<Slice>& slices) const {
    if (!source) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    BlockDataHandle* from = source.DataHandleForCopy();
    if (from == nullptr) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

    const size_t total = source.Size();
    size_t offset = 0;
    for (const auto& slice : slices) {
        if (offset >= total) break;
        const size_t chunk = std::min(slice.size, total - offset);
        if (chunk == 0) continue;
        if (slice.ptr == nullptr) {
            LOG(ERROR) << "Read destination slice is null";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        CopyRequest request;
        request.source =
            CopyEndpoint::FromHandle(*from, offset, chunk, UUID{0, 0});
        request.destination = CopyEndpoint::FromHost(slice.ptr, chunk);
        request.length = chunk;
        auto copied = Run(request);
        if (!copied) return copied;
        offset += chunk;
    }
    if (offset != total) {
        LOG(ERROR) << "Read covered " << offset << " of " << total << " bytes";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> LocalCopyEngine::WriteFromSlices(
    const std::vector<Slice>& slices, MutableBlock& destination) const {
    if (!destination) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    BlockDataHandle* into = destination.DataHandleForCopy();
    if (into == nullptr) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

    const size_t capacity = destination.Size();
    size_t offset = 0;
    for (const auto& slice : slices) {
        if (slice.size == 0) continue;
        if (slice.ptr == nullptr) {
            LOG(ERROR) << "Write source slice is null";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (offset > capacity || slice.size > capacity - offset) {
            LOG(ERROR) << "Write of " << slice.size << " bytes at " << offset
                       << " does not fit a block of " << capacity;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        CopyRequest request;
        request.source = CopyEndpoint::FromHost(slice.ptr, slice.size);
        request.destination =
            CopyEndpoint::FromHandle(*into, offset, slice.size, UUID{0, 0});
        request.length = slice.size;
        auto copied = Run(request);
        if (!copied) return copied;
        offset += slice.size;
    }
    if (offset != capacity) {
        // Symmetric with ReadToSlices, and not merely tidy: a short slice set
        // used to return OK after writing a prefix, so the caller would
        // Complete() and commit a block whose tail is whatever the allocator
        // last left there. Put is safe today only because it happens to
        // allocate exactly the slice total.
        LOG(ERROR) << "Write covered " << offset << " of " << capacity
                   << " bytes";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> LocalCopyEngine::ReadToBuffer(
    const ImmutableBlock& source, void* destination, size_t size) const {
    if (!source || destination == nullptr) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (size < source.Size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    BlockDataHandle* from = source.DataHandleForCopy();
    if (from == nullptr) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

    CopyRequest request;
    request.source =
        CopyEndpoint::FromHandle(*from, 0, source.Size(), UUID{0, 0});
    request.destination = CopyEndpoint::FromHost(destination, size);
    request.length = source.Size();
    return Run(request);
}

LocalCopyStats LocalCopyEngine::Stats() const {
    LocalCopyStats stats;
    stats.copies = copies_.load(std::memory_order_relaxed);
    stats.bytes = bytes_.load(std::memory_order_relaxed);
    stats.failures = failures_.load(std::memory_order_relaxed);
    stats.unroutable = unroutable_.load(std::memory_order_relaxed);
    for (const auto& capabilities : registry_.Describe()) {
        stats.copier_names.push_back(capabilities.name);
    }
    stats.copier_uses = registry_.Uses();
    return stats;
}

std::vector<CopierCapabilities> LocalCopyEngine::Describe() const {
    return registry_.Describe();
}

}  // namespace mooncake::v2
