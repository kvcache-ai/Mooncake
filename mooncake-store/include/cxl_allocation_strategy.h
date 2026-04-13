#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <set>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "allocation_strategy.h"
#include "allocator.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Allocation strategy for CXL storage tier.
 *
 * This strategy allocates replicas from CXL storage.
 * It is used when CXL support is enabled and the preferred storage level is
 * CXL. For RAM storage allocations, the configured allocation strategy (RANDOM
 * or FREE_RATIO_FIRST) is used instead.
 */
class CxlAllocationStrategy : public AllocationStrategy {
   public:
    CxlAllocationStrategy() = default;
    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments =
            std::vector<std::string>(),
        const std::set<std::string>& excluded_segments =
            std::set<std::string>()) {
        if (slice_length == 0 || replica_num == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // CXL is a global shared memory segment, only single replica is
        // supported
        if (replica_num > 1) {
            LOG(ERROR) << "CXL allocation only supports single replica, "
                       << "requested " << replica_num;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        if (preferred_segments.empty()) {
            LOG(ERROR) << "Preferred_segments is empty.";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        const std::string& cxl_segment_name = preferred_segments[0];

        VLOG(1) << "Do cxl allocate, overwritten segment=" << cxl_segment_name;

        const auto cxl_allocators =
            allocator_manager.getAllocators(cxl_segment_name);

        if (cxl_allocators == nullptr || cxl_allocators->size() == 0) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        std::shared_ptr<BufferAllocatorBase> cxl_allocator;
        for (const auto& cand : *cxl_allocators) {
            if (cand->getStorageLevel() == StorageLevel::CXL) {
                cxl_allocator = cand;
                break;
            }
        }
        if (!cxl_allocator) {
            LOG(ERROR) << "No CXL allocator in preferred_segment";
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        std::vector<Replica> replicas;
        replicas.reserve(1);

        auto buffer = cxl_allocator->allocate(slice_length);
        if (!buffer) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        buffer->change_to_cxl(cxl_segment_name);
        replicas.emplace_back(std::move(buffer), StorageLevel::CXL,
                              ReplicaStatus::PROCESSING);

        VLOG(1) << "Successfully allocated one CXL replica.";
        return replicas;
    }

    tl::expected<Replica, ErrorCode> AllocateFrom(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const std::string& segment_name) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
};

}  // namespace mooncake