#include "region_initial_state.h"

#include <limits>

namespace mooncake {

tl::expected<RegionInitialState, ErrorCode> BuildRegionInitialState(
    const RegionResourceSpec& spec,
    std::span<const AllocatedBuffer::Descriptor> descriptors) {
    if (spec.id == UUID{0, 0} || spec.name.empty() || spec.base == 0 ||
        spec.size == 0 ||
        spec.base > std::numeric_limits<uintptr_t>::max() - spec.size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uintptr_t end = spec.base + spec.size;
    RegionInitialState state;
    state.allocations.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        if ((descriptor.transport_endpoint_ != spec.transport_endpoint &&
             descriptor.transport_endpoint_ != spec.name) ||
            descriptor.size_ == 0 || descriptor.buffer_address_ < spec.base ||
            descriptor.buffer_address_ >= end ||
            descriptor.size_ > end - descriptor.buffer_address_) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        state.allocations.push_back(
            {descriptor.buffer_address_ - spec.base, descriptor.size_});
    }
    return state;
}

}  // namespace mooncake
