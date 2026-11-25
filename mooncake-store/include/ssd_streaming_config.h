// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

namespace mooncake::ssd_offload {

// Matches OffsetAllocator::normalizedAllocationSize() (3-bit mantissa bin
// rounding) so config validation rejects non-power-of-two slot sizes whose
// rounded bins exceed the staging arena capacity. Returns {normalized_slot,
// effective_arena} (both 0 on invalid/overflow).
inline std::pair<uint64_t, uint64_t> OffsetAllocatorSlotBudget(
    size_t slot_bytes, uint64_t arena_bytes) noexcept {
    if (slot_bytes == 0 || arena_bytes == 0) return {0, 0};
    constexpr uint32_t kMantissaBits = 3;
    constexpr uint32_t kMantissaValue = 1u << kMantissaBits;
    constexpr uint32_t kMantissaMask = kMantissaValue - 1;
    constexpr uint64_t kMaxBinSize = 4026531840ull;  // SmallFloat::MAX_BIN_SIZE
    uint64_t multiplier_bits = 0;
    while (kMaxBinSize < (arena_bytes >> multiplier_bits)) {
        ++multiplier_bits;
    }
    const uint64_t quantum = uint64_t{1} << multiplier_bits;
    if (slot_bytes > std::numeric_limits<uint64_t>::max() - (quantum - 1)) {
        return {0, 0};
    }
    const uint64_t fake_size = (slot_bytes + quantum - 1) >> multiplier_bits;
    if (fake_size > kMaxBinSize) return {0, 0};
    uint32_t rounded = static_cast<uint32_t>(fake_size);
    if (rounded >= kMantissaValue) {
        const uint32_t highest_bit =
            static_cast<uint32_t>(std::bit_width(rounded) - 1);
        const uint32_t mantissa_shift = highest_bit - kMantissaBits;
        const uint32_t exp = mantissa_shift + 1;
        uint32_t mantissa = (rounded >> mantissa_shift) & kMantissaMask;
        const uint32_t low_mask = (uint32_t{1} << mantissa_shift) - 1;
        if ((rounded & low_mask) != 0) ++mantissa;
        const uint32_t encoded = (exp << kMantissaBits) + mantissa;
        const uint32_t out_exp = encoded >> kMantissaBits;
        const uint32_t out_mantissa = encoded & kMantissaMask;
        rounded = (out_mantissa | kMantissaValue) << (out_exp - 1);
    }
    const uint64_t normalized_slot = static_cast<uint64_t>(rounded)
                                     << multiplier_bits;
    const uint64_t effective_arena = (arena_bytes >> multiplier_bits)
                                     << multiplier_bits;
    return {normalized_slot, effective_arena};
}

struct StreamingReadConfig {
    bool enabled{false};
    bool use_uring{false};
    size_t slot_bytes{1024 * 1024};
    size_t slots{4};
    size_t read_workers{4};
    size_t write_workers{2};
    size_t max_outstanding{64};
    size_t max_chunks{65536};
    size_t max_fragments{65536};

    bool Validate(uint64_t arena_bytes) const {
        if (!enabled) return true;
        if (slot_bytes == 0 || slot_bytes > 64ULL * 1024 * 1024 || slots == 0 ||
            slots > 64 || read_workers == 0 || read_workers > 64 ||
            write_workers == 0 || write_workers > 64 ||
            max_outstanding < slots || max_outstanding > 65536 ||
            max_chunks == 0 || max_fragments == 0) {
            return false;
        }
        const auto [normalized_slot, effective_arena] =
            OffsetAllocatorSlotBudget(slot_bytes, arena_bytes);
        return normalized_slot > 0 &&
               normalized_slot <= effective_arena / slots;
    }
};

}  // namespace mooncake::ssd_offload
