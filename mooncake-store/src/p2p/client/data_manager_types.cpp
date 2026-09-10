#include "p2p/client/data_manager_types.h"

namespace mooncake {

// Moved verbatim from tiered_backend.cpp: this string is the segment identity
// reported to Master and the per-tier metric label, so its exact shape is part
// of the external contract and must not change with the refactor.
std::string MakeTierSegmentName(const UUID& id) {
    return "tier_" + std::to_string(id.first) + "_" + std::to_string(id.second);
}

std::string TierView::GetName() const { return MakeTierSegmentName(id); }

}  // namespace mooncake
