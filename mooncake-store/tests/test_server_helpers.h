#pragma once

#include <string>
#include <ylt/util/tl/expected.hpp>

#include "embedded_master.h"
#include "utils.h"

namespace mooncake {
namespace testing {

// Tests historically constructed InProcMaster. The production implementation
// now lives in EmbeddedMaster; keep the old name as an alias.
using InProcMaster = EmbeddedMaster;

// Helper: return all segments body or error
inline tl::expected<std::string, int> GetAllSegments(const InProcMaster& m) {
    return httpGet(m.http_metrics_base() + "/get_all_segments");
}

// Helper: check if a client hostname appears in segment list
inline tl::expected<bool, int> CheckSegmentVisible(
    const InProcMaster& m, const std::string& local_hostname) {
    auto r = GetAllSegments(m);
    if (!r) return tl::unexpected(r.error());
    const std::string& body = r.value();
    if (body.empty() || body.find(local_hostname) == std::string::npos) {
        return false;
    }
    return true;
}

}  // namespace testing
}  // namespace mooncake
