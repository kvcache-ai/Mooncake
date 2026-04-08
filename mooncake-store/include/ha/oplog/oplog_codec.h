#pragma once

#include <string>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "ha/oplog/oplog_manager.h"
#include "types.h"

namespace mooncake {
namespace ha {
namespace oplog {

std::string SerializeEntryPayload(const OpLogEntry& entry);

tl::expected<OpLogEntry, ErrorCode> DeserializeEntryPayload(
    std::string_view payload);

OpLogAppendRequest BuildAppendRequest(const OpLogEntry& entry);

}  // namespace oplog
}  // namespace ha
}  // namespace mooncake
