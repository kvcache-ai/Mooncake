#pragma once

#include <chrono>
#include <cstddef>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"

namespace mooncake {
namespace ha {

class OpLogStore {
   public:
    virtual ~OpLogStore() = default;

    virtual tl::expected<OpLogSequenceId, ErrorCode> Append(
        const OpLogAppendRequest& request) = 0;

    virtual tl::expected<OpLogPollResult, ErrorCode> PollFrom(
        OpLogSequenceId start_seq, size_t max_records,
        std::chrono::milliseconds timeout) = 0;

    virtual tl::expected<OpLogSequenceId, ErrorCode> GetLatestSequence() = 0;
};

}  // namespace ha
}  // namespace mooncake
