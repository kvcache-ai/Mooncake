// Copyright 2025 KVCache.AI
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

#pragma once

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "types.h"

namespace mooncake {

// Master-side backpressure that is safe to retry: TASK_PENDING_LIMIT
// _EXCEEDED is the OpLog in-flight window being full, and the writer
// itself is healthy (slots free within milliseconds). A ScanMeta burst
// (e.g. all ranks of a TP worker restarting over an existing SSD cache)
// can transiently fill the window even when each batch alone fits; see
// issue #5563.
inline bool IsRetryableMasterBackpressure(ErrorCode error) {
    return error == ErrorCode::TASK_PENDING_LIMIT_EXCEEDED ||
           error == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
}

inline constexpr int kNotifyMasterMaxAttempts = 8;
inline constexpr int kNotifyMasterMaxBackoffMs = 16;

// Only for the synchronous startup re-registration (FileStorage::Init()).
// The background ReRegisterOffloadedObjects() scan must NOT retry on
// backpressure: its bucket snapshot is taken under the bucket lock, and an
// eviction during the backoff window could delete the SSD copy, making the
// stale snapshot re-advertise a replica that no longer exists. That path
// is periodic and simply retries on its next heartbeat round instead.
//
// The batch RPC does not report which entry hit backpressure, and entries
// before the failing one are already registered; each re-registration
// consumes a fresh OpLog slot on the master. Resending the whole batch can
// therefore starve the tail when the window frees only one slot at a
// time. Instead, a range that exhausts its attempts is split in half and
// each half is (re)submitted independently: completed ranges are never
// resent, and the granularity converges towards the window size. Worst
// case degrades to per-entry RPCs; the normal path is a single RPC.
//
// notify_range(begin, end) must deliver batch entries [begin, end);
// sleep_ms(backoff_ms) paces retries between attempts (injectable so
// tests can observe the schedule without wall-clock delays).
template <typename NotifyRangeFn, typename SleepMsFn>
tl::expected<void, ErrorCode> NotifyRangesWithBackpressureRetry(
    size_t total_entries, NotifyRangeFn&& notify_range, SleepMsFn&& sleep_ms) {
    std::vector<std::pair<size_t, size_t>> pending{{0, total_entries}};
    while (!pending.empty()) {
        const auto [begin, end] = pending.back();
        pending.pop_back();
        for (int attempt = 0;; ++attempt) {
            auto result = notify_range(begin, end);
            if (result) {
                // Success for this range only: keep draining the pending
                // ranges instead of reporting the whole batch done.
                break;
            }
            if (!IsRetryableMasterBackpressure(result.error())) {
                return result;
            }
            if (attempt + 1 >= kNotifyMasterMaxAttempts) {
                if (end - begin <= 1) {
                    LOG(ERROR) << "NotifyOffloadSuccess still failing after "
                               << kNotifyMasterMaxAttempts
                               << " attempts, error: " << result.error();
                    return result;
                }
                const size_t mid = begin + (end - begin) / 2;
                LOG(WARNING)
                    << "NotifyOffloadSuccess hit sustained backpressure ("
                    << result.error() << ") for entries [" << begin << ","
                    << end << "), splitting into [" << begin << "," << mid
                    << ") and [" << mid << "," << end << ")";
                pending.emplace_back(mid, end);
                pending.emplace_back(begin, mid);
                break;
            }
            const int backoff_ms =
                std::min(1 << attempt, kNotifyMasterMaxBackoffMs);
            LOG(WARNING) << "NotifyOffloadSuccess hit transient backpressure ("
                         << result.error() << ") for entries [" << begin << ","
                         << end << "), retry " << (attempt + 1) << "/"
                         << kNotifyMasterMaxAttempts << " in " << backoff_ms
                         << "ms";
            sleep_ms(backoff_ms);
        }
    }
    return {};
}

}  // namespace mooncake
