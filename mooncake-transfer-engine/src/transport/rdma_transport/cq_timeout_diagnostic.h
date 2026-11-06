// Copyright 2026 KVCache.AI
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

#ifndef CQ_TIMEOUT_DIAGNOSTIC_H
#define CQ_TIMEOUT_DIAGNOSTIC_H

#include <cstdint>

namespace mooncake {

// Tracks CQ timeout windows independently from WorkerPool so the decision can
// be tested deterministically without RDMA hardware or wall-clock sleeps.
// Poll timestamps stop advancing while an idle worker is parked, so a stale
// poll gap only indicates a stall when the CQ has also remained non-empty for
// the stall window. Transfer timeouts are based on completion progress rather
// than the time since the CQ first became non-empty.
class CqTimeoutDiagnostic {
   public:
    enum : uint64_t {
        kStallWindowNs = 5ull * 1000 * 1000 * 1000,
        kNoProgressWindowNs = 30ull * 1000 * 1000 * 1000,
        kMinReportIntervalNs = 5ull * 1000 * 1000 * 1000,
    };

    struct Verdict {
        bool report = false;
        uint64_t cq_nonzero_age_ms = 0;
        uint64_t no_progress_age_ms = 0;
        uint64_t poll_gap_ms = 0;
    };

    // Must be called from one monitor thread, once per monitor tick.
    Verdict observe(uint64_t now_ns, int64_t cq_outstanding,
                    uint64_t processed_slices, uint64_t last_poll_ts_ns) {
        Verdict verdict;

        const bool progressed =
            !have_processed_ || processed_slices != last_processed_slices_;
        last_processed_slices_ = processed_slices;
        have_processed_ = true;

        if (cq_outstanding <= 0) {
            cq_nonzero_since_ns_ = 0;
            no_progress_since_ns_ = 0;
            return verdict;
        }

        if (cq_nonzero_since_ns_ == 0 || cq_nonzero_since_ns_ > now_ns)
            cq_nonzero_since_ns_ = now_ns;
        if (no_progress_since_ns_ == 0 || progressed ||
            no_progress_since_ns_ > now_ns)
            no_progress_since_ns_ = now_ns;

        const uint64_t cq_nonzero_age_ns = now_ns - cq_nonzero_since_ns_;
        const uint64_t no_progress_age_ns = now_ns - no_progress_since_ns_;
        const bool has_poll_timestamp =
            last_poll_ts_ns > 0 && last_poll_ts_ns <= now_ns;
        const uint64_t poll_gap_ns =
            has_poll_timestamp ? now_ns - last_poll_ts_ns : 0;

        verdict.cq_nonzero_age_ms = cq_nonzero_age_ns / 1000000ull;
        verdict.no_progress_age_ms = no_progress_age_ns / 1000000ull;
        verdict.poll_gap_ms = poll_gap_ns / 1000000ull;

        const bool poll_stalled = cq_nonzero_age_ns >= kStallWindowNs &&
                                  poll_gap_ns >= kStallWindowNs;
        const bool transfer_timed_out =
            no_progress_age_ns >= kNoProgressWindowNs;
        if (!poll_stalled && !transfer_timed_out) return verdict;

        if (have_report_ && now_ns - last_report_ns_ < kMinReportIntervalNs)
            return verdict;

        last_report_ns_ = now_ns;
        have_report_ = true;
        verdict.report = true;
        return verdict;
    }

   private:
    uint64_t cq_nonzero_since_ns_ = 0;
    uint64_t no_progress_since_ns_ = 0;
    uint64_t last_processed_slices_ = 0;
    bool have_processed_ = false;
    uint64_t last_report_ns_ = 0;
    bool have_report_ = false;
};

}  // namespace mooncake

#endif  // CQ_TIMEOUT_DIAGNOSTIC_H
