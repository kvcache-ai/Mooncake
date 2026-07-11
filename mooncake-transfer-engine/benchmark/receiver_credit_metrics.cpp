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

#include "receiver_credit_metrics.h"

#include <algorithm>
#include <fstream>
#include <limits>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {

ReceiverCapacityTracker::ReceiverCapacityTracker(uint64_t capacity_bytes,
                                                 uint64_t capacity_slots) {
    state_.capacity_bytes = capacity_bytes;
    state_.capacity_slots = capacity_slots;
}

bool ReceiverCapacityTracker::checkedAdd(uint64_t lhs, uint64_t rhs,
                                         uint64_t* result) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) return false;
    *result = lhs + rhs;
    return true;
}

bool ReceiverCapacityTracker::tryReserve(uint64_t bytes, uint64_t slots,
                                         bool enforce_capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t next_bytes = 0;
    uint64_t next_slots = 0;
    const bool arithmetic_ok =
        checkedAdd(state_.current_bytes, bytes, &next_bytes) &&
        checkedAdd(state_.current_slots, slots, &next_slots);
    const bool exceeds_capacity = !arithmetic_ok ||
                                  next_bytes > state_.capacity_bytes ||
                                  next_slots > state_.capacity_slots;

    if (exceeds_capacity && enforce_capacity) {
        ++state_.stalled_total;
        return false;
    }
    if (!arithmetic_ok) {
        ++state_.capacity_violation_total;
        return false;
    }

    state_.current_bytes = next_bytes;
    state_.current_slots = next_slots;
    state_.peak_bytes = std::max(state_.peak_bytes, state_.current_bytes);
    state_.peak_slots = std::max(state_.peak_slots, state_.current_slots);
    ++state_.admitted_total;
    if (exceeds_capacity) ++state_.capacity_violation_total;
    return true;
}

bool ReceiverCapacityTracker::release(uint64_t bytes, uint64_t slots) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (bytes > state_.current_bytes || slots > state_.current_slots) {
        ++state_.invalid_release_total;
        return false;
    }
    state_.current_bytes -= bytes;
    state_.current_slots -= slots;
    return true;
}

ReceiverCapacitySnapshot ReceiverCapacityTracker::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_;
}

bool appendReceiverCreditRunJsonl(const std::string& path,
                                  const ReceiverCreditRunReport& report,
                                  std::string* error) {
    const auto& receiver = report.receiver;
    nlohmann::json record = {
        {"schema_version", 1},
        {"run_id", report.run_id},
        {"mode", report.mode},
        {"condition", report.condition},
        {"sender_count", report.sender_count},
        {"repetition", report.repetition},
        {"offered", report.offered},
        {"completed", report.completed},
        {"data_errors", report.data_errors},
        {"throughput_gbps", report.throughput_gbps},
        {"p99_us", report.p99_us},
        {"oracle_throughput_gbps", report.oracle_throughput_gbps},
        {"receiver",
         {{"capacity_bytes", receiver.capacity_bytes},
          {"capacity_slots", receiver.capacity_slots},
          {"current_bytes", receiver.current_bytes},
          {"current_slots", receiver.current_slots},
          {"peak_bytes", receiver.peak_bytes},
          {"peak_slots", receiver.peak_slots},
          {"admitted_total", receiver.admitted_total},
          {"stalled_total", receiver.stalled_total},
          {"capacity_violation_total", receiver.capacity_violation_total},
          {"invalid_release_total", receiver.invalid_release_total}}},
    };

    std::ofstream output(path, std::ios::app);
    if (!output) {
        *error = "failed to open receiver-credit JSONL output: " + path;
        return false;
    }
    output << record.dump() << '\n';
    if (!output) {
        *error = "failed to write receiver-credit JSONL output: " + path;
        return false;
    }
    return true;
}

}  // namespace tent
}  // namespace mooncake
