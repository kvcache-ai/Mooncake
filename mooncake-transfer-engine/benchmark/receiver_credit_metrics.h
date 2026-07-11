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

#ifndef TEBENCH_RECEIVER_CREDIT_METRICS_H
#define TEBENCH_RECEIVER_CREDIT_METRICS_H

#include <cstdint>
#include <mutex>
#include <string>

namespace mooncake {
namespace tent {

struct ReceiverCapacitySnapshot {
    uint64_t capacity_bytes{0};
    uint64_t capacity_slots{0};
    uint64_t current_bytes{0};
    uint64_t current_slots{0};
    uint64_t peak_bytes{0};
    uint64_t peak_slots{0};
    uint64_t admitted_total{0};
    uint64_t stalled_total{0};
    uint64_t capacity_violation_total{0};
    uint64_t invalid_release_total{0};
};

// Benchmark-only receiver resource accounting. In observe mode reservations
// are recorded even when they exceed capacity, exposing aggregate overcommit.
// In enforced mode a complete bytes+slots charge is accepted atomically only
// when both dimensions fit.
class ReceiverCapacityTracker {
   public:
    ReceiverCapacityTracker(uint64_t capacity_bytes, uint64_t capacity_slots);

    bool tryReserve(uint64_t bytes, uint64_t slots, bool enforce_capacity);
    bool release(uint64_t bytes, uint64_t slots);
    ReceiverCapacitySnapshot snapshot() const;

   private:
    static bool checkedAdd(uint64_t lhs, uint64_t rhs, uint64_t* result);

    mutable std::mutex mutex_;
    ReceiverCapacitySnapshot state_;
};

struct ReceiverCreditRunReport {
    std::string run_id;
    std::string mode;
    std::string condition;
    int sender_count{0};
    int repetition{0};
    uint64_t offered{0};
    uint64_t completed{0};
    uint64_t data_errors{0};
    double throughput_gbps{0.0};
    double p99_us{0.0};
    double oracle_throughput_gbps{0.0};
    ReceiverCapacitySnapshot receiver;
};

bool appendReceiverCreditRunJsonl(const std::string& path,
                                  const ReceiverCreditRunReport& report,
                                  std::string* error);

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_RECEIVER_CREDIT_METRICS_H
