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

#ifndef XFER_UTILS_H
#define XFER_UTILS_H

#include <string>
#include <unordered_map>
#include <cmath>
#include <sstream>
#include <iomanip>
#include <glog/logging.h>
#include <vector>
#include <algorithm>
#include <numeric>
#include <stdexcept>
#include <chrono>

#define CHECK_FAIL(call)                                        \
    do {                                                        \
        auto status_ = call;                                    \
        if (!status_.ok()) {                                    \
            LOG(INFO) << "Found error: " << status_.ToString(); \
            exit(EXIT_FAILURE);                                 \
        }                                                       \
    } while (0)

namespace mooncake {
namespace v1 {
struct XferBenchConfig {
    static void loadFromFlags();

    static std::string worker_type;
    static std::string seg_name;
    static std::string seg_type;
    static std::string target_seg_name;
    static std::string op_type;
    static bool check_consistency;

    static size_t total_buffer_size;
    static size_t start_block_size;
    static size_t max_block_size;
    static size_t start_batch_size;
    static size_t max_batch_size;
    static int duration;
    static int num_threads;

    static std::string metadata_type;
    static std::string metadata_url_list;
    static std::string xport_type;
};

struct XferMetricStats {
   public:
    double min() const {
        if (samples.empty()) return 0.0;
        return *std::min_element(samples.begin(), samples.end());
    }

    double max() const {
        if (samples.empty()) return 0.0;
        return *std::max_element(samples.begin(), samples.end());
    }

    double avg() const {
        if (samples.empty()) return 0.0;
        double sum = std::accumulate(samples.begin(), samples.end(), 0.0);
        return sum / samples.size();
    }

    double p90() { return percentile(90.0); }

    double p95() { return percentile(95.0); }

    double p99() { return percentile(99.0); }

    void add(double value) { samples.push_back(value); }

    void clear() { samples.clear(); }

    size_t count() { return samples.size(); }

   private:
    double percentile(double p);

   private:
    std::vector<double> samples;
};

struct XferBenchStats {
    XferMetricStats total_duration;
    XferMetricStats transfer_duration;
};

class XferBenchTimer {
   public:
    XferBenchTimer() : start_ts_(getCurrentTimeNs()) {}

    void reset() {
        start_ts_ = getCurrentTimeNs();
    }

    uint64_t lap_us(bool reset = true) {
        auto now_ts = getCurrentTimeNs();
        auto duration = now_ts - start_ts_;
        if (reset) start_ts_ = now_ts;
        return duration / 1000;
    }

   private:
    inline uint64_t getCurrentTimeNs() {
        auto ret = std::chrono::steady_clock::now().time_since_epoch();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(ret)
            .count();
    }

    uint64_t start_ts_;
};

void printStatsHeader();

void printStats(size_t block_size, size_t batch_size, XferBenchStats &stats);

}  // namespace v1
}  // namespace mooncake

#endif  // XFER_UTILS_H