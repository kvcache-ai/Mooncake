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

#ifndef TRANSFER_ENGINE_METRICS_H_
#define TRANSFER_ENGINE_METRICS_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include "metrics_exporter.h"

namespace mooncake {

// Prometheus metrics for the Classic Transfer Engine, process-global so that
// every engine in a process reports into one scrape target (as TENT does).
//
// The metric set follows TENT's read/write split under a mooncake_te_ prefix:
//   mooncake_te_{read,write}_bytes_total       counter
//   mooncake_te_{read,write}_requests_total    counter (all observed outcomes)
//   mooncake_te_{read,write}_failures_total    counter
//   mooncake_te_{read,write}_timeouts_total    counter
//   mooncake_te_{read,write}_latency_us        histogram
//   mooncake_te_{read,write}_size_bytes        histogram
//
// An eligible task is recorded once when the shared status-query path first
// observes a terminal state (see MultiTransport::recordTaskEnd()). Queries may
// come from the application or internal batch cleanup. Tasks without a terminal
// observation are not recorded; cleanup skips tasks already marked finished.
class TransferEngineMetrics {
   public:
    enum class Direction { Read, Write };

    static TransferEngineMetrics& instance();

    // Enable task collection process-wide from now on, independently of the
    // exporter lifecycle. Checking this flag does not construct the singleton.
    static void enable() {
        collection_enabled_.store(true, std::memory_order_relaxed);
    }
    static bool isEnabled() {
        return collection_enabled_.load(std::memory_order_relaxed);
    }

    // Start HTTP using MC_TE_METRIC_*; repeated calls are ignored until
    // shutdown.
    void initializeFromEnv();
    void shutdown() { exporter_.stop(); }

    void recordCompleted(Direction direction, size_t bytes,
                         uint64_t latency_us);
    void recordFailed(Direction direction);
    void recordTimeout(Direction direction);

    uint16_t httpPort() const { return exporter_.httpPort(); }
    std::string prometheusText() { return exporter_.prometheusText(); }
    std::string jsonText() { return exporter_.jsonText(); }
    std::string summaryText() { return exporter_.summaryText(); }

    struct DirectionMetrics {
        explicit DirectionMetrics(std::string direction);

        ylt::metric::counter_t bytes_total;
        ylt::metric::counter_t requests_total;
        ylt::metric::counter_t failures_total;
        ylt::metric::counter_t timeouts_total;
        metrics::Histogram latency_us;
        metrics::Histogram size_bytes;
    };

    DirectionMetrics& forDirection(Direction direction) {
        return direction == Direction::Read ? read_ : write_;
    }

   private:
    TransferEngineMetrics();
    ~TransferEngineMetrics() = default;
    TransferEngineMetrics(const TransferEngineMetrics&) = delete;
    TransferEngineMetrics& operator=(const TransferEngineMetrics&) = delete;

    std::string buildSummary();

    static std::atomic<bool> collection_enabled_;

    DirectionMetrics read_{"read"};
    DirectionMetrics write_{"write"};

    // Declared last: its destructor stops the threads that read the metrics
    // above.
    metrics::MetricsExporter exporter_{"Transfer Engine"};
};

}  // namespace mooncake

#endif  // TRANSFER_ENGINE_METRICS_H_
