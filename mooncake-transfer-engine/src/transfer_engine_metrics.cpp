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

#include "transfer_engine_metrics.h"

#include <iomanip>
#include <sstream>
#include <vector>

namespace mooncake {

TransferEngineMetrics& TransferEngineMetrics::instance() {
    static TransferEngineMetrics instance;
    return instance;
}

namespace {

// TENT's bucket boundaries, so both engines bucket alike.
const std::vector<double> kLatencyBucketsUs{
    100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000};
const std::vector<double> kSizeBuckets{1024,     4096,      16384,     65536,
                                       262144,   1048576,   4194304,   16777216,
                                       67108864, 268435456, 1073741824};

std::string metricName(const std::string& direction, const char* suffix) {
    return "mooncake_te_" + direction + suffix;
}

}  // namespace

TransferEngineMetrics::DirectionMetrics::DirectionMetrics(std::string direction)
    : bytes_total(
          metricName(direction, "_bytes_total"),
          "Total bytes transferred by completed " + direction + " transfers"),
      requests_total(metricName(direction, "_requests_total"),
                     "Total " + direction + " transfers observed terminal"),
      failures_total(metricName(direction, "_failures_total"),
                     "Total " + direction + " transfers failed or canceled"),
      timeouts_total(metricName(direction, "_timeouts_total"),
                     "Total " + direction + " transfers timed out"),
      latency_us(
          metricName(direction, "_latency_us"),
          "Latency distribution of completed " + direction +
              " transfers, submission to first observation, in microseconds",
          kLatencyBucketsUs),
      size_bytes(metricName(direction, "_size_bytes"),
                 "Size distribution of completed " + direction +
                     " transfers, in bytes",
                 kSizeBuckets) {
    // ylt leaves an untouched counter out of /metrics; inc(0) marks it touched
    // so the series reads 0 instead of first appearing at 1.
    bytes_total.inc(0);
    requests_total.inc(0);
    failures_total.inc(0);
    timeouts_total.inc(0);
}

TransferEngineMetrics::TransferEngineMetrics() {
    for (auto* dir : {&read_, &write_}) {
        exporter_.addCounter(&dir->bytes_total);
        exporter_.addCounter(&dir->requests_total);
        exporter_.addCounter(&dir->failures_total);
        exporter_.addCounter(&dir->timeouts_total);
        exporter_.addHistogram(&dir->latency_us);
        exporter_.addHistogram(&dir->size_bytes);
    }
    exporter_.setSummaryProvider([this]() { return buildSummary(); });
}

void TransferEngineMetrics::initializeFromEnv() {
    exporter_.start(metrics::ExporterConfig::fromEnv("MC_TE_METRIC"));
}

void TransferEngineMetrics::recordCompleted(Direction direction, size_t bytes,
                                            uint64_t latency_us) {
    auto& dir = forDirection(direction);
    dir.requests_total.inc();
    dir.bytes_total.inc(static_cast<int64_t>(bytes));
    dir.size_bytes.observe(static_cast<int64_t>(bytes));
    dir.latency_us.observe(static_cast<int64_t>(latency_us));
}

void TransferEngineMetrics::recordFailed(Direction direction) {
    auto& dir = forDirection(direction);
    dir.requests_total.inc();
    dir.failures_total.inc();
}

void TransferEngineMetrics::recordTimeout(Direction direction) {
    auto& dir = forDirection(direction);
    dir.requests_total.inc();
    dir.timeouts_total.inc();
}

std::string TransferEngineMetrics::buildSummary() {
    auto formatBytes = [](double bytes) -> std::string {
        std::ostringstream s;
        s << std::fixed << std::setprecision(2);
        if (bytes >= 1e12)
            s << bytes / 1e12 << " TB";
        else if (bytes >= 1e9)
            s << bytes / 1e9 << " GB";
        else if (bytes >= 1e6)
            s << bytes / 1e6 << " MB";
        else if (bytes >= 1e3)
            s << bytes / 1e3 << " KB";
        else
            s << bytes << " B";
        return s.str();
    };

    std::ostringstream oss;
    auto append = [&](const char* label, DirectionMetrics& dir) {
        oss << label << ": " << formatBytes(dir.bytes_total.value()) << " ("
            << static_cast<uint64_t>(dir.requests_total.value()) << " reqs, "
            << static_cast<uint64_t>(dir.failures_total.value()) << " fails, "
            << static_cast<uint64_t>(dir.timeouts_total.value())
            << " timeouts)";
    };
    append("Read", read_);
    oss << " | ";
    append("Write", write_);
    return oss.str();
}

}  // namespace mooncake
