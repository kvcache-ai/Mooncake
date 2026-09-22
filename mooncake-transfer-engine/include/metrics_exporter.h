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

#ifndef MOONCAKE_METRICS_EXPORTER_H
#define MOONCAKE_METRICS_EXPORTER_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <ylt/metric/counter.hpp>
#include <ylt/metric/histogram.hpp>

namespace cinatra {
class coro_http_server;
}  // namespace cinatra

namespace mooncake {
namespace metrics {

// ylt's histogram_t exposes neither its bucket boundaries nor its sum, and
// /metrics/json needs both, so track them alongside.
struct Histogram {
    Histogram(std::string name, std::string help, std::vector<double> buckets)
        : boundaries(buckets),
          histogram(std::move(name), std::move(help), std::move(buckets)) {}

    void observe(int64_t value) {
        histogram.observe(value);
        sum.fetch_add(value, std::memory_order_relaxed);
    }

    int64_t count();

    const std::vector<double> boundaries;
    ylt::metric::histogram_t histogram;
    std::atomic<int64_t> sum{0};
};

struct ExporterConfig {
    std::string http_host = "0.0.0.0";
    uint16_t http_port = 0;  // 0: no HTTP server
    uint16_t http_server_threads = 1;

    // Reads <prefix>_HTTP_HOST, _HTTP_PORT and _HTTP_THREADS. Malformed values
    // keep the default.
    static ExporterConfig fromEnv(const std::string& prefix);
};

// Metric registry plus the /metrics, /metrics/summary, /metrics/json and
// /health endpoints TENT serves. Register metrics before start(); the
// registered pointers must outlive the exporter.
class MetricsExporter {
   public:
    explicit MetricsExporter(std::string name);
    ~MetricsExporter();

    MetricsExporter(const MetricsExporter&) = delete;
    MetricsExporter& operator=(const MetricsExporter&) = delete;

    void addCounter(ylt::metric::counter_t* counter);
    void addHistogram(Histogram* histogram);
    void setSummaryProvider(std::function<std::string()> provider);

    void start(const ExporterConfig& config);

    // Must not overlap start() or another stop() call.
    void stop();

    // Port the HTTP server is listening on, 0 when there is none.
    uint16_t httpPort() const {
        return bound_http_port_.load(std::memory_order_relaxed);
    }

    std::string prometheusText();
    std::string jsonText();
    std::string summaryText();

   private:
    void startHttpServer(const ExporterConfig& config);

    const std::string name_;
    std::atomic<bool> started_{false};
    std::atomic<uint16_t> bound_http_port_{0};

    std::vector<ylt::metric::counter_t*> counters_;
    std::vector<Histogram*> histograms_;
    std::function<std::string()> summary_provider_;

    std::unique_ptr<cinatra::coro_http_server> http_server_;
};

}  // namespace metrics
}  // namespace mooncake

#endif  // MOONCAKE_METRICS_EXPORTER_H
