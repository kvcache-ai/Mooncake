#pragma once

#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "trace_config.h"
#include "trace_record.h"

namespace mooncake::tracing {

class TraceExporter {
   public:
    virtual ~TraceExporter() = default;
    virtual void Export(const TraceRecord& record) = 0;
};

struct TraceExporterStats {
    size_t queued_records{0};
    size_t queued_bytes{0};
    size_t dropped_records{0};
    size_t queue_full_count{0};
    size_t retry_count{0};
    size_t fallback_count{0};
    size_t collector_unreachable_count{0};
    size_t spooled_records{0};
    size_t spool_failures{0};
};

class TraceSampler {
   public:
    explicit TraceSampler(TraceConfig config);
    bool ShouldSample(const TraceRecord& record) const;

   private:
    TraceConfig config_;
};

class InMemoryTraceExporter : public TraceExporter {
   public:
    void Export(const TraceRecord& record) override;
    std::vector<TraceRecord> Snapshot() const;

   private:
    mutable std::mutex mutex_;
    std::vector<TraceRecord> records_;
};

class JsonlTraceExporter : public TraceExporter {
   public:
    explicit JsonlTraceExporter(std::string file_path);
    void Export(const TraceRecord& record) override;

   private:
    std::string file_path_;
    std::mutex mutex_;
};

class OtlpHttpTraceExporter : public TraceExporter {
   public:
    OtlpHttpTraceExporter(std::string endpoint, std::string path,
                          std::string headers, int timeout_ms);
    void Export(const TraceRecord& record) override;
    bool TryExport(const TraceRecord& record,
                   std::string* error_message = nullptr);

   private:
    std::string endpoint_;
    std::string path_;
    std::string headers_;
    int timeout_ms_{3000};
    std::mutex mutex_;
};

class AsyncRemoteTraceExporter : public TraceExporter {
   public:
    using RemoteSendFn =
        std::function<bool(const TraceRecord&, std::string* error_message)>;

    AsyncRemoteTraceExporter(TraceConfig config,
                             std::shared_ptr<TraceExporter> fallback_exporter,
                             RemoteSendFn send_fn = {});
    ~AsyncRemoteTraceExporter() override;

    void Export(const TraceRecord& record) override;
    bool FlushForTest(std::chrono::milliseconds timeout);
    TraceExporterStats SnapshotStats() const;

   private:
    size_t EstimateRecordBytes(const TraceRecord& record) const;
    bool TrySendRemote(const TraceRecord& record, std::string* error_message);
    bool TrySpool(const TraceRecord& record, std::string* error_message);
    void WorkerMain();

    TraceConfig config_;
    std::shared_ptr<TraceExporter> fallback_exporter_;
    std::vector<RemoteSendFn> remote_send_fns_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable drained_cv_;
    std::deque<TraceRecord> queue_;
    size_t queued_bytes_{0};
    bool stop_{false};
    bool worker_busy_{false};
    TraceExporterStats stats_;
    std::thread worker_;
};

std::shared_ptr<TraceExporter> MakeDefaultExporter(
    const std::string& service_name);

}  // namespace mooncake::tracing
