#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "trace_record.h"

namespace mooncake::tracing {

class TraceExporter {
   public:
    virtual ~TraceExporter() = default;
    virtual void Export(const TraceRecord& record) = 0;
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

   private:
    std::string endpoint_;
    std::string path_;
    std::string headers_;
    int timeout_ms_{3000};
    std::mutex mutex_;
};

std::shared_ptr<TraceExporter> MakeDefaultExporter(
    const std::string& service_name);

}  // namespace mooncake::tracing
