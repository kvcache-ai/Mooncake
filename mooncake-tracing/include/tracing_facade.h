#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "trace_config.h"
#include "trace_context.h"
#include "trace_exporter.h"
#include "trace_record.h"

namespace mooncake::tracing {

class Span {
   public:
    Span() = default;
    Span(class TracingFacade* facade, TraceRecord record);
    Span(Span&& other) noexcept;
    Span& operator=(Span&& other) noexcept;
    Span(const Span&) = delete;
    Span& operator=(const Span&) = delete;
    ~Span();

    void SetAttribute(const std::string& key, const std::string& value);
    void AddEvent(const std::string& name, const TraceAttrs& attrs = {});
    void SetStatus(const std::string& status);
    TraceContext context() const;
    bool valid() const { return facade_ != nullptr; }
    void End();

   private:
    class TracingFacade* facade_{nullptr};
    std::unique_ptr<TraceRecord> record_;
    bool ended_{false};
};

class TracingFacade {
   public:
    explicit TracingFacade(TraceConfig config = {});

    bool enabled() const { return config_.enabled; }
    const TraceConfig& config() const { return config_; }

    Span StartSpan(const std::string& span_name,
                   const TraceContext* parent = nullptr,
                   const TraceAttrs& attrs = {});

    Span StartSpanFromCarrier(const std::string& span_name,
                              const TraceCarrier& carrier,
                              const TraceAttrs& attrs = {});

    void Export(TraceRecord&& record);

    static TracingFacade& Instance(const std::string& service_name,
                                   const std::string& process_role = "");

   private:
    TraceConfig config_;
    std::shared_ptr<TraceExporter> exporter_;
    std::unique_ptr<TraceSampler> sampler_;
};

}  // namespace mooncake::tracing
