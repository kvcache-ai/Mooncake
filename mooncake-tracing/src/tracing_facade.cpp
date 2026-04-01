#include "tracing_facade.h"

#include <chrono>
#include <functional>
#include <utility>

namespace mooncake::tracing {
namespace {
int64_t NowUnixNano() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

double DeterministicSampleValue(const TraceRecord& record) {
    std::hash<std::string> hasher;
    const auto seed = !record.trace_id.empty() ? record.trace_id : record.span_id;
    const auto value = hasher(seed);
    return static_cast<double>(value % 1000000ULL) / 1000000.0;
}
}  // namespace

Span::Span(TracingFacade* facade, TraceRecord record)
    : facade_(facade), record_(std::make_unique<TraceRecord>(std::move(record))) {}
Span::Span(Span&& other) noexcept { *this = std::move(other); }
Span& Span::operator=(Span&& other) noexcept {
    if (this != &other) {
        End();
        facade_ = other.facade_;
        record_ = std::move(other.record_);
        ended_ = other.ended_;
        other.facade_ = nullptr;
        other.ended_ = true;
    }
    return *this;
}
Span::~Span() { End(); }
void Span::SetAttribute(const std::string& key, const std::string& value) {
    if (record_) record_->attrs.emplace_back(key, value);
}
void Span::AddEvent(const std::string& name, const TraceAttrs& attrs) {
    if (record_) record_->events.push_back(TraceEvent{name, NowUnixNano(), attrs});
}
void Span::SetStatus(const std::string& status) {
    if (record_) record_->status = status;
}
TraceContext Span::context() const {
    if (!record_) return {};
    return TraceContext{record_->trace_id, record_->span_id,
                        record_->parent_span_id, record_->correlation_id,
                        false};
}

TraceSampler::TraceSampler(TraceConfig config) : config_(std::move(config)) {}

bool TraceSampler::ShouldSample(const TraceRecord& record) const {
    if (config_.sampling_mode == "off") {
        return false;
    }
    if (config_.sampling_mode == "debug") {
        return true;
    }
    if (config_.sampling_mode == "diag" && !record.events.empty()) {
        return true;
    }
    if (record.status == "ERROR") {
        return true;
    }
    if (config_.sampling_slow_threshold_ms > 0 &&
        record.end_time_unix_nano > record.start_time_unix_nano) {
        const auto duration_ms =
            (record.end_time_unix_nano - record.start_time_unix_nano) / 1000000;
        if (duration_ms >= config_.sampling_slow_threshold_ms) {
            return true;
        }
    }
    return DeterministicSampleValue(record) < config_.sampling_base_ratio;
}

void Span::End() {
    if (!facade_ || !record_ || ended_) return;
    record_->end_time_unix_nano = NowUnixNano();
    facade_->Export(std::move(*record_));
    record_.reset();
    ended_ = true;
}

TracingFacade::TracingFacade(TraceConfig config) : config_(std::move(config)) {
    if (config_.enabled && config_.exporter_mode != "off") {
        if (config_.exporter_mode == "inmemory") {
            exporter_ = std::make_shared<InMemoryTraceExporter>();
        } else if (config_.exporter_mode == "remote") {
            exporter_ = std::make_shared<AsyncRemoteTraceExporter>(
                config_, std::make_shared<JsonlTraceExporter>(config_.jsonl_path));
        } else if (config_.exporter_mode == "otlp_http" ||
                   config_.exporter_mode == "otlp") {
            exporter_ = std::make_shared<AsyncRemoteTraceExporter>(
                config_, std::make_shared<JsonlTraceExporter>(config_.jsonl_path));
        } else {
            exporter_ = std::make_shared<JsonlTraceExporter>(config_.jsonl_path);
        }
    }
    sampler_ = std::make_unique<TraceSampler>(config_);
}
Span TracingFacade::StartSpan(const std::string& span_name,
                              const TraceContext* parent,
                              const TraceAttrs& attrs) {
    if (!config_.enabled || !exporter_) return Span();
    const auto root = RootContext();
    TraceContext ctx;
    if (!parent) {
        ctx = root;
    } else {
        ctx.trace_id = parent->trace_id.empty() ? root.trace_id : parent->trace_id;
        ctx.parent_span_id = parent->span_id;
        ctx.span_id = root.span_id;
        ctx.correlation_id =
            parent->correlation_id.empty() ? root.correlation_id
                                           : parent->correlation_id;
        ctx.context_missing = parent->context_missing;
    }
    TraceRecord rec;
    rec.trace_id = ctx.trace_id;
    rec.span_id = ctx.span_id;
    rec.parent_span_id = ctx.parent_span_id;
    rec.correlation_id = ctx.correlation_id;
    rec.service_name = config_.service_name;
    rec.node_id = config_.node_id;
    rec.process_role = config_.process_role;
    rec.span_name = span_name;
    rec.start_time_unix_nano = NowUnixNano();
    rec.attrs = attrs;
    if (ctx.context_missing) rec.attrs.emplace_back("context.missing", "true");
    return Span(this, std::move(rec));
}
Span TracingFacade::StartSpanFromCarrier(const std::string& span_name,
                                         const TraceCarrier& carrier,
                                         const TraceAttrs& attrs) {
    if (carrier.empty()) {
        auto ctx = ChildContextFromCarrier(carrier);
        return StartSpan(span_name, &ctx, attrs);
    }
    TraceContext parent;
    parent.trace_id = carrier.trace_id;
    parent.span_id = carrier.span_id;
    parent.correlation_id = carrier.correlation_id;
    return StartSpan(span_name, &parent, attrs);
}
void TracingFacade::Export(TraceRecord&& record) {
    if (!exporter_) return;
    if (sampler_ && !sampler_->ShouldSample(record)) {
        return;
    }
    exporter_->Export(record);
}
TracingFacade& TracingFacade::Instance(const std::string& service_name,
                                       const std::string& process_role) {
    static std::mutex mutex;
    static std::vector<std::unique_ptr<TracingFacade>> facades;
    std::lock_guard<std::mutex> lock(mutex);
    for (auto& facade : facades) {
        if (facade->config().service_name == service_name &&
            facade->config().process_role == process_role)
            return *facade;
    }
    facades.push_back(std::make_unique<TracingFacade>(
        LoadTraceConfig(service_name, process_role)));
    return *facades.back();
}

}  // namespace mooncake::tracing
