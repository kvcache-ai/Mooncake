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

#include "tent/metrics/tent_metrics.h"

#include <glog/logging.h>
#include <tent/thirdparty/nlohmann/json.h>

#include <array>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <unordered_set>

#ifndef TENT_BUILD_VERSION
#define TENT_BUILD_VERSION "unknown"
#endif

#ifndef TENT_GIT_COMMIT
#define TENT_GIT_COMMIT "unknown"
#endif

namespace mooncake::tent {

TentMetrics& TentMetrics::instance() {
    static TentMetrics instance;
    return instance;
}

TentMetrics::~TentMetrics() { shutdown(); }

void TentMetrics::setDiagnosticsProvider(const void* owner,
                                         DiagnosticsProvider provider) {
    if (owner == nullptr || !provider) return;
    std::lock_guard<std::mutex> lock(diagnostics_provider_mutex_);
    diagnostics_provider_owner_ = owner;
    diagnostics_provider_ = std::move(provider);
}

void TentMetrics::clearDiagnosticsProvider(const void* owner) {
    std::lock_guard<std::mutex> lock(diagnostics_provider_mutex_);
    if (owner != diagnostics_provider_owner_) return;
    diagnostics_provider_ = nullptr;
    diagnostics_provider_owner_ = nullptr;
}

bool TentMetrics::getDiagnosticsSnapshot(DiagnosticsSnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(diagnostics_provider_mutex_);
    if (!diagnostics_provider_) return false;
    try {
        snapshot = diagnostics_provider_();
        return true;
    } catch (const std::exception& error) {
        LOG(ERROR) << "TENT diagnostics provider failed: " << error.what();
    } catch (...) {
        LOG(ERROR) << "TENT diagnostics provider failed with unknown error";
    }
    return false;
}

#if TENT_METRICS_ENABLED

namespace {
constexpr std::array<std::string_view, 2> kSerializationFormatNames{
    "prometheus", "json"};

uint64_t unixTimestampSeconds() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

std::string diagnosticsJson(const EngineDiagnosticsSnapshot& snapshot,
                            bool provider_available) {
    const bool ready =
        provider_available && snapshot.readiness_reasons.empty();
    nlohmann::json body = {
        {"schema_version", 1},
        {"build",
         {{"version", TENT_BUILD_VERSION}, {"git_commit", TENT_GIT_COMMIT}}},
        {"ready", ready},
        {"provider_available", provider_available},
        {"readiness_reasons", snapshot.readiness_reasons},
        {"engine",
         {{"available", snapshot.engine_available},
          {"control_plane_available", snapshot.control_plane_available},
          {"loaded_transport_count", snapshot.loaded_transport_count},
          {"loaded_transports", snapshot.loaded_transports}}},
        {"progress_worker",
         {{"enabled", snapshot.progress_worker_enabled},
          {"running", snapshot.progress_worker_running},
          {"last_progress_age_seconds",
           snapshot.worker_progress_age_seconds},
          {"stall_threshold_seconds",
           snapshot.worker_stall_threshold_seconds}}},
        {"runtime_queue",
         {{"enabled", snapshot.runtime_queue_enabled},
          {"outstanding_owners", snapshot.outstanding_owners},
          {"outstanding_bytes", snapshot.outstanding_bytes},
          {"dispatch_inflight_owners",
           snapshot.dispatch_inflight_owners},
          {"dispatch_inflight_bytes", snapshot.dispatch_inflight_bytes}}},
        {"work",
         {{"alive_batches", snapshot.alive_batches},
          {"oldest_inflight_age_us", snapshot.oldest_inflight_age_us}}},
        {"redaction",
         {{"peer_identifiers_omitted", true},
          {"memory_addresses_omitted", true},
          {"request_identifiers_omitted", true}}}};
    return body.dump();
}

std::string escapePrometheusLabelValue(std::string_view value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char c : value) {
        switch (c) {
            case '\\':
                escaped.append("\\\\");
                break;
            case '"':
                escaped.append("\\\"");
                break;
            case '\n':
                escaped.append("\\n");
                break;
            default:
                escaped.push_back(c);
                break;
        }
    }
    return escaped;
}

const char* operationName(Request::OpCode operation) {
    return operation == Request::READ ? "read" : "write";
}

size_t outcomeIndex(TransferStatusEnum status) {
    switch (status) {
        case COMPLETED:
            return 0;
        case FAILED:
            return 1;
        case TIMEOUT:
            return 2;
        case CANCELED:
            return 3;
        default:
            return 1;
    }
}

const char* outcomeName(TransferStatusEnum status) {
    switch (outcomeIndex(status)) {
        case 0:
            return "success";
        case 1:
            return "failed";
        case 2:
            return "timeout";
        case 3:
            return "canceled";
    }
    return "failed";
}

const char* taskFailureReasonName(TentMetrics::TaskFailureReason reason) {
    switch (reason) {
        case TentMetrics::TaskFailureReason::Submit:
            return "submit";
        case TentMetrics::TaskFailureReason::Poll:
            return "poll";
        case TentMetrics::TaskFailureReason::Timeout:
            return "timeout";
        case TentMetrics::TaskFailureReason::Canceled:
            return "canceled";
    }
    return "unknown";
}

const char* failurePhaseName(TentMetrics::FailurePhase phase) {
    switch (phase) {
        case TentMetrics::FailurePhase::Metadata:
            return "metadata";
        case TentMetrics::FailurePhase::Admission:
            return "admission";
        case TentMetrics::FailurePhase::Selection:
            return "selection";
        case TentMetrics::FailurePhase::Connect:
            return "connect";
        case TentMetrics::FailurePhase::Register:
            return "register";
        case TentMetrics::FailurePhase::Submit:
            return "submit";
        case TentMetrics::FailurePhase::Queue:
            return "queue";
        case TentMetrics::FailurePhase::Post:
            return "post";
        case TentMetrics::FailurePhase::Poll:
            return "poll";
        case TentMetrics::FailurePhase::Completion:
            return "completion";
        case TentMetrics::FailurePhase::Retry:
            return "retry";
        case TentMetrics::FailurePhase::Failover:
            return "failover";
        case TentMetrics::FailurePhase::Notify:
            return "notify";
        case TentMetrics::FailurePhase::Cleanup:
            return "cleanup";
        case TentMetrics::FailurePhase::Shutdown:
            return "shutdown";
    }
    return "internal";
}

const char* failureReasonName(TentMetrics::FailureReason reason) {
    switch (reason) {
        case TentMetrics::FailureReason::Timeout:
            return "timeout";
        case TentMetrics::FailureReason::Unavailable:
            return "unavailable";
        case TentMetrics::FailureReason::QueueFull:
            return "queue_full";
        case TentMetrics::FailureReason::ResourceExhausted:
            return "resource_exhausted";
        case TentMetrics::FailureReason::Protocol:
            return "protocol";
        case TentMetrics::FailureReason::StaleGeneration:
            return "stale_generation";
        case TentMetrics::FailureReason::WcError:
            return "wc_error";
        case TentMetrics::FailureReason::CqError:
            return "cq_error";
        case TentMetrics::FailureReason::MrError:
            return "mr_error";
        case TentMetrics::FailureReason::PeerClosed:
            return "peer_closed";
        case TentMetrics::FailureReason::Canceled:
            return "canceled";
        case TentMetrics::FailureReason::Internal:
            return "internal";
    }
    return "internal";
}

const char* memoryOperationName(TentMetrics::MemoryOperation operation) {
    switch (operation) {
        case TentMetrics::MemoryOperation::Register:
            return "register";
        case TentMetrics::MemoryOperation::Unregister:
            return "unregister";
    }
    return "unknown";
}

const char* operationOutcomeName(TentMetrics::OperationOutcome outcome) {
    switch (outcome) {
        case TentMetrics::OperationOutcome::Success:
            return "success";
        case TentMetrics::OperationOutcome::Failure:
            return "failure";
    }
    return "unknown";
}

const char* workerKindName(TentMetrics::WorkerKind worker) {
    switch (worker) {
        case TentMetrics::WorkerKind::EngineProgress:
            return "engine_progress";
        case TentMetrics::WorkerKind::Staging:
            return "staging";
        case TentMetrics::WorkerKind::Rdma:
            return "rdma";
    }
    return "unknown";
}

const char* stagingCleanupKindName(TentMetrics::StagingCleanupKind kind) {
    switch (kind) {
        case TentMetrics::StagingCleanupKind::Group:
            return "group";
        case TentMetrics::StagingCleanupKind::Batch:
            return "batch";
        case TentMetrics::StagingCleanupKind::RemoteOperation:
            return "remote_operation";
    }
    return "unknown";
}

const char* rpcMethodName(TentMetrics::RpcMethod method) {
    switch (method) {
        case TentMetrics::RpcMethod::GetSegmentDesc:
            return "get_segment_desc";
        case TentMetrics::RpcMethod::BootstrapRdma:
            return "bootstrap_rdma";
        case TentMetrics::RpcMethod::SendData:
            return "send_data";
        case TentMetrics::RpcMethod::RecvData:
            return "recv_data";
        case TentMetrics::RpcMethod::Notify:
            return "notify";
        case TentMetrics::RpcMethod::Probe:
            return "probe";
        case TentMetrics::RpcMethod::Delegate:
            return "delegate";
        case TentMetrics::RpcMethod::Pin:
            return "pin";
        case TentMetrics::RpcMethod::Unpin:
            return "unpin";
        case TentMetrics::RpcMethod::SubscribeSegmentUpdate:
            return "subscribe_segment_update";
        case TentMetrics::RpcMethod::NotifySegmentUpdated:
            return "notify_segment_updated";
        case TentMetrics::RpcMethod::BootstrapUb:
            return "bootstrap_ub";
        case TentMetrics::RpcMethod::Unknown:
            return "unknown";
    }
    return "unknown";
}

const char* rpcOutcomeName(TentMetrics::RpcOutcome outcome) {
    switch (outcome) {
        case TentMetrics::RpcOutcome::Success:
            return "success";
        case TentMetrics::RpcOutcome::ConnectionError:
            return "connection_error";
        case TentMetrics::RpcOutcome::RpcError:
            return "rpc_error";
    }
    return "unknown";
}

const char* rdmaSliceEventName(TentMetrics::RdmaSliceEvent event) {
    switch (event) {
        case TentMetrics::RdmaSliceEvent::Enqueued:
            return "enqueued";
        case TentMetrics::RdmaSliceEvent::PostAttempt:
            return "post_attempt";
        case TentMetrics::RdmaSliceEvent::PathError:
            return "path_error";
        case TentMetrics::RdmaSliceEvent::PostError:
            return "post_error";
        case TentMetrics::RdmaSliceEvent::CompletionSuccess:
            return "completion_success";
        case TentMetrics::RdmaSliceEvent::CompletionError:
            return "completion_error";
        case TentMetrics::RdmaSliceEvent::CompletionFlush:
            return "completion_flush";
        case TentMetrics::RdmaSliceEvent::LateCompletion:
            return "late_completion";
        case TentMetrics::RdmaSliceEvent::SoftwareTimeout:
            return "software_timeout";
    }
    return "unknown";
}

const char* rdmaRetryCauseName(TentMetrics::RdmaRetryCause cause) {
    switch (cause) {
        case TentMetrics::RdmaRetryCause::Endpoint:
            return "endpoint";
        case TentMetrics::RdmaRetryCause::Post:
            return "post";
        case TentMetrics::RdmaRetryCause::Completion:
            return "completion";
    }
    return "unknown";
}

const char* rdmaCompletionOutcomeName(
    TentMetrics::RdmaCompletionOutcome outcome) {
    switch (outcome) {
        case TentMetrics::RdmaCompletionOutcome::Success:
            return "success";
        case TentMetrics::RdmaCompletionOutcome::WcError:
            return "wc_error";
        case TentMetrics::RdmaCompletionOutcome::Flush:
            return "flush";
        case TentMetrics::RdmaCompletionOutcome::Late:
            return "late";
    }
    return "unknown";
}

const char* rdmaEndpointEventName(TentMetrics::RdmaEndpointEvent event) {
    switch (event) {
        case TentMetrics::RdmaEndpointEvent::Constructed:
            return "constructed";
        case TentMetrics::RdmaEndpointEvent::ConstructFailure:
            return "construct_failure";
        case TentMetrics::RdmaEndpointEvent::ReadyActive:
            return "ready_active";
        case TentMetrics::RdmaEndpointEvent::ReadyPassive:
            return "ready_passive";
        case TentMetrics::RdmaEndpointEvent::Retiring:
            return "retiring";
        case TentMetrics::RdmaEndpointEvent::Destroyed:
            return "destroyed";
        case TentMetrics::RdmaEndpointEvent::DestroyError:
            return "destroy_error";
    }
    return "unknown";
}

const char* rdmaQpTypeName(TentMetrics::RdmaQpType qp_type) {
    switch (qp_type) {
        case TentMetrics::RdmaQpType::Data:
            return "data";
        case TentMetrics::RdmaQpType::Notification:
            return "notification";
    }
    return "unknown";
}

const char* rdmaQpTransitionName(TentMetrics::RdmaQpTransition transition) {
    switch (transition) {
        case TentMetrics::RdmaQpTransition::Create:
            return "create";
        case TentMetrics::RdmaQpTransition::Reset:
            return "reset";
        case TentMetrics::RdmaQpTransition::Init:
            return "init";
        case TentMetrics::RdmaQpTransition::Rtr:
            return "rtr";
        case TentMetrics::RdmaQpTransition::Rts:
            return "rts";
        case TentMetrics::RdmaQpTransition::Error:
            return "error";
        case TentMetrics::RdmaQpTransition::Destroy:
            return "destroy";
    }
    return "unknown";
}

const char* rdmaNotificationEventName(
    TentMetrics::RdmaNotificationEvent event) {
    switch (event) {
        case TentMetrics::RdmaNotificationEvent::RecvPosted:
            return "recv_posted";
        case TentMetrics::RdmaNotificationEvent::RecvPostError:
            return "recv_post_error";
        case TentMetrics::RdmaNotificationEvent::RecvDelivered:
            return "recv_delivered";
        case TentMetrics::RdmaNotificationEvent::RecvInvalid:
            return "recv_invalid";
        case TentMetrics::RdmaNotificationEvent::RecvDropped:
            return "recv_dropped";
        case TentMetrics::RdmaNotificationEvent::SendPosted:
            return "send_posted";
        case TentMetrics::RdmaNotificationEvent::SendCompleted:
            return "send_completed";
        case TentMetrics::RdmaNotificationEvent::SendUnavailable:
            return "send_unavailable";
        case TentMetrics::RdmaNotificationEvent::SendEncodeError:
            return "send_encode_error";
        case TentMetrics::RdmaNotificationEvent::SendPostError:
            return "send_post_error";
        case TentMetrics::RdmaNotificationEvent::CompletionError:
            return "completion_error";
        case TentMetrics::RdmaNotificationEvent::CqPollError:
            return "cq_poll_error";
        case TentMetrics::RdmaNotificationEvent::UnknownQp:
            return "unknown_qp";
        case TentMetrics::RdmaNotificationEvent::ChannelDisabled:
            return "channel_disabled";
    }
    return "unknown";
}

const char* streamEventName(TentMetrics::StreamEvent event) {
    switch (event) {
        case TentMetrics::StreamEvent::Queued:
            return "queued";
        case TentMetrics::StreamEvent::AttemptStarted:
            return "attempt_started";
        case TentMetrics::StreamEvent::AttemptSuccess:
            return "attempt_success";
        case TentMetrics::StreamEvent::ConnectStarted:
            return "connect_started";
        case TentMetrics::StreamEvent::ConnectSuccess:
            return "connect_success";
        case TentMetrics::StreamEvent::ConnectionError:
            return "connection_error";
        case TentMetrics::StreamEvent::IoError:
            return "io_error";
        case TentMetrics::StreamEvent::Timeout:
            return "timeout";
        case TentMetrics::StreamEvent::PeerClosed:
            return "peer_closed";
        case TentMetrics::StreamEvent::Canceled:
            return "canceled";
        case TentMetrics::StreamEvent::RemoteError:
            return "remote_error";
        case TentMetrics::StreamEvent::RetryIssued:
            return "retry_issued";
        case TentMetrics::StreamEvent::RetryExhausted:
            return "retry_exhausted";
    }
    return "unknown";
}

const char* streamOutcomeName(TentMetrics::StreamOutcome outcome) {
    switch (outcome) {
        case TentMetrics::StreamOutcome::Success:
            return "success";
        case TentMetrics::StreamOutcome::ConnectionError:
            return "connection_error";
        case TentMetrics::StreamOutcome::IoError:
            return "io_error";
        case TentMetrics::StreamOutcome::Timeout:
            return "timeout";
        case TentMetrics::StreamOutcome::PeerClosed:
            return "peer_closed";
        case TentMetrics::StreamOutcome::Canceled:
            return "canceled";
        case TentMetrics::StreamOutcome::RemoteError:
            return "remote_error";
    }
    return "unknown";
}

const char* shmMappingEventName(TentMetrics::ShmMappingEvent event) {
    switch (event) {
        case TentMetrics::ShmMappingEvent::CacheHit:
            return "cache_hit";
        case TentMetrics::ShmMappingEvent::CacheMiss:
            return "cache_miss";
        case TentMetrics::ShmMappingEvent::MetadataError:
            return "metadata_error";
        case TentMetrics::ShmMappingEvent::OpenError:
            return "open_error";
        case TentMetrics::ShmMappingEvent::StatError:
            return "stat_error";
        case TentMetrics::ShmMappingEvent::SizeMismatch:
            return "size_mismatch";
        case TentMetrics::ShmMappingEvent::MapError:
            return "map_error";
        case TentMetrics::ShmMappingEvent::Created:
            return "created";
    }
    return "unknown";
}

const char* shmCopyOutcomeName(TentMetrics::ShmCopyOutcome outcome) {
    switch (outcome) {
        case TentMetrics::ShmCopyOutcome::Success:
            return "success";
        case TentMetrics::ShmCopyOutcome::Error:
            return "error";
    }
    return "unknown";
}

const char* segmentEventName(TentMetrics::SegmentEvent event) {
    switch (event) {
        case TentMetrics::SegmentEvent::SnapshotPublished:
            return "snapshot_published";
        case TentMetrics::SegmentEvent::MutationError:
            return "mutation_error";
        case TentMetrics::SegmentEvent::RegistryPublishSuccess:
            return "registry_publish_success";
        case TentMetrics::SegmentEvent::RegistryPublishError:
            return "registry_publish_error";
        case TentMetrics::SegmentEvent::RegistryDeleteSuccess:
            return "registry_delete_success";
        case TentMetrics::SegmentEvent::RegistryDeleteError:
            return "registry_delete_error";
        case TentMetrics::SegmentEvent::CacheHit:
            return "cache_hit";
        case TentMetrics::SegmentEvent::CacheMiss:
            return "cache_miss";
        case TentMetrics::SegmentEvent::CacheReset:
            return "cache_reset";
        case TentMetrics::SegmentEvent::FetchSuccess:
            return "fetch_success";
        case TentMetrics::SegmentEvent::FetchError:
            return "fetch_error";
        case TentMetrics::SegmentEvent::EntryInvalidated:
            return "entry_invalidated";
        case TentMetrics::SegmentEvent::GenerationAdvanced:
            return "generation_advanced";
    }
    return "unknown";
}

const char* selectionEventName(TentMetrics::SelectionEvent event) {
    switch (event) {
        case TentMetrics::SelectionEvent::Selected:
            return "selected";
        case TentMetrics::SelectionEvent::FallbackSelected:
            return "fallback_selected";
        case TentMetrics::SelectionEvent::CandidateUnavailable:
            return "candidate_unavailable";
        case TentMetrics::SelectionEvent::PolicyMiss:
            return "policy_miss";
        case TentMetrics::SelectionEvent::CandidateEmpty:
            return "candidate_empty";
        case TentMetrics::SelectionEvent::HintRejected:
            return "hint_rejected";
        case TentMetrics::SelectionEvent::HintUnavailable:
            return "hint_unavailable";
        case TentMetrics::SelectionEvent::Exhausted:
            return "exhausted";
        case TentMetrics::SelectionEvent::MetadataRefresh:
            return "metadata_refresh";
    }
    return "unknown";
}

const char* admissionOwnerKindName(TentMetrics::AdmissionOwnerKind kind) {
    switch (kind) {
        case TentMetrics::AdmissionOwnerKind::User:
            return "user";
        case TentMetrics::AdmissionOwnerKind::Staging:
            return "staging";
        case TentMetrics::AdmissionOwnerKind::All:
            return "all";
    }
    return "all";
}

const char* admissionEventName(TentMetrics::AdmissionEvent event) {
    switch (event) {
        case TentMetrics::AdmissionEvent::Admitted:
            return "admitted";
        case TentMetrics::AdmissionEvent::RejectedBatchCapacity:
            return "rejected_batch_capacity";
        case TentMetrics::AdmissionEvent::RejectedOwnerCapacity:
            return "rejected_owner_capacity";
        case TentMetrics::AdmissionEvent::RejectedByteCapacity:
            return "rejected_byte_capacity";
        case TentMetrics::AdmissionEvent::RejectedUserOwnerCapacity:
            return "rejected_user_owner_capacity";
        case TentMetrics::AdmissionEvent::RejectedUserByteCapacity:
            return "rejected_user_byte_capacity";
        case TentMetrics::AdmissionEvent::RejectedDispatchWindow:
            return "rejected_dispatch_window";
        case TentMetrics::AdmissionEvent::Dispatched:
            return "dispatched";
        case TentMetrics::AdmissionEvent::DeadlineDropped:
            return "deadline_dropped";
        case TentMetrics::AdmissionEvent::Canceled:
            return "canceled";
    }
    return "unknown";
}

TentMetrics::StreamEvent streamOutcomeEvent(TentMetrics::StreamOutcome outcome) {
    switch (outcome) {
        case TentMetrics::StreamOutcome::Success:
            return TentMetrics::StreamEvent::AttemptSuccess;
        case TentMetrics::StreamOutcome::ConnectionError:
            return TentMetrics::StreamEvent::ConnectionError;
        case TentMetrics::StreamOutcome::IoError:
            return TentMetrics::StreamEvent::IoError;
        case TentMetrics::StreamOutcome::Timeout:
            return TentMetrics::StreamEvent::Timeout;
        case TentMetrics::StreamOutcome::PeerClosed:
            return TentMetrics::StreamEvent::PeerClosed;
        case TentMetrics::StreamOutcome::Canceled:
            return TentMetrics::StreamEvent::Canceled;
        case TentMetrics::StreamOutcome::RemoteError:
            return TentMetrics::StreamEvent::RemoteError;
    }
    return TentMetrics::StreamEvent::IoError;
}
}  // namespace

size_t TentMetrics::outcomeSlot(TransportType tp, Request::OpCode op,
                                TransferStatusEnum status) {
    return attemptSlot(tp, op) * kOutcomeValueDomain + outcomeIndex(status);
}

Status TentMetrics::initialize(const MetricsConfig& config) {
    // Validate configuration before touching initialized_. An invalid config
    // (e.g. port 0, zero HTTP threads) would otherwise cause confusing
    // failures inside initHttpServer(); fail fast with a clear error instead.
    // Validating before the compare_exchange avoids a window where
    // initialized_ is set to true and then rolled back on failure.
    std::string error_msg;
    if (!MetricsConfigLoader::validateConfig(config, &error_msg)) {
        LOG(ERROR) << "Invalid TENT metrics config: " << error_msg
                   << "; metrics disabled";
        return Status::InvalidArgument(
            "Invalid TENT metrics config: " + error_msg + LOC_MARK);
    }

    // Use compare_exchange to prevent race condition during initialization
    bool expected = false;
    if (!initialized_.compare_exchange_strong(expected, true)) {
        return Status::OK();  // Already initialized by another thread
    }

    config_ = config;

    // Set runtime enabled state from config
    runtime_enabled_.store(config_.enabled, std::memory_order_relaxed);

    // Register all metrics to vectors for unified serialization
    registerMetrics();

    // Initialize and start HTTP server on the configured port. If the port is
    // busy (e.g. another rank was given the same port), degrade to log-only
    // metrics rather than falsely reporting a listening endpoint.
    Status http_status = initHttpServer();
    const bool http_ok = http_status.ok();
    if (!http_ok) {
        http_start_failures_.fetch_add(1, std::memory_order_relaxed);
        LOG(WARNING) << "TENT metrics HTTP endpoint unavailable on "
                     << config_.http_host << ":" << config_.http_port << " ("
                     << http_status.ToString()
                     << "); continuing with log-only metrics";
    }

    // Start periodic metric reporting thread if interval > 0
    if (config_.report_interval_seconds > 0) {
        metric_report_running_ = true;
        metric_report_thread_ = std::thread([this]() {
            while (metric_report_running_) {
                std::string summary = getSummaryString();
                LOG(INFO) << "TENT Metrics: " << summary;

                // Use condition variable for interruptible sleep
                std::unique_lock<std::mutex> lock(metric_report_mutex_);
                metric_report_cv_.wait_for(
                    lock, std::chrono::seconds(config_.report_interval_seconds),
                    [this]() { return !metric_report_running_.load(); });
            }
        });
    }

    if (http_ok) {
        LOG(INFO) << "TENT metrics initialized successfully, HTTP server "
                     "listening on "
                  << config_.http_host << ":"
                  << bound_http_port_.load(std::memory_order_relaxed)
                  << ", runtime_enabled="
                  << (runtime_enabled_.load() ? "true" : "false");
    } else {
        LOG(INFO) << "TENT metrics initialized in log-only mode (HTTP endpoint "
                     "disabled), runtime_enabled="
                  << (runtime_enabled_.load() ? "true" : "false");
    }
    return Status::OK();
}

Status TentMetrics::initHttpServer() {
    using namespace coro_http;

    // Create HTTP server with configurable threads on the configured port.
    // Port assignment is intentionally deterministic: co-located ranks should
    // be given distinct ports explicitly (e.g. base_port + local_rank), not
    // auto-scanned, so a rank's metrics port stays predictable.
    http_server_ = std::make_unique<coro_http_server>(
        config_.http_server_threads, config_.http_port);

    registerHttpHandlers();

    // Start the HTTP server asynchronously. async_start() returns a future that
    // already holds a result ONLY when startup failed (e.g. the port is already
    // in use); on success the future stays pending while the server keeps
    // running. Same idiom as mooncake-store's rpc_service.cpp /
    // real_client.cpp.
    auto ec = http_server_->async_start();
    if (ec.hasResult()) {
        http_server_.reset();
        return Status::RpcServiceError(
            "Failed to start TENT metrics HTTP server" LOC_MARK);
    }

    // Record the bound port (read by httpPort() from other threads, so it must
    // be the atomic, not config_).
    bound_http_port_.store(config_.http_port, std::memory_order_relaxed);
    return Status::OK();
}

void TentMetrics::registerHttpHandlers() {
    using namespace coro_http;

    // Register /metrics endpoint for Prometheus
    http_server_->set_http_handler<GET>(
        "/metrics", [this](coro_http_request& req, coro_http_response& resp) {
            std::string metrics = getPrometheusMetrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(metrics));
        });

    // OpenMetrics is opt-in so the existing Prometheus 0.0.4 response remains
    // byte-compatible. Retained transport-attempt traces may appear here as
    // exemplars on the matching histogram's +Inf bucket.
    http_server_->set_http_handler<GET>(
        "/metrics/openmetrics",
        [this](coro_http_request& req, coro_http_response& resp) {
            std::string metrics = getOpenMetrics();
            resp.add_header(
                "Content-Type",
                "application/openmetrics-text; version=1.0.0; charset=utf-8");
            resp.set_status_and_content(status_type::ok, std::move(metrics));
        });

    // Register /metrics/summary endpoint for human-readable summary
    http_server_->set_http_handler<GET>(
        "/metrics/summary",
        [this](coro_http_request& req, coro_http_response& resp) {
            std::string summary = getSummaryString();
            resp.add_header("Content-Type", "text/plain");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    // Register /metrics/json endpoint for JSON format
    http_server_->set_http_handler<GET>(
        "/metrics/json",
        [this](coro_http_request& req, coro_http_response& resp) {
            std::string json = getJsonMetrics();
            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, std::move(json));
        });

    // Register /health endpoint for health check
    http_server_->set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain");
            resp.set_status_and_content(status_type::ok, "OK");
        });

    // Process liveness is deliberately independent from engine readiness.
    http_server_->set_http_handler<GET>(
        "/live", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(
                status_type::ok,
                R"({"schema_version":1,"status":"live"})");
        });

    auto diagnostics = [this](coro_http_response& resp, bool readiness) {
        DiagnosticsSnapshot snapshot;
        const bool provider_available = getDiagnosticsSnapshot(snapshot);
        if (!provider_available) {
            snapshot.readiness_reasons = {"engine_diagnostics_unavailable"};
        }
        const bool ready =
            provider_available && snapshot.readiness_reasons.empty();
        resp.add_header("Content-Type", "application/json");
        resp.set_status_and_content(
            readiness && !ready ? status_type::service_unavailable
                                : status_type::ok,
            diagnosticsJson(snapshot, provider_available));
    };

    http_server_->set_http_handler<GET>(
        "/ready", [diagnostics](coro_http_request& req,
                                 coro_http_response& resp) {
            diagnostics(resp, true);
        });

    http_server_->set_http_handler<GET>(
        "/diagnostics", [diagnostics](coro_http_request& req,
                                       coro_http_response& resp) {
            diagnostics(resp, false);
        });
}

void TentMetrics::shutdown() {
    if (!initialized_) return;

    // Stop metric reporting thread
    metric_report_running_ = false;
    metric_report_cv_.notify_all();  // Wake up the sleeping thread immediately
    if (metric_report_thread_.joinable()) {
        metric_report_thread_.join();
    }

    // Stop HTTP server
    if (http_server_) {
        http_server_->stop();
        http_server_.reset();
    }

    // Clear metric vectors
    counters_.clear();
    histograms_.clear();
    gauges_.clear();

    // Reset bound port so httpPort() returns 0 after shutdown, not a stale
    // port from a previous initialization. Without this, a re-initialize
    // that fails to bind would cause httpPort() to report the old port.
    bound_http_port_.store(0, std::memory_order_relaxed);

    initialized_ = false;
    LOG(INFO) << "TENT metrics shutdown complete";
}

void TentMetrics::registerMetrics() {
    // Register all counters as base metric_t* pointers so that counters with
    // different label arities (N=1 per-transport, N=2 failover from→to and
    // transport-attempt operation labels) share one vector for Prometheus
    // serialize(). CachedDynamicCounter is-a basic_dynamic_counter, so its
    // ylt serialize() output is unchanged.
    counters_ = {
        &read_bytes_total_,
        &write_bytes_total_,
        &read_requests_total_,
        &write_requests_total_,
        &read_failures_total_,
        &write_failures_total_,
        &failover_total_,
        &transport_attempts_total_,
        &transport_attempt_failures_total_,
        &logical_requests_started_total_,
        &logical_request_outcomes_total_,
        &transport_attempt_outcomes_total_,
        &task_failures_total_,
        &terminal_failures_total_,
        &memory_operations_total_,
        &memory_operation_bytes_total_,
        &deadline_infeasible_total_,
        &quarantined_batches_total_,
        &staging_cleanup_timeouts_total_,
        &rpc_attempts_total_,
        &rpc_retries_total_,
        &rdma_slice_events_total_,
        &rdma_retries_total_,
        &rdma_retry_exhausted_total_,
        &rdma_cq_poll_errors_total_,
        &rdma_endpoint_events_total_,
        &rdma_qp_transition_failures_total_,
        &rdma_notification_events_total_,
        &stream_events_total_,
        &shm_mapping_events_total_,
        &shm_copy_events_total_,
        &segment_events_total_,
        &selection_events_total_,
        &admission_events_total_,
    };

    gauges_ = {
        &inflight_attempts_,
        &logical_requests_inflight_,
        &registered_buffer_bytes_,
        &worker_last_progress_timestamp_seconds_,
        &staging_inflight_tasks_,
        &staging_inflight_bytes_,
        &staging_cleanup_pending_,
        &rdma_worker_inflight_slices_,
        &rdma_posted_inflight_slices_,
        &admission_queue_owners_,
        &admission_queue_bytes_,
    };

    // Register the N=1 per-transport histograms for unified serialization.
    // The N=2 transport_attempt_latency_ histogram is serialized separately
    // (it can't share this N=1-typed vector); see getPrometheusMetrics /
    // getJsonMetrics.
    histograms_ = {
        &read_latency_, &write_latency_,    &read_size_,      &write_size_,
        &deadline_mlu_, &stage_queue_wait_, &stage_dispatch_, &stage_transport_,
        &admission_wait_latency_,
    };
}

void TentMetrics::recordReadCompleted(TransportType tp, size_t bytes,
                                      double latency_seconds) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    const size_t slot = transportSlot(tp);
    // The label is only constructed on the first use of this slot (cache
    // miss); the steady state is a relaxed atomic add.
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    auto bytes_val = static_cast<int64_t>(bytes);
    read_bytes_total_.incCached(slot, label, bytes_val);
    read_requests_total_.incCached(slot, label);
    read_size_.observeCached(slot, label, bytes_val);
    if (latency_seconds > 0.0) {
        int64_t latency_us = static_cast<int64_t>(latency_seconds * 1000000.0);
        read_latency_.observeCached(slot, label, latency_us);
    }
}

void TentMetrics::recordWriteCompleted(TransportType tp, size_t bytes,
                                       double latency_seconds) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;

    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    auto bytes_val = static_cast<int64_t>(bytes);
    write_bytes_total_.incCached(slot, label, bytes_val);
    write_requests_total_.incCached(slot, label);
    write_size_.observeCached(slot, label, bytes_val);
    if (latency_seconds > 0.0) {
        int64_t latency_us = static_cast<int64_t>(latency_seconds * 1000000.0);
        write_latency_.observeCached(slot, label, latency_us);
    }
}

bool TentMetrics::recordLogicalRequestStarted(Request::OpCode operation) {
    if (!initialized_) return false;
    const size_t slot = operationSlot(operation);
    auto label = [operation] {
        return std::array<std::string, 1>{operationName(operation)};
    };
    // Current-state accounting must survive runtime sampling transitions.
    logical_requests_inflight_.incCached(slot, label);
    if (runtime_enabled_.load(std::memory_order_relaxed)) {
        logical_requests_started_total_.incCached(slot, label);
    }
    return true;
}

void TentMetrics::recordLogicalRequestFinished(TransportType tp,
                                               Request::OpCode operation,
                                               TransferStatusEnum status,
                                               double latency_us) {
    if (!initialized_) return;
    const size_t operation_slot = operationSlot(operation);
    logical_requests_inflight_.incCached(
        operation_slot,
        [operation] {
            return std::array<std::string, 1>{operationName(operation)};
        },
        -1);
    if (!runtime_enabled_.load(std::memory_order_relaxed)) return;

    const size_t slot = outcomeSlot(tp, operation, status);
    auto labels = [tp, operation, status] {
        return std::array<std::string, 3>{transportTypeName(tp),
                                          operationName(operation),
                                          outcomeName(status)};
    };
    logical_request_outcomes_total_.incCached(slot, labels);
    if (latency_us >= 0.0) {
        logical_request_outcome_latency_.observeCached(
            slot, labels, static_cast<int64_t>(latency_us));
    }
}

void TentMetrics::recordDeadlineMLU(TransportType tp, double mlu) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    if (mlu < 0.0) return;
    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    auto mlu_permille = static_cast<int64_t>(mlu * 1000.0);
    deadline_mlu_.observeCached(slot, label, mlu_permille);
}

void TentMetrics::recordDeadlineInfeasible(TransportType tp) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    deadline_infeasible_total_.incCached(transportSlot(tp), [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    });
}

void TentMetrics::recordBatchQuarantined() {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    quarantined_batches_total_.inc();
}

void TentMetrics::recordTaskFailure(TransportType tp,
                                    TaskFailureReason reason) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    task_failures_total_.incCached(taskFailureSlot(tp, reason), [tp, reason] {
        return std::array<std::string, 2>{transportTypeName(tp),
                                          taskFailureReasonName(reason)};
    });

    switch (reason) {
        case TaskFailureReason::Submit:
            recordTerminalFailure(tp, FailurePhase::Submit,
                                  FailureReason::Internal);
            break;
        case TaskFailureReason::Poll:
            recordTerminalFailure(tp, FailurePhase::Poll,
                                  FailureReason::Internal);
            break;
        case TaskFailureReason::Timeout:
            recordTerminalFailure(tp, FailurePhase::Completion,
                                  FailureReason::Timeout);
            break;
        case TaskFailureReason::Canceled:
            recordTerminalFailure(tp, FailurePhase::Completion,
                                  FailureReason::Canceled);
            break;
    }
}

void TentMetrics::recordTerminalFailure(TransportType tp, FailurePhase phase,
                                        FailureReason reason) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    terminal_failures_total_.inc(std::array<std::string, 3>{
        transportTypeName(tp), failurePhaseName(phase),
        failureReasonName(reason)});
}

void TentMetrics::recordMemoryOperation(TransportType tp,
                                        MemoryOperation operation,
                                        OperationOutcome outcome,
                                        size_t bytes) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const std::array<std::string, 3> labels{transportTypeName(tp),
                                            memoryOperationName(operation),
                                            operationOutcomeName(outcome)};
    memory_operations_total_.inc(labels);
    memory_operation_bytes_total_.inc(labels, static_cast<int64_t>(bytes));
}

void TentMetrics::recordWorkerProgress(WorkerKind worker) {
    if (!initialized_) return;
    worker_last_progress_timestamp_seconds_.setCached(
        static_cast<size_t>(worker),
        [worker] { return std::array<std::string, 1>{workerKindName(worker)}; },
        static_cast<int64_t>(unixTimestampSeconds()));
}

void TentMetrics::recordStagingTaskStarted(Request::OpCode operation,
                                           size_t bytes) {
    if (!initialized_) return;
    const size_t slot = operationSlot(operation);
    auto label = [operation] {
        return std::array<std::string, 1>{operationName(operation)};
    };
    staging_inflight_tasks_.incCached(slot, label);
    staging_inflight_bytes_.incCached(slot, label, static_cast<int64_t>(bytes));
}

void TentMetrics::recordStagingTaskFinished(Request::OpCode operation,
                                            size_t bytes) {
    if (!initialized_) return;
    const size_t slot = operationSlot(operation);
    auto label = [operation] {
        return std::array<std::string, 1>{operationName(operation)};
    };
    staging_inflight_tasks_.incCached(slot, label, -1);
    staging_inflight_bytes_.incCached(slot, label,
                                      -static_cast<int64_t>(bytes));
}

void TentMetrics::recordStagingCleanupPending(StagingCleanupKind kind,
                                              int64_t delta) {
    if (!initialized_) return;
    staging_cleanup_pending_.incCached(
        static_cast<size_t>(kind),
        [kind] {
            return std::array<std::string, 1>{stagingCleanupKindName(kind)};
        },
        delta);
}

void TentMetrics::recordStagingCleanupTimeout() {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    staging_cleanup_timeouts_total_.inc();
}

void TentMetrics::recordRpcAttempt(RpcMethod method, RpcOutcome outcome,
                                   double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = rpcAttemptSlot(method, outcome);
    auto label = [method, outcome] {
        return std::array<std::string, 2>{rpcMethodName(method),
                                          rpcOutcomeName(outcome)};
    };
    rpc_attempts_total_.incCached(slot, label);
    if (latency_us >= 0.0) {
        rpc_attempt_latency_.observeCached(slot, label,
                                           static_cast<int64_t>(latency_us));
    }
}

void TentMetrics::recordRpcRetry(RpcMethod method) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    rpc_retries_total_.incCached(static_cast<size_t>(method), [method] {
        return std::array<std::string, 1>{rpcMethodName(method)};
    });
}

void TentMetrics::recordRdmaSliceEvent(Request::OpCode operation,
                                       RdmaSliceEvent event, int64_t count) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed) ||
        count <= 0)
        return;
    rdma_slice_events_total_.incCached(
        rdmaEventSlot(operation, event),
        [operation, event] {
            return std::array<std::string, 2>{operationName(operation),
                                              rdmaSliceEventName(event)};
        },
        count);
}

void TentMetrics::recordRdmaRetry(Request::OpCode operation,
                                  RdmaRetryCause cause) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    rdma_retries_total_.incCached(
        rdmaRetrySlot(operation, cause), [operation, cause] {
            return std::array<std::string, 2>{operationName(operation),
                                              rdmaRetryCauseName(cause)};
        });
}

void TentMetrics::recordRdmaRetryExhausted(Request::OpCode operation,
                                           RdmaRetryCause cause) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    rdma_retry_exhausted_total_.incCached(
        rdmaRetrySlot(operation, cause), [operation, cause] {
            return std::array<std::string, 2>{operationName(operation),
                                              rdmaRetryCauseName(cause)};
        });
}

void TentMetrics::recordRdmaCqPollError() {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    rdma_cq_poll_errors_total_.inc();
}

void TentMetrics::recordRdmaWorkerInflight(Request::OpCode operation,
                                           int64_t delta) {
    if (!initialized_ || delta == 0) return;
    rdma_worker_inflight_slices_.incCached(
        operationSlot(operation),
        [operation] {
            return std::array<std::string, 1>{operationName(operation)};
        },
        delta);
}

void TentMetrics::recordRdmaPostedInflight(Request::OpCode operation,
                                           int64_t delta) {
    if (!initialized_ || delta == 0) return;
    rdma_posted_inflight_slices_.incCached(
        operationSlot(operation),
        [operation] {
            return std::array<std::string, 1>{operationName(operation)};
        },
        delta);
}

void TentMetrics::recordRdmaCompletion(Request::OpCode operation,
                                       RdmaCompletionOutcome outcome,
                                       double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    RdmaSliceEvent event = RdmaSliceEvent::CompletionError;
    switch (outcome) {
        case RdmaCompletionOutcome::Success:
            event = RdmaSliceEvent::CompletionSuccess;
            break;
        case RdmaCompletionOutcome::WcError:
            event = RdmaSliceEvent::CompletionError;
            break;
        case RdmaCompletionOutcome::Flush:
            event = RdmaSliceEvent::CompletionFlush;
            break;
        case RdmaCompletionOutcome::Late:
            event = RdmaSliceEvent::LateCompletion;
            break;
    }
    recordRdmaSliceEvent(operation, event);
    if (latency_us < 0.0) return;
    rdma_completion_latency_.observeCached(
        rdmaCompletionSlot(operation, outcome),
        [operation, outcome] {
            return std::array<std::string, 2>{
                operationName(operation), rdmaCompletionOutcomeName(outcome)};
        },
        static_cast<int64_t>(latency_us));
}

void TentMetrics::recordRdmaEndpointEvent(RdmaEndpointEvent event) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    rdma_endpoint_events_total_.incCached(static_cast<size_t>(event), [event] {
        return std::array<std::string, 1>{rdmaEndpointEventName(event)};
    });
}

void TentMetrics::recordRdmaQpTransitionFailure(
    RdmaQpType qp_type, RdmaQpTransition transition) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    rdma_qp_transition_failures_total_.incCached(
        rdmaQpFailureSlot(qp_type, transition), [qp_type, transition] {
            return std::array<std::string, 2>{rdmaQpTypeName(qp_type),
                                              rdmaQpTransitionName(transition)};
        });
}

void TentMetrics::recordRdmaNotificationEvent(RdmaNotificationEvent event,
                                               int64_t count) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed) ||
        count <= 0)
        return;
    rdma_notification_events_total_.incCached(
        static_cast<size_t>(event),
        [event] {
            return std::array<std::string, 1>{
                rdmaNotificationEventName(event)};
        },
        count);
}

void TentMetrics::recordStreamEvent(TransportType transport,
                                    Request::OpCode operation,
                                    StreamEvent event, int64_t count) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed) ||
        count <= 0)
        return;
    stream_events_total_.incCached(
        streamEventSlot(transport, operation, event),
        [transport, operation, event] {
            return std::array<std::string, 3>{
                transportTypeName(transport), operationName(operation),
                streamEventName(event)};
        },
        count);
}

void TentMetrics::recordStreamAttempt(TransportType transport,
                                      Request::OpCode operation,
                                      StreamOutcome outcome,
                                      double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    recordStreamEvent(transport, operation, streamOutcomeEvent(outcome));
    if (latency_us < 0.0) return;
    stream_attempt_latency_.observeCached(
        streamAttemptSlot(transport, operation, outcome),
        [transport, operation, outcome] {
            return std::array<std::string, 3>{
                transportTypeName(transport), operationName(operation),
                streamOutcomeName(outcome)};
        },
        static_cast<int64_t>(latency_us));
}

void TentMetrics::recordStreamQueueWait(TransportType transport,
                                        Request::OpCode operation,
                                        double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed) ||
        latency_us < 0.0)
        return;
    stream_queue_wait_.observeCached(
        streamOperationSlot(transport, operation),
        [transport, operation] {
            return std::array<std::string, 2>{transportTypeName(transport),
                                              operationName(operation)};
        },
        static_cast<int64_t>(latency_us));
}

void TentMetrics::recordShmMappingEvent(ShmMappingEvent event) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    shm_mapping_events_total_.incCached(
        static_cast<size_t>(event), [event] {
            return std::array<std::string, 1>{shmMappingEventName(event)};
        });
}

void TentMetrics::recordShmCopy(Request::OpCode operation,
                                ShmCopyOutcome outcome, double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = shmCopySlot(operation, outcome);
    auto labels = [operation, outcome] {
        return std::array<std::string, 2>{operationName(operation),
                                          shmCopyOutcomeName(outcome)};
    };
    shm_copy_events_total_.incCached(slot, labels);
    if (latency_us >= 0.0) {
        shm_copy_latency_.observeCached(slot, labels,
                                        static_cast<int64_t>(latency_us));
    }
}

void TentMetrics::recordSegmentEvent(SegmentEvent event) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    segment_events_total_.incCached(
        static_cast<size_t>(event), [event] {
            return std::array<std::string, 1>{segmentEventName(event)};
        });
}

void TentMetrics::recordSelectionEvent(SelectionEvent event,
                                       TransportType transport) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    selection_events_total_.incCached(
        selectionEventSlot(event, transport), [event, transport] {
            return std::array<std::string, 2>{selectionEventName(event),
                                              transportTypeName(transport)};
        });
}

void TentMetrics::recordAdmissionEvent(AdmissionOwnerKind kind,
                                       AdmissionEvent event, int64_t count) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed) ||
        count <= 0)
        return;
    admission_events_total_.incCached(
        admissionEventSlot(kind, event),
        [kind, event] {
            return std::array<std::string, 2>{admissionOwnerKindName(kind),
                                              admissionEventName(event)};
        },
        count);
}

void TentMetrics::recordAdmissionQueued(AdmissionOwnerKind kind,
                                        int64_t owner_delta,
                                        int64_t byte_delta) {
    if (!initialized_) return;
    const size_t slot = static_cast<size_t>(kind);
    auto label = [kind] {
        return std::array<std::string, 1>{admissionOwnerKindName(kind)};
    };
    if (owner_delta != 0)
        admission_queue_owners_.incCached(slot, label, owner_delta);
    if (byte_delta != 0)
        admission_queue_bytes_.incCached(slot, label, byte_delta);
}

void TentMetrics::recordAdmissionWait(AdmissionOwnerKind kind,
                                      double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed) ||
        latency_us < 0.0)
        return;
    admission_wait_latency_.observeCached(
        static_cast<size_t>(kind),
        [kind] {
            return std::array<std::string, 1>{admissionOwnerKindName(kind)};
        },
        static_cast<int64_t>(latency_us));
}

// Gauge updates below deliberately ignore runtime_enabled_: gauges track
// engine state through paired add/sub operations (attempt start/finish,
// register/unregister), and skipping one half of a pair across a
// setEnabled() transition would permanently corrupt the value. Counters and
// histograms are samples and may be dropped while disabled; gauges are not.
void TentMetrics::recordInflightAttemptStarted(TransportType tp) {
    if (!initialized_) return;
    inflight_attempts_.incCached(transportSlot(tp), [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    });
}

void TentMetrics::recordInflightAttemptFinished(TransportType tp) {
    if (!initialized_) return;
    inflight_attempts_.incCached(
        transportSlot(tp),
        [tp] { return std::array<std::string, 1>{transportTypeName(tp)}; }, -1);
}

void TentMetrics::recordRegisteredBufferBytes(TransportType tp, int64_t delta) {
    if (!initialized_) return;
    registered_buffer_bytes_.incCached(
        transportSlot(tp),
        [tp] { return std::array<std::string, 1>{transportTypeName(tp)}; },
        delta);
}

void TentMetrics::recordStageLatency(Stage stage, TransportType tp,
                                     double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    if (latency_us < 0.0) return;
    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    int64_t val = static_cast<int64_t>(latency_us);
    switch (stage) {
        case Stage::QueueWait:
            stage_queue_wait_.observeCached(slot, label, val);
            break;
        case Stage::Dispatch:
            stage_dispatch_.observeCached(slot, label, val);
            break;
        case Stage::Transport:
            stage_transport_.observeCached(slot, label, val);
            break;
    }
}

void TentMetrics::recordReadFailed(TransportType tp) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    read_failures_total_.incCached(slot, label);
    read_requests_total_.incCached(slot, label);
}

void TentMetrics::recordWriteFailed(TransportType tp) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = transportSlot(tp);
    auto label = [tp] {
        return std::array<std::string, 1>{transportTypeName(tp)};
    };
    write_failures_total_.incCached(slot, label);
    write_requests_total_.incCached(slot, label);
}

void TentMetrics::recordTransportFailover(TransportType from,
                                          TransportType to) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    // Rare event on a large (transport × transport) label domain: plain
    // locked path, no cached cells.
    failover_total_.inc(std::array<std::string, 2>{transportTypeName(from),
                                                   transportTypeName(to)});
}

void TentMetrics::recordTransportAttemptStarted(TransportType tp,
                                                Request::OpCode operation) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = attemptSlot(tp, operation);
    transport_attempts_total_.incCached(slot, [tp, operation] {
        return std::array<std::string, 2>{transportTypeName(tp),
                                          operationName(operation)};
    });
}

void TentMetrics::recordTransportAttemptFinished(TransportType tp,
                                                 Request::OpCode operation,
                                                 TransferStatusEnum status,
                                                 double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed))
        return;
    const size_t slot = attemptSlot(tp, operation);
    auto label = [tp, operation] {
        return std::array<std::string, 2>{transportTypeName(tp),
                                          operationName(operation)};
    };
    if (status != COMPLETED) {
        transport_attempt_failures_total_.incCached(slot, label);
    }
    const size_t outcome_slot = outcomeSlot(tp, operation, status);
    auto outcome_labels = [tp, operation, status] {
        return std::array<std::string, 3>{transportTypeName(tp),
                                          operationName(operation),
                                          outcomeName(status)};
    };
    transport_attempt_outcomes_total_.incCached(outcome_slot, outcome_labels);
    if (latency_us >= 0.0) {
        auto latency_val = static_cast<int64_t>(latency_us);
        transport_attempt_latency_.observeCached(slot, label, latency_val);
        transport_attempt_outcome_latency_.observeCached(
            outcome_slot, outcome_labels, latency_val);
    }
}

void TentMetrics::recordTransportAttemptExemplar(
    TransportType tp, Request::OpCode operation, uint64_t trace_id_high,
    uint64_t trace_id_low, double latency_us) {
    if (!initialized_ || !runtime_enabled_.load(std::memory_order_relaxed) ||
        trace_id_low == 0 || latency_us < 0.0 || tp < 0 ||
        tp >= kNumTransportTypes)
        return;
    auto& exemplar = attempt_exemplars_[attemptSlot(tp, operation)];
    uint64_t sequence = exemplar.sequence.load(std::memory_order_acquire);
    while (true) {
        if (sequence & 1U) {
            sequence = exemplar.sequence.load(std::memory_order_acquire);
            continue;
        }
        if (exemplar.sequence.compare_exchange_weak(
                sequence, sequence + 1, std::memory_order_acq_rel,
                std::memory_order_acquire))
            break;
    }
    exemplar.trace_id_high.store(trace_id_high, std::memory_order_relaxed);
    exemplar.trace_id_low.store(trace_id_low, std::memory_order_relaxed);
    exemplar.value_us.store(static_cast<int64_t>(latency_us),
                            std::memory_order_relaxed);
    exemplar.sequence.store(sequence + 2, std::memory_order_release);
}

std::string TentMetrics::getPrometheusMetrics() {
    return serializePrometheusMetrics(false);
}

std::string TentMetrics::getOpenMetrics() {
    return serializePrometheusMetrics(true);
}

std::string TentMetrics::serializePrometheusMetrics(bool open_metrics) {
    if (!initialized_) return "";

    serialization_attempts_[kPrometheusFormat].fetch_add(
        1, std::memory_order_relaxed);
    try {
        std::string result;
        result.reserve(kPrometheusBufferSize);

        // Counters: ylt's counter_t::serialize() is reliable — no evidence of
        // silent drops in practice. Kept as-is.
        for (auto* counter : counters_) {
            std::string tmp;
            counter->serialize(tmp);
            result += tmp;
        }

        // Histograms: do NOT use ylt's basic_dynamic_histogram::serialize().
        // It silently drops the entire metric (including the # HELP / # TYPE
        // header it already wrote) whenever every label combo has sum_==0 —
        // via `if (value == 0) continue; ... if (value_str.empty())
        // str.clear();`. That condition is reachable in production: e.g.
        // stage_queue_wait_us with sub-microsecond latencies that truncate to
        // 0 under int64_t observation. The JSON endpoint's custom serializer
        // walks get_bucket_counts() directly and is unaffected, which is why
        // /metrics/json reported count=4846 while /metrics omitted the metric
        // entirely. Using the same bucket-walk here closes that drift.
        for (const auto& hist : histograms_) {
            serializeHistogramPrometheus(*hist, result);
        }
        // N=2 attempt histograms go through the same helper, instantiated for
        // N=2.
        serializeHistogramPrometheus(transport_attempt_latency_, result,
                                     open_metrics);
        serializeHistogramPrometheus(logical_request_outcome_latency_, result);
        serializeHistogramPrometheus(transport_attempt_outcome_latency_,
                                     result);
        serializeHistogramPrometheus(rpc_attempt_latency_, result);
        serializeHistogramPrometheus(rdma_completion_latency_, result);
        serializeHistogramPrometheus(stream_attempt_latency_, result);
        serializeHistogramPrometheus(stream_queue_wait_, result);
        serializeHistogramPrometheus(shm_copy_latency_, result);

        // Gauges: current value per label.
        for (auto* gauge : gauges_) {
            serializeGaugePrometheus(*gauge, result);
        }

        const uint64_t success_timestamp = unixTimestampSeconds();
        appendSelfMetricsPrometheus(result, success_timestamp);
        if (open_metrics) result.append("# EOF\n");
        last_serialization_success_timestamp_seconds_[kPrometheusFormat].store(
            success_timestamp, std::memory_order_relaxed);
        return result;
    } catch (const std::exception& e) {
        serialization_failures_[kPrometheusFormat].fetch_add(
            1, std::memory_order_relaxed);
        LOG(ERROR) << "Failed to serialize Prometheus metrics: " << e.what();
        return "";
    }
}

namespace {
// Sum values across all label combos of a dynamic counter. Works with both
// raw pointers (counter members) and shared_ptr (histogram bucket counters).
template <typename CounterPtr>
int64_t sumCounterValues(CounterPtr counter) {
    int64_t total = 0;
    for (auto& e : counter->copy()) {
        total += e->value.load(std::memory_order_relaxed);
    }
    return total;
}

template <typename CounterPtr>
int64_t maxCounterValue(CounterPtr counter) {
    int64_t maximum = 0;
    for (auto& e : counter->copy()) {
        maximum = std::max(maximum, e->value.load(std::memory_order_relaxed));
    }
    return maximum;
}

template <uint8_t N>
void serializeHistogramToJson(nlohmann::json& root,
                              metrics::CachedDynamicHistogram<N>& hist) {
    auto bucket_counters = hist.bucketCounters();
    const auto& boundaries = hist.boundaries();
    int64_t total_count = 0;
    int64_t total_sum = 0;
    nlohmann::json buckets_obj;
    for (size_t i = 0; i < bucket_counters.size(); ++i) {
        int64_t bucket_total = sumCounterValues(bucket_counters[i]);
        total_count += bucket_total;
        if (i < boundaries.size()) {
            buckets_obj[std::to_string(static_cast<int64_t>(boundaries[i]))] =
                bucket_total;
        }
    }
    // The sum counter is maintained by the histogram itself (one add per
    // observe), matching the Prometheus path.
    for (auto& e : hist.sumCounter()->copy()) {
        total_sum += e->value.load();
    }
    nlohmann::json hist_obj;
    hist_obj["count"] = total_count;
    hist_obj["sum"] = total_sum;
    hist_obj["buckets"] = buckets_obj;
    root[hist.name()] = hist_obj;
}
}  // namespace

template <uint8_t N>
void TentMetrics::serializeHistogramPrometheus(
    metrics::CachedDynamicHistogram<N>& hist, std::string& out,
    bool include_exemplars) const {
    // Walk the same bucket-counter / sum-counter data the JSON path uses, so
    // the two endpoints cannot drift. Unlike ylt's serialize() this never
    // silently drops a histogram that has observed >=1 sample: ylt clears its
    // output string (taking the # HELP / # TYPE header with it) whenever
    // every label combo has sum_==0, which is reachable in production when
    // sub-microsecond latencies truncate to 0 under int64_t observation.
    //
    // Templated over label arity N: the per-transport histograms are N=1 and
    // the transport-attempt latency histogram is N=2. For N=1 the emitted
    // text is byte-identical to the original single-label implementation.
    auto bucket_counters = hist.bucketCounters();
    if (bucket_counters.empty()) return;

    const auto& boundaries = hist.boundaries();
    const auto& label_names = hist.labelsName();

    // A unique key per label tuple, used to dedup combos and look up the sum.
    auto combo_key = [](const std::array<std::string, N>& lv) {
        std::string k;
        for (uint8_t i = 0; i < N; ++i) {
            k.append(lv[i]);
            k.push_back('\x1f');  // separator that cannot appear in a label
        }
        return k;
    };
    // Render `name0="v0",name1="v1"` for a label tuple (no surrounding braces).
    auto append_labels = [&](std::string& dst,
                             const std::array<std::string, N>& lv) {
        for (uint8_t i = 0; i < N; ++i) {
            if (i) dst.append(",");
            dst.append(label_names[i]).append("=\"").append(lv[i]).append("\"");
        }
    };

    // Build the union of label combos across ALL buckets. A bucket counter
    // only sees a combo after that combo has been observed in that bucket,
    // so no single bucket sees every combo: e.g. queue_wait (sub-us ->
    // bucket[0]) and transport_us (>=100us -> higher buckets) live in
    // disjoint buckets. Unioning across buckets recovers the full set.
    std::vector<const std::array<std::string, N>*> label_combos;
    std::unordered_set<std::string> seen;
    for (auto* bucket_counter : bucket_counters) {
        for (auto& e : bucket_counter->copy()) {
            if (seen.insert(combo_key(e->label)).second) {
                label_combos.push_back(&e->label);
            }
        }
    }
    if (label_combos.empty()) return;

    // Pre-compute per-combo total counts so we can (a) skip totally-empty
    // combos and (b) decide whether to emit the # HELP / # TYPE header at
    // all. A combo with sum==0 but real observations is still emitted.
    std::vector<std::pair<const std::array<std::string, N>*, int64_t>>
        active_combos;
    for (auto* labels_value : label_combos) {
        int64_t total_count = 0;
        for (auto* bucket_counter : bucket_counters) {
            total_count += bucket_counter->value(*labels_value);
        }
        if (total_count > 0) {
            active_combos.emplace_back(labels_value, total_count);
        }
    }
    if (active_combos.empty()) return;

    // Read back the per-combo sum from the histogram's sum counter
    // (maintained alongside each observe() call). copy() returns a vector of
    // {label, value} pairs; build a lookup map for O(1) access per combo.
    std::unordered_map<std::string, int64_t> sum_by_combo;
    for (auto& e : hist.sumCounter()->copy()) {
        sum_by_combo[combo_key(e->label)] = e->value.load();
    }

    const std::string& name = hist.name();
    const std::string help_str{hist.help()};

    // Emit the header once per metric (matches ylt's serialize_head()).
    out.append("# HELP ").append(name).append(" ").append(help_str).append(
        "\n");
    out.append("# TYPE ").append(name).append(" histogram\n");

    for (auto& [labels_value, total_count] : active_combos) {
        int64_t cumulative = 0;
        for (size_t i = 0; i < bucket_counters.size(); ++i) {
            cumulative += bucket_counters[i]->value(*labels_value);
            out.append(name).append("_bucket{");
            append_labels(out, *labels_value);
            out.append(",");
            if (i < boundaries.size()) {
                out.append("le=\"")
                    .append(std::to_string(boundaries[i]))
                    .append("\"} ");
            } else {
                out.append("le=\"+Inf\"} ");
            }
            out.append(std::to_string(cumulative));
            if constexpr (N == 2) {
                if (include_exemplars && i + 1 == bucket_counters.size() &&
                    name == "tent_transport_attempt_latency_us") {
                    const TransportType transport =
                        parseTransportType((*labels_value)[0]);
                    const Request::OpCode operation =
                        (*labels_value)[1] == "read" ? Request::READ
                                                        : Request::WRITE;
                    if (transport >= 0 && transport < kNumTransportTypes) {
                        const auto& exemplar =
                            attempt_exemplars_[attemptSlot(transport,
                                                            operation)];
                        for (int attempt = 0; attempt < 3; ++attempt) {
                            const uint64_t before = exemplar.sequence.load(
                                std::memory_order_acquire);
                            if (before & 1U) continue;
                            const uint64_t trace_high =
                                exemplar.trace_id_high.load(
                                    std::memory_order_relaxed);
                            const uint64_t trace_low =
                                exemplar.trace_id_low.load(
                                    std::memory_order_relaxed);
                            const int64_t value = exemplar.value_us.load(
                                std::memory_order_relaxed);
                            const uint64_t after = exemplar.sequence.load(
                                std::memory_order_acquire);
                            if (before != after || (after & 1U)) continue;
                            if (trace_low != 0) {
                                std::ostringstream trace;
                                trace << std::hex << std::setfill('0')
                                      << std::setw(16) << trace_high
                                      << std::setw(16) << trace_low;
                                out.append(" # {trace_id=\"")
                                    .append(trace.str())
                                    .append("\"} ")
                                    .append(std::to_string(value));
                            }
                            break;
                        }
                    }
                }
            }
            out.append("\n");
        }

        // _sum: read from the sum counter maintained by each observe() call.
        // Falls back to 0 if the combo is not yet in the counter (should not
        // happen for active combos, but defensive).
        int64_t total_sum = 0;
        auto it = sum_by_combo.find(combo_key(*labels_value));
        if (it != sum_by_combo.end()) {
            total_sum = it->second;
        }
        out.append(name).append("_sum{");
        append_labels(out, *labels_value);
        out.append("} ").append(std::to_string(total_sum)).append("\n");

        out.append(name).append("_count{");
        append_labels(out, *labels_value);
        out.append("} ").append(std::to_string(total_count)).append("\n");
    }
}

void TentMetrics::serializeGaugePrometheus(
    metrics::CachedDynamicCounter<1>& gauge, std::string& out) const {
    auto cells = gauge.copy();
    if (cells.empty()) return;
    const std::string& name = gauge.str_name();
    out.append("# HELP ")
        .append(name)
        .append(" ")
        .append(gauge.help())
        .append("\n");
    out.append("# TYPE ").append(name).append(" gauge\n");
    for (auto& e : cells) {
        out.append(name)
            .append("{")
            .append(gauge.labels_name()[0])
            .append("=\"")
            .append(e->label[0])
            .append("\"} ")
            .append(std::to_string(e->value.load(std::memory_order_relaxed)))
            .append("\n");
    }
}

void TentMetrics::appendSelfMetricsPrometheus(
    std::string& out, uint64_t prometheus_success_timestamp) const {
    const auto append_scalar = [&out](std::string_view name,
                                      std::string_view help,
                                      std::string_view type, uint64_t value) {
        out.append("# HELP ")
            .append(name)
            .append(" ")
            .append(help)
            .append("\n# TYPE ")
            .append(name)
            .append(" ")
            .append(type)
            .append("\n")
            .append(name)
            .append(" ")
            .append(std::to_string(value))
            .append("\n");
    };

    out.append(
           "# HELP tent_metrics_build_info TENT metrics build information\n")
        .append("# TYPE tent_metrics_build_info gauge\n")
        .append("tent_metrics_build_info{version=\"")
        .append(escapePrometheusLabelValue(TENT_BUILD_VERSION))
        .append("\",commit=\"")
        .append(escapePrometheusLabelValue(TENT_GIT_COMMIT))
        .append("\",compile_time_enabled=\"true\"} 1\n");

    append_scalar("tent_metrics_runtime_enabled",
                  "Whether runtime transfer metric recording is enabled",
                  "gauge", isEnabled() ? 1 : 0);
    append_scalar(
        "tent_metrics_http_server_up",
        "Whether the TENT metrics HTTP server is listening", "gauge",
        bound_http_port_.load(std::memory_order_relaxed) != 0 ? 1 : 0);
    append_scalar("tent_metrics_http_start_failures_total",
                  "Total failed TENT metrics HTTP server start attempts",
                  "counter",
                  http_start_failures_.load(std::memory_order_relaxed));

    const auto append_by_format =
        [&out, prometheus_success_timestamp](
            std::string_view name, std::string_view help, std::string_view type,
            const std::array<std::atomic<uint64_t>, kSerializationFormatCount>&
                values,
            bool include_current_prometheus_success) {
            out.append("# HELP ")
                .append(name)
                .append(" ")
                .append(help)
                .append("\n# TYPE ")
                .append(name)
                .append(" ")
                .append(type)
                .append("\n");
            for (size_t i = 0; i < kSerializationFormatCount; ++i) {
                const uint64_t value =
                    i == kPrometheusFormat && include_current_prometheus_success
                        ? prometheus_success_timestamp
                        : values[i].load(std::memory_order_relaxed);
                out.append(name)
                    .append("{format=\"")
                    .append(kSerializationFormatNames[i])
                    .append("\"} ")
                    .append(std::to_string(value))
                    .append("\n");
            }
        };

    append_by_format("tent_metrics_serialization_attempts_total",
                     "Total TENT metrics serialization attempts by format",
                     "counter", serialization_attempts_, false);
    append_by_format("tent_metrics_serialization_failures_total",
                     "Total failed TENT metrics serializations by format",
                     "counter", serialization_failures_, false);
    append_by_format(
        "tent_metrics_last_success_timestamp_seconds",
        "Unix timestamp of the last successful TENT metrics serialization",
        "gauge", last_serialization_success_timestamp_seconds_, true);
}

std::string TentMetrics::getJsonMetrics() {
    if (!initialized_) return "{}";

    serialization_attempts_[kJsonFormat].fetch_add(1,
                                                   std::memory_order_relaxed);
    try {
        nlohmann::json root;

        // Counters: aggregate (sum) across all transport label values so the
        // JSON endpoint stays a simple flat {name: total} view. Per-transport
        // breakdown is available via the Prometheus endpoint.
        root[read_bytes_total_.str_name()] =
            sumCounterValues(&read_bytes_total_);
        root[write_bytes_total_.str_name()] =
            sumCounterValues(&write_bytes_total_);
        root[read_requests_total_.str_name()] =
            sumCounterValues(&read_requests_total_);
        root[write_requests_total_.str_name()] =
            sumCounterValues(&write_requests_total_);
        root[read_failures_total_.str_name()] =
            sumCounterValues(&read_failures_total_);
        root[write_failures_total_.str_name()] =
            sumCounterValues(&write_failures_total_);
        root[failover_total_.str_name()] = sumCounterValues(&failover_total_);
        root[transport_attempts_total_.str_name()] =
            sumCounterValues(&transport_attempts_total_);
        root[transport_attempt_failures_total_.str_name()] =
            sumCounterValues(&transport_attempt_failures_total_);
        root[logical_requests_started_total_.str_name()] =
            sumCounterValues(&logical_requests_started_total_);
        root[logical_request_outcomes_total_.str_name()] =
            sumCounterValues(&logical_request_outcomes_total_);
        root[transport_attempt_outcomes_total_.str_name()] =
            sumCounterValues(&transport_attempt_outcomes_total_);
        root[task_failures_total_.str_name()] =
            sumCounterValues(&task_failures_total_);
        root[terminal_failures_total_.str_name()] =
            sumCounterValues(&terminal_failures_total_);
        root[memory_operations_total_.str_name()] =
            sumCounterValues(&memory_operations_total_);
        root[memory_operation_bytes_total_.str_name()] =
            sumCounterValues(&memory_operation_bytes_total_);
        root[deadline_infeasible_total_.str_name()] =
            sumCounterValues(&deadline_infeasible_total_);
        root[quarantined_batches_total_.str_name()] =
            static_cast<int64_t>(quarantined_batches_total_.value());
        root[staging_cleanup_timeouts_total_.str_name()] =
            static_cast<int64_t>(staging_cleanup_timeouts_total_.value());
        root[rpc_attempts_total_.str_name()] =
            sumCounterValues(&rpc_attempts_total_);
        root[rpc_retries_total_.str_name()] =
            sumCounterValues(&rpc_retries_total_);
        root[rdma_slice_events_total_.str_name()] =
            sumCounterValues(&rdma_slice_events_total_);
        root[rdma_retries_total_.str_name()] =
            sumCounterValues(&rdma_retries_total_);
        root[rdma_retry_exhausted_total_.str_name()] =
            sumCounterValues(&rdma_retry_exhausted_total_);
        root[rdma_cq_poll_errors_total_.str_name()] =
            static_cast<int64_t>(rdma_cq_poll_errors_total_.value());
        root[rdma_endpoint_events_total_.str_name()] =
            sumCounterValues(&rdma_endpoint_events_total_);
        root[rdma_qp_transition_failures_total_.str_name()] =
            sumCounterValues(&rdma_qp_transition_failures_total_);
        root[rdma_notification_events_total_.str_name()] =
            sumCounterValues(&rdma_notification_events_total_);
        root[stream_events_total_.str_name()] =
            sumCounterValues(&stream_events_total_);
        root[shm_mapping_events_total_.str_name()] =
            sumCounterValues(&shm_mapping_events_total_);
        root[shm_copy_events_total_.str_name()] =
            sumCounterValues(&shm_copy_events_total_);
        root[segment_events_total_.str_name()] =
            sumCounterValues(&segment_events_total_);
        root[selection_events_total_.str_name()] =
            sumCounterValues(&selection_events_total_);
        root[admission_events_total_.str_name()] =
            sumCounterValues(&admission_events_total_);

        // Gauges: aggregate across transport labels (total in-flight attempts
        // / total registered bytes). Per-transport breakdown is via the
        // Prometheus endpoint.
        root[inflight_attempts_.str_name()] =
            sumCounterValues(&inflight_attempts_);
        root[logical_requests_inflight_.str_name()] =
            sumCounterValues(&logical_requests_inflight_);
        root[registered_buffer_bytes_.str_name()] =
            sumCounterValues(&registered_buffer_bytes_);
        root[worker_last_progress_timestamp_seconds_.str_name()] =
            maxCounterValue(&worker_last_progress_timestamp_seconds_);
        root[staging_inflight_tasks_.str_name()] =
            sumCounterValues(&staging_inflight_tasks_);
        root[staging_inflight_bytes_.str_name()] =
            sumCounterValues(&staging_inflight_bytes_);
        root[staging_cleanup_pending_.str_name()] =
            sumCounterValues(&staging_cleanup_pending_);
        root[rdma_worker_inflight_slices_.str_name()] =
            sumCounterValues(&rdma_worker_inflight_slices_);
        root[rdma_posted_inflight_slices_.str_name()] =
            sumCounterValues(&rdma_posted_inflight_slices_);
        root[admission_queue_owners_.str_name()] =
            sumCounterValues(&admission_queue_owners_);
        root[admission_queue_bytes_.str_name()] =
            sumCounterValues(&admission_queue_bytes_);

        // Histograms: sum bucket counts across all transport labels. The
        // templated helper also reads back the histogram's sum counter so the
        // JSON endpoint emits "sum" alongside "count" (and stays in sync with
        // the Prometheus endpoint).
        serializeHistogramToJson(root, read_latency_);
        serializeHistogramToJson(root, write_latency_);
        serializeHistogramToJson(root, read_size_);
        serializeHistogramToJson(root, write_size_);
        serializeHistogramToJson(root, deadline_mlu_);
        serializeHistogramToJson(root, stage_queue_wait_);
        serializeHistogramToJson(root, stage_dispatch_);
        serializeHistogramToJson(root, stage_transport_);
        serializeHistogramToJson(root, transport_attempt_latency_);
        serializeHistogramToJson(root, logical_request_outcome_latency_);
        serializeHistogramToJson(root, transport_attempt_outcome_latency_);
        serializeHistogramToJson(root, rpc_attempt_latency_);
        serializeHistogramToJson(root, rdma_completion_latency_);
        serializeHistogramToJson(root, stream_attempt_latency_);
        serializeHistogramToJson(root, stream_queue_wait_);
        serializeHistogramToJson(root, shm_copy_latency_);
        serializeHistogramToJson(root, admission_wait_latency_);

        root["tent_metrics_build_info"] = {
            {"version", TENT_BUILD_VERSION},
            {"commit", TENT_GIT_COMMIT},
            {"compile_time_enabled", true},
        };
        root["tent_metrics_runtime_enabled"] = isEnabled() ? 1 : 0;
        root["tent_metrics_http_server_up"] =
            bound_http_port_.load(std::memory_order_relaxed) != 0 ? 1 : 0;
        root["tent_metrics_http_start_failures_total"] =
            http_start_failures_.load(std::memory_order_relaxed);

        nlohmann::json serialization_attempts;
        nlohmann::json serialization_failures;
        nlohmann::json last_success;
        const uint64_t success_timestamp = unixTimestampSeconds();
        for (size_t i = 0; i < kSerializationFormatCount; ++i) {
            const auto format = std::string(kSerializationFormatNames[i]);
            serialization_attempts[format] =
                serialization_attempts_[i].load(std::memory_order_relaxed);
            serialization_failures[format] =
                serialization_failures_[i].load(std::memory_order_relaxed);
            last_success[format] =
                i == kJsonFormat
                    ? success_timestamp
                    : last_serialization_success_timestamp_seconds_[i].load(
                          std::memory_order_relaxed);
        }
        root["tent_metrics_serialization_attempts_total"] =
            std::move(serialization_attempts);
        root["tent_metrics_serialization_failures_total"] =
            std::move(serialization_failures);
        root["tent_metrics_last_success_timestamp_seconds"] =
            std::move(last_success);

        std::string result = root.dump(2);  // Pretty print with 2-space indent
        last_serialization_success_timestamp_seconds_[kJsonFormat].store(
            success_timestamp, std::memory_order_relaxed);
        return result;
    } catch (const std::exception& e) {
        serialization_failures_[kJsonFormat].fetch_add(
            1, std::memory_order_relaxed);
        LOG(ERROR) << "Failed to serialize JSON metrics: " << e.what();
        return R"({"error": "Failed to serialize metrics"})";
    }
}

std::string TentMetrics::getSummaryString() {
    if (!initialized_) return "Metrics not initialized";

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);

    // Aggregate across all transport labels — summary is intentionally a
    // single total line, not per-transport. Per-transport breakdown is via
    // Prometheus.
    double read_bytes = sumCounterValues(&read_bytes_total_);
    double write_bytes = sumCounterValues(&write_bytes_total_);
    double read_reqs = sumCounterValues(&read_requests_total_);
    double write_reqs = sumCounterValues(&write_requests_total_);
    double read_fails = sumCounterValues(&read_failures_total_);
    double write_fails = sumCounterValues(&write_failures_total_);
    double failovers = sumCounterValues(&failover_total_);

    // Format bytes in human-readable form
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

    oss << "Read: " << formatBytes(read_bytes) << " ("
        << static_cast<uint64_t>(read_reqs) << " reqs, "
        << static_cast<uint64_t>(read_fails) << " fails) | "
        << "Write: " << formatBytes(write_bytes) << " ("
        << static_cast<uint64_t>(write_reqs) << " reqs, "
        << static_cast<uint64_t>(write_fails) << " fails) | "
        << "Failovers: " << static_cast<uint64_t>(failovers)
        << " | Quarantined batches: "
        << static_cast<uint64_t>(quarantined_batches_total_.value());

    return oss.str();
}

#else  // !TENT_METRICS_ENABLED

// Stub implementations when metrics are disabled at compile time
Status TentMetrics::initialize(const MetricsConfig& config) {
    config_ = config;
    initialized_ = true;
    LOG(INFO)
        << "TENT metrics disabled at compile time (TENT_METRICS_ENABLED=0)";
    return Status::OK();
}

void TentMetrics::shutdown() { initialized_ = false; }

void TentMetrics::recordReadCompleted(TransportType, size_t, double) {}
void TentMetrics::recordWriteCompleted(TransportType, size_t, double) {}
void TentMetrics::recordReadFailed(TransportType) {}
void TentMetrics::recordWriteFailed(TransportType) {}
bool TentMetrics::recordLogicalRequestStarted(Request::OpCode) { return false; }
void TentMetrics::recordLogicalRequestFinished(TransportType, Request::OpCode,
                                               TransferStatusEnum, double) {}
void TentMetrics::recordTransportFailover(TransportType, TransportType) {}
void TentMetrics::recordTransportAttemptStarted(TransportType,
                                                Request::OpCode) {}
void TentMetrics::recordTransportAttemptFinished(TransportType, Request::OpCode,
                                                 TransferStatusEnum, double) {}
void TentMetrics::recordTransportAttemptExemplar(
    TransportType, Request::OpCode, uint64_t, uint64_t, double) {}
void TentMetrics::recordDeadlineMLU(TransportType, double) {}
void TentMetrics::recordDeadlineInfeasible(TransportType) {}
void TentMetrics::recordBatchQuarantined() {}
void TentMetrics::recordTaskFailure(TransportType, TaskFailureReason) {}
void TentMetrics::recordTerminalFailure(TransportType, FailurePhase,
                                        FailureReason) {}
void TentMetrics::recordMemoryOperation(TransportType, MemoryOperation,
                                        OperationOutcome, size_t) {}
void TentMetrics::recordWorkerProgress(WorkerKind) {}
void TentMetrics::recordStagingTaskStarted(Request::OpCode, size_t) {}
void TentMetrics::recordStagingTaskFinished(Request::OpCode, size_t) {}
void TentMetrics::recordStagingCleanupPending(StagingCleanupKind, int64_t) {}
void TentMetrics::recordStagingCleanupTimeout() {}
void TentMetrics::recordRpcAttempt(RpcMethod, RpcOutcome, double) {}
void TentMetrics::recordRpcRetry(RpcMethod) {}
void TentMetrics::recordRdmaSliceEvent(Request::OpCode, RdmaSliceEvent,
                                       int64_t) {}
void TentMetrics::recordRdmaRetry(Request::OpCode, RdmaRetryCause) {}
void TentMetrics::recordRdmaRetryExhausted(Request::OpCode, RdmaRetryCause) {}
void TentMetrics::recordRdmaCqPollError() {}
void TentMetrics::recordRdmaWorkerInflight(Request::OpCode, int64_t) {}
void TentMetrics::recordRdmaPostedInflight(Request::OpCode, int64_t) {}
void TentMetrics::recordRdmaCompletion(Request::OpCode, RdmaCompletionOutcome,
                                       double) {}
void TentMetrics::recordRdmaEndpointEvent(RdmaEndpointEvent) {}
void TentMetrics::recordRdmaQpTransitionFailure(RdmaQpType, RdmaQpTransition) {}
void TentMetrics::recordRdmaNotificationEvent(RdmaNotificationEvent, int64_t) {}
void TentMetrics::recordStreamEvent(TransportType, Request::OpCode, StreamEvent,
                                    int64_t) {}
void TentMetrics::recordStreamAttempt(TransportType, Request::OpCode,
                                      StreamOutcome, double) {}
void TentMetrics::recordStreamQueueWait(TransportType, Request::OpCode,
                                        double) {}
void TentMetrics::recordShmMappingEvent(ShmMappingEvent) {}
void TentMetrics::recordShmCopy(Request::OpCode, ShmCopyOutcome, double) {}
void TentMetrics::recordSegmentEvent(SegmentEvent) {}
void TentMetrics::recordSelectionEvent(SelectionEvent, TransportType) {}
void TentMetrics::recordAdmissionEvent(AdmissionOwnerKind, AdmissionEvent,
                                       int64_t) {}
void TentMetrics::recordAdmissionQueued(AdmissionOwnerKind, int64_t, int64_t) {}
void TentMetrics::recordAdmissionWait(AdmissionOwnerKind, double) {}
void TentMetrics::recordInflightAttemptStarted(TransportType) {}
void TentMetrics::recordInflightAttemptFinished(TransportType) {}
void TentMetrics::recordRegisteredBufferBytes(TransportType, int64_t) {}
void TentMetrics::recordStageLatency(Stage, TransportType, double) {}

std::string TentMetrics::getPrometheusMetrics() {
    return "# TENT metrics disabled at compile time\n";
}

std::string TentMetrics::getOpenMetrics() {
    return "# TENT metrics disabled at compile time\n# EOF\n";
}

std::string TentMetrics::getJsonMetrics() {
    return R"({"status": "disabled", "message": "TENT metrics disabled at compile time"})";
}

std::string TentMetrics::getSummaryString() {
    return "TENT metrics disabled at compile time";
}

#endif  // TENT_METRICS_ENABLED

}  // namespace mooncake::tent
