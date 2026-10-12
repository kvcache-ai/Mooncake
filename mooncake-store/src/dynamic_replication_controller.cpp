#include "dynamic_replication_controller.h"

#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <tuple>
#include <utility>

namespace mooncake {

DynamicReplicationController::DynamicReplicationController(
    const std::string& mode, uint32_t heat_window_seconds,
    double admission_qps_threshold, size_t max_memory_replicas)
    : mode_(mode == "observe"   ? Mode::kObserve
            : mode == "enforce" ? Mode::kEnforce
                                : Mode::kOff),
      heat_window_seconds_(std::max<uint32_t>(1, heat_window_seconds)),
      admission_qps_threshold_(
          admission_qps_threshold > 0.0 ? admission_qps_threshold : 0.8),
      max_memory_replicas_(std::max<size_t>(1, max_memory_replicas)) {
    if (Enabled()) {
        LOG(INFO) << "Dynamic MEMORY replication mode enabled: mode=" << mode
                  << ", heat_window_seconds=" << heat_window_seconds_
                  << ", admission_qps_threshold=" << admission_qps_threshold_
                  << ", max_memory_replicas=" << max_memory_replicas_;
    }
}

DynamicReplicationController::~DynamicReplicationController() { Stop(); }

void DynamicReplicationController::Start(SubmitFn submit) {
    submit_ = std::move(submit);
    admission_running_ = true;
    admission_thread_ =
        std::thread(&DynamicReplicationController::AdmissionThreadFunc, this);
    VLOG(1) << "action=start_dynamic_replication_admission_thread";
}

void DynamicReplicationController::Stop() {
    admission_running_ = false;
    admission_cv_.notify_all();
    if (admission_thread_.joinable()) {
        admission_thread_.join();
    }
}

bool DynamicReplicationController::WantsAnotherMemoryReplica(
    size_t memory_replicas) const {
    return memory_replicas > 0 && memory_replicas < max_memory_replicas_;
}

// --- Heat admission ---------------------------------------------------------

uint64_t DynamicReplicationController::StableScore(const std::string& key,
                                                   const std::string& segment) {
    uint64_t hash = 1469598103934665603ULL;
    auto mix = [&hash](std::string_view value) {
        for (const unsigned char c : value) {
            hash ^= c;
            hash *= 1099511628211ULL;
        }
        hash ^= 0xff;
        hash *= 1099511628211ULL;
    };
    mix(key);
    mix(segment);
    return hash;
}

uint32_t DynamicReplicationController::AdmissionMinHits() const {
    const double hits =
        std::ceil(admission_qps_threshold_ * heat_window_seconds_);
    return std::max<uint32_t>(1, static_cast<uint32_t>(hits));
}

void DynamicReplicationController::CleanupWindowsLocked(
    std::chrono::steady_clock::time_point now, std::chrono::seconds window) {
    size_t scanned = 0;
    while (scanned < kWindowCleanupBudget && !window_order_.empty()) {
        auto key = std::move(window_order_.front());
        window_order_.pop_front();
        auto it = windows_.find(key);
        if (it != windows_.end()) {
            if (now - it->second.window_start > window * 2) {
                windows_.erase(it);
            } else {
                window_order_.push_back(std::move(key));
            }
        }
        scanned++;
    }
}

bool DynamicReplicationController::ObserveAccess(const TenantId& tenant_id,
                                                 const std::string& key) {
    if (!Enabled()) {
        return false;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto window = std::chrono::seconds(heat_window_seconds_);
    const auto admission_key = tenant_id.MakeScopedKey(key);

    std::lock_guard<std::mutex> lock(heat_mutex_);
    if (windows_.size() >= kWindowEntryLimit && now >= next_window_cleanup_) {
        next_window_cleanup_ = now + kWindowCleanupInterval;
        CleanupWindowsLocked(now, window);
    }

    auto counter_it = windows_.find(admission_key);
    if (counter_it == windows_.end()) {
        if (windows_.size() >= kWindowEntryLimit) {
            return false;
        }
        counter_it = windows_.emplace(admission_key, Window{}).first;
        window_order_.push_back(admission_key);
    }

    auto& counter = counter_it->second;
    if (counter.window_start.time_since_epoch().count() == 0 ||
        now - counter.window_start >= window) {
        counter.window_start = now;
        counter.hits = 0;
    }
    counter.hits++;
    return counter.hits == AdmissionMinHits();
}

bool DynamicReplicationController::HeatAdmitted(const TenantId& tenant_id,
                                                const std::string& key) {
    if (!Enabled()) {
        return false;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto window = std::chrono::seconds(heat_window_seconds_);
    const auto admission_key = tenant_id.MakeScopedKey(key);

    std::lock_guard<std::mutex> lock(heat_mutex_);
    auto counter_it = windows_.find(admission_key);
    if (counter_it == windows_.end()) {
        return false;
    }
    const auto& counter = counter_it->second;
    if (counter.window_start.time_since_epoch().count() == 0 ||
        now - counter.window_start >= window) {
        return false;
    }
    return counter.hits >= AdmissionMinHits();
}

void DynamicReplicationController::RecordAccess(const TenantId& tenant_id,
                                                const std::string& key) {
    if (!ObserveAccess(tenant_id, key)) {
        return;
    }
    if (mode_ == Mode::kObserve) {
        VLOG(1) << "dynamic_replication_observe_would_propose key=" << key;
        return;
    }
    if (!Enforcing()) {
        return;
    }
    Enqueue(tenant_id, key);
}

void DynamicReplicationController::Enqueue(const TenantId& tenant_id,
                                           const std::string& key) {
    auto scoped_key = tenant_id.MakeScopedKey(key);
    std::lock_guard<std::mutex> lock(admission_mutex_);
    if (admission_queued_.contains(scoped_key) ||
        admission_queue_.size() >= kAdmissionQueueLimit) {
        return;
    }
    admission_queue_.push(QueuedObject{tenant_id, key});
    admission_queued_.insert(std::move(scoped_key));
    admission_cv_.notify_one();
}

void DynamicReplicationController::AdmissionThreadFunc() {
    VLOG(1) << "action=dynamic_replication_admission_thread_started";
    while (admission_running_) {
        std::vector<QueuedObject> batch;
        {
            std::unique_lock<std::mutex> lock(admission_mutex_);
            admission_cv_.wait_for(
                lock, std::chrono::milliseconds(kAdmissionThreadSleepMs), [&] {
                    return !admission_running_.load() ||
                           !admission_queue_.empty();
                });
            if (!admission_running_) {
                break;
            }
            while (!admission_queue_.empty() &&
                   batch.size() < kAdmissionBatchSize) {
                auto object = std::move(admission_queue_.front());
                admission_queue_.pop();
                admission_queued_.erase(
                    object.tenant_id.MakeScopedKey(object.key));
                batch.push_back(std::move(object));
            }
        }
        for (const auto& object : batch) {
            submit_(object.tenant_id, object.key);
        }
    }
    VLOG(1) << "action=dynamic_replication_admission_thread_stopped";
}

// --- Proposals ---------------------------------------------------------------

ReplicaActionProposal DynamicReplicationController::MakeAutoProposal(
    const TenantId& tenant_id, const std::string& key) const {
    ReplicaActionProposal proposal;
    proposal.action = ReplicaActionType::ADD;
    proposal.proposal_id = generate_uuid();
    proposal.tenant_id = tenant_id.value();
    proposal.key = key;
    proposal.expire_at_ms_epoch = NowMs() + kLeaseTtl.count();
    return proposal;
}

tl::expected<void, ErrorCode> DynamicReplicationController::ValidateProposal(
    const ReplicaActionProposal& proposal, int64_t now_ms) const {
    if (!Enforcing()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (proposal.action != ReplicaActionType::ADD || proposal.key.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (proposal.proposal_id == UUID{0, 0}) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!proposal.requester_domain.empty() || !proposal.target_domain.empty()) {
        // Domain-aware admission and placement are reserved for the next stage.
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (proposal.expire_at_ms_epoch > 0 &&
        proposal.expire_at_ms_epoch < now_ms) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

std::optional<DynamicReplicationController::Plan>
DynamicReplicationController::SelectPlan(
    ScopedSegmentAccess& segment_access, const ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& is_readable,
    const std::optional<std::string>& preferred_target_segment,
    std::string target_domain) const {
    const std::string& object_key = metadata.user_key;
    std::unordered_set<std::string> existing_segments;
    std::unordered_set<std::string> existing_hosts;
    std::vector<std::string> source_segments;
    std::unordered_map<std::string, std::shared_ptr<ClientLivenessRecord>>
        source_liveness_by_segment;
    size_t memory_replicas = 0;

    std::vector<std::pair<Segment, UUID>> segments;
    if (segment_access.GetAllSegments(segments) != ErrorCode::OK) {
        return std::nullopt;
    }
    std::unordered_map<std::string, Segment> segments_by_name;
    for (const auto& [segment, client_id] : segments) {
        (void)client_id;
        segments_by_name.emplace(segment.name, segment);
    }

    metadata.VisitReplicas(
        [&is_readable](const Replica& replica) {
            return is_readable(replica) && replica.is_memory_replica();
        },
        [&](const Replica& replica) {
            memory_replicas++;
            const auto liveness = replica.getClientLiveness();
            for (const auto& segment_name : replica.get_segment_names()) {
                if (!segment_name.has_value()) {
                    continue;
                }
                existing_segments.insert(*segment_name);
                source_segments.push_back(*segment_name);
                source_liveness_by_segment[*segment_name] = liveness;
                auto segment_it = segments_by_name.find(*segment_name);
                if (segment_it != segments_by_name.end() &&
                    !segment_it->second.host_id.empty()) {
                    existing_hosts.insert(segment_it->second.host_id);
                }
            }
        });

    if (source_segments.empty() || memory_replicas == 0 ||
        memory_replicas >= max_memory_replicas_) {
        return std::nullopt;
    }

    std::sort(source_segments.begin(), source_segments.end());
    source_segments.erase(
        std::unique(source_segments.begin(), source_segments.end()),
        source_segments.end());
    const std::string source_segment = *std::min_element(
        source_segments.begin(), source_segments.end(),
        [&](const auto& lhs, const auto& rhs) {
            return StableScore(object_key, lhs) < StableScore(object_key, rhs);
        });

    auto is_valid_target = [&](const Segment& segment) {
        if (existing_segments.contains(segment.name) ||
            !segment_access.IsSegmentAllocatable(segment.name)) {
            return false;
        }
        size_t used = 0;
        size_t capacity = 0;
        if (segment_access.QuerySegments(segment.name, used, capacity) !=
                ErrorCode::OK ||
            capacity == 0 || used >= capacity ||
            capacity - used < metadata.size) {
            return false;
        }
        const double util_after = static_cast<double>(used + metadata.size) /
                                  static_cast<double>(capacity);
        return util_after < kTargetHighWatermark;
    };

    auto target_score = [&](const Segment& segment) {
        size_t used = 0;
        size_t capacity = 0;
        if (segment_access.QuerySegments(segment.name, used, capacity) !=
                ErrorCode::OK ||
            capacity == 0) {
            return std::tuple<bool, double, uint64_t>(
                false, std::numeric_limits<double>::max(),
                std::numeric_limits<uint64_t>::max());
        }
        const bool different_host = segment.host_id.empty() ||
                                    existing_hosts.empty() ||
                                    !existing_hosts.contains(segment.host_id);
        const double util =
            static_cast<double>(used) / static_cast<double>(capacity);
        return std::tuple<bool, double, uint64_t>(
            different_host, util, StableScore(object_key, segment.name));
    };

    std::optional<std::string> target_segment;
    if (preferred_target_segment.has_value()) {
        const auto preferred = std::find_if(
            segments.begin(), segments.end(), [&](const auto& entry) {
                return entry.first.name == *preferred_target_segment;
            });
        if (preferred != segments.end() && is_valid_target(preferred->first)) {
            target_segment = preferred->first.name;
        }
    }
    if (!target_segment.has_value()) {
        std::optional<std::tuple<bool, double, uint64_t>> best_score;
        for (const auto& [segment, client_id] : segments) {
            (void)client_id;
            if (!is_valid_target(segment)) {
                continue;
            }
            const auto score = target_score(segment);
            if (!best_score.has_value() ||
                std::get<0>(score) > std::get<0>(*best_score) ||
                (std::get<0>(score) == std::get<0>(*best_score) &&
                 std::get<1>(score) < std::get<1>(*best_score)) ||
                (std::get<0>(score) == std::get<0>(*best_score) &&
                 std::get<1>(score) == std::get<1>(*best_score) &&
                 std::get<2>(score) < std::get<2>(*best_score))) {
                best_score = score;
                target_segment = segment.name;
            }
        }
    }

    if (!target_segment.has_value()) {
        return std::nullopt;
    }
    const auto source_liveness =
        source_liveness_by_segment.find(source_segment);
    if (source_liveness == source_liveness_by_segment.end() ||
        !source_liveness->second) {
        return std::nullopt;
    }
    return Plan{
        .source_segment = source_segment,
        .target_segment = *target_segment,
        .target_domain = std::move(target_domain),
        .source_liveness = source_liveness->second,
    };
}

size_t DynamicReplicationController::RecordReplicaRemoval(
    ObjectMetadata& metadata, const std::vector<ReplicaID>& replica_ids) const {
    if (replica_ids.empty()) {
        return 0;
    }
    const size_t removed = metadata.ForgetDynamicReplicas(replica_ids);
    if (removed > 0) {
        metadata.SetDynamicReplicationRecreateAfter(
            std::chrono::steady_clock::now() + kRecreateCooldown);
    }
    return removed;
}

uint64_t DynamicReplicationController::VersionEpoch(
    const ObjectMetadata& metadata) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            metadata.put_start_time.time_since_epoch())
            .count());
}

int64_t DynamicReplicationController::NowMs() {
    return static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

}  // namespace mooncake
