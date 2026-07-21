#include "client_offboarding.h"

#include <glog/logging.h>

#include <shared_mutex>
#include <unordered_set>
#include <utility>

#include <boost/functional/hash.hpp>

#include "client_lifecycle_event.h"
#include "master_metric_manager.h"
#include "master_service.h"
#include "segment.h"

namespace mooncake {

ClientOffboardingWorker::~ClientOffboardingWorker() { Stop(); }

void ClientOffboardingWorker::Start() {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    std::lock_guard<std::mutex> queue_lock(mutex_);
    if (running_) {
        return;
    }
    running_ = true;
    try {
        thread_ = std::thread(&ClientOffboardingWorker::ThreadFunc, this);
    } catch (...) {
        running_ = false;
        throw;
    }
}

void ClientOffboardingWorker::Stop() {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    {
        std::lock_guard<std::mutex> queue_lock(mutex_);
        if (!running_ && !thread_.joinable()) {
            return;
        }
        // Accepted jobs are durable for this process lifetime: unlike the
        // best-effort HTTP metadata worker, shutdown drains this queue.
        running_ = false;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
    const auto incomplete =
        pending_jobs_.exchange(0, std::memory_order_acq_rel);
    if (incomplete > 0) {
        MasterMetricManager::instance().dec_client_offboarding_queue_depth(
            static_cast<int64_t>(incomplete));
    }
}

bool ClientOffboardingWorker::Schedule(ClientOffboardingJob job) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
            return false;
        }
        job.snapshot_barrier_generation = snapshot_barrier_generation_;
        jobs_.push_back(std::move(job));
        pending_jobs_.fetch_add(1, std::memory_order_release);
        MasterMetricManager::instance().inc_client_offboarding_queue_depth();
    }
    cv_.notify_one();
    return true;
}

bool ClientOffboardingWorker::ShouldSkipSnapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_jobs_.load(std::memory_order_acquire) == 0 &&
        !failed_job_requires_snapshot_skip_) {
        return false;
    }

    // One skipped snapshot cycle covers every job scheduled in the current
    // generation. A failed job requests a skip only when no snapshot attempt
    // observed it while it was pending.
    ++snapshot_barrier_generation_;
    failed_job_requires_snapshot_skip_ = false;
    return true;
}

void ClientOffboardingWorker::ThreadFunc() {
    LOG(INFO) << "Client offboarding worker started";
    while (true) {
        std::deque<ClientOffboardingJob> jobs;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&] { return !running_ || !jobs_.empty(); });
            if (!running_ && jobs_.empty()) {
                break;
            }
            jobs.swap(jobs_);
        }

        std::vector<bool> jobs_completed(jobs.size(), false);
        std::vector<ClientLeaseExpiredEvent> completed_events;
        completed_events.reserve(jobs.size());
        bool quota_recompute_needed = false;

        // Respect the service lock order (Client -> snapshot). Mount paths may
        // create records while waiting for snapshot access, but cannot attach
        // resources until this batch releases the snapshot lock.
        std::shared_lock<std::shared_mutex> client_lock(service_->client_mutex_);
        std::shared_lock<std::shared_mutex> snapshot_lock(
            service_->snapshot_mutex_);
        std::unordered_set<UUID, boost::hash<UUID>> retained_clients;
        retained_clients.reserve(service_->client_liveness_records_.size());
        for (const auto& [client_id, record] :
             service_->client_liveness_records_) {
            if (record->ShouldRetainResources()) {
                retained_clients.insert(client_id);
            }
        }
        client_lock.unlock();

        // Every job in this snapshot was prepared before this scan started.
        // Jobs submitted during the scan remain queued for the next batch.
        service_->ClearInvalidHandles(retained_clients);

        {
            ScopedSegmentAccess segment_access =
                service_->segment_manager_.getSegmentAccess();
            for (size_t job_index = 0; job_index < jobs.size(); ++job_index) {
                const auto& job = jobs[job_index];
                bool completed = job.all_segments_prepared;
                ClientLeaseExpiredEvent event{job.client_id, {}};
                event.unmounted_memory_segment_names.reserve(
                    job.segments.size());

                for (const auto& segment : job.segments) {
                    const auto err = segment_access.CommitUnmountSegment(
                        segment.segment_id, job.client_id,
                        segment.metrics_dec_capacity);
                    if (err != ErrorCode::OK) {
                        completed = false;
                        MasterMetricManager::instance()
                            .inc_client_offboarding_failure();
                        LOG(ERROR)
                            << "client_id=" << job.client_id
                            << ", segment_name=" << segment.segment_name
                            << ", error=commit_client_offboarding_failed ("
                            << err << ")";
                        continue;
                    }
                    quota_recompute_needed = true;
                    event.unmounted_memory_segment_names.push_back(
                        segment.segment_name);
                    LOG(INFO) << "client_id=" << job.client_id
                              << ", segment_name=" << segment.segment_name
                              << ", action=unmount_offline_mem_segment";
                }

                // LOCAL_DISK replicas were removed by the metadata scan. Drop
                // the Client registration even when it had no memory Segment.
                segment_access.UnmountLocalDiskSegment(job.client_id);
                quota_recompute_needed = true;
                jobs_completed[job_index] = completed;
                if (completed) {
                    completed_events.push_back(std::move(event));
                }
            }
        }
        snapshot_lock.unlock();

        if (quota_recompute_needed) {
            service_->RecomputeTenantEffectiveQuotas();
        }

        for (size_t job_index = 0; job_index < jobs.size(); ++job_index) {
            const auto& job = jobs[job_index];
            if (!jobs_completed[job_index]) {
                bool snapshot_skip_already_observed = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    snapshot_skip_already_observed =
                        job.snapshot_barrier_generation !=
                        snapshot_barrier_generation_;
                    if (!snapshot_skip_already_observed) {
                        failed_job_requires_snapshot_skip_ = true;
                    }
                    pending_jobs_.fetch_sub(1, std::memory_order_acq_rel);
                }
                MasterMetricManager::instance()
                    .dec_client_offboarding_queue_depth();

                // The Offline record remains as a ReMount barrier, but this
                // failed job does not permanently block later snapshots. If
                // no snapshot cycle observed it while pending, preserve one
                // consumable skip for the next snapshot attempt.
                LOG(ERROR) << "client_id=" << job.client_id
                           << ", error=client_offboarding_incomplete"
                           << ", snapshot_skip_already_observed="
                           << snapshot_skip_already_observed;
                continue;
            }

            {
                std::unique_lock<std::shared_mutex> lock(
                    service_->client_mutex_);
                const auto current =
                    service_->client_liveness_records_.find(job.client_id);
                if (current != service_->client_liveness_records_.end() &&
                    current->second == job.liveness) {
                    service_->client_liveness_records_.erase(current);
                    MasterMetricManager::instance()
                        .on_client_liveness_record_removed(
                            ClientLivenessState::OFFLINE);
                }
            }

            {
                std::lock_guard<std::mutex> lock(mutex_);
                pending_jobs_.fetch_sub(1, std::memory_order_acq_rel);
            }
            MasterMetricManager::instance().dec_client_offboarding_queue_depth();
            const auto duration_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - job.enqueued_at)
                    .count();
            MasterMetricManager::instance()
                .observe_client_offboarding_duration_ms(duration_ms);
            LOG(INFO) << "client_id=" << job.client_id
                      << ", action=client_offboarding_complete"
                      << ", duration_ms=" << duration_ms;
        }

        // Callbacks are external observers. Publish only after all Master
        // locks are released and the old incarnation has been erased.
        for (const auto& event : completed_events) {
            service_->NotifyClientLeaseExpired(event);
        }
    }
    LOG(INFO) << "Client offboarding worker stopped";
}

}  // namespace mooncake
