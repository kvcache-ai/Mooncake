#include "client_offboarding.h"

#include <glog/logging.h>

#include <shared_mutex>
#include <unordered_set>
#include <utility>

#include <boost/functional/hash.hpp>

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
        // Accepted jobs are durable for this process lifetime: unlike a
        // best-effort metadata cleanup, shutdown drains this queue. Jobs that
        // still fail during the drain are dropped; no further snapshot can be
        // taken in this process, so the barrier no longer matters.
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
        jobs_.push_back(std::move(job));
        pending_jobs_.fetch_add(1, std::memory_order_release);
        MasterMetricManager::instance().inc_client_offboarding_queue_depth();
    }
    cv_.notify_one();
    return true;
}

bool ClientOffboardingWorker::ShouldSkipSnapshot() {
    // A pending job (first attempt or retry) means reclamation has not
    // converged yet. Persisting now would snapshot partially offboarded
    // resources, which an HA restore would rebuild with fresh Active
    // liveness — resurrecting resources a lease expiry already retracted.
    return pending_jobs_.load(std::memory_order_acquire) != 0;
}

void ClientOffboardingWorker::ReprepareJob(
    ClientOffboardingJob& job, ScopedSegmentAccess& segment_access) {
    job.segments.clear();
    job.all_segments_prepared = true;

    std::vector<Segment> segments;
    const auto get_segments_result =
        segment_access.GetClientSegments(job.client_id, segments);
    if (get_segments_result != ErrorCode::OK &&
        get_segments_result != ErrorCode::SEGMENT_NOT_FOUND) {
        job.all_segments_prepared = false;
        LOG(ERROR) << "client_id=" << job.client_id
                   << ", error=get_client_segments_for_offboarding_failed ("
                   << get_segments_result << ")";
        return;
    }

    for (const auto& segment : segments) {
        size_t metrics_dec_capacity = 0;
        const auto prepare_result =
            segment_access.PrepareUnmountSegment(segment.id,
                                                 metrics_dec_capacity);
        if (prepare_result != ErrorCode::OK) {
            // The Segment stays mounted; the next retry re-enumerates it.
            job.all_segments_prepared = false;
            LOG(ERROR) << "client_id=" << job.client_id
                       << ", segment_name=" << segment.name
                       << ", error=prepare_client_offboarding_failed ("
                       << prepare_result << ")";
            continue;
        }
        job.segments.push_back({
            .segment_id = segment.id,
            .segment_name = segment.name,
            .metrics_dec_capacity = metrics_dec_capacity,
        });
    }
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
        bool quota_recompute_needed = false;
        bool any_retry_requeued = false;

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

        // Every job in this batch was prepared before this scan started.
        // Jobs submitted during the scan remain queued for the next batch.
        service_->ClearInvalidHandles(retained_clients);

        {
            ScopedSegmentAccess segment_access =
                service_->segment_manager_.getSegmentAccess();
            for (size_t job_index = 0; job_index < jobs.size(); ++job_index) {
                auto& job = jobs[job_index];
                if (job.needs_reprepare) {
                    ReprepareJob(job, segment_access);
                }
                bool completed = job.all_segments_prepared;

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
                    // Best-effort HTTP metadata cleanup, aligned with the
                    // synchronous reclamation path: the Offline liveness
                    // record is still present at this point, so a new
                    // incarnation cannot have re-mounted the same segment
                    // name yet.
                    service_->cleanupHttpMetadata(segment.segment_name);
                    LOG(INFO) << "client_id=" << job.client_id
                              << ", segment_name=" << segment.segment_name
                              << ", action=unmount_offline_mem_segment";
                }

                // LOCAL_DISK registrations live in the LocalSsdManager. Drop
                // the Client's registration even when it had no memory
                // Segment.
                if (service_->local_ssd_manager_.UnregisterClient(
                        job.client_id)) {
                    quota_recompute_needed = true;
                }
                jobs_completed[job_index] = completed;
            }
        }
        snapshot_lock.unlock();

        if (quota_recompute_needed) {
            service_->RecomputeTenantEffectiveQuotas();
        }

        for (size_t job_index = 0; job_index < jobs.size(); ++job_index) {
            auto& job = jobs[job_index];
            if (!jobs_completed[job_index]) {
                // The job stays queued and the snapshot barrier stays raised
                // until cleanup converges. Failure causes are transient
                // (e.g. a Segment mid-graceful-unmount), so retrying
                // converges instead of starving snapshots permanently.
                const UUID client_id = job.client_id;
                const uint64_t retry_count = job.retry_count + 1;
                bool requeued = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (running_) {
                        job.needs_reprepare = true;
                        job.retry_count = retry_count;
                        jobs_.push_back(std::move(job));
                        requeued = true;
                    }
                }
                if (requeued) {
                    any_retry_requeued = true;
                    MasterMetricManager::instance().inc_client_offboarding_retry();
                    LOG(ERROR) << "client_id=" << client_id
                               << ", error=client_offboarding_incomplete"
                               << ", retry_count=" << retry_count;
                } else {
                    LOG(ERROR) << "client_id=" << client_id
                               << ", error=client_offboarding_dropped_at_stop";
                }
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

        if (any_retry_requeued) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kRetryBackoffMs));
        }
    }
    LOG(INFO) << "Client offboarding worker stopped";
}

}  // namespace mooncake
