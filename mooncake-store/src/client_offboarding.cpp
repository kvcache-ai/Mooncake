#include "client_offboarding.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include <glog/logging.h>

#include "master_metric_manager.h"
#include "master_service.h"

namespace mooncake {

ClientOffboardingWorker::~ClientOffboardingWorker() { Stop(); }

void ClientOffboardingWorker::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
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
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ && !thread_.joinable()) {
            return;
        }
        running_ = false;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
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

std::chrono::seconds ClientOffboardingWorker::RetryDelay(
    uint64_t retry_count) {
    constexpr uint64_t kBackoffs[] = {1, 2, 4, 8, 16, 30};
    const size_t index = static_cast<size_t>(std::min<uint64_t>(
        retry_count == 0 ? 0 : retry_count - 1,
        std::size(kBackoffs) - 1));
    return std::chrono::seconds(kBackoffs[index]);
}

void ClientOffboardingWorker::CompleteJob(const ClientOffboardingJob& job) {
    pending_jobs_.fetch_sub(1, std::memory_order_acq_rel);
    MasterMetricManager::instance().dec_client_offboarding_queue_depth();
    const auto duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - job.enqueued_at)
            .count();
    MasterMetricManager::instance().observe_client_offboarding_duration_ms(
        duration_ms);
    LOG(INFO) << "client_id=" << job.client_id
              << ", action=client_offboarding_complete"
              << ", retries=" << job.retry_count
              << ", duration_ms=" << duration_ms;
}

void ClientOffboardingWorker::DropJob(const ClientOffboardingJob& job,
                                      const char* reason) {
    pending_jobs_.fetch_sub(1, std::memory_order_acq_rel);
    MasterMetricManager::instance().dec_client_offboarding_queue_depth();
    LOG(ERROR) << "client_id=" << job.client_id
               << ", action=client_offboarding_dropped"
               << ", reason=" << reason
               << ", retry_count=" << job.retry_count;
}

void ClientOffboardingWorker::ThreadFunc() {
    LOG(INFO) << "Client offboarding worker started";
    while (true) {
        ClientOffboardingJob job;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (running_ && jobs_.empty()) {
                cv_.wait(lock);
            }
            if (!running_) {
                auto dropped = std::move(jobs_);
                jobs_.clear();
                lock.unlock();
                for (const auto& queued : dropped) {
                    DropJob(queued, "master_stopping");
                }
                break;
            }

            const auto next = std::min_element(
                jobs_.begin(), jobs_.end(), [](const auto& lhs, const auto& rhs) {
                    return lhs.next_attempt_at < rhs.next_attempt_at;
                });
            const auto now = std::chrono::steady_clock::now();
            if (next->next_attempt_at > now) {
                cv_.wait_until(lock, next->next_attempt_at);
                continue;
            }
            job = std::move(*next);
            jobs_.erase(next);
        }

        if (service_->ProcessClientOffboardingJob(job)) {
            CompleteJob(job);
            continue;
        }

        ++job.retry_count;
        MasterMetricManager::instance().inc_client_offboarding_retry();
        const bool alert = ShouldAlert(job.retry_count);
        if (alert) {
            MasterMetricManager::instance().inc_client_offboarding_alert();
        }
        LOG(ERROR) << "client_id=" << job.client_id
                   << ", action=client_offboarding_retry"
                   << ", retry_count=" << job.retry_count
                   << ", alert=" << (alert ? "true" : "false")
                   << ", pending_prepare_segments="
                   << job.pending_prepare_segments.size()
                   << ", prepared_segments=" << job.prepared_segments.size()
                   << ", metadata_cleanup_accepted="
                   << (job.metadata_cleanup_accepted ? "true" : "false");

        bool requeued = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (running_) {
                job.next_attempt_at = std::chrono::steady_clock::now() +
                                      RetryDelay(job.retry_count);
                jobs_.push_back(std::move(job));
                requeued = true;
            }
        }
        if (requeued) {
            cv_.notify_one();
        } else {
            DropJob(job, "master_stopping_after_attempt");
        }
    }
    LOG(INFO) << "Client offboarding worker stopped";
}

}  // namespace mooncake
