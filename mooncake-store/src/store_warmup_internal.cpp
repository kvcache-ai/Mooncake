#include "store_warmup_internal.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <stdexcept>
#include <utility>

namespace mooncake::internal {
namespace {

uint64_t UintFromEnv(const char* name, uint64_t default_value, bool allow_zero,
                     uint64_t max_value) {
    const char* env = std::getenv(name);
    if (!env || env[0] == '\0') return default_value;

    try {
        if (env[0] == '-') throw std::invalid_argument("negative value");
        size_t pos = 0;
        const uint64_t value = std::stoull(env, &pos);
        if (pos != std::strlen(env) || (!allow_zero && value == 0) ||
            value > max_value) {
            LOG(WARNING) << "Invalid " << name << "='" << env << "', using "
                         << default_value;
            return default_value;
        }
        return value;
    } catch (const std::exception& e) {
        LOG(WARNING) << "Invalid " << name << "='" << env << "': " << e.what()
                     << ", using " << default_value;
        return default_value;
    }
}

}  // namespace

StoreWarmupOptions LoadStoreWarmupOptions() {
    StoreWarmupOptions options;
    options.read_size = static_cast<size_t>(
        UintFromEnv("MC_STORE_WARMUP_READ_SIZE", kDefaultWarmupReadSize, false,
                    kMaxWarmupReadSize));
    options.timeout = std::chrono::milliseconds(
        UintFromEnv("MC_STORE_WARMUP_TIMEOUT_MS", kDefaultWarmupTimeoutMs,
                    false, kMaxWarmupTimeoutMs));
    options.max_targets = static_cast<size_t>(
        UintFromEnv("MC_STORE_WARMUP_MAX_TARGETS", kDefaultWarmupMaxTargets,
                    true, kMaxWarmupTargets));

    const uint64_t requested_concurrency =
        UintFromEnv("MC_STORE_WARMUP_CONCURRENCY", kDefaultWarmupConcurrency,
                    false, std::numeric_limits<uint64_t>::max());
    options.concurrency = static_cast<size_t>(
        std::min<uint64_t>(requested_concurrency, kMaxWarmupConcurrency));
    if (requested_concurrency > kMaxWarmupConcurrency) {
        LOG(WARNING) << "MC_STORE_WARMUP_CONCURRENCY=" << requested_concurrency
                     << " exceeds max " << kMaxWarmupConcurrency << ", clamped";
    }
    return options;
}

uint64_t NormalizeWarmupMaxTargets(uint64_t requested) {
    if (requested == 0) return kMaxWarmupTargetsPerRequest;
    return std::min(requested, kMaxWarmupTargetsPerRequest);
}

WarmupBatchCleanup::WarmupBatchCleanup(PollBatchFn poll_batch,
                                       ReleaseBatchFn release_batch)
    : poll_batch_(std::move(poll_batch)),
      release_batch_(std::move(release_batch)) {}

WarmupBatchCleanup::~WarmupBatchCleanup() {
    if (!ShutdownFor(kWarmupShutdownTimeout)) {
        LOG(FATAL) << "warmup: cleanup destroyed with "
                   << PendingTransferCount()
                   << " non-terminal transfer(s); refusing unsafe teardown";
    }
}

bool WarmupBatchCleanup::TryFreeWarmupBatchID(
    PendingWarmupBatchIDRelease& batch) {
    ++batch.release_attempts;
    auto status = release_batch_(batch.batch_id);
    if (status.ok()) return true;

    if (batch.release_attempts == 1 || batch.release_attempts % 100 == 0) {
        LOG(WARNING) << "warmup: freeBatchID failed for '" << batch.segment_name
                     << "' on attempt " << batch.release_attempts << ": "
                     << status.message()
                     << "; retaining lightweight batch ID for retry";
    }
    return false;
}

bool WarmupBatchCleanup::ReleaseOrTrack(PendingWarmupBatch batch) {
    if (!batch.transfer_terminal) {
        TrackPendingTransfer(std::move(batch));
        return false;
    }

    // A terminal transfer can no longer write its destination. Release the
    // potentially large probe allocation before retrying batch ID cleanup.
    batch.buffer.reset();
    PendingWarmupBatchIDRelease batch_id_release{batch.batch_id,
                                                 std::move(batch.segment_name)};
    if (TryFreeWarmupBatchID(batch_id_release)) return true;

    std::lock_guard<std::mutex> lock(mutex_);
    pending_batch_id_releases_.emplace_back(std::move(batch_id_release));
    if (!worker_.joinable()) {
        worker_ = std::thread([this]() { WorkerMain(); });
    }
    cv_.notify_all();
    return false;
}

void WarmupBatchCleanup::TrackPendingTransfer(PendingWarmupBatch batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_transfers_.emplace_back(std::move(batch));
    if (!worker_.joinable()) {
        worker_ = std::thread([this]() { WorkerMain(); });
    }
    cv_.notify_all();
}

bool WarmupBatchCleanup::DrainFor(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [this]() {
        return pending_transfers_.empty() &&
               pending_batch_id_releases_.empty() &&
               processing_transfers_ == 0 && processing_batch_id_releases_ == 0;
    });
}

size_t WarmupBatchCleanup::PendingCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_transfers_.size() + pending_batch_id_releases_.size() +
           processing_transfers_ + processing_batch_id_releases_;
}

size_t WarmupBatchCleanup::PendingTransferCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_transfers_.size() + processing_transfers_;
}

size_t WarmupBatchCleanup::PendingBatchIDReleaseCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_batch_id_releases_.size() + processing_batch_id_releases_;
}

bool WarmupBatchCleanup::ShutdownFor(std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopped_) return true;
    if (!worker_.joinable()) {
        stopped_ = true;
        return true;
    }

    const bool safe_to_stop = cv_.wait_until(lock, deadline, [this]() {
        return pending_transfers_.empty() && processing_transfers_ == 0 &&
               processing_batch_id_releases_ == 0;
    });
    if (!safe_to_stop) return false;

    stop_requested_ = true;
    cv_.notify_all();
    lock.unlock();
    worker_.join();
    lock.lock();
    stopped_ = true;
    return true;
}

void WarmupBatchCleanup::WorkerMain() {
    constexpr auto kRetryInterval = std::chrono::milliseconds(10);
    while (true) {
        bool made_progress = false;
        std::optional<PendingWarmupBatch> transfer;
        std::optional<PendingWarmupBatchIDRelease> batch_id_release;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() {
                return stop_requested_ || !pending_transfers_.empty() ||
                       !pending_batch_id_releases_.empty();
            });
            if (stop_requested_) {
                if (!pending_batch_id_releases_.empty()) {
                    LOG(ERROR) << "warmup: abandoning "
                               << pending_batch_id_releases_.size()
                               << " batch ID release(s) at final teardown; "
                                  "Transfer Engine teardown must reclaim them";
                    pending_batch_id_releases_.clear();
                }
                return;
            }

            // Claim at most one item from each queue per round so a transfer
            // that remains pending cannot starve lightweight batch-ID retries.
            if (!pending_transfers_.empty()) {
                transfer.emplace(std::move(pending_transfers_.front()));
                pending_transfers_.pop_front();
                ++processing_transfers_;
            }
            if (!pending_batch_id_releases_.empty()) {
                batch_id_release.emplace(
                    std::move(pending_batch_id_releases_.front()));
                pending_batch_id_releases_.pop_front();
                ++processing_batch_id_releases_;
            }
        }

        if (transfer) {
            const auto state = poll_batch_(transfer->batch_id);
            std::optional<PendingWarmupBatchIDRelease> retry_batch_id;
            if (state == WarmupBatchState::kTerminal) {
                made_progress = true;
                transfer->buffer.reset();
                PendingWarmupBatchIDRelease release{
                    transfer->batch_id, std::move(transfer->segment_name)};
                if (!TryFreeWarmupBatchID(release)) {
                    retry_batch_id.emplace(std::move(release));
                }
            } else if (state == WarmupBatchState::kStatusError) {
                ++transfer->status_error_attempts;
                if (transfer->status_error_attempts == 1 ||
                    transfer->status_error_attempts % 100 == 0) {
                    LOG(WARNING)
                        << "warmup: failed to query transfer status for '"
                        << transfer->segment_name << "', batch "
                        << transfer->batch_id
                        << ", attempts=" << transfer->status_error_attempts
                        << "; retaining probe buffer because transfer "
                           "termination cannot be confirmed";
                }
            } else {
                transfer->status_error_attempts = 0;
            }

            {
                std::lock_guard<std::mutex> lock(mutex_);
                --processing_transfers_;
                if (state != WarmupBatchState::kTerminal) {
                    pending_transfers_.emplace_back(std::move(*transfer));
                } else if (retry_batch_id) {
                    pending_batch_id_releases_.emplace_back(
                        std::move(*retry_batch_id));
                }
                cv_.notify_all();
            }
        }

        if (batch_id_release) {
            const bool released = TryFreeWarmupBatchID(*batch_id_release);
            made_progress = made_progress || released;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                --processing_batch_id_releases_;
                if (!released) {
                    pending_batch_id_releases_.emplace_back(
                        std::move(*batch_id_release));
                }
                cv_.notify_all();
            }
        }

        if (!made_progress) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, kRetryInterval,
                         [this]() { return stop_requested_; });
        }
    }
}

}  // namespace mooncake::internal
