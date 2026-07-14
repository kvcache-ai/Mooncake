#include "store_warmup_internal.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <stdexcept>

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

WarmupBatchCleanup::WarmupBatchCleanup(PollBatchFn poll_batch,
                                       ReleaseBatchFn release_batch)
    : poll_batch_(std::move(poll_batch)),
      release_batch_(std::move(release_batch)) {}

WarmupBatchCleanup::~WarmupBatchCleanup() { Shutdown(); }

bool WarmupBatchCleanup::TryReleaseWarmupBatch(PendingWarmupBatch& batch) {
    if (!batch.transfer_terminal) return false;

    ++batch.release_attempts;
    auto status = release_batch_(batch.batch_id);
    if (status.ok()) return true;

    if (batch.release_attempts == 1 || batch.release_attempts % 100 == 0) {
        LOG(WARNING) << "warmup: freeBatchID failed for '" << batch.segment_name
                     << "' on attempt " << batch.release_attempts << ": "
                     << status.message()
                     << "; retaining batch and probe buffer for retry";
    }
    return false;
}

bool WarmupBatchCleanup::ReleaseOrTrack(PendingWarmupBatch batch) {
    if (TryReleaseWarmupBatch(batch)) return true;
    Track(std::move(batch));
    return false;
}

void WarmupBatchCleanup::Track(PendingWarmupBatch batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.emplace_back(std::move(batch));
    if (!worker_.joinable()) {
        worker_ = std::thread([this]() { WorkerMain(); });
    }
    cv_.notify_all();
}

bool WarmupBatchCleanup::DrainFor(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [this]() { return pending_.empty(); });
}

size_t WarmupBatchCleanup::PendingCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_.size();
}

void WarmupBatchCleanup::Shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!worker_.joinable()) return;
        stopping_ = true;
    }
    cv_.notify_all();
    worker_.join();
}

void WarmupBatchCleanup::WorkerMain() {
    constexpr auto kRetryInterval = std::chrono::milliseconds(10);
    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
        if (pending_.empty()) {
            if (stopping_) return;
            cv_.wait(lock, [this]() { return stopping_ || !pending_.empty(); });
            continue;
        }

        cv_.wait_for(lock, kRetryInterval);
        for (auto it = pending_.begin(); it != pending_.end();) {
            if (!it->transfer_terminal) {
                const auto state = poll_batch_(it->batch_id);
                if (state == WarmupBatchState::kTerminal) {
                    it->transfer_terminal = true;
                }
            }

            if (it->transfer_terminal && TryReleaseWarmupBatch(*it)) {
                it = pending_.erase(it);
                cv_.notify_all();
            } else {
                ++it;
            }
        }
    }
}

}  // namespace mooncake::internal
