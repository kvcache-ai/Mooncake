#include "ha/oplog/ordered_oplog_writer.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ha/oplog/oplog_test_failpoint.h"
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
#include "ha_metric_manager.h"
#endif

namespace mooncake {

struct OrderedOpLogWriter::Impl {
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    using Clock = std::chrono::steady_clock;
#endif

    struct PendingEntry {
        OpLogEntry entry;
        DurableCallback callback;
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
        Clock::time_point committed_at{};
        Clock::time_point durable_at{};
#endif
    };

    explicit Impl(OrderedOpLogWriterConfig config, WriteBatchFn write_batch)
        : config(std::move(config)), write_batch(std::move(write_batch)) {
        if (this->config.max_entries_per_batch == 0) {
            this->config.max_entries_per_batch = 1;
        }
        if (this->config.initial_durable_prefix.last_seq == UINT64_MAX ||
            this->config.initial_durable_prefix.batch_id == UINT64_MAX) {
            accepting = false;
            last_error = ErrorCode::INVALID_PARAMS;
            return;
        }
        next_sequence_id = this->config.initial_durable_prefix.last_seq + 1;
    }

    void SealCommittedEntriesIfIdle() {
        if (batch_busy || committed_entries.empty()) {
            return;
        }
        const size_t count =
            std::min(committed_entries.size(), config.max_entries_per_batch);
        ready_entries.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            ready_entries.push_back(std::move(committed_entries.front()));
            committed_entries.pop_front();
        }
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
        HAMetricManager::instance().set_batch_record_committed_queue_depth(
            committed_entries.size() + ready_entries.size());
#endif
        open_waiting_slots -= count;
        batch_busy = true;
    }

    OrderedOpLogWriterConfig config;
    WriteBatchFn write_batch;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool accepting{true};
    bool running{false};
    bool stop_requested{false};
    bool callback_stop_requested{false};
    ErrorCode last_error{ErrorCode::OK};
    uint64_t next_reservation_id{1};
    uint64_t next_sequence_id{1};
    DurablePrefix durable_prefix{config.initial_durable_prefix};
    size_t open_waiting_slots{0};
    std::unordered_set<uint64_t> active_reservations;
    std::deque<PendingEntry> committed_entries;
    std::vector<PendingEntry> ready_entries;
    bool batch_busy{false};
    std::deque<PendingEntry> callback_entries;
    std::thread writer_thread;
    std::thread callback_thread;
};

OrderedOpLogWriter::Reservation::Reservation() = default;

OrderedOpLogWriter::Reservation::Reservation(OrderedOpLogWriter* writer,
                                             uint64_t id)
    : writer_(writer), id_(id) {}

OrderedOpLogWriter::Reservation::Reservation(Reservation&& other) noexcept
    : writer_(other.writer_), id_(other.id_) {
    other.writer_ = nullptr;
    other.id_ = 0;
}

OrderedOpLogWriter::Reservation& OrderedOpLogWriter::Reservation::operator=(
    Reservation&& other) noexcept {
    if (this != &other) {
        if (writer_ != nullptr) {
            writer_->Abort(std::move(*this));
        }
        writer_ = other.writer_;
        id_ = other.id_;
        other.writer_ = nullptr;
        other.id_ = 0;
    }
    return *this;
}

OrderedOpLogWriter::Reservation::~Reservation() {
    if (writer_ != nullptr) {
        writer_->Abort(std::move(*this));
    }
}

OrderedOpLogWriter::PendingHandle::PendingHandle() = default;

OrderedOpLogWriter::PendingHandle::PendingHandle(uint64_t sequence_id)
    : sequence_id_(sequence_id) {}

uint64_t OrderedOpLogWriter::PendingHandle::sequence_id() const {
    return sequence_id_;
}

OrderedOpLogWriter::OrderedOpLogWriter(OrderedOpLogWriterConfig config,
                                       WriteBatchFn write_batch)
    : impl_(std::make_unique<Impl>(std::move(config), std::move(write_batch))) {
}

OrderedOpLogWriter::~OrderedOpLogWriter() { Stop(); }

tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
OrderedOpLogWriter::Reserve() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (!impl_->accepting) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (impl_->open_waiting_slots >= impl_->config.max_entries_per_batch) {
        return tl::make_unexpected(ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    }
    const uint64_t id = impl_->next_reservation_id++;
    impl_->active_reservations.insert(id);
    ++impl_->open_waiting_slots;
    return Reservation(this, id);
}

tl::expected<OrderedOpLogWriter::PendingHandle, ErrorCode>
OrderedOpLogWriter::Commit(Reservation&& reservation, OpLogEntry entry,
                           DurableCallback callback) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (reservation.writer_ != this ||
        impl_->active_reservations.erase(reservation.id_) == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (impl_->stop_requested) {
        --impl_->open_waiting_slots;
        reservation.writer_ = nullptr;
        reservation.id_ = 0;
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::string reason;
    if (!ValidateOpLogBatchEntry(entry, &reason)) {
        --impl_->open_waiting_slots;
        reservation.writer_ = nullptr;
        reservation.id_ = 0;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    reservation.writer_ = nullptr;
    reservation.id_ = 0;
    entry.timestamp_ms = 0;
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    entry.prefix_hash = 0;
    entry.sequence_id = impl_->next_sequence_id++;
    const uint64_t sequence_id = entry.sequence_id;
    Impl::PendingEntry pending{.entry = std::move(entry),
                               .callback = std::move(callback)};
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    pending.committed_at = Impl::Clock::now();
#endif
    impl_->committed_entries.push_back(std::move(pending));
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    HAMetricManager::instance().set_batch_record_committed_queue_depth(
        impl_->committed_entries.size() + impl_->ready_entries.size());
#endif
    impl_->SealCommittedEntriesIfIdle();
    impl_->cv.notify_all();
    return PendingHandle(sequence_id);
}

void OrderedOpLogWriter::Abort(Reservation&& reservation) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (reservation.writer_ == this &&
        impl_->active_reservations.erase(reservation.id_) != 0) {
        --impl_->open_waiting_slots;
    }
    reservation.writer_ = nullptr;
    reservation.id_ = 0;
}

bool OrderedOpLogWriter::IsAccepting() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->accepting;
}

ErrorCode OrderedOpLogWriter::LastError() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->last_error;
}

void OrderedOpLogWriter::Start() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->running || impl_->stop_requested) {
        return;
    }
    impl_->callback_stop_requested = false;
    impl_->running = true;
    impl_->callback_thread = std::thread([this] {
        while (true) {
            Impl::PendingEntry callback_entry;
            {
                std::unique_lock<std::mutex> lock(impl_->mutex);
                impl_->cv.wait(lock, [&] {
                    return impl_->callback_stop_requested ||
                           !impl_->callback_entries.empty();
                });
                if (impl_->callback_stop_requested &&
                    impl_->callback_entries.empty()) {
                    return;
                }
                callback_entry = std::move(impl_->callback_entries.front());
                impl_->callback_entries.pop_front();
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                HAMetricManager::instance()
                    .set_batch_record_callback_queue_depth(
                        impl_->callback_entries.size());
#endif
            }
            if (callback_entry.callback) {
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                const auto callback_started_at = Impl::Clock::now();
                const auto latency_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        callback_started_at - callback_entry.durable_at)
                        .count();
                HAMetricManager::instance()
                    .observe_batch_record_callback_latency_us(latency_us);
#endif
                callback_entry.callback(callback_entry.entry);
            }
        }
    });
    impl_->writer_thread = std::thread([this] {
        while (true) {
            std::vector<Impl::PendingEntry> entries;
            DurablePrefix expected_prefix;
            {
                std::unique_lock<std::mutex> lock(impl_->mutex);
                impl_->cv.wait(lock, [&] {
                    return impl_->stop_requested ||
                           !impl_->ready_entries.empty();
                });
                if (impl_->stop_requested && impl_->ready_entries.empty()) {
                    return;
                }
                entries = std::move(impl_->ready_entries);
                impl_->ready_entries.clear();
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                HAMetricManager::instance()
                    .set_batch_record_committed_queue_depth(
                        impl_->committed_entries.size());
#endif
                expected_prefix = impl_->durable_prefix;
            }

            OpLogBatchRecord batch;
            batch.batch_id = expected_prefix.batch_id + 1;
            batch.first_seq = entries.front().entry.sequence_id;
            batch.last_seq = entries.back().entry.sequence_id;
            batch.entries.reserve(entries.size());
            for (auto& entry : entries) {
                batch.entries.push_back(std::move(entry.entry));
            }

            while (true) {
                TestFailPoint::Wait("batch_before_txn");
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                const auto txn_started_at = Impl::Clock::now();
#endif
                ErrorCode err = impl_->write_batch(batch, expected_prefix);
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                const auto txn_latency_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        Impl::Clock::now() - txn_started_at)
                        .count();
                HAMetricManager::instance().observe_batch_record_txn_latency_us(
                    txn_latency_us);
#endif
                if (err == ErrorCode::OK) {
                    TestFailPoint::Wait("batch_txn_succeeded_before_callback");
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                    const auto durable_at = Impl::Clock::now();
                    for (const auto& entry : entries) {
                        HAMetricManager::instance()
                            .observe_batch_record_commit_to_durable_us(
                                std::chrono::duration_cast<
                                    std::chrono::microseconds>(
                                    durable_at - entry.committed_at)
                                    .count());
                    }
#endif
                    {
                        std::lock_guard<std::mutex> lock(impl_->mutex);
                        impl_->durable_prefix = {.batch_id = batch.batch_id,
                                                 .last_seq = batch.last_seq};
                        impl_->last_error = ErrorCode::OK;
                        impl_->accepting = !impl_->stop_requested;
                        for (size_t i = 0; i < entries.size(); ++i) {
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                            entries[i].durable_at = durable_at;
#endif
                            entries[i].entry = batch.entries[i];
                            impl_->callback_entries.push_back(
                                std::move(entries[i]));
                        }
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                        auto& metrics = HAMetricManager::instance();
                        metrics.inc_batch_record_durable_batches();
                        metrics.inc_batch_record_durable_entries(
                            batch.entries.size());
                        metrics.observe_batch_record_batch_entries(
                            batch.entries.size());
                        metrics.set_batch_record_last_batch_id(batch.batch_id);
                        metrics.set_batch_record_durable_sequence(
                            batch.last_seq);
                        metrics.set_batch_record_callback_queue_depth(
                            impl_->callback_entries.size());
#endif
                        impl_->batch_busy = false;
                        impl_->SealCommittedEntriesIfIdle();
                    }
                    impl_->cv.notify_all();
                    break;
                }

                {
                    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                    HAMetricManager::instance().inc_batch_record_retries();
#endif
                    impl_->last_error = err;
                    impl_->accepting = false;
                    if (impl_->stop_requested) {
                        return;
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    });
}

void OrderedOpLogWriter::Stop() {
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        impl_->accepting = false;
        impl_->stop_requested = true;
        if (!impl_->running) {
            return;
        }
    }
    impl_->cv.notify_all();
    if (impl_->writer_thread.joinable()) {
        impl_->writer_thread.join();
    }
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        impl_->callback_stop_requested = true;
    }
    impl_->cv.notify_all();
    if (impl_->callback_thread.joinable()) {
        impl_->callback_thread.join();
    }
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->running = false;
}

}  // namespace mooncake
