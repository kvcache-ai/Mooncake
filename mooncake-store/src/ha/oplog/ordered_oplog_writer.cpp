#include "ha/oplog/ordered_oplog_writer.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <asio/steady_timer.hpp>
#include <asio/thread_pool.hpp>
#include <async_simple/Promise.h>
#include <glog/logging.h>

#include "ha/oplog/oplog_test_failpoint.h"
#include "ha_metric_manager.h"

namespace mooncake {

namespace {

thread_local const void* current_writer_worker = nullptr;

struct WriterWorkerScope {
    const void* previous;
    explicit WriterWorkerScope(const void* current)
        : previous(current_writer_worker) {
        current_writer_worker = current;
    }
    ~WriterWorkerScope() { current_writer_worker = previous; }
};

// Wraps asio::thread_pool(0) with explicit worker threads so a partial thread
// creation failure stops the scheduler before joining already-started threads
// (standalone asio::thread_pool(n) unwinds thread_group::join() without
// stopping the scheduler).
class DurableWaitTimerPool {
   public:
    using Executor = asio::thread_pool::executor_type;
    using Timer =
        asio::basic_waitable_timer<std::chrono::steady_clock,
                                   asio::wait_traits<std::chrono::steady_clock>,
                                   Executor>;

    explicit DurableWaitTimerPool(size_t thread_count) : pool_(0) {
        threads_.reserve(thread_count);
        try {
            for (size_t i = 0; i < thread_count; ++i) {
                threads_.emplace_back([this] { pool_.attach(); });
            }
        } catch (...) {
            Stop();
            throw;
        }
    }

    ~DurableWaitTimerPool() { Stop(); }

    DurableWaitTimerPool(const DurableWaitTimerPool&) = delete;
    DurableWaitTimerPool& operator=(const DurableWaitTimerPool&) = delete;

    Executor get_executor() noexcept { return pool_.get_executor(); }

    void Stop() {
        pool_.stop();
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        threads_.clear();
    }

   private:
    asio::thread_pool pool_;
    std::vector<std::thread> threads_;
};

std::optional<std::chrono::steady_clock::time_point> ComputeWaitDeadline(
    std::chrono::milliseconds timeout) {
    if (timeout == std::chrono::milliseconds::max()) {
        return std::nullopt;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto max_time = std::chrono::steady_clock::time_point::max();
    if (now >= std::chrono::steady_clock::time_point{}) {
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(max_time -
                                                                  now);
        if (timeout >= remaining) {
            return std::nullopt;
        }
    }
    const auto max_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::duration::max());
    if (timeout >= max_ms) {
        return std::nullopt;
    }
    return now + timeout;
}

}  // namespace

struct OrderedOpLogWriter::Impl {
    static constexpr size_t kTimerPoolThreads = 4;

    struct TimedWaiter;
    using UntimedWaiters =
        std::multimap<uint64_t, async_simple::Promise<ErrorCode>>;
    using TimedWaiters = std::multimap<uint64_t, std::shared_ptr<TimedWaiter>>;
    struct TimedWaiter {
        async_simple::Promise<ErrorCode> promise;
        // Constructed, armed, cancelled, and destroyed exclusively under mutex.
        std::optional<DurableWaitTimerPool::Timer> timer;
        // Protected by mutex; position is valid only while pending is true.
        TimedWaiters::iterator position;
        bool pending{false};
    };
    struct TakenWaiters {
        UntimedWaiters untimed;
        TimedWaiters timed;
    };

#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    using Clock = std::chrono::steady_clock;
#endif

    struct PendingEntry {
        OpLogEntry entry;
        DurableCallback callback;
        // Entries committed together via CommitBatch share a non-zero group
        // id and must be sealed into the same batch record.
        uint64_t group{0};
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
        Clock::time_point committed_at{};
        Clock::time_point durable_at{};
#endif
    };

    explicit Impl(OrderedOpLogWriterConfig config, WriteBatchFn write_batch,
                  TerminalCallback terminal_callback)
        : config(std::move(config)),
          write_batch(std::move(write_batch)),
          terminal_callback(std::move(terminal_callback)) {
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

    void OnWaitTimeout(const std::weak_ptr<TimedWaiter>& weak_waiter) {
        WriterWorkerScope worker_scope(this);
        TestFailPoint::Wait("durable_wait_before_timeout");
        std::shared_ptr<TimedWaiter> timed_out;
        ErrorCode error = ErrorCode::PERSISTENT_FAIL;
        {
            std::lock_guard<std::mutex> lock(mutex);
            timed_out = weak_waiter.lock();
            if (!timed_out || !timed_out->pending) return;
            timed_out->pending = false;
            timed_out->timer.reset();
            timed_waiters.erase(timed_out->position);
            error = last_error == ErrorCode::OK ? ErrorCode::PERSISTENT_FAIL
                                                : last_error;
        }
        timed_out->promise.setValue(ErrorCode{error});
    }

    void PublishRuntime(bool activate = false) {
        if (!activate && runtime_owner == 0) return;
        HAMetricManager::WriterRuntimeSnapshot snapshot;
        snapshot.accepting = accepting;
        snapshot.retry_count = retry_count;
        snapshot.retry_delay_ms = retry_delay_ms;
        snapshot.waiting_slots = open_waiting_slots;
        snapshot.committed_queue_depth =
            committed_entries.size() + ready_entries.size();
        snapshot.callback_queue_depth = callback_entries.size();
        snapshot.durable_batch_id = durable_prefix.batch_id;
        snapshot.durable_sequence = durable_prefix.last_seq;
        snapshot.last_error = static_cast<int64_t>(last_error);
        snapshot.terminal_reason = terminal_reason;
        snapshot.stuck_range = stuck_range;
        if (activate) {
            runtime_owner =
                HAMetricManager::instance().activate_writer_runtime(snapshot);
        } else {
            HAMetricManager::instance().update_writer_runtime(runtime_owner,
                                                              snapshot);
        }
    }

    void SealCommittedEntriesIfIdle() {
        if (batch_busy || committed_entries.empty()) {
            return;
        }
        size_t count =
            std::min(committed_entries.size(), config.max_entries_per_batch);
        // Never split a commit group across records: if the cap lands in the
        // middle of a group, extend the seal to the end of that group. The
        // cap is a batching efficiency knob; a group's atomicity is not.
        while (count < committed_entries.size() &&
               committed_entries[count].group != 0 &&
               committed_entries[count].group ==
                   committed_entries[count - 1].group) {
            ++count;
        }
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

    // Called with mutex held. Extracting nodes avoids allocating while
    // advancing the prefix and leaves later waiters untouched.
    TakenWaiters TakeCoveredWaiters() {
        TakenWaiters covered;
        auto untimed_end = untimed_waiters.upper_bound(durable_prefix.last_seq);
        for (auto it = untimed_waiters.begin(); it != untimed_end;) {
            covered.untimed.insert(untimed_waiters.extract(it++));
        }
        auto timed_end = timed_waiters.upper_bound(durable_prefix.last_seq);
        for (auto it = timed_waiters.begin(); it != timed_end;) {
            auto current = it++;
            current->second->pending = false;
            current->second->timer.reset();
            covered.timed.insert(timed_waiters.extract(current));
        }
        return covered;
    }

    TakenWaiters TakeAllWaiters() {
        TakenWaiters waiters;
        waiters.untimed.swap(untimed_waiters);
        for (auto& waiter : timed_waiters) {
            waiter.second->pending = false;
            waiter.second->timer.reset();
        }
        waiters.timed.swap(timed_waiters);
        return waiters;
    }

    // A continuation may run inline and reenter the writer, so never call
    // this while holding mutex. Merge both maps to preserve sequence order;
    // for identical sequences, untimed waiters complete before timed waiters
    // while registration order within each map is preserved.
    static void CompleteWaiters(TakenWaiters waiters, ErrorCode error) {
        auto untimed_it = waiters.untimed.begin();
        auto timed_it = waiters.timed.begin();
        while (untimed_it != waiters.untimed.end() &&
               timed_it != waiters.timed.end()) {
            if (untimed_it->first <= timed_it->first) {
                untimed_it->second.setValue(ErrorCode{error});
                ++untimed_it;
            } else {
                timed_it->second->promise.setValue(ErrorCode{error});
                ++timed_it;
            }
        }
        for (; untimed_it != waiters.untimed.end(); ++untimed_it) {
            untimed_it->second.setValue(ErrorCode{error});
        }
        for (; timed_it != waiters.timed.end(); ++timed_it) {
            timed_it->second->promise.setValue(ErrorCode{error});
        }
    }

    void EnterTerminalState(ErrorCode error,
                            OrderedOpLogWriterTerminalReason reason) {
        TerminalCallback callback;
        std::optional<OrderedOpLogWriterTerminalState> state;
        TakenWaiters waiters;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (stop_requested || terminal_state.has_value()) {
                return;
            }
            terminal_state = {
                .error = error,
                .reason = reason,
                .durable_prefix = durable_prefix,
                .occurred_at_ms = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count())};
            accepting = false;
            last_error = error;
            retry_delay_ms = 0;
            terminal_reason =
                reason == OrderedOpLogWriterTerminalReason::kRetryTimeout
                    ? "retry_timeout"
                : reason == OrderedOpLogWriterTerminalReason::kFenced
                    ? "fenced"
                    : "non_retryable_write_error";
            PublishRuntime();
            callback = terminal_callback;
            state = terminal_state;
            waiters = TakeAllWaiters();
        }
        cv.notify_all();
        CompleteWaiters(std::move(waiters), error);
        if (callback) {
            callback(*state);
        }
    }

    OrderedOpLogWriterConfig config;
    WriteBatchFn write_batch;
    TerminalCallback terminal_callback;
    std::optional<OrderedOpLogWriterTerminalState> terminal_state;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool accepting{true};
    bool running{false};
    bool stop_requested{false};
    bool callback_stop_requested{false};
    bool joining{false};
    bool joined{false};
    std::thread::id join_owner;
    ErrorCode last_error{ErrorCode::OK};
    uint64_t retry_count{0};
    uint64_t runtime_owner{0};
    uint64_t retry_delay_ms{0};
    std::string terminal_reason;
    std::optional<std::pair<uint64_t, uint64_t>> stuck_range;
    uint64_t next_reservation_id{1};
    uint64_t next_sequence_id{1};
    uint64_t next_group_id{1};
    DurablePrefix durable_prefix{config.initial_durable_prefix};
    UntimedWaiters untimed_waiters;
    TimedWaiters timed_waiters;
    size_t open_waiting_slots{0};
    std::unordered_set<uint64_t> active_reservations;
    std::deque<PendingEntry> committed_entries;
    std::vector<PendingEntry> ready_entries;
    bool batch_busy{false};
    std::deque<PendingEntry> callback_entries;
    std::thread writer_thread;
    std::thread callback_thread;
    // Lock order: timer_pool_init_mutex -> mutex. Serializes first-time lazy
    // creation of timer_pool without holding mutex across thread startup.
    std::mutex timer_pool_init_mutex;
    std::unique_ptr<DurableWaitTimerPool> timer_pool;
};

namespace {

bool IsRetryableWriteError(ErrorCode error) {
    return error == ErrorCode::ETCD_OPERATION_ERROR ||
           error == ErrorCode::ETCD_CTX_CANCELLED ||
           error == ErrorCode::PERSISTENT_FAIL;
}

}  // namespace

OrderedOpLogWriter::Reservation::Reservation() = default;

OrderedOpLogWriter::Reservation::Reservation(OrderedOpLogWriter* writer,
                                             uint64_t id, uint64_t slots)
    : writer_(writer), id_(id), slots_(slots) {}

OrderedOpLogWriter::Reservation::Reservation(Reservation&& other) noexcept
    : writer_(other.writer_), id_(other.id_), slots_(other.slots_) {
    other.writer_ = nullptr;
    other.id_ = 0;
    other.slots_ = 0;
}

OrderedOpLogWriter::Reservation& OrderedOpLogWriter::Reservation::operator=(
    Reservation&& other) noexcept {
    if (this != &other) {
        if (writer_ != nullptr) {
            writer_->Abort(std::move(*this));
        }
        writer_ = other.writer_;
        id_ = other.id_;
        slots_ = other.slots_;
        other.writer_ = nullptr;
        other.id_ = 0;
        other.slots_ = 0;
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
                                       WriteBatchFn write_batch,
                                       TerminalCallback terminal_callback)
    : impl_(std::make_unique<Impl>(std::move(config), std::move(write_batch),
                                   std::move(terminal_callback))) {}

void OrderedOpLogWriter::ActivateRuntimeMetrics() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->runtime_owner == 0) impl_->PublishRuntime(true);
}

OrderedOpLogWriter::~OrderedOpLogWriter() {
    CHECK(current_writer_worker != impl_.get())
        << "OrderedOpLogWriter cannot be destroyed on a writer-owned thread";
    Stop();
}

tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
OrderedOpLogWriter::Reserve() {
    return ReserveBatch(1);
}

tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
OrderedOpLogWriter::ReserveBatch(size_t entry_count) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->terminal_state.has_value()) {
        return tl::make_unexpected(impl_->terminal_state->error);
    }
    if (!impl_->accepting) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (entry_count == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (entry_count > impl_->config.max_entries_per_batch ||
        impl_->open_waiting_slots >
            impl_->config.max_entries_per_batch - entry_count) {
        return tl::make_unexpected(ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    }
    const uint64_t id = impl_->next_reservation_id++;
    impl_->active_reservations.insert(id);
    impl_->open_waiting_slots += entry_count;
    impl_->PublishRuntime();
    return Reservation(this, id, entry_count);
}

tl::expected<OrderedOpLogWriter::PendingHandle, ErrorCode>
OrderedOpLogWriter::Commit(Reservation&& reservation, OpLogEntry entry,
                           DurableCallback callback) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (reservation.writer_ != this ||
        impl_->active_reservations.erase(reservation.id_) == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto reject_reservation =
        [&](ErrorCode error) -> tl::expected<PendingHandle, ErrorCode> {
        impl_->open_waiting_slots -= reservation.slots_;
        reservation.writer_ = nullptr;
        reservation.id_ = 0;
        reservation.slots_ = 0;
        impl_->PublishRuntime();
        return tl::make_unexpected(error);
    };
    if (impl_->terminal_state.has_value()) {
        return reject_reservation(impl_->terminal_state->error);
    }
    if (impl_->stop_requested) {
        return reject_reservation(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::string reason;
    if (!ValidateOpLogBatchEntry(entry, &reason)) {
        return reject_reservation(ErrorCode::INVALID_PARAMS);
    }
    reservation.writer_ = nullptr;
    reservation.id_ = 0;
    reservation.slots_ = 0;
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
    impl_->PublishRuntime();
    impl_->cv.notify_all();
    return PendingHandle(sequence_id);
}

tl::expected<std::vector<OrderedOpLogWriter::PendingHandle>, ErrorCode>
OrderedOpLogWriter::CommitBatch(Reservation&& reservation,
                                std::vector<OpLogEntry> entries,
                                DurableCallback callback) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (reservation.writer_ != this ||
        impl_->active_reservations.erase(reservation.id_) == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto reject_reservation = [&](ErrorCode error)
        -> tl::expected<std::vector<PendingHandle>, ErrorCode> {
        impl_->open_waiting_slots -= reservation.slots_;
        reservation.writer_ = nullptr;
        reservation.id_ = 0;
        reservation.slots_ = 0;
        impl_->PublishRuntime();
        return tl::make_unexpected(error);
    };
    if (entries.empty() || entries.size() != reservation.slots_) {
        return reject_reservation(ErrorCode::INVALID_PARAMS);
    }
    if (impl_->terminal_state.has_value()) {
        return reject_reservation(impl_->terminal_state->error);
    }
    if (impl_->stop_requested) {
        return reject_reservation(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    for (const auto& entry : entries) {
        std::string reason;
        if (!ValidateOpLogBatchEntry(entry, &reason)) {
            return reject_reservation(ErrorCode::INVALID_PARAMS);
        }
    }
    reservation.writer_ = nullptr;
    reservation.id_ = 0;
    reservation.slots_ = 0;
    const uint64_t group = impl_->next_group_id++;
    std::vector<PendingHandle> handles;
    handles.reserve(entries.size());
    for (auto& entry : entries) {
        entry.timestamp_ms = 0;
        entry.checksum = ComputeOpLogChecksum(entry.payload);
        entry.prefix_hash = 0;
        entry.sequence_id = impl_->next_sequence_id++;
        handles.push_back(PendingHandle(entry.sequence_id));
        Impl::PendingEntry pending{.entry = std::move(entry),
                                   .callback = callback};
        pending.group = group;
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
        pending.committed_at = Impl::Clock::now();
#endif
        impl_->committed_entries.push_back(std::move(pending));
    }
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    HAMetricManager::instance().set_batch_record_committed_queue_depth(
        impl_->committed_entries.size() + impl_->ready_entries.size());
#endif
    impl_->SealCommittedEntriesIfIdle();
    impl_->PublishRuntime();
    impl_->cv.notify_all();
    return handles;
}

void OrderedOpLogWriter::Abort(Reservation&& reservation) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (reservation.writer_ == this &&
        impl_->active_reservations.erase(reservation.id_) != 0) {
        impl_->open_waiting_slots -= reservation.slots_;
    }
    reservation.writer_ = nullptr;
    reservation.id_ = 0;
    reservation.slots_ = 0;
    impl_->PublishRuntime();
}

async_simple::Future<ErrorCode> OrderedOpLogWriter::AwaitDurable(
    uint64_t sequence, std::chrono::milliseconds timeout) {
    if (timeout < std::chrono::milliseconds::zero()) {
        return async_simple::makeReadyFuture<ErrorCode>(
            ErrorCode::INVALID_PARAMS);
    }
    // Declared before lock so any unused spare pool is stopped and joined
    // outside impl_->mutex.
    std::unique_ptr<DurableWaitTimerPool> spare_pool;
    std::unique_lock<std::mutex> lock(impl_->mutex);
    auto check_ready = [&]() -> std::optional<async_simple::Future<ErrorCode>> {
        if (impl_->durable_prefix.last_seq >= sequence) {
            return async_simple::makeReadyFuture<ErrorCode>(ErrorCode::OK);
        }
        if (impl_->terminal_state.has_value()) {
            return async_simple::makeReadyFuture<ErrorCode>(
                ErrorCode{impl_->terminal_state->error});
        }
        if (impl_->stop_requested) {
            return async_simple::makeReadyFuture<ErrorCode>(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        if (timeout == std::chrono::milliseconds::zero()) {
            return async_simple::makeReadyFuture<ErrorCode>(
                impl_->last_error == ErrorCode::OK ? ErrorCode::PERSISTENT_FAIL
                                                   : impl_->last_error);
        }
        return std::nullopt;
    };
    if (auto ready = check_ready()) {
        return std::move(*ready);
    }
    const auto deadline = ComputeWaitDeadline(timeout);
    if (!deadline.has_value()) {
        async_simple::Promise<ErrorCode> promise;
        auto future = promise.getFuture();
        impl_->untimed_waiters.emplace(sequence, std::move(promise));
        return future;
    }
    if (!impl_->timer_pool) {
        lock.unlock();
        std::lock_guard<std::mutex> init_lock(impl_->timer_pool_init_mutex);
        lock.lock();
        if (auto ready = check_ready()) {
            return std::move(*ready);
        }
        if (!impl_->timer_pool) {
            lock.unlock();
            spare_pool =
                std::make_unique<DurableWaitTimerPool>(Impl::kTimerPoolThreads);
            lock.lock();
            if (auto ready = check_ready()) {
                return std::move(*ready);
            }
            impl_->timer_pool = std::move(spare_pool);
        }
    }
    auto waiter = std::make_shared<Impl::TimedWaiter>();
    auto future = waiter->promise.getFuture();
    waiter->position = impl_->timed_waiters.emplace(sequence, waiter);
    waiter->pending = true;
    const auto unregister = [this](Impl::TimedWaiter* pending) {
        pending->pending = false;
        pending->timer.reset();
        impl_->timed_waiters.erase(pending->position);
    };
    std::unique_ptr<Impl::TimedWaiter, decltype(unregister)> registration(
        waiter.get(), unregister);
    waiter->timer.emplace(impl_->timer_pool->get_executor(), *deadline);
    waiter->timer->async_wait(
        [impl = impl_.get(), weak_waiter = std::weak_ptr<Impl::TimedWaiter>(
                                 waiter)](const asio::error_code& ec) {
            if (ec == asio::error::operation_aborted) return;
            impl->OnWaitTimeout(weak_waiter);
        });
    registration.release();
    return future;
}

bool OrderedOpLogWriter::IsAccepting() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->accepting;
}

ErrorCode OrderedOpLogWriter::LastError() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->last_error;
}

std::optional<OrderedOpLogWriterTerminalState>
OrderedOpLogWriter::GetTerminalState() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->terminal_state;
}

void OrderedOpLogWriter::SetTerminalCallback(TerminalCallback callback) {
    std::optional<OrderedOpLogWriterTerminalState> terminal;
    TerminalCallback notify;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        impl_->terminal_callback = std::move(callback);
        notify = impl_->terminal_callback;
        terminal = impl_->terminal_state;
    }
    if (terminal && notify) {
        notify(*terminal);
    }
}

void OrderedOpLogWriter::Start() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->running || impl_->stop_requested) {
        return;
    }
    impl_->callback_stop_requested = false;
    impl_->running = true;
    impl_->callback_thread = std::thread([this] {
        current_writer_worker = impl_.get();
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
                impl_->PublishRuntime();
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
        current_writer_worker = impl_.get();
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
                impl_->stuck_range =
                    std::make_pair(expected_prefix.last_seq + 1,
                                   expected_prefix.last_seq + entries.size());
                impl_->PublishRuntime();
            }

            OpLogBatchRecord batch;
            batch.batch_id = expected_prefix.batch_id + 1;
            batch.first_seq = entries.front().entry.sequence_id;
            batch.last_seq = entries.back().entry.sequence_id;
            batch.entries.reserve(entries.size());
            for (auto& entry : entries) {
                batch.entries.push_back(std::move(entry.entry));
            }

            constexpr auto kInitialRetryDelay = std::chrono::milliseconds(1);
            constexpr auto kMaxRetryDelay = std::chrono::milliseconds(1000);
            auto retry_delay = kInitialRetryDelay;
            std::optional<std::chrono::steady_clock::time_point> retry_deadline;
            bool retry_attempt = false;

            while (true) {
                TestFailPoint::Wait("batch_before_txn");
                if (retry_attempt) {
                    std::optional<ErrorCode> timeout_error;
                    {
                        std::lock_guard<std::mutex> lock(impl_->mutex);
                        if (impl_->stop_requested) {
                            return;
                        }
                        if (retry_deadline.has_value() &&
                            std::chrono::steady_clock::now() >=
                                *retry_deadline) {
                            timeout_error = impl_->last_error;
                        }
                    }
                    if (timeout_error.has_value()) {
                        impl_->EnterTerminalState(
                            *timeout_error,
                            OrderedOpLogWriterTerminalReason::kRetryTimeout);
                        return;
                    }
                }
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
                    Impl::TakenWaiters waiters;
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
                        waiters = impl_->TakeCoveredWaiters();
                        impl_->last_error = ErrorCode::OK;
                        impl_->accepting = !impl_->stop_requested;
                        impl_->retry_delay_ms = 0;
                        impl_->stuck_range.reset();
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
                        impl_->PublishRuntime();
                    }
                    impl_->cv.notify_all();
                    Impl::CompleteWaiters(std::move(waiters), ErrorCode::OK);
                    break;
                }

                if (!IsRetryableWriteError(err)) {
                    const auto reason =
                        err == ErrorCode::ETCD_TRANSACTION_FAIL
                            ? OrderedOpLogWriterTerminalReason::kFenced
                            : OrderedOpLogWriterTerminalReason::
                                  kNonRetryableWriteError;
                    impl_->EnterTerminalState(err, reason);
                    return;
                }
                {
                    std::unique_lock<std::mutex> lock(impl_->mutex);
                    if (impl_->stop_requested) {
                        return;
                    }
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
                    HAMetricManager::instance().inc_batch_record_retries();
#endif
                    ++impl_->retry_count;
                    impl_->last_error = err;
                    impl_->accepting = false;
                    impl_->retry_delay_ms = retry_delay.count();
                    impl_->PublishRuntime();
                    if (!retry_deadline.has_value() &&
                        impl_->config.retry_timeout.count() > 0) {
                        retry_deadline = std::chrono::steady_clock::now() +
                                         impl_->config.retry_timeout;
                    }
                    if (impl_->stop_requested ||
                        impl_->cv.wait_for(lock, retry_delay, [this] {
                            return impl_->stop_requested;
                        })) {
                        return;
                    }
                }
                retry_delay = std::min(retry_delay * 2, kMaxRetryDelay);
                retry_attempt = true;
            }
        }
    });
}

void OrderedOpLogWriter::Stop() {
    bool running;
    std::unique_ptr<DurableWaitTimerPool> pool_to_stop;
    const bool worker = current_writer_worker == impl_.get();
    Impl::TakenWaiters waiters;
    ErrorCode waiter_error;
    {
        std::unique_lock<std::mutex> lock(impl_->mutex);
        impl_->accepting = false;
        impl_->stop_requested = true;
        impl_->retry_delay_ms = 0;
        impl_->PublishRuntime();
        const auto caller = std::this_thread::get_id();
        // Worker callbacks can request shutdown, but cannot wait for a joiner
        // that may itself be joining this worker. The owner completes Stop.
        if (impl_->joining) {
            if (!worker && impl_->join_owner != caller) {
                impl_->cv.wait(lock, [this] { return impl_->joined; });
            }
            return;
        }
        if (impl_->joined) return;
        running = impl_->running;
        waiters = impl_->TakeAllWaiters();
        if (!worker) {
            impl_->joining = true;
            impl_->join_owner = caller;
            pool_to_stop = std::move(impl_->timer_pool);
        }
        waiter_error = impl_->terminal_state.has_value()
                           ? impl_->terminal_state->error
                           : ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    impl_->cv.notify_all();
    Impl::CompleteWaiters(std::move(waiters), waiter_error);
    if (worker) return;
    if (pool_to_stop) {
        pool_to_stop->Stop();
        pool_to_stop.reset();
    }
    if (running && impl_->writer_thread.joinable()) {
        impl_->writer_thread.join();
    }
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        impl_->callback_stop_requested = true;
    }
    impl_->cv.notify_all();
    if (running && impl_->callback_thread.joinable()) {
        impl_->callback_thread.join();
    }
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->running = false;
    impl_->joined = true;
    impl_->cv.notify_all();
}

}  // namespace mooncake
