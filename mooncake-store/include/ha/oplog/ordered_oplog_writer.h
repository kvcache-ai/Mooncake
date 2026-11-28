#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <async_simple/Future.h>
#include <async_simple/Promise.h>
#include <ylt/util/tl/expected.hpp>

#include "ha/oplog/oplog_batch_types.h"
#include "types.h"

namespace mooncake {

struct OrderedOpLogWriterConfig {
    size_t max_entries_per_batch{1024};
    DurablePrefix initial_durable_prefix{};
    std::chrono::milliseconds retry_timeout{0};
};

enum class OrderedOpLogWriterTerminalReason {
    kRetryTimeout,
    kFenced,
    kNonRetryableWriteError,
};

struct OrderedOpLogWriterTerminalState {
    ErrorCode error;
    OrderedOpLogWriterTerminalReason reason;
    DurablePrefix durable_prefix;
    uint64_t occurred_at_ms;
};

class OrderedOpLogWriter {
   public:
    using DurableCallback = std::function<void(const OpLogEntry&)>;
    using TerminalCallback =
        std::function<void(const OrderedOpLogWriterTerminalState&)>;
    using WriteBatchFn =
        std::function<ErrorCode(const OpLogBatchRecord&, const DurablePrefix&)>;

    class Reservation {
       public:
        Reservation();
        Reservation(Reservation&& other) noexcept;
        Reservation& operator=(Reservation&& other) noexcept;
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;
        ~Reservation();

       private:
        friend class OrderedOpLogWriter;
        Reservation(OrderedOpLogWriter* writer, uint64_t id, uint64_t slots);

        OrderedOpLogWriter* writer_{nullptr};
        uint64_t id_{0};
        uint64_t slots_{0};
    };

    class PendingHandle {
       public:
        PendingHandle();
        uint64_t sequence_id() const;

       private:
        friend class OrderedOpLogWriter;
        explicit PendingHandle(uint64_t sequence_id);

        uint64_t sequence_id_{0};
    };

    OrderedOpLogWriter(OrderedOpLogWriterConfig config,
                       WriteBatchFn write_batch,
                       TerminalCallback terminal_callback = {});
    virtual ~OrderedOpLogWriter();

    tl::expected<Reservation, ErrorCode> Reserve();
    // Reserve slots for one indivisible group of entries, to be committed
    // together via CommitBatch. The same admission cap applies, so a group
    // larger than max_entries_per_batch is rejected up front instead of
    // producing an oversized record the backend might refuse.
    tl::expected<Reservation, ErrorCode> ReserveBatch(size_t entry_count);
    virtual tl::expected<PendingHandle, ErrorCode> Commit(
        Reservation&& reservation, OpLogEntry entry, DurableCallback callback);
    // Commit all entries as one indivisible group: they are sealed into a
    // single batch record, so the durable prefix either covers every entry
    // or none of them. Splitting a repair-like set across records would let
    // a durable prefix capture a proper subset. The callback fires once per
    // entry, same as committing each entry on its own. The reservation must
    // come from ReserveBatch with a matching entry_count.
    virtual tl::expected<std::vector<PendingHandle>, ErrorCode> CommitBatch(
        Reservation&& reservation, std::vector<OpLogEntry> entries,
        DurableCallback callback);
    void Abort(Reservation&& reservation);

    // Return a future for durability, independently of callback completion,
    // without blocking the calling thread. An already-covered sequence is
    // immediately ready with OK, even after terminal failure or Stop().
    // Otherwise, terminal failure completes it with the terminal error, and
    // Stop() completes it with UNAVAILABLE_IN_CURRENT_STATUS. Promises are
    // fulfilled outside the writer mutex without an implicit executor. For
    // blocking continuations on timed or untimed waits, attach via(executor) or
    // co_await from a coroutine bound to an executor.
    // A finite timeout ends only this wait, returning the last write error or
    // PERSISTENT_FAIL (use GetTerminalState() to distinguish a per-wait timeout
    // from terminal writer failure). The mutation may still commit: timeout is
    // not proof of rejection, and must not trigger an unconditional replay of
    // the mutation. Finite timeouts fire on a lazily created 4-thread timer
    // pool that Stop() cancels and joins; Stop() waits for any in-flight inline
    // timeout continuations on that pool, and blocking all 4 pool workers with
    // inline continuations stalls further timeout notifications until one
    // returns. Do not destroy the writer from writer-owned threads (including
    // inline continuations or thenValue captures that async_simple may destroy
    // on the completing thread). Resource initialization failures propagate to
    // the caller without registering a wait. Negative timeouts return
    // INVALID_PARAMS; zero polls the current prefix; max() retains the
    // unbounded wait with no timer allocation.
    async_simple::Future<ErrorCode> AwaitDurable(
        uint64_t sequence,
        std::chrono::milliseconds timeout = std::chrono::milliseconds::max());

    bool IsAccepting() const;
    ErrorCode LastError() const;
    std::optional<OrderedOpLogWriterTerminalState> GetTerminalState() const;
    void SetTerminalCallback(TerminalCallback callback);
    // Called by the service only after installing this writer. Older writers
    // can no longer publish runtime metrics after this handoff.
    void ActivateRuntimeMetrics();
    void Start();
    // Concurrent callers share one join. Calls on writer-owned threads only
    // request shutdown; the owner must call Stop() from another thread.
    virtual void Stop();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
