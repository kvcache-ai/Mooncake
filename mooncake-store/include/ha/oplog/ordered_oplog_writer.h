#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

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
        Reservation(OrderedOpLogWriter* writer, uint64_t id);

        OrderedOpLogWriter* writer_{nullptr};
        uint64_t id_{0};
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
    virtual tl::expected<PendingHandle, ErrorCode> Commit(
        Reservation&& reservation, OpLogEntry entry, DurableCallback callback);
    void Abort(Reservation&& reservation);

    bool IsAccepting() const;
    ErrorCode LastError() const;
    std::optional<OrderedOpLogWriterTerminalState> GetTerminalState() const;
    void Start();
    virtual void Stop();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
