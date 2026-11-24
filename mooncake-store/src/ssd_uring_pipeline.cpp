// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_uring_pipeline.h"
#if defined(__linux__) && defined(USE_URING)
#include <liburing.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <climits>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <thread>
#include <cerrno>

namespace mooncake::ssd_offload {
namespace {
bool Same(const SlotToken& a, const SlotToken& b) {
    return a.request_id == b.request_id && a.attempt_id == b.attempt_id &&
           a.slot_index == b.slot_index && a.generation == b.generation &&
           a.chunk_index == b.chunk_index;
}
bool RingSupportsOpcode(io_uring& ring, int opcode) noexcept {
    io_uring_probe* probe = io_uring_get_probe_ring(&ring);
    if (!probe) return false;
    const bool supported = io_uring_opcode_supported(probe, opcode) != 0;
    io_uring_free_probe(probe);
    return supported;
}
std::chrono::steady_clock::time_point ClampedDeadline(
    std::chrono::milliseconds timeout) noexcept {
    const auto start = std::chrono::steady_clock::now();
    const auto clamped = std::max(timeout, std::chrono::milliseconds::zero());
    const auto max_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::time_point::max() - start);
    return clamped >= max_ms ? std::chrono::steady_clock::time_point::max()
                             : start + clamped;
}
}  // namespace

struct UringReadExecutor::Impl {
    static constexpr unsigned kMaxReadRetries = 3;
    struct Operation {
        std::shared_ptr<ReadSource> source;
        int fd;
        std::span<std::byte> buffer;
        uint64_t offset;
        Completion completion;
        uint64_t id{0};
        unsigned retries{0};
        size_t done{0};
    };
    struct Worker {
        explicit Worker(size_t depth) : active(depth) {}
        ~Worker() {
            if (initialized) io_uring_queue_exit(&ring);
            if (signal >= 0) close(signal);
        }
        bool Init() {
            if (io_uring_queue_init(unsigned(active.size()), &ring, 0) < 0)
                return false;
            initialized = true;
            if (!RingSupportsOpcode(ring, IORING_OP_READ)) return false;
            signal = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            return signal >= 0;
        }
        bool Wake() const noexcept {
            uint64_t one = 1;
            ssize_t result;
            do {
                result = ::write(signal, &one, sizeof(one));
            } while (result < 0 && errno == EINTR);
            return result == ssize_t(sizeof(one)) ||
                   (result < 0 && errno == EAGAIN);
        }
        io_uring ring{};
        int signal{-1};
        bool initialized{false};
        uint64_t next_id{0};
        std::deque<std::unique_ptr<Operation>> queued;   // executor mutex
        std::vector<std::unique_ptr<Operation>> active;  // ring owner only
        std::thread thread;
    };
    explicit Impl(size_t limit) : limit(limit) {}
    ~Impl() { Shutdown(); }
    bool Init(size_t count) {
        const size_t depth = std::min(size_t{32}, (limit + count - 1) / count);
        for (size_t i = 0; i < count; ++i) {
            auto worker = std::make_unique<Worker>(depth);
            if (!worker->Init()) return false;
            workers.push_back(std::move(worker));
        }
        for (auto& worker : workers)
            worker->thread =
                std::thread([this, ptr = worker.get()] { Work(*ptr); });
        return true;
    }
    void Fail() noexcept {
        std::lock_guard lock(mutex);
        if (failed) return;
        failed = stopping = true;
        credit.notify_all();
        for (auto& worker : workers) (void)worker->Wake();
    }
    bool Submit(std::unique_ptr<Operation> operation, bool wait = false) {
        Worker* worker;
        {
            std::unique_lock lock(mutex);
            if (wait)
                credit.wait(lock, [&] { return stopping || pending < limit; });
            if (stopping || pending == limit) return false;
            worker = workers[next_worker].get();
            worker->queued.push_back(std::move(operation));
            // Enqueue is the acceptance point. A later wake failure cannot
            // authorize the caller to release or retry this buffer.
            ++pending;
            next_worker = (next_worker + 1) % workers.size();
        }
        if (!worker->Wake()) Fail();
        return true;
    }
    void Complete(std::unique_ptr<Operation> operation, ReadResult result) {
        operation->source.reset();
        try {
            operation->completion(result);
        } catch (...) {
        }
        operation.reset();
        std::lock_guard lock(mutex);
        --pending;
        credit.notify_all();
    }
    void PrepareRead(Worker& worker, io_uring_sqe* sqe, Operation& op) {
        // A new ID distinguishes this submission from a retired CQE.
        op.id = ++worker.next_id;
        io_uring_prep_read(sqe, op.fd, op.buffer.data() + op.done,
                           unsigned(op.buffer.size() - op.done),
                           op.offset + op.done);
        sqe->user_data = op.id;
    }
    bool Harvest(Worker& worker) {
        bool prepared_retry = false;
        io_uring_cqe* cqe = nullptr;
        while (io_uring_peek_cqe(&worker.ring, &cqe) == 0) {
            const auto id = cqe->user_data;
            const int completed = cqe->res;
            io_uring_cqe_seen(&worker.ring, cqe);
            auto found = std::find_if(
                worker.active.begin(), worker.active.end(),
                [id](const auto& op) { return op && op->id == id; });
            if (found == worker.active.end()) {
                Fail();
                continue;
            }
            auto& op = **found;
            bool retry = false;
            Error error = completed < 0 ? Error::kIoError : Error::kOk;
            if (completed == -EINTR || completed == -EAGAIN) {
                retry = op.retries++ < kMaxReadRetries;
            } else if (completed > 0) {
                if (size_t(completed) > op.buffer.size() - op.done) {
                    error = Error::kIoError;
                } else {
                    op.done += size_t(completed);
                    op.retries = 0;
                    const auto info = op.source->info();
                    const uint64_t end =
                        info.object_bytes > UINT64_MAX - info.value_file_offset
                            ? UINT64_MAX
                            : info.value_file_offset + info.object_bytes;
                    const auto required =
                        op.offset < end ? std::min<uint64_t>(op.buffer.size(),
                                                             end - op.offset)
                                        : 0;
                    retry = op.done < required &&
                            (info.io_mode != ReadIoMode::kDirect ||
                             op.done % kDirectReadAlignment == 0);
                }
            }
            if (retry) {
                std::lock_guard lock(mutex);
                if (!failed && worker.next_id != UINT64_MAX) {
                    if (auto* sqe = io_uring_get_sqe(&worker.ring)) {
                        PrepareRead(worker, sqe, op);
                        prepared_retry = true;
                        continue;
                    }
                }
                error = Error::kIoError;
            }
            const int sys_errno = (error == Error::kIoError && completed < 0 &&
                                   completed > std::numeric_limits<int>::min())
                                      ? -completed
                                      : 0;
            Complete(std::move(*found), {op.done, error, sys_errno});
        }
        return prepared_retry;
    }
    void Prepare(Worker& worker) {
        for (auto& active : worker.active) {
            if (active) continue;
            std::lock_guard lock(mutex);
            if (failed || worker.queued.empty()) break;
            if (worker.next_id == UINT64_MAX) {
                failed = stopping = true;
                credit.notify_all();
                for (auto& other : workers) (void)other->Wake();
                break;
            }
            auto* sqe = io_uring_get_sqe(&worker.ring);
            if (!sqe) break;
            active = std::move(worker.queued.front());
            worker.queued.pop_front();
            // IDs live in the worker's ring domain across all requests. A
            // request-local slot index is insufficient for a shared ring.
            PrepareRead(worker, sqe, *active);
        }
    }
    void Flush(Worker& worker) {
        for (unsigned attempt = 0;
             attempt < 4 && io_uring_sq_ready(&worker.ring); ++attempt) {
            const int result = io_uring_submit(&worker.ring);
            if (result > 0) continue;  // Includes partial submissions.
            if (result < 0 && result != -EINTR && result != -EAGAIN &&
                result != -ENOMEM && result != -EBUSY)
                Fail();
            // Every prepared/published operation stays active until its CQE.
            // A failed enter does not prove which SQEs the kernel accepted.
            break;
        }
    }
    void Work(Worker& worker) {
        for (;;) {
            Harvest(worker);
            // Only queued operations have never been prepared or published.
            // A permanent failure can safely complete these with an error.
            for (size_t i = 0; i < worker.active.size(); ++i) {
                std::unique_ptr<Operation> rejected;
                {
                    std::lock_guard lock(mutex);
                    if (!failed || worker.queued.empty()) break;
                    rejected = std::move(worker.queued.front());
                    worker.queued.pop_front();
                }
                Complete(std::move(rejected), {0, Error::kIoError});
            }
            Prepare(worker);
            Flush(worker);
            if (Harvest(worker)) continue;
            {
                std::lock_guard lock(mutex);
                const bool idle = std::none_of(
                    worker.active.begin(), worker.active.end(),
                    [](const auto& operation) { return bool(operation); });
                if (stopping && worker.queued.empty() && idle) return;
                if (failed && !worker.queued.empty()) continue;
                if (!worker.queued.empty() &&
                    std::any_of(
                        worker.active.begin(), worker.active.end(),
                        [](const auto& operation) { return !operation; }) &&
                    io_uring_sq_space_left(&worker.ring))
                    continue;
            }
            pollfd wake[] = {{worker.ring.ring_fd, POLLIN, 0},
                             {worker.signal, POLLIN, 0}};
            // Retry SQ submissions without a CQE. A bounded backup poll also
            // makes queued work/shutdown progress if an eventfd write fails.
            const int result =
                poll(wake, 2, io_uring_sq_ready(&worker.ring) ? 1 : 100);
            if ((result < 0 && errno != EINTR) ||
                (result > 0 && ((wake[0].revents | wake[1].revents) &
                                (POLLERR | POLLHUP | POLLNVAL)))) {
                Fail();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            if (result > 0 && (wake[1].revents & POLLIN)) {
                uint64_t value;
                while (::read(worker.signal, &value, sizeof(value)) < 0 &&
                       errno == EINTR) {
                }
            }
        }
    }
    void Shutdown() {
        std::lock_guard serial(shutdown_mutex);
        {
            std::lock_guard lock(mutex);
            stopping = true;
            credit.notify_all();
            for (auto& worker : workers) (void)worker->Wake();
        }
        for (auto& worker : workers)
            if (worker->thread.joinable()) worker->thread.join();
    }
    const size_t limit;
    size_t pending{0}, next_worker{0};
    bool stopping{false}, failed{false};
    mutable std::mutex mutex;
    std::condition_variable credit;
    std::mutex shutdown_mutex;
    std::vector<std::unique_ptr<Worker>> workers;
};

std::shared_ptr<UringReadExecutor> UringReadExecutor::Create(
    size_t workers, size_t max_outstanding) noexcept {
    if (!workers || workers > 64 || !max_outstanding || max_outstanding > 65536)
        return {};
    try {
        auto impl = std::make_unique<Impl>(max_outstanding);
        if (!impl->Init(workers)) return {};
        return std::shared_ptr<UringReadExecutor>(
            new UringReadExecutor(std::move(impl)));
    } catch (...) {
        return {};
    }
}
UringReadExecutor::UringReadExecutor(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
UringReadExecutor::~UringReadExecutor() = default;
bool UringReadExecutor::TrySubmit(std::shared_ptr<ReadSource> source, int fd,
                                  std::span<std::byte> buffer, uint64_t offset,
                                  Completion completion) noexcept {
    return Submit(std::move(source), fd, buffer, offset, std::move(completion),
                  false);
}
bool UringReadExecutor::WaitSubmit(std::shared_ptr<ReadSource> source, int fd,
                                   std::span<std::byte> buffer, uint64_t offset,
                                   Completion completion) noexcept {
    return Submit(std::move(source), fd, buffer, offset, std::move(completion),
                  true);
}
bool UringReadExecutor::Submit(std::shared_ptr<ReadSource> source, int fd,
                               std::span<std::byte> buffer,
                               uint64_t file_offset, Completion completion,
                               bool wait) noexcept {
    constexpr auto max_offset = uint64_t(std::numeric_limits<off_t>::max());
    if (!source || fd < 0 || source->native_read_fd() != fd || !buffer.data() ||
        buffer.empty() || buffer.size() > INT_MAX ||
        buffer.size() > std::numeric_limits<uintptr_t>::max() -
                            reinterpret_cast<uintptr_t>(buffer.data()) ||
        file_offset > max_offset || buffer.size() > max_offset - file_offset ||
        !completion)
        return false;
    if (source->info().io_mode == ReadIoMode::kDirect &&
        (reinterpret_cast<uintptr_t>(buffer.data()) % kDirectReadAlignment ||
         buffer.size() % kDirectReadAlignment ||
         file_offset % kDirectReadAlignment))
        return false;
    try {
        return impl_->Submit(std::make_unique<Impl::Operation>(Impl::Operation{
                                 std::move(source), fd, buffer, file_offset,
                                 std::move(completion)}),
                             wait);
    } catch (...) {
        return false;  // Allocation failed before enqueue/acceptance.
    }
}
void UringReadExecutor::Shutdown() { impl_->Shutdown(); }
bool UringReadExecutor::healthy() const noexcept {
    std::lock_guard lock(impl_->mutex);
    return !impl_->stopping && !impl_->failed;
}
size_t UringReadExecutor::outstanding() const {
    std::lock_guard lock(impl_->mutex);
    return impl_->pending;
}
size_t UringReadExecutor::worker_count() const noexcept {
    return impl_->workers.size();
}

struct UringPipelineDriver::Impl {
    enum class Stage { kIdle, kRead, kWrite, kReady };
    struct Record {
        Stage stage{Stage::kIdle};
        IoCompletion event;
        bool cancelled{false};
        int fd{-1};
        std::byte* buffer{nullptr};
        uint64_t offset{0};
        size_t requested{0};
        size_t done{0};
        unsigned retries{0};
    };
    Impl(std::vector<std::shared_ptr<ReadSource>> sources,
         std::vector<size_t> chunk_sources, std::vector<int> source_fds,
         std::shared_ptr<SourceDrainedWriteTarget> target,
         BoundedExecutor& writes, std::vector<SlotBuffer> buffers)
        : sources(std::move(sources)),
          chunk_sources(std::move(chunk_sources)),
          source_fds(std::move(source_fds)),
          target(std::move(target)),
          writes(writes),
          buffers(std::move(buffers)),
          records(this->buffers.size()) {}
    bool Init(Options options) {
        if (options.reader && !options.fixed_buffers) {
            if (!options.reader->healthy()) return false;
            reader = std::move(options.reader);
            return true;
        }
        if (io_uring_queue_init(unsigned(std::max(size_t{2}, buffers.size())),
                                &ring, 0) < 0)
            return false;
        initialized = true;
        signal = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        // Poll the ring fd for CQEs and this eventfd for write completions.
        if (signal < 0) return false;
        // CreateImpl validated nonempty, bounded and nonoverlapping slots.
        // Register only these borrowed slots; failure keeps ordinary READ.
        if (options.fixed_buffers && buffers.size() - 1 <= UINT16_MAX) {
            std::array<iovec, 64> iovecs{};
            for (size_t i = 0; i < buffers.size(); ++i)
                iovecs[i] = {buffers[i].data, buffers[i].capacity};
            buffers_registered =
                io_uring_register_buffers(&ring, iovecs.data(),
                                          unsigned(buffers.size())) == 0;
        }
        const int required_op =
            buffers_registered ? IORING_OP_READ_FIXED : IORING_OP_READ;
        if (!RingSupportsOpcode(ring, required_op)) return false;
        return true;
    }
    ~Impl() {
        {
            std::lock_guard lock(mutex);
            stopping = true;
            for (auto& record : records) record.cancelled = true;
        }
        // Accepted SQEs, including ones not submitted yet, must produce a CQE.
        // Source buffers and chunk metadata remain pinned through this barrier.
        for (;;) {
            {
                std::lock_guard lock(mutex);
                if (!reads_running && !writes_running) break;
            }
            IoCompletion ignored;
            Wait(ignored, std::chrono::milliseconds(100));
        }
        if (buffers_registered) {
            while (io_uring_unregister_buffers(&ring) == -EINTR) {
            }
        }
        if (initialized) io_uring_queue_exit(&ring);
        if (signal >= 0) close(signal);
    }
    Error Rebind(std::shared_ptr<ReadSource> next) noexcept {
        if (!next) return Error::kInvalidArgument;
        const auto fd = next->native_read_fd();
        if (!fd || *fd < 0) return Error::kNotSupported;
        try {
            std::lock_guard lock(mutex);
            if (!chunk_sources.empty() || stopping || reads_running ||
                writes_running ||
                std::any_of(records.begin(), records.end(), [](const auto& r) {
                    return r.stage != Stage::kIdle;
                }))
                return Error::kInvalidState;
            if (fcntl(*fd, F_GETFD) < 0) {
                stopping = true;
                return Error::kIoError;
            }
            source_fds[0] = *fd;
            sources[0] = std::move(next);
            return Error::kOk;
        } catch (...) {
            return Error::kIoError;
        }
    }
    bool Valid(const SlotToken& token, const ReadChunk& chunk,
               SlotBuffer buffer) const {
        return token.request_id && token.attempt_id && token.generation &&
               token.slot_index < buffers.size() &&
               (chunk_sources.empty() ||
                token.chunk_index < chunk_sources.size()) &&
               buffer.data && chunk.read_bytes &&
               buffer.data == buffers[token.slot_index].data &&
               buffer.capacity == buffers[token.slot_index].capacity &&
               chunk.read_bytes <= buffer.capacity &&
               chunk.read_bytes <= INT_MAX &&
               chunk.file_offset <=
                   uint64_t(std::numeric_limits<off_t>::max()) &&
               chunk.read_bytes <= uint64_t(std::numeric_limits<off_t>::max()) -
                                       chunk.file_offset;
    }
    bool Flush() noexcept {
        // SQPOLL is disabled: accepted reads are batched until the scheduler
        // next polls. Submission errors are not completion/drain evidence.
        bool submitted_any = false;
        while (initialized && io_uring_sq_ready(&ring)) {
            const int submitted = io_uring_submit(&ring);
            if (submitted > 0) {
                submitted_any = true;
                continue;
            }
            if (submitted < 0 && submitted != -EINTR && submitted != -EAGAIN &&
                submitted != -ENOMEM && submitted != -EBUSY) {
                std::lock_guard lock(mutex);
                stopping = true;
            }
            break;
        }
        return submitted_any;
    }
    bool Read(const SlotToken& token, const ReadChunk& chunk,
              SlotBuffer buffer) noexcept {
        if (!Valid(token, chunk, buffer)) return false;
        std::unique_lock lock(mutex);
        auto& record = records[token.slot_index];
        if (stopping || record.stage != Stage::kIdle) return false;
        const size_t source_index =
            chunk_sources.empty() ? 0 : chunk_sources[token.chunk_index];
        const int fd = source_fds[source_index];
        if (reader) {
            record.stage = Stage::kRead;
            record.cancelled = false;
            record.event = {token, SlotStage::kReading, 0, Error::kOk};
            ++reads_running;
            lock.unlock();
            bool accepted = false;
            try {
                accepted = reader->WaitSubmit(
                    sources[source_index], fd, {buffer.data, chunk.read_bytes},
                    chunk.file_offset, [this, token](ReadResult result) {
                        std::lock_guard lock(mutex);
                        auto& completed = records[token.slot_index];
                        completed.event.bytes = result.bytes;
                        completed.event.result = completed.cancelled
                                                     ? Error::kCancelled
                                                     : result.error;
                        completed.event.sys_errno =
                            completed.cancelled ? 0 : result.sys_errno;
                        completed.stage = Stage::kReady;
                        --reads_running;
                        // Last driver access, including notification, stays
                        // under the drain lock. The closure holds only a raw
                        // pointer and value token after this point.
                        changed.notify_all();
                    });
            } catch (...) {
            }
            if (!accepted) {
                lock.lock();
                record.stage = Stage::kIdle;
                --reads_running;
                changed.notify_all();
            }
            return accepted;
        }
        if (sources[source_index]->info().io_mode == ReadIoMode::kDirect &&
            (reinterpret_cast<uintptr_t>(buffer.data) % kDirectReadAlignment !=
                 0 ||
             chunk.read_bytes % kDirectReadAlignment != 0 ||
             chunk.file_offset % kDirectReadAlignment != 0))
            return false;
        record.fd = fd;
        record.buffer = buffer.data;
        record.offset = chunk.file_offset;
        record.requested = chunk.read_bytes;
        record.done = 0;
        record.retries = 0;
        if (!PrepareRead(record, token.slot_index)) return false;
        record.stage = Stage::kRead;
        record.cancelled = false;
        record.event = {token, SlotStage::kReading, 0, Error::kOk};
        ++reads_running;
        return true;
    }
    bool Write(const SlotToken& token, const ReadChunk& chunk,
               SlotBuffer buffer) noexcept {
        if (!Valid(token, chunk, buffer)) return false;
        {
            std::lock_guard lock(mutex);
            auto& record = records[token.slot_index];
            if (stopping || record.stage != Stage::kIdle) return false;
            record.stage = Stage::kWrite;
            record.cancelled = false;
            record.event = {token, SlotStage::kWriting, 0, Error::kOk};
            ++writes_running;
        }
        bool accepted = false;
        try {
            accepted =
                writes.WaitSubmit([this, token, buffer, chunk_ptr = &chunk] {
                    bool cancelled;
                    {
                        std::lock_guard lock(mutex);
                        cancelled = records[token.slot_index].cancelled;
                    }
                    const auto result =
                        cancelled ? Error::kCancelled
                                  : target->WriteAndDrain(*chunk_ptr, buffer);
                    {
                        std::lock_guard lock(mutex);
                        auto& record = records[token.slot_index];
                        record.event.result = result;
                        record.event.bytes =
                            result == Error::kOk ? chunk_ptr->payload_bytes : 0;
                        record.stage = Stage::kReady;
                        uint64_t one = 1;
                        // Signal is part of the callback's physical quiescence.
                        // Keep the lock/count until its last access to this
                        // ring.
                        if (signal >= 0) {
                            ssize_t written;
                            do {
                                written = ::write(signal, &one, sizeof(one));
                            } while (written < 0 && errno == EINTR);
                            if (written != ssize_t(sizeof(one)) &&
                                !(written < 0 && errno == EAGAIN)) {
                                stopping = true;
                                wait_failed = true;
                            }
                        }
                        --writes_running;
                        changed.notify_all();
                    }
                });
        } catch (...) {
        }
        if (!accepted) {
            std::lock_guard lock(mutex);
            records[token.slot_index].stage = Stage::kIdle;
            --writes_running;
            changed.notify_all();
        }
        return accepted;
    }
    void Cancel(const SlotToken& token, SlotStage stage) noexcept {
        std::lock_guard lock(mutex);
        if (token.slot_index >= records.size()) return;
        auto& record = records[token.slot_index];
        if (Same(token, record.event.token) && record.event.stage == stage)
            record.cancelled = true;
        // Cancellation stops refill in ReadPipeline. Already accepted READs
        // are still flushed/drained rather than pretending cancellation ended
        // I/O.
    }
    bool PrepareRead(Record& record, size_t slot) noexcept {
        auto* sqe = io_uring_get_sqe(&ring);
        if (!sqe) return false;
        if (buffers_registered)
            io_uring_prep_read_fixed(
                sqe, record.fd, record.buffer + record.done,
                unsigned(record.requested - record.done),
                record.offset + record.done, unsigned(slot));
        else
            io_uring_prep_read(sqe, record.fd, record.buffer + record.done,
                               unsigned(record.requested - record.done),
                               record.offset + record.done);
        sqe->user_data = slot + 1;
        return true;
    }
    bool Harvest() {
        bool prepared_retry = false;
        io_uring_cqe* cqe = nullptr;
        while (initialized && io_uring_peek_cqe(&ring, &cqe) == 0) {
            const auto id = cqe->user_data;
            const int completed = cqe->res;
            io_uring_cqe_seen(&ring, cqe);
            std::lock_guard lock(mutex);
            if (!id || id > records.size()) {
                stopping = true;
                continue;
            }
            auto& record = records[size_t(id - 1)];
            if (record.stage != Stage::kRead) {
                stopping = true;
                continue;
            }
            bool retry = false;
            Error error = completed < 0 ? Error::kIoError : Error::kOk;
            if (completed == -EINTR || completed == -EAGAIN) {
                retry = record.retries++ < 3;
            } else if (completed > 0) {
                if (size_t(completed) > record.requested - record.done) {
                    error = Error::kIoError;
                } else {
                    record.done += size_t(completed);
                    record.retries = 0;
                    const auto& source = sources
                        [chunk_sources.empty()
                             ? 0
                             : chunk_sources[record.event.token.chunk_index]];
                    const auto info = source->info();
                    const uint64_t end =
                        info.object_bytes > UINT64_MAX - info.value_file_offset
                            ? UINT64_MAX
                            : info.value_file_offset + info.object_bytes;
                    const auto required =
                        record.offset < end
                            ? std::min<uint64_t>(record.requested,
                                                 end - record.offset)
                            : 0;
                    retry = record.done < required &&
                            (info.io_mode != ReadIoMode::kDirect ||
                             record.done % kDirectReadAlignment == 0);
                }
            }
            if (retry) {
                if (!stopping && !record.cancelled &&
                    PrepareRead(record, size_t(id - 1))) {
                    prepared_retry = true;
                    continue;
                }
                error = Error::kIoError;
            }
            record.event.bytes = record.done;
            record.event.result = record.cancelled ? Error::kCancelled : error;
            record.event.sys_errno =
                (!record.cancelled && error == Error::kIoError &&
                 completed < 0 && completed > std::numeric_limits<int>::min())
                    ? -completed
                    : 0;
            record.stage = Stage::kReady;
            --reads_running;
        }
        return prepared_retry;
    }
    bool Wait(IoCompletion& event, std::chrono::milliseconds timeout) {
        const auto until = ClampedDeadline(timeout);
        if (reader) {
            std::unique_lock lock(mutex);
            auto ready = [&] {
                return std::find_if(records.begin(), records.end(),
                                    [](const auto& record) {
                                        return record.stage == Stage::kReady;
                                    });
            };
            if (!changed.wait_until(lock, until, [&] {
                    return ready() != records.end() ||
                           (!reads_running && !writes_running);
                }))
                return false;
            const auto found = ready();
            if (found == records.end()) return false;
            event = found->event;
            found->stage = Stage::kIdle;
            return true;
        }
        for (;;) {
            Harvest();
            while (Flush()) {
                if (!Harvest()) break;
            }
            bool failed_wait = false;
            {
                std::lock_guard lock(mutex);
                for (auto& record : records) {
                    if (record.stage != Stage::kReady) continue;
                    event = record.event;
                    record.stage = Stage::kIdle;
                    return true;
                }
                if (!reads_running && !writes_running) return false;
                failed_wait = wait_failed;
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= until || signal < 0) return false;
            const auto ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(until -
                                                                      now)
                    .count();
            if (failed_wait) {
                // A wake-channel failure cannot prove completion. Stop new
                // admissions and keep harvesting actual CQEs/write records,
                // with bounded backoff instead of spinning on a broken fd.
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            pollfd wake[] = {{ring.ring_fd, POLLIN, 0}, {signal, POLLIN, 0}};
            const int result =
                poll(wake, 2,
                     int(std::min<int64_t>(io_uring_sq_ready(&ring) ? 1 : 100,
                                           std::max<int64_t>(1, ms))));
            if ((result < 0 && errno != EINTR) ||
                (result > 0 && ((wake[0].revents | wake[1].revents) &
                                (POLLERR | POLLHUP | POLLNVAL)))) {
                std::lock_guard lock(mutex);
                stopping = true;
                wait_failed = true;
                continue;
            }
            if (result > 0 && (wake[1].revents & POLLIN)) {
                uint64_t value;
                while (read(signal, &value, sizeof(value)) < 0 &&
                       errno == EINTR) {
                }
            }
        }
    }
    std::vector<std::shared_ptr<ReadSource>> sources;
    const std::vector<size_t> chunk_sources;
    std::vector<int> source_fds;
    std::shared_ptr<SourceDrainedWriteTarget> target;
    BoundedExecutor& writes;
    std::vector<SlotBuffer> buffers;
    std::vector<Record> records;
    std::mutex mutex;
    std::condition_variable changed;
    std::shared_ptr<UringReadExecutor> reader;
    size_t reads_running{0}, writes_running{0};
    bool stopping{false}, initialized{false}, wait_failed{false};
    bool buffers_registered{false};
    io_uring ring{};
    int signal{-1};
};
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::shared_ptr<ReadSource> source,
    std::shared_ptr<SourceDrainedWriteTarget> target, BoundedExecutor& writes,
    std::vector<SlotBuffer> buffers) {
    return Create(std::move(source), std::move(target), writes,
                  std::move(buffers), Options{});
}
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::vector<std::shared_ptr<ReadSource>> sources,
    std::vector<size_t> chunk_sources,
    std::shared_ptr<SourceDrainedWriteTarget> target, BoundedExecutor& writes,
    std::vector<SlotBuffer> buffers) {
    return Create(std::move(sources), std::move(chunk_sources),
                  std::move(target), writes, std::move(buffers), Options{});
}
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::shared_ptr<ReadSource> source,
    std::shared_ptr<SourceDrainedWriteTarget> target, BoundedExecutor& writes,
    std::vector<SlotBuffer> buffers, Options options) {
    return CreateImpl({std::move(source)}, {}, false, std::move(target), writes,
                      std::move(buffers), options);
}
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::vector<std::shared_ptr<ReadSource>> sources,
    std::vector<size_t> chunk_sources,
    std::shared_ptr<SourceDrainedWriteTarget> target, BoundedExecutor& writes,
    std::vector<SlotBuffer> buffers, Options options) {
    return CreateImpl(std::move(sources), std::move(chunk_sources), true,
                      std::move(target), writes, std::move(buffers), options);
}
UringPipelineDriver::Creation UringPipelineDriver::CreateImpl(
    std::vector<std::shared_ptr<ReadSource>> sources,
    std::vector<size_t> chunk_sources, bool mapped,
    std::shared_ptr<SourceDrainedWriteTarget> target, BoundedExecutor& writes,
    std::vector<SlotBuffer> buffers, Options options) {
    if (sources.empty() || (mapped && chunk_sources.empty()) ||
        std::any_of(chunk_sources.begin(), chunk_sources.end(),
                    [&](size_t index) { return index >= sources.size(); }))
        return {Error::kInvalidArgument, {}};
    std::vector<int> source_fds;
    source_fds.reserve(sources.size());
    for (const auto& source : sources) {
        if (!source) return {Error::kInvalidArgument, {}};
        const auto fd = source->native_read_fd();
        if (!fd || *fd < 0 || fcntl(*fd, F_GETFD) < 0) return {};
        source_fds.push_back(*fd);
    }
    if (!target || buffers.empty() || buffers.size() > 64)
        return {Error::kInvalidArgument, {}};
    std::vector<std::pair<uintptr_t, uintptr_t>> spans;
    for (auto buffer : buffers) {
        auto begin = reinterpret_cast<uintptr_t>(buffer.data);
        if (!begin || !buffer.capacity || buffer.capacity > INT_MAX ||
            buffer.capacity > std::numeric_limits<uintptr_t>::max() - begin)
            return {Error::kInvalidArgument, {}};
        spans.emplace_back(begin, begin + buffer.capacity);
    }
    std::sort(spans.begin(), spans.end());
    for (size_t i = 1; i < spans.size(); ++i)
        if (spans[i].first < spans[i - 1].second)
            return {Error::kInvalidArgument, {}};
    try {
        auto impl = std::make_unique<Impl>(
            std::move(sources), std::move(chunk_sources), std::move(source_fds),
            std::move(target), writes, std::move(buffers));
        if (!impl->Init(options)) return {Error::kNotSupported, {}};
        return {Error::kOk, std::unique_ptr<UringPipelineDriver>(
                                new UringPipelineDriver(std::move(impl)))};
    } catch (...) {
        return {Error::kIoError, {}};
    }
}
UringPipelineDriver::UringPipelineDriver(std::unique_ptr<Impl> p)
    : impl_(std::move(p)) {}
UringPipelineDriver::~UringPipelineDriver() = default;
Error UringPipelineDriver::RebindSource(
    std::shared_ptr<ReadSource> source) noexcept {
    return impl_->Rebind(std::move(source));
}
bool UringPipelineDriver::SubmitRead(const SlotToken& t, const ReadChunk& c,
                                     SlotBuffer b) noexcept {
    return impl_->Read(t, c, b);
}
bool UringPipelineDriver::SubmitWrite(const SlotToken& t, const ReadChunk& c,
                                      SlotBuffer b) noexcept {
    return impl_->Write(t, c, b);
}
void UringPipelineDriver::RequestCancel(const SlotToken& t,
                                        SlotStage s) noexcept {
    impl_->Cancel(t, s);
}
bool UringPipelineDriver::WaitForEvent(IoCompletion& c,
                                       std::chrono::milliseconds t) {
    return impl_->Wait(c, t);
}
}  // namespace mooncake::ssd_offload
#else
namespace mooncake::ssd_offload {
struct UringReadExecutor::Impl {};
UringReadExecutor::UringReadExecutor(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
UringReadExecutor::~UringReadExecutor() = default;
std::shared_ptr<UringReadExecutor> UringReadExecutor::Create(size_t,
                                                             size_t) noexcept {
    return {};
}
bool UringReadExecutor::TrySubmit(std::shared_ptr<ReadSource>, int,
                                  std::span<std::byte>, uint64_t,
                                  Completion) noexcept {
    return false;
}
bool UringReadExecutor::WaitSubmit(std::shared_ptr<ReadSource>, int,
                                   std::span<std::byte>, uint64_t,
                                   Completion) noexcept {
    return false;
}
void UringReadExecutor::Shutdown() {}
bool UringReadExecutor::healthy() const noexcept { return false; }
size_t UringReadExecutor::outstanding() const { return 0; }
size_t UringReadExecutor::worker_count() const noexcept { return 0; }
struct UringPipelineDriver::Impl {};
UringPipelineDriver::UringPipelineDriver(std::unique_ptr<Impl> p)
    : impl_(std::move(p)) {}
UringPipelineDriver::~UringPipelineDriver() = default;
Error UringPipelineDriver::RebindSource(std::shared_ptr<ReadSource>) noexcept {
    return Error::kNotSupported;
}
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::shared_ptr<ReadSource>, std::shared_ptr<SourceDrainedWriteTarget>,
    BoundedExecutor&, std::vector<SlotBuffer>) {
    return {};
}
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::vector<std::shared_ptr<ReadSource>>, std::vector<size_t>,
    std::shared_ptr<SourceDrainedWriteTarget>, BoundedExecutor&,
    std::vector<SlotBuffer>) {
    return {};
}
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::shared_ptr<ReadSource>, std::shared_ptr<SourceDrainedWriteTarget>,
    BoundedExecutor&, std::vector<SlotBuffer>, Options) {
    return {};
}
UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::vector<std::shared_ptr<ReadSource>>, std::vector<size_t>,
    std::shared_ptr<SourceDrainedWriteTarget>, BoundedExecutor&,
    std::vector<SlotBuffer>, Options) {
    return {};
}
bool UringPipelineDriver::SubmitRead(const SlotToken&, const ReadChunk&,
                                     SlotBuffer) noexcept {
    return false;
}
bool UringPipelineDriver::SubmitWrite(const SlotToken&, const ReadChunk&,
                                      SlotBuffer) noexcept {
    return false;
}
void UringPipelineDriver::RequestCancel(const SlotToken&, SlotStage) noexcept {}
bool UringPipelineDriver::WaitForEvent(IoCompletion&,
                                       std::chrono::milliseconds) {
    return false;
}
}  // namespace mooncake::ssd_offload
#endif
