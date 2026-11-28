// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_uring_pipeline.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <liburing.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>

namespace mooncake::ssd_offload {
namespace {

void FreeUringProbe(io_uring_probe* probe) noexcept {
    // liburing < 2.0 (e.g., EL8 liburing 1.0.7) allocates probes via calloc()
    // and does not provide io_uring_free_probe().
#if defined(IO_URING_VERSION_MAJOR) && IO_URING_VERSION_MAJOR >= 2
    io_uring_free_probe(probe);
#else
    std::free(probe);
#endif
}

bool RingSupportsOpcode(io_uring& ring, int opcode) noexcept {
    io_uring_probe* probe = io_uring_get_probe_ring(&ring);
    if (!probe) return false;
    const bool supported = io_uring_opcode_supported(probe, opcode) != 0;
    FreeUringProbe(probe);
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
        bool submitted{false};
    };

    struct Worker {
        explicit Worker(size_t depth) : active(depth) {}
        ~Worker() {
            if (initialized) io_uring_queue_exit(&ring);
            if (signal >= 0) close(signal);
        }
        bool Init() {
            const int init_rc =
                io_uring_queue_init(unsigned(active.size()), &ring, 0);
            if (init_rc < 0) {
                LOG(WARNING)
                    << "io_uring_queue_init failed (errno=" << -init_rc << ")";
                return false;
            }
            initialized = true;
            if (!RingSupportsOpcode(ring, IORING_OP_READ)) {
                LOG(WARNING) << "io_uring does not support IORING_OP_READ";
                return false;
            }
            // Exercise io_uring_enter upfront so seccomp policies that allow
            // setup/register but block io_uring_enter fail Create() cleanly.
            auto* sqe = io_uring_get_sqe(&ring);
            if (!sqe) return false;
            io_uring_prep_nop(sqe);
            int enter_rc;
            do {
                enter_rc = io_uring_submit_and_wait(&ring, 1);
            } while (enter_rc == -EINTR);
            if (enter_rc < 0) {
                LOG(WARNING)
                    << "io_uring_enter NOP probe failed (errno=" << -enter_rc
                    << ")";
                return false;
            }
            io_uring_cqe* cqe = nullptr;
            if (io_uring_peek_cqe(&ring, &cqe) != 0) {
                LOG(WARNING) << "io_uring NOP probe produced no CQE";
                return false;
            }
            const int nop_res = cqe->res;
            io_uring_cqe_seen(&ring, cqe);
            if (nop_res < 0) {
                LOG(WARNING)
                    << "io_uring NOP probe failed (errno=" << -nop_res << ")";
                return false;
            }
            signal = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            if (signal < 0) {
                LOG(WARNING)
                    << "eventfd failed for UringReadExecutor (errno=" << errno
                    << ")";
                return false;
            }
            return true;
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
        bool submit_broken{false};
        bool ring_poll_broken{false};
        bool signal_broken{false};
        int broken_errno{0};
        uint64_t next_id{0};
        std::deque<std::unique_ptr<Operation>> queued;   // executor mutex
        std::vector<std::unique_ptr<Operation>> active;  // ring owner only
        std::deque<Operation*> pending_submit;           // ring owner only
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
        for (auto& worker : workers) {
            worker->thread =
                std::thread([this, ptr = worker.get()] { Work(*ptr); });
        }
        return true;
    }

    void Fail(const char* reason, int err = 0) noexcept {
        bool latched = false;
        {
            std::lock_guard lock(mutex);
            if (!failed) {
                failed = stopping = true;
                failure_errno = err;
                latched = true;
                credit.notify_all();
                for (auto& worker : workers) (void)worker->Wake();
            }
        }
        if (latched) {
            LOG(WARNING) << "UringReadExecutor disabled (" << reason
                         << ", errno=" << err
                         << "); subsequent streaming reads will fall back to "
                            "pread";
        }
    }

    bool Submit(std::unique_ptr<Operation> operation) {
        Worker* worker;
        {
            std::unique_lock lock(mutex);
            credit.wait(lock, [&] { return stopping || pending < limit; });
            if (stopping || pending == limit) return false;
            worker = workers[next_worker].get();
            worker->queued.push_back(std::move(operation));
            // Enqueue is the acceptance point. A later wake failure cannot
            // authorize the caller to release or retry this buffer.
            ++pending;
            next_worker = (next_worker + 1) % workers.size();
        }
        if (!worker->Wake()) Fail("eventfd wake failed", errno);
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
        op.submitted = false;
        io_uring_prep_read(sqe, op.fd, op.buffer.data() + op.done,
                           unsigned(op.buffer.size() - op.done),
                           op.offset + op.done);
        sqe->user_data = op.id;
        worker.pending_submit.push_back(&op);
    }

    void FailUnsubmittedActive(Worker& worker, int err) {
        worker.pending_submit.clear();
        for (auto& slot : worker.active) {
            if (!slot || slot->submitted) continue;
            const size_t done = slot->done;
            Complete(std::move(slot), {done, Error::kIoError, err});
        }
    }

    static bool HasActiveOps(const Worker& worker) noexcept {
        return std::any_of(worker.active.begin(), worker.active.end(),
                           [](const auto& op) { return bool(op); });
    }

    bool Harvest(Worker& worker) {
        if (!worker.initialized) return false;
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
                Fail("unexpected cqe user_data");
                worker.submit_broken = true;
                FailUnsubmittedActive(worker, worker.broken_errno);
                continue;
            }
            auto& op = **found;
            const auto step =
                ClassifyUringReadResult(completed, op.done, op.buffer.size(),
                                        op.retries, kMaxReadRetries);
            op.done = step.next_done;
            op.retries = step.next_retries;
            Error error = step.error;
            int sys_errno = step.sys_errno;
            if (step.retry) {
                bool id_exhausted = false;
                bool requeued = false;
                {
                    std::lock_guard lock(mutex);
                    if (!failed && !worker.submit_broken) {
                        if (worker.next_id == UINT64_MAX) {
                            id_exhausted = true;
                        } else if (auto* sqe = io_uring_get_sqe(&worker.ring)) {
                            PrepareRead(worker, sqe, op);
                            prepared_retry = true;
                            continue;
                        } else {
                            // SQ ring is temporarily full; push back to the
                            // front of queued so the next Flush/Prepare cycle
                            // retries without failing the read spuriously.
                            (*found)->submitted = false;
                            worker.queued.push_front(std::move(*found));
                            requeued = true;
                        }
                    }
                }
                if (requeued) {
                    prepared_retry = true;
                    continue;
                }
                if (id_exhausted) Fail("sqe id exhausted");
                error = Error::kIoError;
                sys_errno = sys_errno ? sys_errno : worker.broken_errno;
            }
            Complete(std::move(*found), {op.done, error, sys_errno});
        }
        return prepared_retry;
    }

    void Prepare(Worker& worker) {
        if (!worker.initialized || worker.submit_broken) return;
        bool id_exhausted = false;
        for (auto& active : worker.active) {
            if (active) continue;
            std::lock_guard lock(mutex);
            if (failed || worker.queued.empty()) break;
            if (worker.next_id == UINT64_MAX) {
                id_exhausted = true;
                break;
            }
            auto* sqe = io_uring_get_sqe(&worker.ring);
            if (!sqe) break;
            active = std::move(worker.queued.front());
            worker.queued.pop_front();
            // IDs live in the worker's ring domain across all requests.
            PrepareRead(worker, sqe, *active);
        }
        if (id_exhausted) Fail("sqe id exhausted");
    }

    void Flush(Worker& worker) {
        if (!worker.initialized || worker.submit_broken) return;
        for (unsigned attempt = 0;
             attempt < 4 && io_uring_sq_ready(&worker.ring); ++attempt) {
            const int result = io_uring_submit(&worker.ring);
            if (result > 0) {
                const size_t accepted = std::min(static_cast<size_t>(result),
                                                 worker.pending_submit.size());
                for (size_t i = 0; i < accepted; ++i) {
                    worker.pending_submit.front()->submitted = true;
                    worker.pending_submit.pop_front();
                }
                continue;  // Includes partial submissions.
            }
            if (result < 0 && result != -EINTR && result != -EAGAIN &&
                result != -ENOMEM && result != -EBUSY) {
                worker.submit_broken = true;
                if (worker.broken_errno == 0) worker.broken_errno = -result;
                Fail("io_uring_submit failed", -result);
                // Any SQEs still in pending_submit were never accepted by the
                // kernel; fail them immediately, while keeping
                // already-submitted operations pinned until their CQEs arrive
                // so kernel DMA/io-wq never writes into freed caller staging
                // buffers.
                FailUnsubmittedActive(worker, -result);
            }
            break;
        }
    }

    void Work(Worker& worker) {
        for (;;) {
            Harvest(worker);
            // Only queued operations have never been prepared or published.
            // A permanent failure can safely complete these with an error.
            for (;;) {
                std::unique_ptr<Operation> rejected;
                int err = 0;
                {
                    std::lock_guard lock(mutex);
                    if (!failed || worker.queued.empty()) break;
                    rejected = std::move(worker.queued.front());
                    worker.queued.pop_front();
                    err = worker.broken_errno ? worker.broken_errno
                                              : failure_errno;
                }
                Complete(std::move(rejected), {0, Error::kIoError, err});
            }
            Prepare(worker);
            Flush(worker);
            if (Harvest(worker)) continue;
            bool idle = false;
            {
                std::lock_guard lock(mutex);
                idle = !HasActiveOps(worker);
                if (stopping && worker.queued.empty() && idle) return;
                if (failed && !worker.queued.empty()) continue;
                if (!failed && !worker.submit_broken &&
                    !worker.queued.empty() &&
                    std::any_of(
                        worker.active.begin(), worker.active.end(),
                        [](const auto& operation) { return !operation; }) &&
                    io_uring_sq_space_left(&worker.ring))
                    continue;
            }
            if (worker.ring_poll_broken) {
                // Keep harvesting CQEs directly from the shared CQ ring until
                // every already-submitted kernel I/O completes; never tear down
                // the ring or release buffers while submitted ops remain.
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            pollfd wake[] = {
                {worker.initialized ? worker.ring.ring_fd : -1, POLLIN, 0},
                {worker.signal_broken ? -1 : worker.signal, POLLIN, 0}};
            // When completely idle with no queued or active SQEs, block in
            // poll() without periodic wakeups until eventfd signals new work or
            // shutdown. Keep a 1 ms retry poll when SQEs are pending submission
            // under transient backpressure, and a 100 ms safety net while I/O
            // is in flight.
            int timeout_ms = -1;
            if (!worker.submit_broken && worker.initialized &&
                io_uring_sq_ready(&worker.ring)) {
                timeout_ms = 1;
            } else if (!idle) {
                timeout_ms = 100;
            }
            const int result = poll(wake, 2, timeout_ms);
            if (result < 0) {
                if (errno == EINTR) continue;
                const int err = errno;
                worker.submit_broken = true;
                worker.ring_poll_broken = true;
                if (worker.broken_errno == 0) worker.broken_errno = err;
                Fail("poll failed", err);
                FailUnsubmittedActive(worker, err);
                continue;
            }
            if (result > 0) {
                const bool ring_err =
                    (wake[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0;
                const bool sig_err =
                    (wake[1].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0;
                if (sig_err) {
                    worker.signal_broken = true;
                    worker.submit_broken = true;
                    Fail("eventfd poll failed", 0);
                    FailUnsubmittedActive(worker, worker.broken_errno);
                }
                if (ring_err) {
                    worker.submit_broken = true;
                    worker.ring_poll_broken = true;
                    Fail("ring_fd poll failed", 0);
                    FailUnsubmittedActive(worker, worker.broken_errno);
                }
                if (!sig_err && (wake[1].revents & POLLIN)) {
                    uint64_t value;
                    while (::read(worker.signal, &value, sizeof(value)) < 0 &&
                           errno == EINTR) {
                    }
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
        for (auto& worker : workers) {
            if (worker->thread.joinable()) worker->thread.join();
        }
    }

    const size_t limit;
    size_t pending{0}, next_worker{0};
    bool stopping{false}, failed{false};
    int failure_errno{0};
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
        if (!impl->Init(workers)) {
            LOG(WARNING) << "UringReadExecutor initialization failed; local "
                            "streaming reads will fall back to pread";
            return {};
        }
        return std::shared_ptr<UringReadExecutor>(
            new UringReadExecutor(std::move(impl)));
    } catch (...) {
        return {};
    }
}

UringReadExecutor::UringReadExecutor(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

UringReadExecutor::~UringReadExecutor() = default;

bool UringReadExecutor::WaitSubmit(std::shared_ptr<ReadSource> source, int fd,
                                   std::span<std::byte> buffer,
                                   uint64_t file_offset,
                                   Completion completion) noexcept {
    constexpr auto max_offset = uint64_t(std::numeric_limits<off_t>::max());
    if (!source || fd < 0 || source->native_read_fd() != fd || !buffer.data() ||
        buffer.empty() || buffer.size() > INT_MAX ||
        buffer.size() > std::numeric_limits<uintptr_t>::max() -
                            reinterpret_cast<uintptr_t>(buffer.data()) ||
        file_offset > max_offset || buffer.size() > max_offset - file_offset ||
        !completion)
        return false;
    try {
        return impl_->Submit(std::make_unique<Impl::Operation>(
            Impl::Operation{std::move(source), fd, buffer, file_offset,
                            std::move(completion)}));
    } catch (...) {
        return false;  // Allocation failed before enqueue/acceptance.
    }
}

void UringReadExecutor::Shutdown() { impl_->Shutdown(); }

bool UringReadExecutor::healthy() const noexcept {
    std::lock_guard lock(impl_->mutex);
    return !impl_->stopping && !impl_->failed;
}

struct UringPipelineDriver::Impl {
    enum class Stage { kIdle, kRead, kWrite, kReady };
    struct Record {
        Stage stage{Stage::kIdle};
        IoCompletion event;
    };

    Impl(std::vector<std::shared_ptr<ReadSource>> sources,
         std::vector<size_t> chunk_sources, std::vector<int> source_fds,
         std::shared_ptr<WriteTarget> target,
         std::shared_ptr<UringReadExecutor> reader, BoundedExecutor* writes,
         std::vector<SlotBuffer> buffers)
        : sources(std::move(sources)),
          chunk_sources(std::move(chunk_sources)),
          source_fds(std::move(source_fds)),
          target(std::move(target)),
          reader(std::move(reader)),
          writes(writes),
          buffers(std::move(buffers)),
          records(this->buffers.size()) {}

    ~Impl() {
        std::unique_lock lock(mutex);
        stopping = true;
        // Every accepted read or write callback decrements its counter and
        // notifies under mutex before dropping its last access to this Impl.
        changed.wait(lock, [&] { return !reads_running && !writes_running; });
    }

    bool Valid(const SlotToken& token, const ReadChunk& chunk,
               SlotBuffer buffer) const {
        return token.generation && token.slot_index < buffers.size() &&
               token.chunk_index < chunk_sources.size() && buffer.data &&
               chunk.read_bytes &&
               buffer.data == buffers[token.slot_index].data &&
               buffer.capacity == buffers[token.slot_index].capacity &&
               chunk.read_bytes <= buffer.capacity &&
               chunk.read_bytes <= INT_MAX &&
               chunk.file_offset <=
                   uint64_t(std::numeric_limits<off_t>::max()) &&
               chunk.read_bytes <= uint64_t(std::numeric_limits<off_t>::max()) -
                                       chunk.file_offset;
    }

    bool Read(const SlotToken& token, const ReadChunk& chunk,
              SlotBuffer buffer) noexcept {
        if (!Valid(token, chunk, buffer)) return false;
        std::unique_lock lock(mutex);
        auto& record = records[token.slot_index];
        if (stopping || record.stage != Stage::kIdle) return false;
        const size_t source_index = chunk_sources[token.chunk_index];
        const int fd = source_fds[source_index];
        record.stage = Stage::kRead;
        record.event = {token, SlotStage::kReading, 0, Error::kOk};
        ++reads_running;
        lock.unlock();

        bool accepted = false;
        try {
            accepted = reader->WaitSubmit(
                sources[source_index], fd, {buffer.data, chunk.read_bytes},
                chunk.file_offset, [this, token](ReadResult result) {
                    std::lock_guard done_lock(mutex);
                    auto& completed = records[token.slot_index];
                    completed.event.bytes = result.bytes;
                    completed.event.result = result.error;
                    completed.event.sys_errno = result.sys_errno;
                    completed.stage = Stage::kReady;
                    --reads_running;
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

    bool Write(const SlotToken& token, const ReadChunk& chunk,
               SlotBuffer buffer) noexcept {
        if (!Valid(token, chunk, buffer)) return false;
        if (writes == nullptr) {
            {
                std::lock_guard lock(mutex);
                auto& record = records[token.slot_index];
                if (stopping || record.stage != Stage::kIdle) return false;
                record.stage = Stage::kWrite;
                record.event = {token, SlotStage::kWriting, 0, Error::kOk};
            }
            const auto result = target->Write(chunk, buffer);
            std::lock_guard lock(mutex);
            auto& completed = records[token.slot_index];
            completed.event.result = result;
            completed.event.bytes =
                result == Error::kOk ? chunk.payload_bytes : 0;
            completed.stage = Stage::kReady;
            changed.notify_all();
            return true;
        }
        {
            std::lock_guard lock(mutex);
            auto& record = records[token.slot_index];
            if (stopping || record.stage != Stage::kIdle) return false;
            record.stage = Stage::kWrite;
            record.event = {token, SlotStage::kWriting, 0, Error::kOk};
            ++writes_running;
        }

        bool accepted = false;
        try {
            accepted =
                writes->WaitSubmit([this, token, buffer, chunk_ptr = &chunk] {
                    const auto result = target->Write(*chunk_ptr, buffer);
                    std::lock_guard done_lock(mutex);
                    auto& completed = records[token.slot_index];
                    completed.event.result = result;
                    completed.event.bytes =
                        result == Error::kOk ? chunk_ptr->payload_bytes : 0;
                    completed.stage = Stage::kReady;
                    --writes_running;
                    changed.notify_all();
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

    bool Wait(IoCompletion& event, std::chrono::milliseconds timeout) {
        const auto until = ClampedDeadline(timeout);
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

    std::vector<std::shared_ptr<ReadSource>> sources;
    const std::vector<size_t> chunk_sources;
    std::vector<int> source_fds;
    std::shared_ptr<WriteTarget> target;
    std::shared_ptr<UringReadExecutor> reader;
    BoundedExecutor* writes;
    std::vector<SlotBuffer> buffers;
    std::vector<Record> records;
    std::mutex mutex;
    std::condition_variable changed;
    size_t reads_running{0}, writes_running{0};
    bool stopping{false};
};

UringPipelineDriver::Creation UringPipelineDriver::Create(
    std::vector<std::shared_ptr<ReadSource>> sources,
    std::vector<size_t> chunk_sources, std::shared_ptr<WriteTarget> target,
    std::shared_ptr<UringReadExecutor> reader, BoundedExecutor* writes,
    std::vector<SlotBuffer> buffers) {
    if (!reader || !reader->healthy()) return {Error::kNotSupported, {}};
    if (sources.empty() || chunk_sources.empty() ||
        std::any_of(chunk_sources.begin(), chunk_sources.end(),
                    [&](size_t index) { return index >= sources.size(); }))
        return {Error::kInvalidArgument, {}};
    std::vector<int> source_fds;
    source_fds.reserve(sources.size());
    for (const auto& source : sources) {
        if (!source) return {Error::kInvalidArgument, {}};
        const auto fd = source->native_read_fd();
        if (!fd || *fd < 0 || fcntl(*fd, F_GETFD) < 0)
            return {Error::kNotSupported, {}};
        source_fds.push_back(*fd);
    }
    if (!target || buffers.empty() || buffers.size() > 64)
        return {Error::kInvalidArgument, {}};
    std::vector<std::pair<uintptr_t, uintptr_t>> spans;
    spans.reserve(buffers.size());
    for (auto buffer : buffers) {
        const auto begin = reinterpret_cast<uintptr_t>(buffer.data);
        if (!begin || !buffer.capacity || buffer.capacity > INT_MAX ||
            buffer.capacity > std::numeric_limits<uintptr_t>::max() - begin)
            return {Error::kInvalidArgument, {}};
        spans.emplace_back(begin, begin + buffer.capacity);
    }
    std::sort(spans.begin(), spans.end());
    for (size_t i = 1; i < spans.size(); ++i) {
        if (spans[i].first < spans[i - 1].second)
            return {Error::kInvalidArgument, {}};
    }
    try {
        auto impl = std::make_unique<Impl>(
            std::move(sources), std::move(chunk_sources), std::move(source_fds),
            std::move(target), std::move(reader), writes, std::move(buffers));
        return {Error::kOk, std::unique_ptr<UringPipelineDriver>(
                                new UringPipelineDriver(std::move(impl)))};
    } catch (...) {
        return {Error::kIoError, {}};
    }
}

UringPipelineDriver::UringPipelineDriver(std::unique_ptr<Impl> p)
    : impl_(std::move(p)) {}

UringPipelineDriver::~UringPipelineDriver() = default;

bool UringPipelineDriver::SubmitRead(const SlotToken& t, const ReadChunk& c,
                                     SlotBuffer b) noexcept {
    return impl_->Read(t, c, b);
}

bool UringPipelineDriver::SubmitWrite(const SlotToken& t, const ReadChunk& c,
                                      SlotBuffer b) noexcept {
    return impl_->Write(t, c, b);
}

bool UringPipelineDriver::WaitForEvent(IoCompletion& c,
                                       std::chrono::milliseconds t) {
    return impl_->Wait(c, t);
}

}  // namespace mooncake::ssd_offload
