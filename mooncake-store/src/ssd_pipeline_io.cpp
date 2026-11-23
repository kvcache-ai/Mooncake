// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_pipeline_io.h"

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake::ssd_offload {

struct BoundedExecutor::Impl {
    explicit Impl(size_t count, size_t limit) : limit(limit) {
        if (!count || !limit)
            throw std::invalid_argument("SSD executor limits must be positive");
        try {
            for (size_t i = 0; i < count; ++i)
                workers.emplace_back([this] { Work(); });
        } catch (...) {
            Shutdown();
            throw;
        }
    }
    void Work() {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock lock(mutex);
                changed.wait(lock, [&] { return stopping || !tasks.empty(); });
                if (tasks.empty()) return;
                task = std::move(tasks.front());
                tasks.pop();
            }
            // I/O tasks encode their errors in completions. Keep this generic
            // executor alive if a non-I/O task unexpectedly throws.
            try {
                task();
            } catch (...) {
            }
            task = {};
            {
                std::lock_guard lock(mutex);
                --pending;
                changed.notify_all();
            }
        }
    }
    bool Submit(std::function<void()> task, bool wait) noexcept {
        try {
            std::unique_lock lock(mutex);
            if (wait) {
                changed.wait(lock,
                             [&] { return stopping || pending < limit; });
            }
            if (stopping || pending == limit || !task) return false;
            tasks.push(std::move(task));
            ++pending;
            changed.notify_all();
            return true;
        } catch (...) {
            return false;
        }
    }
    void Shutdown() {
        std::lock_guard serial(shutdown_mutex);
        {
            std::lock_guard lock(mutex);
            stopping = true;
            changed.notify_all();
        }
        for (auto& worker : workers)
            if (worker.joinable()) worker.join();
    }
    size_t limit, pending{0};
    bool stopping{false};
    mutable std::mutex mutex;
    std::mutex shutdown_mutex;
    std::condition_variable changed;
    std::queue<std::function<void()>> tasks;
    std::vector<std::thread> workers;
};

BoundedExecutor::BoundedExecutor(size_t workers, size_t max_outstanding)
    : impl_(std::make_unique<Impl>(workers, max_outstanding)) {}
BoundedExecutor::~BoundedExecutor() { Shutdown(); }
bool BoundedExecutor::TrySubmit(std::function<void()> task) noexcept {
    return impl_->Submit(std::move(task), false);
}
bool BoundedExecutor::WaitSubmit(std::function<void()> task) noexcept {
    return impl_->Submit(std::move(task), true);
}
void BoundedExecutor::Shutdown() { impl_->Shutdown(); }
size_t BoundedExecutor::outstanding() const {
    std::lock_guard lock(impl_->mutex);
    return impl_->pending;
}

namespace {
bool SameToken(const SlotToken& a, const SlotToken& b) {
    return a.request_id == b.request_id && a.attempt_id == b.attempt_id &&
           a.slot_index == b.slot_index && a.generation == b.generation &&
           a.chunk_index == b.chunk_index;
}
}  // namespace

struct ThreadedPipelineDriver::Impl {
    enum class State { kIdle, kRunning, kReady };
    struct Record {
        State state{State::kIdle};
        IoCompletion event;
        bool cancelled{false};
    };
    Impl(std::vector<std::shared_ptr<ReadSource>> sources,
         std::vector<size_t> chunk_sources, bool mapped_sources,
         std::shared_ptr<SourceDrainedWriteTarget> target,
         BoundedExecutor& reads, BoundedExecutor& writes, size_t count)
        : sources(std::move(sources)),
          chunk_sources(std::move(chunk_sources)),
          target(std::move(target)),
          reads(reads),
          writes(writes),
          records(count) {
        if (this->sources.empty() ||
            std::any_of(this->sources.begin(), this->sources.end(),
                        [](const auto& source) { return !source; }) ||
            (mapped_sources && this->chunk_sources.empty()) ||
            std::any_of(this->chunk_sources.begin(), this->chunk_sources.end(),
                        [&](size_t source) {
                            return source >= this->sources.size();
                        }) ||
            !this->target || !count)
            throw std::invalid_argument(
                "SSD I/O driver requires valid sources, mapping, target and "
                "slots");
    }
    ~Impl() {
        std::unique_lock lock(mutex);
        stopping = true;
        for (auto& record : records) record.cancelled = true;
        changed.wait(lock, [&] { return running == 0; });
    }
    bool Submit(const SlotToken& token, const ReadChunk& chunk,
                SlotBuffer buffer, SlotStage stage) noexcept {
        if (!token.request_id || !token.attempt_id || !token.generation ||
            token.slot_index >= records.size() || !buffer.data ||
            !chunk.read_bytes || chunk.read_bytes > buffer.capacity ||
            (!chunk_sources.empty() &&
             token.chunk_index >= chunk_sources.size()))
            return false;
        // Resolve before acceptance; a queued read owns this exact fd/lease,
        // independent of any other source progressing in the shared slots.
        std::shared_ptr<ReadSource> source;
        if (stage == SlotStage::kReading)
            source = sources[chunk_sources.empty()
                                 ? 0
                                 : chunk_sources[token.chunk_index]];
        {
            std::lock_guard lock(mutex);
            auto& record = records[token.slot_index];
            if (stopping || record.state != State::kIdle) return false;
            record.event = {token, stage, 0, Error::kOk};
            record.cancelled = false;
            record.state = State::kRunning;
            ++running;
        }
        bool accepted = false;
        try {
            auto task = [this, token, stage, buffer, chunk_ptr = &chunk,
                         source = std::move(source)]() mutable {
                bool cancelled;
                {
                    std::lock_guard lock(mutex);
                    cancelled = records[token.slot_index].cancelled;
                }
                IoCompletion event{token, stage, 0, Error::kCancelled};
                if (!cancelled) {
                    if (stage == SlotStage::kReading) {
                        const auto result =
                            source->ReadAt({buffer.data, chunk_ptr->read_bytes},
                                           chunk_ptr->file_offset);
                        event.bytes = result.bytes;
                        event.result = result.error;
                    } else {
                        event.result =
                            target->WriteAndDrain(*chunk_ptr, buffer);
                    }
                }
                // No source access remains after ReadAt returns. Drop the
                // job's extra fd/lease hold before publishing its completion;
                // the immutable source table still pins it through teardown.
                source.reset();
                // Notification is inside the lock: after the scheduler can
                // consume this event, the worker never accesses this driver
                // again. Driver destruction cannot race a late notify().
                std::lock_guard lock(mutex);
                auto& record = records[token.slot_index];
                record.event = event;
                record.state = State::kReady;
                --running;
                changed.notify_all();
            };
            accepted = (stage == SlotStage::kReading ? reads : writes)
                           .WaitSubmit(std::move(task));
        } catch (...) {
            // Task allocation failed before executor acceptance.
        }
        if (!accepted) {
            std::lock_guard lock(mutex);
            records[token.slot_index].state = State::kIdle;
            --running;
            changed.notify_all();
        }
        return accepted;
    }
    void Cancel(const SlotToken& token, SlotStage stage) {
        std::lock_guard lock(mutex);
        if (token.slot_index >= records.size()) return;
        auto& record = records[token.slot_index];
        if (record.state == State::kRunning && record.event.stage == stage &&
            SameToken(record.event.token, token))
            record.cancelled = true;
    }
    bool Wait(IoCompletion& event, std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex);
        auto ready = [&] {
            return std::find_if(records.begin(), records.end(),
                                [](const auto& record) {
                                    return record.state == State::kReady;
                                });
        };
        if (!changed.wait_for(lock, timeout, [&] {
                return ready() != records.end() || running == 0;
            }))
            return false;
        auto found = ready();
        if (found == records.end()) return false;
        event = found->event;
        found->state = State::kIdle;
        return true;
    }
    const std::vector<std::shared_ptr<ReadSource>> sources;
    const std::vector<size_t> chunk_sources;
    std::shared_ptr<SourceDrainedWriteTarget> target;
    BoundedExecutor& reads;
    BoundedExecutor& writes;
    std::vector<Record> records;
    std::mutex mutex;
    std::condition_variable changed;
    size_t running{0};
    bool stopping{false};
};

ThreadedPipelineDriver::ThreadedPipelineDriver(
    std::shared_ptr<ReadSource> source,
    std::shared_ptr<SourceDrainedWriteTarget> target, BoundedExecutor& reads,
    BoundedExecutor& writes, size_t slot_count)
    : impl_(std::make_unique<Impl>(
          std::vector<std::shared_ptr<ReadSource>>{std::move(source)},
          std::vector<size_t>{}, false, std::move(target), reads, writes,
          slot_count)) {}
ThreadedPipelineDriver::ThreadedPipelineDriver(
    std::vector<std::shared_ptr<ReadSource>> sources,
    std::vector<size_t> chunk_sources,
    std::shared_ptr<SourceDrainedWriteTarget> target, BoundedExecutor& reads,
    BoundedExecutor& writes, size_t slot_count)
    : impl_(std::make_unique<Impl>(std::move(sources), std::move(chunk_sources),
                                   true, std::move(target), reads, writes,
                                   slot_count)) {}
ThreadedPipelineDriver::~ThreadedPipelineDriver() = default;
bool ThreadedPipelineDriver::SubmitRead(const SlotToken& token,
                                        const ReadChunk& chunk,
                                        SlotBuffer buffer) noexcept {
    return impl_->Submit(token, chunk, buffer, SlotStage::kReading);
}
bool ThreadedPipelineDriver::SubmitWrite(const SlotToken& token,
                                         const ReadChunk& chunk,
                                         SlotBuffer buffer) noexcept {
    return impl_->Submit(token, chunk, buffer, SlotStage::kWriting);
}
void ThreadedPipelineDriver::RequestCancel(const SlotToken& token,
                                           SlotStage stage) noexcept {
    impl_->Cancel(token, stage);
}
bool ThreadedPipelineDriver::WaitForEvent(IoCompletion& event,
                                          std::chrono::milliseconds timeout) {
    return impl_->Wait(event, timeout);
}

}  // namespace mooncake::ssd_offload
