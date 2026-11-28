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
            // I/O tasks encode their errors in completions. Keep this executor
            // alive if a callback unexpectedly throws.
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
    bool WaitSubmit(std::function<void()> task) noexcept {
        if (!task) return false;
        try {
            std::unique_lock lock(mutex);
            changed.wait(lock, [&] { return stopping || pending < limit; });
            if (stopping) return false;
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
bool BoundedExecutor::WaitSubmit(std::function<void()> task) noexcept {
    return impl_->WaitSubmit(std::move(task));
}
void BoundedExecutor::Shutdown() { impl_->Shutdown(); }

struct ThreadedPipelineDriver::Impl {
    enum class State { kIdle, kRunning, kReady };
    struct Record {
        State state{State::kIdle};
        IoCompletion event;
    };
    Impl(std::vector<std::shared_ptr<ReadSource>> sources,
         std::vector<size_t> chunk_sources, std::shared_ptr<WriteTarget> target,
         BoundedExecutor& reads, BoundedExecutor* writes, size_t count)
        : sources(std::move(sources)),
          chunk_sources(std::move(chunk_sources)),
          target(std::move(target)),
          reads(reads),
          writes(writes),
          records(count) {
        if (this->sources.empty() ||
            std::any_of(this->sources.begin(), this->sources.end(),
                        [](const auto& source) { return !source; }) ||
            this->chunk_sources.empty() ||
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
        changed.wait(lock, [&] { return running == 0; });
    }
    bool Submit(const SlotToken& token, const ReadChunk& chunk,
                SlotBuffer buffer, SlotStage stage) noexcept {
        if (!token.generation || token.slot_index >= records.size() ||
            !buffer.data || !chunk.read_bytes ||
            chunk.read_bytes > buffer.capacity ||
            token.chunk_index >= chunk_sources.size())
            return false;
        if (stage == SlotStage::kWriting && writes == nullptr) {
            {
                std::lock_guard lock(mutex);
                auto& record = records[token.slot_index];
                if (stopping || record.state != State::kIdle) return false;
                record.event = {token, stage, 0, Error::kOk};
                record.state = State::kRunning;
            }
            const auto result = target->Write(chunk, buffer);
            std::lock_guard lock(mutex);
            auto& record = records[token.slot_index];
            record.event.result = result;
            record.event.bytes = result == Error::kOk ? chunk.payload_bytes : 0;
            record.state = State::kReady;
            changed.notify_all();
            return true;
        }
        std::shared_ptr<ReadSource> source;
        if (stage == SlotStage::kReading)
            source = sources[chunk_sources[token.chunk_index]];
        {
            std::lock_guard lock(mutex);
            auto& record = records[token.slot_index];
            if (stopping || record.state != State::kIdle) return false;
            record.event = {token, stage, 0, Error::kOk};
            record.state = State::kRunning;
            ++running;
        }
        bool accepted = false;
        try {
            const size_t read_bytes = chunk.read_bytes;
            const uint64_t file_offset = chunk.file_offset;
            auto task = [this, token, stage, buffer, read_bytes, file_offset,
                         chunk_ptr = &chunk,
                         source = std::move(source)]() mutable {
                IoCompletion event{token, stage, 0, Error::kOk};
                if (stage == SlotStage::kReading) {
                    const auto result =
                        source->ReadAt({buffer.data, read_bytes}, file_offset);
                    event.bytes = result.bytes;
                    event.result = result.error;
                    event.sys_errno = result.sys_errno;
                } else {
                    event.result = target->Write(*chunk_ptr, buffer);
                    event.bytes = event.result == Error::kOk
                                      ? chunk_ptr->payload_bytes
                                      : 0;
                }
                source.reset();
                // Notify under mutex so destructor drain cannot race a late
                // notify().
                std::lock_guard lock(mutex);
                auto& record = records[token.slot_index];
                record.event = event;
                record.state = State::kReady;
                --running;
                changed.notify_all();
            };
            accepted = (stage == SlotStage::kReading ? reads : *writes)
                           .WaitSubmit(std::move(task));
        } catch (...) {
        }
        if (!accepted) {
            std::lock_guard lock(mutex);
            records[token.slot_index].state = State::kIdle;
            --running;
            changed.notify_all();
        }
        return accepted;
    }
    bool Wait(IoCompletion& event, std::chrono::milliseconds timeout) {
        const auto start = std::chrono::steady_clock::now();
        const auto clamped =
            std::max(timeout, std::chrono::milliseconds::zero());
        const auto max_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::time_point::max() - start);
        const auto until = clamped >= max_ms
                               ? std::chrono::steady_clock::time_point::max()
                               : start + clamped;
        std::unique_lock lock(mutex);
        auto ready = [&] {
            return std::find_if(records.begin(), records.end(),
                                [](const auto& record) {
                                    return record.state == State::kReady;
                                });
        };
        if (!changed.wait_until(lock, until, [&] {
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
    std::shared_ptr<WriteTarget> target;
    BoundedExecutor& reads;
    BoundedExecutor* writes;
    std::vector<Record> records;
    std::mutex mutex;
    std::condition_variable changed;
    size_t running{0};
    bool stopping{false};
};

ThreadedPipelineDriver::ThreadedPipelineDriver(
    std::vector<std::shared_ptr<ReadSource>> sources,
    std::vector<size_t> chunk_sources, std::shared_ptr<WriteTarget> target,
    BoundedExecutor& reads, BoundedExecutor* writes, size_t slot_count)
    : impl_(std::make_unique<Impl>(std::move(sources), std::move(chunk_sources),
                                   std::move(target), reads, writes,
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
bool ThreadedPipelineDriver::WaitForEvent(IoCompletion& event,
                                          std::chrono::milliseconds timeout) {
    return impl_->Wait(event, timeout);
}

}  // namespace mooncake::ssd_offload
