// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace mooncake::ssd_offload {

enum class Error {
    kOk,
    kInvalidArgument,
    kOverflow,
    kPlanLimit,
    kInvalidState,
    kStaleEvent,
    kIoError,
    kTransferError,
    kNotSupported,
};

struct DestinationSlice {
    size_t destination_index;
    uint64_t offset;
    uint64_t length;
};

struct ObjectRead {
    uint64_t object_bytes;
    uint64_t value_file_offset;
    uint64_t source_offset;
    std::vector<DestinationSlice> destinations;
};

struct PlanOptions {
    size_t slot_bytes;
    size_t alignment{1};
    size_t max_chunks{65536};
    size_t max_fragments{65536};
};

struct WriteFragment {
    size_t buffer_offset;
    DestinationSlice destination;
};

struct ReadChunk {
    uint64_t source_offset;
    uint64_t file_offset;
    size_t read_bytes;
    size_t payload_offset;
    size_t payload_bytes;
    std::vector<WriteFragment> fragments;
};

struct ReadPlan {
    size_t slot_bytes{0};
    size_t alignment{0};
    uint64_t payload_bytes{0};
    uint64_t io_bytes{0};
    std::vector<ReadChunk> chunks;
};

struct PlanResult {
    Error error{Error::kOk};
    ReadPlan plan;
};

// Any failure returns an empty plan; no partial plan may be submitted.
PlanResult BuildReadPlan(const ObjectRead& object, PlanOptions options);

// Pure completion classifier for buffered io_uring read CQEs: handles short
// reads, transient -EINTR/-EAGAIN retries, EOF, and over-long completions.
struct UringReadStep {
    bool retry{false};
    size_t next_done{0};
    unsigned next_retries{0};
    Error error{Error::kOk};
    int sys_errno{0};
};

UringReadStep ClassifyUringReadResult(int completed, size_t done,
                                      size_t requested, unsigned retries,
                                      unsigned max_retries = 3) noexcept;

enum class SlotStage { kFree, kReading, kWriting };
enum class PipelineState {
    kReady,
    kRunning,
    kDraining,
    kSucceeded,
    kFailed,
};

struct SlotBuffer {
    std::byte* data;
    size_t capacity;
};

struct SlotToken {
    size_t slot_index;
    uint64_t generation;
    size_t chunk_index;
};

struct IoCompletion {
    SlotToken token{};
    SlotStage stage{SlotStage::kFree};
    size_t bytes{0};
    Error result{Error::kOk};
    int sys_errno{0};
};

class PipelineDriver {
   public:
    virtual ~PipelineDriver() = default;

    // Called on the scheduler thread. No inline completion callbacks.
    // false means rejected with NO outstanding I/O. After true, the driver
    // borrows both buffer and chunk until the completion event is delivered or
    // the driver is destroyed (so the owning ReadPlan must outlive the driver).
    virtual bool SubmitRead(const SlotToken& token, const ReadChunk& chunk,
                            SlotBuffer buffer) noexcept = 0;
    virtual bool SubmitWrite(const SlotToken& token, const ReadChunk& chunk,
                             SlotBuffer buffer) noexcept = 0;
    virtual bool WaitForEvent(IoCompletion&, std::chrono::milliseconds) {
        return false;
    }
};

// Single scheduler thread; worker threads deliver completions via WaitForEvent.
// Caller owns staging slots and driver and keeps them alive until inflight()==0
// (or relies on driver destruction draining accepted I/O first).
class ReadPipeline {
   public:
    ReadPipeline(ReadPlan plan, std::vector<SlotBuffer> buffers,
                 PipelineDriver& driver);
    ReadPipeline(const ReadPipeline&) = delete;
    ReadPipeline& operator=(const ReadPipeline&) = delete;
    ReadPipeline(ReadPipeline&&) = delete;
    ReadPipeline& operator=(ReadPipeline&&) = delete;

    Error Start();
    Error ReadDrained(const SlotToken& token, size_t bytes_read,
                      Error result = Error::kOk);
    Error WriteDrained(const SlotToken& token, Error result = Error::kOk);

    PipelineState state() const { return state_; }
    Error error() const { return error_; }
    size_t inflight() const { return inflight_; }

   private:
    struct Slot {
        SlotBuffer buffer;
        SlotStage stage{SlotStage::kFree};
        uint64_t generation{0};
        size_t chunk_index{0};
    };
    Error Validate() const;
    void Pump();
    void Abort(Error error);
    void Release(size_t slot_index);
    void UpdateTerminal();
    bool Matches(const SlotToken& token, SlotStage stage) const;
    SlotToken Token(size_t slot_index) const;

    ReadPlan plan_;
    PipelineDriver& driver_;
    std::vector<Slot> slots_;
    PipelineState state_{PipelineState::kReady};
    Error error_{Error::kOk};
    size_t next_chunk_{0}, inflight_{0}, completed_chunks_{0};
};

}  // namespace mooncake::ssd_offload
