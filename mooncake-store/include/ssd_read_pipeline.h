// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

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
    kCancelled,
    kNotSupported,
};

// Opaque registry identity, not a pointer or an RNIC access key.
struct DestinationSlice {
    uint64_t region_id;
    uint64_t generation;
    uint64_t offset;
    uint64_t length;
};

struct ReadRange {
    uint64_t source_offset;
    std::vector<DestinationSlice> destinations;
};

struct ObjectRead {
    uint64_t object_bytes;
    uint64_t value_file_offset;
    std::vector<ReadRange> ranges;
};

struct PlanOptions {
    size_t slot_bytes;
    size_t alignment;
    size_t max_chunks;
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

enum class SlotStage { kFree, kReading, kWriting };
enum class PipelineState {
    kReady,
    kRunning,
    kDraining,
    kSucceeded,
    kCancelled,
    kFailed,
};

struct SlotBuffer {
    std::byte* data;
    size_t capacity;
};

struct SlotToken {
    uint64_t request_id;
    uint64_t attempt_id;
    size_t slot_index;
    uint64_t generation;
    size_t chunk_index;
};

class PipelineDriver {
   public:
    virtual ~PipelineDriver() = default;

    // Called on the scheduler thread. No inline completion callbacks.
    // false means rejected with NO outstanding device access. After true,
    // the driver must deliver a staging-drained event even on
    // cancellation/error. Destination drain is independently tracked by the
    // remote write target. ReadChunk and buffers stay alive until that event
    // has been consumed.
    virtual bool SubmitRead(const SlotToken& token, const ReadChunk& chunk,
                            SlotBuffer buffer) noexcept = 0;
    virtual bool SubmitWrite(const SlotToken& token, const ReadChunk& chunk,
                             SlotBuffer buffer) noexcept = 0;
    // Best effort only. This call does not release a slot.
    virtual void RequestCancel(const SlotToken&, SlotStage) noexcept {}
};

// Single scheduler thread. Other workers enqueue events for that thread.
// Caller owns arena/MRs and driver; keep them and this object alive until
// inflight()==0. A software timeout is NOT a drained completion.
class ReadPipeline {
   public:
    ReadPipeline(uint64_t request_id, uint64_t attempt_id, ReadPlan plan,
                 std::vector<SlotBuffer> buffers, PipelineDriver& driver);
    ReadPipeline(const ReadPipeline&) = delete;
    ReadPipeline& operator=(const ReadPipeline&) = delete;
    ReadPipeline(ReadPipeline&&) = delete;
    ReadPipeline& operator=(ReadPipeline&&) = delete;

    Error Start();
    void Cancel();
    Error ReadDrained(const SlotToken& token, size_t bytes_read,
                      Error result = Error::kOk);
    Error WriteDrained(const SlotToken& token, Error result = Error::kOk);

    PipelineState state() const { return state_; }
    Error error() const { return error_; }
    size_t inflight() const { return inflight_; }
    // Successfully written bytes, including writes drained after cancellation.
    // Only kSucceeded permits publication of the complete destination object.
    uint64_t completed_bytes() const { return completed_bytes_; }

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

    uint64_t request_id_, attempt_id_;
    ReadPlan plan_;
    PipelineDriver& driver_;
    std::vector<Slot> slots_;
    PipelineState state_{PipelineState::kReady};
    Error error_{Error::kOk};
    size_t next_chunk_{0}, inflight_{0}, completed_chunks_{0};
    uint64_t completed_bytes_{0};
};

}  // namespace mooncake::ssd_offload
