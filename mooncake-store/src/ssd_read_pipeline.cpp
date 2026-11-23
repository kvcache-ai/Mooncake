// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_pipeline.h"

#include <algorithm>
#include <limits>
#include <tuple>
#include <utility>

namespace mooncake::ssd_offload {
namespace {

constexpr uint64_t kMaxFileOffset = std::numeric_limits<int64_t>::max();

bool ValidShape(size_t slot_bytes, size_t alignment) {
    return alignment != 0 && (alignment & (alignment - 1)) == 0 &&
           slot_bytes != 0 && slot_bytes % alignment == 0 &&
           slot_bytes <= kMaxFileOffset;
}

bool Add(uint64_t& total, uint64_t amount) {
    if (amount > std::numeric_limits<uint64_t>::max() - total) return false;
    total += amount;
    return true;
}

bool DisjointDestinations(std::vector<DestinationSlice>& destinations) {
    std::sort(
        destinations.begin(), destinations.end(),
        [](const auto& left, const auto& right) {
            return std::tie(left.region_id, left.generation, left.offset) <
                   std::tie(right.region_id, right.generation, right.offset);
        });
    for (size_t i = 1; i < destinations.size(); ++i) {
        const auto& previous = destinations[i - 1];
        const auto& current = destinations[i];
        if (previous.region_id == current.region_id &&
            previous.generation == current.generation &&
            current.offset < previous.offset + previous.length)
            return false;
    }
    return true;
}

}  // namespace

PlanResult BuildReadPlan(const ObjectRead& object, PlanOptions options) {
    auto fail = [](Error error) { return PlanResult{error, {}}; };
    if (!ValidShape(options.slot_bytes, options.alignment) ||
        options.max_chunks == 0 || options.max_fragments == 0)
        return fail(Error::kInvalidArgument);

    // Disallow racing writes into overlapping logical destination ranges.
    // Aliases between different region IDs must be rejected by the registry.
    std::vector<DestinationSlice> destinations;
    for (const auto& range : object.ranges) {
        for (const auto& destination : range.destinations) {
            if (destination.length == 0) continue;
            if (destination.region_id == 0 || destination.generation == 0)
                return fail(Error::kInvalidArgument);
            if (destination.length > UINT64_MAX - destination.offset)
                return fail(Error::kOverflow);
            if (destinations.size() == options.max_fragments)
                return fail(Error::kPlanLimit);
            destinations.push_back(destination);
        }
    }
    if (!DisjointDestinations(destinations))
        return fail(Error::kInvalidArgument);

    ReadPlan plan;
    plan.slot_bytes = options.slot_bytes;
    plan.alignment = options.alignment;
    size_t fragment_count = 0;
    for (const auto& range : object.ranges) {
        uint64_t range_bytes = 0;
        for (const auto& destination : range.destinations) {
            if (destination.length == 0) continue;
            if (destination.region_id == 0 || destination.generation == 0)
                return fail(Error::kInvalidArgument);
            if (destination.length > UINT64_MAX - destination.offset ||
                !Add(range_bytes, destination.length))
                return fail(Error::kOverflow);
        }
        if (range.source_offset > object.object_bytes ||
            range_bytes > object.object_bytes - range.source_offset)
            return fail(Error::kInvalidArgument);
        if (!Add(plan.payload_bytes, range_bytes))
            return fail(Error::kOverflow);
        if (range_bytes == 0) continue;
        if (object.value_file_offset > kMaxFileOffset ||
            range.source_offset > kMaxFileOffset - object.value_file_offset)
            return fail(Error::kOverflow);
        uint64_t position = object.value_file_offset + range.source_offset;
        if (range_bytes > kMaxFileOffset - position)
            return fail(Error::kOverflow);

        uint64_t remaining = range_bytes;
        uint64_t source_offset = range.source_offset;
        size_t destination_index = 0;
        uint64_t destination_consumed = 0;
        while (remaining != 0) {
            if (plan.chunks.size() == options.max_chunks)
                return fail(Error::kPlanLimit);
            const uint64_t file_offset =
                position & ~(static_cast<uint64_t>(options.alignment) - 1);
            const size_t head = static_cast<size_t>(position - file_offset);
            const size_t payload = static_cast<size_t>(
                std::min<uint64_t>(remaining, options.slot_bytes - head));
            // head+payload <= slot_bytes; division avoids round-up overflow.
            const size_t used = head + payload;
            const size_t read_bytes =
                (used / options.alignment + (used % options.alignment != 0)) *
                options.alignment;
            if (read_bytes > kMaxFileOffset - file_offset ||
                !Add(plan.io_bytes, read_bytes))
                return fail(Error::kOverflow);
            ReadChunk chunk{source_offset, file_offset, read_bytes,
                            head,          payload,     {}};
            size_t fragment_remaining = payload;
            size_t buffer_offset = head;
            while (fragment_remaining != 0) {
                const auto& destination = range.destinations[destination_index];
                const uint64_t available =
                    destination.length - destination_consumed;
                if (available == 0) {
                    ++destination_index;
                    destination_consumed = 0;
                    continue;
                }
                const auto length = static_cast<size_t>(
                    std::min<uint64_t>(fragment_remaining, available));
                auto fragment_destination = destination;
                fragment_destination.offset += destination_consumed;
                fragment_destination.length = length;
                if (fragment_count == options.max_fragments)
                    return fail(Error::kPlanLimit);
                ++fragment_count;
                chunk.fragments.push_back(
                    {buffer_offset, fragment_destination});
                buffer_offset += length;
                fragment_remaining -= length;
                destination_consumed += length;
            }
            plan.chunks.push_back(std::move(chunk));
            remaining -= payload;
            position += payload;
            source_offset += payload;
        }
    }
    return {Error::kOk, std::move(plan)};
}

ReadPipeline::ReadPipeline(uint64_t request_id, uint64_t attempt_id,
                           ReadPlan plan, std::vector<SlotBuffer> buffers,
                           PipelineDriver& driver)
    : request_id_(request_id),
      attempt_id_(attempt_id),
      plan_(std::move(plan)),
      driver_(driver) {
    for (const auto buffer : buffers) slots_.push_back({buffer});
}

Error ReadPipeline::Validate() const {
    if (!request_id_ || !attempt_id_ ||
        !ValidShape(plan_.slot_bytes, plan_.alignment))
        return Error::kInvalidArgument;
    uint64_t payload_bytes = 0, io_bytes = 0;
    std::vector<DestinationSlice> destinations;
    for (const auto& chunk : plan_.chunks) {
        if (chunk.read_bytes == 0 || chunk.read_bytes > plan_.slot_bytes ||
            chunk.read_bytes % plan_.alignment != 0 ||
            chunk.file_offset % plan_.alignment != 0 ||
            chunk.file_offset > kMaxFileOffset ||
            chunk.read_bytes > kMaxFileOffset - chunk.file_offset ||
            chunk.payload_bytes == 0 ||
            chunk.payload_offset > chunk.read_bytes ||
            chunk.payload_bytes > chunk.read_bytes - chunk.payload_offset)
            return Error::kInvalidArgument;
        size_t cursor = chunk.payload_offset;
        const size_t end = cursor + chunk.payload_bytes;
        for (const auto& fragment : chunk.fragments) {
            const auto& destination = fragment.destination;
            if (fragment.buffer_offset != cursor || !destination.region_id ||
                !destination.generation || destination.length == 0 ||
                destination.length > UINT64_MAX - destination.offset ||
                destination.length > end - cursor)
                return Error::kInvalidArgument;
            cursor += static_cast<size_t>(destination.length);
            destinations.push_back(destination);
        }
        if (cursor != end || !Add(payload_bytes, chunk.payload_bytes) ||
            !Add(io_bytes, chunk.read_bytes))
            return Error::kInvalidArgument;
    }
    if (payload_bytes != plan_.payload_bytes || io_bytes != plan_.io_bytes ||
        !DisjointDestinations(destinations))
        return Error::kInvalidArgument;
    if (plan_.chunks.empty()) return Error::kOk;
    if (slots_.empty()) return Error::kInvalidArgument;
    for (size_t i = 0; i < slots_.size(); ++i) {
        const auto& buffer = slots_[i].buffer;
        const auto base = reinterpret_cast<uintptr_t>(buffer.data);
        if (!buffer.data || buffer.capacity < plan_.slot_bytes ||
            base % plan_.alignment != 0 ||
            plan_.slot_bytes > std::numeric_limits<uintptr_t>::max() - base)
            return Error::kInvalidArgument;
        for (size_t j = 0; j < i; ++j) {
            const auto other =
                reinterpret_cast<uintptr_t>(slots_[j].buffer.data);
            if (base < other + plan_.slot_bytes &&
                other < base + plan_.slot_bytes)
                return Error::kInvalidArgument;
        }
    }
    return Error::kOk;
}

Error ReadPipeline::Start() {
    if (state_ != PipelineState::kReady) return Error::kInvalidState;
    const auto validation = Validate();
    if (validation != Error::kOk) {
        Abort(validation);
        return validation;
    }
    state_ = PipelineState::kRunning;
    Pump();
    return error_;
}

SlotToken ReadPipeline::Token(size_t slot_index) const {
    const auto& slot = slots_[slot_index];
    return {request_id_, attempt_id_, slot_index, slot.generation,
            slot.chunk_index};
}

bool ReadPipeline::Matches(const SlotToken& token, SlotStage stage) const {
    if (token.request_id != request_id_ || token.attempt_id != attempt_id_ ||
        token.slot_index >= slots_.size())
        return false;
    const auto& slot = slots_[token.slot_index];
    return slot.stage == stage && slot.generation == token.generation &&
           slot.chunk_index == token.chunk_index;
}

void ReadPipeline::Pump() {
    for (size_t i = 0; i < slots_.size() && state_ == PipelineState::kRunning &&
                       next_chunk_ < plan_.chunks.size();
         ++i) {
        auto& slot = slots_[i];
        if (slot.stage != SlotStage::kFree) continue;
        if (slot.generation == UINT64_MAX) {
            Abort(Error::kOverflow);
            break;
        }
        ++slot.generation;
        slot.chunk_index = next_chunk_++;
        slot.stage = SlotStage::kReading;
        ++inflight_;
        if (!driver_.SubmitRead(Token(i), plan_.chunks[slot.chunk_index],
                                slot.buffer)) {
            // Rejection guarantees no kernel/device is using this slot.
            Release(i);
            Abort(Error::kIoError);
        }
    }
    UpdateTerminal();
}

void ReadPipeline::Abort(Error error) {
    if (error_ != Error::kOk) return;
    error_ = error;
    state_ = PipelineState::kDraining;
    for (size_t i = 0; i < slots_.size(); ++i)
        if (slots_[i].stage != SlotStage::kFree)
            driver_.RequestCancel(Token(i), slots_[i].stage);
    UpdateTerminal();
}

void ReadPipeline::Cancel() {
    if (state_ == PipelineState::kReady || state_ == PipelineState::kRunning)
        Abort(Error::kCancelled);
}

void ReadPipeline::Release(size_t slot_index) {
    slots_[slot_index].stage = SlotStage::kFree;
    --inflight_;
}

void ReadPipeline::UpdateTerminal() {
    if (inflight_ != 0) return;
    if (error_ != Error::kOk) {
        state_ = error_ == Error::kCancelled ? PipelineState::kCancelled
                                             : PipelineState::kFailed;
    } else if (state_ == PipelineState::kRunning &&
               completed_chunks_ == plan_.chunks.size()) {
        state_ = PipelineState::kSucceeded;
    }
}

Error ReadPipeline::ReadDrained(const SlotToken& token, size_t bytes_read,
                                Error result) {
    if (!Matches(token, SlotStage::kReading)) return Error::kStaleEvent;
    auto& slot = slots_[token.slot_index];
    const auto& chunk = plan_.chunks[slot.chunk_index];
    if (state_ == PipelineState::kDraining) {
        Release(token.slot_index);
    } else if (result != Error::kOk ||
               bytes_read < chunk.payload_offset + chunk.payload_bytes ||
               bytes_read > chunk.read_bytes) {
        Release(token.slot_index);
        Abort(Error::kIoError);
    } else {
        slot.stage = SlotStage::kWriting;
        if (!driver_.SubmitWrite(token, chunk, slot.buffer)) {
            Release(token.slot_index);
            Abort(Error::kTransferError);
        }
    }
    Pump();
    return Error::kOk;
}

Error ReadPipeline::WriteDrained(const SlotToken& token, Error result) {
    if (!Matches(token, SlotStage::kWriting)) return Error::kStaleEvent;
    if (result == Error::kOk) {
        ++completed_chunks_;
        completed_bytes_ += plan_.chunks[token.chunk_index].payload_bytes;
    }
    Release(token.slot_index);
    if (result != Error::kOk) Abort(Error::kTransferError);
    Pump();
    return Error::kOk;
}

}  // namespace mooncake::ssd_offload
