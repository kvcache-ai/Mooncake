// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_transfer_target.h"
#include "transfer_engine.h"
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <limits>

namespace mooncake::ssd_offload {
namespace {
bool RegistrationCovers(const TransferMetadata::SegmentDesc& desc,
                        uint64_t address, uint64_t bytes, int key = 0) {
    if (key == 0) {
        // TCP dispatch validates one BufferDesc for a complete frame. Adjacent
        // MRs cannot authorize a larger frame; reject before any I/O.
        return std::any_of(
            desc.buffers.begin(), desc.buffers.end(), [&](const auto& buffer) {
                return address >= buffer.addr &&
                       address - buffer.addr <= buffer.length &&
                       bytes <= buffer.length - (address - buffer.addr);
            });
    }
    uint64_t cursor = address;
    if (bytes > std::numeric_limits<uint64_t>::max() - address) return false;
    const auto end = address + bytes;
    do {
        uint64_t covered = cursor;
        bool found = false;
        for (const auto& buffer : desc.buffers) {
            if ((key == 1 && buffer.lkey.empty()) ||
                (key == 2 && buffer.rkey.empty()) || cursor < buffer.addr ||
                cursor - buffer.addr > buffer.length ||
                buffer.length >
                    std::numeric_limits<uint64_t>::max() - buffer.addr)
                continue;
            const auto last = buffer.addr + buffer.length;
            if (last > covered || (!bytes && last == covered)) {
                covered = last;
                found = true;
            }
        }
        if (!found || (bytes && covered == cursor)) return false;
        if (covered >= end) return true;
        cursor = covered;
    } while (cursor < end);
    return false;
}
class ClassicWriteTarget final : public SourceDrainedWriteTarget {
   public:
    ClassicWriteTarget(std::shared_ptr<TransferEngine> engine,
                       std::vector<ResolvedDestination> views, size_t limit,
                       bool local_destination, bool rdma = false)
        : engine_(std::move(engine)),
          views_(std::move(views)),
          limit_(limit),
          local_destination_(local_destination),
          rdma_(rdma) {}
    Error WriteAndDrain(const ReadChunk& chunk,
                        SlotBuffer slot) noexcept override {
        try {
            if (!slot.data || !chunk.read_bytes ||
                chunk.read_bytes > slot.capacity ||
                chunk.fragments.size() > limit_)
                return Error::kInvalidArgument;
            const auto source_begin = reinterpret_cast<uintptr_t>(slot.data);
            if (slot.capacity >
                std::numeric_limits<uintptr_t>::max() - source_begin)
                return Error::kInvalidArgument;
            const auto local =
                engine_->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
            if (!local || !RegistrationCovers(
                              *local, reinterpret_cast<uint64_t>(slot.data),
                              slot.capacity, rdma_ ? 1 : 0))
                return Error::kInvalidArgument;
            // Complete fragment validation precedes the first TE submission.
            for (const auto& fragment : chunk.fragments) {
                const auto& d = fragment.destination;
                if (!d.region_id || d.region_id > views_.size() ||
                    d.generation != 1)
                    return Error::kInvalidArgument;
                const auto& view = views_[d.region_id - 1];
                if (d.offset > view.length ||
                    d.length > view.length - d.offset ||
                    fragment.buffer_offset > chunk.read_bytes ||
                    d.length > chunk.read_bytes - fragment.buffer_offset)
                    return Error::kInvalidArgument;
                const auto begin = view.address + d.offset;
                if (local_destination_ && d.length &&
                    begin < source_begin + slot.capacity &&
                    begin + d.length > source_begin)
                    return Error::kInvalidArgument;
            }
            std::vector<size_t> local_offsets, remote_offsets, lengths;
            local_offsets.reserve(chunk.fragments.size());
            remote_offsets.reserve(chunk.fragments.size());
            lengths.reserve(chunk.fragments.size());
            uint64_t remote_begin = std::numeric_limits<uint64_t>::max();
            uint64_t remote_end = 0;
            for (const auto& fragment : chunk.fragments) {
                const auto& view = views_[fragment.destination.region_id - 1];
                const auto begin = view.address + fragment.destination.offset;
                remote_begin = std::min(remote_begin, begin);
                remote_end =
                    std::max(remote_end, begin + fragment.destination.length);
            }
            for (const auto& fragment : chunk.fragments) {
                const auto& view = views_[fragment.destination.region_id - 1];
                local_offsets.push_back(fragment.buffer_offset);
                remote_offsets.push_back(
                    view.address + fragment.destination.offset - remote_begin);
                lengths.push_back(fragment.destination.length);
            }
            std::vector<TransferEngine::ScatterTransferRange> ranges;
            if (!chunk.fragments.empty()) {
                // Preparation requires one endpoint, and the complete per-view
                // authorization above still precedes submission. This span is
                // only an address encoding envelope: gaps never become writes.
                if (remote_end - remote_begin >
                    std::numeric_limits<size_t>::max())
                    return Error::kInvalidArgument;
                ranges.push_back({TransferRequest::WRITE,
                                  views_.front().transport_endpoint,
                                  remote_begin,
                                  size_t(remote_end - remote_begin),
                                  slot.data,
                                  slot.capacity,
                                  local_offsets,
                                  remote_offsets,
                                  lengths,
                                  {}});
                // Preserve destination ACK even if routing switches to TCP.
                ranges.back().require_tcp_write_ack = true;
            }
            // submitScatter owns request metadata before publication. Even a
            // submission exception retains or drains committed tasks before
            // return/throw. A wait timeout is never used as this slot's
            // barrier.
            auto operation = engine_->submitScatter(ranges);
            if (operation.wait().ok()) return Error::kOk;
            destinations_drained_.store(false, std::memory_order_relaxed);
            return Error::kTransferError;
        } catch (...) {
            // Source references have drained during operation unwinding. A
            // transport failure/exception is still no remote destination proof.
            destinations_drained_.store(false, std::memory_order_relaxed);
            return Error::kTransferError;
        }
    }
    bool DestinationsDrained() const noexcept override {
        return destinations_drained_.load(std::memory_order_relaxed);
    }

   private:
    std::shared_ptr<TransferEngine> engine_;
    std::vector<ResolvedDestination> views_;
    size_t limit_;
    bool local_destination_;
    bool rdma_;
    std::atomic<bool> destinations_drained_{true};
};
}  // namespace
SsdTransferTargetPreparation PrepareSsdTcpWriteTarget(
    std::shared_ptr<TransferEngine> engine,
    std::vector<ResolvedDestination> views, size_t limit) {
    if (!limit || limit > 65536 || views.size() > limit)
        return {DestinationError::kLimit, {}};
    if (!engine || engine->isUsingTent() || !engine->isTcpOnly() ||
        engine->getTransport("shm"))
        return {DestinationError::kNotSupported, {}};
    const char* legacy = std::getenv("MC_TCP_PROTO");
    if (legacy && std::string_view(legacy) == "1")
        return {DestinationError::kNotSupported, {}};
    std::vector<std::pair<uint64_t, uint64_t>> intervals;
    std::string endpoint;
    for (const auto& view : views) {
        if (view.memory != DestinationMemory::kHost)
            return {DestinationError::kNotSupported, {}};
        if (!view.address || view.transport_endpoint.empty() ||
            view.transport_endpoint.size() > 4096 ||
            (!endpoint.empty() && endpoint != view.transport_endpoint))
            return {DestinationError::kInvalidArgument, {}};
        if (view.address > std::numeric_limits<uintptr_t>::max() ||
            view.length > std::numeric_limits<size_t>::max() ||
            view.length > std::numeric_limits<uint64_t>::max() - view.address)
            return {DestinationError::kOverflow, {}};
        endpoint = view.transport_endpoint;
        if (view.length)
            intervals.emplace_back(view.address, view.address + view.length);
    }
    std::sort(intervals.begin(), intervals.end());
    for (size_t i = 1; i < intervals.size(); ++i)
        if (intervals[i].first < intervals[i - 1].second)
            return {DestinationError::kOverlap, {}};
    bool local_destination = false;
    if (!views.empty()) {
        const auto segment = engine->openSegment(endpoint);
        struct SegmentCloser {
            TransferEngine& engine;
            SegmentHandle segment;
            ~SegmentCloser() { engine.closeSegment(segment); }
        } closer{*engine, segment};
        auto desc = engine->getMetadata()->getSegmentDescByID(segment);
        if (!desc || desc->protocol != "tcp" || desc->tcp_proto_version < 2)
            return {DestinationError::kNotSupported, {}};
        auto covered = [&] {
            return std::all_of(
                views.begin(), views.end(), [&](const auto& view) {
                    return RegistrationCovers(*desc, view.address, view.length);
                });
        };
        // A destination may have been registered after the owner's last cache
        // snapshot. Refresh once for the complete batch, never per fragment.
        if (!covered())
            desc = engine->getMetadata()->getSegmentDescByID(segment, true);
        if (!desc || desc->protocol != "tcp" || desc->tcp_proto_version < 2)
            return {DestinationError::kNotSupported, {}};
        if (!covered()) return {DestinationError::kInvalidArgument, {}};
        const auto local =
            engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        local_destination = local &&
                            desc->tcp_data_host == local->tcp_data_host &&
                            desc->tcp_data_port == local->tcp_data_port;
    }
    return {DestinationError::kOk,
            std::make_shared<ClassicWriteTarget>(
                std::move(engine), std::move(views), limit, local_destination)};
}
SsdTransferTargetPreparation PrepareSsdRdmaWriteTarget(
    std::shared_ptr<TransferEngine> engine,
    std::vector<ResolvedDestination> views, size_t limit) {
    if (!limit || limit > 65536 || views.size() > limit)
        return {DestinationError::kLimit, {}};
    if (!engine || engine->isUsingTent() || !engine->getTransport("rdma"))
        return {DestinationError::kNotSupported, {}};
    std::vector<std::pair<uint64_t, uint64_t>> intervals;
    std::string endpoint;
    for (const auto& view : views) {
        if (view.memory != DestinationMemory::kHost)
            return {DestinationError::kNotSupported, {}};
        if (!view.address || view.transport_endpoint.empty() ||
            view.transport_endpoint.size() > 4096 ||
            (!endpoint.empty() && endpoint != view.transport_endpoint))
            return {DestinationError::kInvalidArgument, {}};
        if (view.address > std::numeric_limits<uintptr_t>::max() ||
            view.length > std::numeric_limits<size_t>::max() ||
            view.length > std::numeric_limits<uint64_t>::max() - view.address)
            return {DestinationError::kOverflow, {}};
        endpoint = view.transport_endpoint;
        if (view.length)
            intervals.emplace_back(view.address, view.address + view.length);
    }
    std::sort(intervals.begin(), intervals.end());
    for (size_t i = 1; i < intervals.size(); ++i)
        if (intervals[i].first < intervals[i - 1].second)
            return {DestinationError::kOverlap, {}};
    bool local_destination = false;
    if (!views.empty()) {
        const auto segment = engine->openSegment(endpoint);
        struct SegmentCloser {
            TransferEngine& engine;
            SegmentHandle segment;
            ~SegmentCloser() { engine.closeSegment(segment); }
        } closer{*engine, segment};
        auto desc = engine->getMetadata()->getSegmentDescByID(segment);
        if (!desc || desc->protocol != "rdma")
            return {DestinationError::kNotSupported, {}};
        auto covered = [&] {
            return std::all_of(views.begin(), views.end(),
                               [&](const auto& view) {
                                   return RegistrationCovers(
                                       *desc, view.address, view.length, 2);
                               });
        };
        // A destination may have been registered after the owner's last cache
        // snapshot. Refresh once for the complete batch, never per fragment.
        if (!covered()) {
            auto refreshed =
                engine->getMetadata()->getSegmentDescByID(segment, true);
            if (refreshed) desc = std::move(refreshed);
        }
        if (!desc || desc->protocol != "rdma")
            return {DestinationError::kNotSupported, {}};
        if (!covered() ||
            !std::all_of(views.begin(), views.end(), [&](const auto& view) {
                return RegistrationCovers(*desc, view.address, view.length, 2);
            }))
            return {DestinationError::kInvalidArgument, {}};
        const auto local =
            engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        local_destination = local && desc->name == local->name;
    }
    return {DestinationError::kOk, std::make_shared<ClassicWriteTarget>(
                                       std::move(engine), std::move(views),
                                       limit, local_destination, true)};
}
}  // namespace mooncake::ssd_offload
