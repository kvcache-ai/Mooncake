// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_transfer_target.h"

#include "transfer_engine.h"

#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <string_view>
#include <thread>
#include <utility>

namespace mooncake::ssd_offload {
namespace {

bool SingleBufferCovers(const TransferMetadata::SegmentDesc& desc,
                        uint64_t address, uint64_t bytes) {
    // Classic TCP validates each frame against a single registered BufferDesc.
    // Adjacent registrations never authorize one combined frame.
    return std::any_of(
        desc.buffers.begin(), desc.buffers.end(), [&](const auto& buffer) {
            return address >= buffer.addr &&
                   address - buffer.addr <= buffer.length &&
                   bytes <= buffer.length - (address - buffer.addr);
        });
}

bool IsInvalidSegment(SegmentHandle segment) noexcept {
    return static_cast<int64_t>(segment) < 0;
}

bool IsTerminalStatus(TransferStatusEnum state) noexcept {
    return state == TransferStatusEnum::COMPLETED ||
           state == TransferStatusEnum::FAILED;
}

class ClassicTcpWriteTarget final : public SourceDrainedWriteTarget {
   public:
    ClassicTcpWriteTarget(
        std::shared_ptr<TransferEngine> engine,
        std::vector<ResolvedDestination> views, size_t limit,
        bool local_destination, SegmentHandle remote_segment,
        std::shared_ptr<TransferMetadata::SegmentDesc> remote_desc)
        : engine_(std::move(engine)),
          views_(std::move(views)),
          limit_(limit),
          local_destination_(local_destination),
          remote_segment_(remote_segment),
          remote_desc_(std::move(remote_desc)) {}

    ~ClassicTcpWriteTarget() override {
        if (engine_ && !IsInvalidSegment(remote_segment_)) {
            (void)engine_->closeSegment(remote_segment_);
        }
    }

    Error WriteAndDrain(const ReadChunk& chunk,
                        SlotBuffer slot) noexcept override {
        try {
            if (!slot.data || !chunk.read_bytes ||
                chunk.read_bytes > slot.capacity ||
                chunk.fragments.size() > limit_) {
                return Error::kInvalidArgument;
            }
            const auto source_begin = reinterpret_cast<uintptr_t>(slot.data);
            if (slot.capacity >
                std::numeric_limits<uintptr_t>::max() - source_begin) {
                return Error::kInvalidArgument;
            }
            const auto metadata = engine_->getMetadata();
            if (!metadata) {
                return Error::kInvalidArgument;
            }
            const auto local = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
            if (!local || !SingleBufferCovers(
                              *local, reinterpret_cast<uint64_t>(slot.data),
                              slot.capacity)) {
                return Error::kInvalidArgument;
            }

            // Validate every fragment before submitting any network write.
            for (const auto& fragment : chunk.fragments) {
                const auto& d = fragment.destination;
                if (d.destination_index >= views_.size()) {
                    return Error::kInvalidArgument;
                }
                const auto& view = views_[d.destination_index];
                if (d.offset > view.length ||
                    d.length > view.length - d.offset ||
                    fragment.buffer_offset > chunk.read_bytes ||
                    d.length > chunk.read_bytes - fragment.buffer_offset) {
                    return Error::kInvalidArgument;
                }
                const auto begin = view.address + d.offset;
                if (local_destination_ && d.length &&
                    begin < source_begin + slot.capacity &&
                    begin + d.length > source_begin) {
                    return Error::kInvalidArgument;
                }
            }

            if (chunk.fragments.empty()) return Error::kOk;
            if (!remote_desc_ || IsInvalidSegment(remote_segment_)) {
                return Error::kInvalidArgument;
            }

            std::vector<const WriteFragment*> ordered;
            if (chunk.fragments.size() > 1) {
                ordered.reserve(chunk.fragments.size());
                for (const auto& fragment : chunk.fragments) {
                    ordered.push_back(&fragment);
                }
                std::sort(ordered.begin(), ordered.end(),
                          [](const auto* a, const auto* b) {
                              return a->buffer_offset < b->buffer_offset;
                          });
            }

            std::vector<TransferRequest> entries;
            entries.reserve(chunk.fragments.size());
            for (size_t i = 0; i < chunk.fragments.size(); ++i) {
                const auto* fragment = chunk.fragments.size() == 1
                                           ? &chunk.fragments.front()
                                           : ordered[i];
                const auto& view =
                    views_[fragment->destination.destination_index];
                const auto dst_addr =
                    view.address + fragment->destination.offset;
                const auto length =
                    static_cast<size_t>(fragment->destination.length);
                if (length == 0) continue;
                auto* src_ptr = slot.data + fragment->buffer_offset;
                // Merge only exact adjacency at both ends within one registered
                // remote BufferDesc; gaps never gain write authority.
                if (!entries.empty() &&
                    static_cast<const std::byte*>(entries.back().source) +
                            entries.back().length ==
                        src_ptr &&
                    entries.back().target_offset + entries.back().length ==
                        dst_addr &&
                    SingleBufferCovers(*remote_desc_,
                                       entries.back().target_offset,
                                       entries.back().length + length)) {
                    entries.back().length += length;
                } else {
                    TransferRequest req;
                    req.opcode = TransferRequest::WRITE;
                    req.source = src_ptr;
                    req.target_id = remote_segment_;
                    req.target_offset = dst_addr;
                    req.length = length;
                    req.task_group_id = 1;
                    entries.push_back(req);
                }
            }
            if (entries.empty()) return Error::kOk;

            const BatchID batch = engine_->allocateBatchID(entries.size());
            if (batch == INVALID_BATCH_ID) {
                return Error::kTransferError;
            }
            const auto submitted =
                engine_->submitAcknowledgedTcpWrite(batch, entries);
            if (!submitted.ok()) {
                // Pre-dispatch rejection leaves no transport work owning slot.
                (void)engine_->freeBatchID(batch);
                return Error::kTransferError;
            }

            // Drain every submitted entry until every transport task reaches
            // COMPLETED or FAILED before returning or freeing the batch, so the
            // staging slot is never reused while TCP I/O may still read it.
            bool all_succeeded = true;
            std::vector<bool> done(entries.size(), false);
            size_t remaining = entries.size();
            unsigned polls = 0;
            while (remaining > 0) {
                bool progressed = false;
                for (size_t i = 0; i < entries.size(); ++i) {
                    if (done[i]) continue;
                    TransferStatus status{};
                    const auto queried =
                        engine_->getTransferStatus(batch, i, status);
                    if (!queried.ok()) {
                        // Only stop polling a task if the batch/task ID is
                        // invalid; transient lookup failures must not release
                        // the staging slot while transport I/O may be active.
                        if (queried.IsInvalidArgument()) {
                            all_succeeded = false;
                            done[i] = true;
                            --remaining;
                            progressed = true;
                        }
                        continue;
                    }
                    if (!IsTerminalStatus(status.s)) continue;
                    if (status.s != TransferStatusEnum::COMPLETED) {
                        all_succeeded = false;
                    }
                    done[i] = true;
                    --remaining;
                    progressed = true;
                }
                if (remaining > 0 && !progressed) {
                    if (polls < 32) {
                        ++polls;
                        std::this_thread::yield();
                    } else {
                        std::this_thread::sleep_for(
                            std::chrono::microseconds(50));
                    }
                }
            }

            const auto freed = engine_->freeBatchID(batch);
            // Once all tasks reached COMPLETED, Status::BatchCleanupDeferred
            // reflects the benign CompletionCallbackPin unwind window in
            // Slice::markSuccess() and is NOT a transfer failure.
            if (!freed.ok() && !freed.IsBatchCleanupDeferred()) {
                all_succeeded = false;
            }
            if (all_succeeded) return Error::kOk;
            destinations_drained_.store(false, std::memory_order_release);
            return Error::kTransferError;
        } catch (...) {
            destinations_drained_.store(false, std::memory_order_release);
            return Error::kTransferError;
        }
    }

    bool DestinationsDrained() const noexcept override {
        return destinations_drained_.load(std::memory_order_acquire);
    }

   private:
    std::shared_ptr<TransferEngine> engine_;
    std::vector<ResolvedDestination> views_;
    size_t limit_;
    bool local_destination_;
    SegmentHandle remote_segment_;
    std::shared_ptr<TransferMetadata::SegmentDesc> remote_desc_;
    std::atomic<bool> destinations_drained_{true};
};

}  // namespace

SsdTransferTargetPreparation PrepareSsdTcpWriteTarget(
    std::shared_ptr<TransferEngine> engine,
    std::vector<ResolvedDestination> views, size_t max_fragments) {
    if (!max_fragments || max_fragments > 65536 ||
        views.size() > max_fragments) {
        return {DestinationError::kLimit, {}};
    }
    if (!engine || engine->isUsingTent() || !engine->isTcpOnly() ||
        engine->getTransport("shm")) {
        return {DestinationError::kNotSupported, {}};
    }
    const char* legacy = std::getenv("MC_TCP_PROTO");
    if (legacy && std::string_view(legacy) == "1") {
        return {DestinationError::kNotSupported, {}};
    }

    std::vector<std::pair<uint64_t, uint64_t>> intervals;
    intervals.reserve(views.size());
    std::string endpoint;
    for (const auto& view : views) {
        if (view.memory != DestinationMemory::kHost) {
            return {DestinationError::kNotSupported, {}};
        }
        if (!view.address || view.transport_endpoint.empty() ||
            view.transport_endpoint.size() > 4096 ||
            (!endpoint.empty() && endpoint != view.transport_endpoint)) {
            return {DestinationError::kInvalidArgument, {}};
        }
        if (view.address > std::numeric_limits<uintptr_t>::max() ||
            view.length > std::numeric_limits<size_t>::max() ||
            view.length > std::numeric_limits<uint64_t>::max() - view.address) {
            return {DestinationError::kOverflow, {}};
        }
        endpoint = view.transport_endpoint;
        if (view.length) {
            intervals.emplace_back(view.address, view.address + view.length);
        }
    }
    std::sort(intervals.begin(), intervals.end());
    for (size_t i = 1; i < intervals.size(); ++i) {
        if (intervals[i].first < intervals[i - 1].second) {
            return {DestinationError::kOverlap, {}};
        }
    }

    bool local_destination = false;
    SegmentHandle segment = static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT);
    std::shared_ptr<TransferMetadata::SegmentDesc> remote;
    if (!views.empty()) {
        const auto metadata = engine->getMetadata();
        if (!metadata) {
            return {DestinationError::kNotSupported, {}};
        }
        segment = engine->openSegment(endpoint);
        if (IsInvalidSegment(segment)) {
            return {DestinationError::kNotSupported, {}};
        }
        bool keep_segment = false;
        struct SegmentGuard {
            TransferEngine& engine;
            SegmentHandle segment;
            bool& keep;
            ~SegmentGuard() {
                if (!keep) (void)engine.closeSegment(segment);
            }
        } guard{*engine, segment, keep_segment};

        auto desc = metadata->getSegmentDescByID(segment);
        if (!desc || desc->protocol != "tcp" || desc->tcp_proto_version < 2) {
            return {DestinationError::kNotSupported, {}};
        }
        auto covered = [&] {
            return std::all_of(
                views.begin(), views.end(), [&](const auto& view) {
                    return SingleBufferCovers(*desc, view.address, view.length);
                });
        };
        if (!covered()) {
            desc = metadata->getSegmentDescByID(segment, true);
        }
        if (!desc || desc->protocol != "tcp" || desc->tcp_proto_version < 2) {
            return {DestinationError::kNotSupported, {}};
        }
        if (!covered()) {
            return {DestinationError::kInvalidArgument, {}};
        }
        remote = std::move(desc);
        const auto local = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        local_destination =
            segment == LOCAL_SEGMENT_ID ||
            (local && (remote->name == local->name ||
                       (remote->tcp_data_host == local->tcp_data_host &&
                        remote->tcp_data_port == local->tcp_data_port)));
        keep_segment = true;
    }

    return {DestinationError::kOk,
            std::make_shared<ClassicTcpWriteTarget>(
                std::move(engine), std::move(views), max_fragments,
                local_destination, segment, std::move(remote))};
}

}  // namespace mooncake::ssd_offload
