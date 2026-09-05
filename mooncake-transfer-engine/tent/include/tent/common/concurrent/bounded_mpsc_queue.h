// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_BOUNDED_MPSC_QUEUE_H
#define TENT_BOUNDED_MPSC_QUEUE_H

#include <atomic>
#include <thread>
#include <unordered_set>
#include <vector>

namespace mooncake {
namespace tent {
template <typename T, size_t Capacity>
struct BoundedMPSCQueue {
    struct alignas(64) Cell {
        std::atomic<uint64_t> sequence;
        T data;
    };

    std::unique_ptr<Cell[]> buffer;
    std::atomic<uint64_t> head{0};
    uint64_t padding1[7];
    std::atomic<uint64_t> tail{0};
    uint64_t padding2[7];

    BoundedMPSCQueue() : buffer(new Cell[Capacity]) {
        for (uint64_t i = 0; i < Capacity; ++i) {
            buffer[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~BoundedMPSCQueue() = default;

    // Best-effort free-slot estimate for admission pre-checks (issue #3661).
    // head/tail are monotonic counters, so (tail - head) is the occupancy.
    // This is an MPSC queue, so the value can go stale the instant it is read
    // (another producer may push, or the consumer may pop); callers must treat
    // a positive result as "had room a moment ago", not a reservation, and
    // still handle a failing try_push. It is used only to avoid *starting* a
    // multi-queue admission that cannot complete, so no list is pushed — and
    // thus no slice is posted — before every target queue is known to have had
    // room.
    bool has_free_slot() const {
        uint64_t t = tail.load(std::memory_order_acquire);
        uint64_t h = head.load(std::memory_order_acquire);
        return (t - h) < Capacity;
    }

    // Non-blocking push: returns false when the queue is full. Both the
    // producer (RdmaTransport::submitTransferTasks) and the worker thread
    // (submitFromTick) enqueue through this so a full queue is reported to
    // the caller instead of wedging the only consumer (issues #3636/#3637).
    bool try_push(T &slice_list) {
        if (slice_list.num_slices == 0) return true;
        uint64_t pos = tail.load(std::memory_order_relaxed);
        Cell *cell = &buffer[pos % Capacity];

        uint64_t seq = cell->sequence.load(std::memory_order_acquire);
        intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
        if (dif == 0) {
            if (tail.compare_exchange_weak(pos, pos + 1,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
                cell->data = slice_list;
                cell->sequence.store(pos + 1, std::memory_order_release);
                return true;
            }
        }
        return false;
    }

    void push(T &slice_list) {
        while (!try_push(slice_list)) {
            std::this_thread::yield();
        }
    }

    T pop() {
        uint64_t pos = head.load(std::memory_order_relaxed);
        Cell *cell = &buffer[pos % Capacity];

        uint64_t seq = cell->sequence.load(std::memory_order_acquire);
        intptr_t dif =
            static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

        if (dif == 0) {
            T result = cell->data;
            head.store(pos + 1, std::memory_order_release);
            cell->sequence.store(pos + Capacity, std::memory_order_release);
            return result;
        } else {
            return T{};
        }
    }

    void pop(std::vector<T> &result) {
        while (true) {
            uint64_t pos = head.load(std::memory_order_relaxed);
            Cell *cell = &buffer[pos % Capacity];
            uint64_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif =
                static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (dif == 0) {
                T item = cell->data;
                head.store(pos + 1, std::memory_order_release);
                cell->sequence.store(pos + Capacity, std::memory_order_release);
                result.push_back(std::move(item));
            } else {
                break;
            }
        }
    }
};
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_BOUNDED_MPSC_QUEUE_H
