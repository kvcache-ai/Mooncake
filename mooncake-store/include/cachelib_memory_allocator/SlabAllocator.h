/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#define FMT_HEADER_ONLY
#include <fmt/core.h>
#include <folly/logging/xlog.h>

#include "Slab.h"
#include "common/Utils.h"

namespace facebook {
namespace cachelib {

// Given a contiguous piece of memory, divides it up into slabs. The slab
// allocator is also responsible for providing memory for the slab headers for
// each slab.
class SlabAllocator {
 public:

  // See Feishu document.
  SlabAllocator(void* headerMemoryStart,
                size_t headerMemorySize,
                void* slabMemoryStart,
                size_t slabMemorySize);

  // free up and unmap the mmaped memory if the allocator was created with
  // one.
  ~SlabAllocator();

  SlabAllocator(const SlabAllocator&) = delete;
  SlabAllocator& operator=(const SlabAllocator&) = delete;

  using LockHolder = std::unique_lock<std::mutex>;

  // return true if any more slabs can be allocated from the slab allocator at
  // this point of time.
  bool allSlabsAllocated() const {
    LockHolder l(lock_);
    return allMemorySlabbed() && freeSlabs_.empty();
  }

  // grab an empty slab from the slab allocator if one is available.
  //
  // @param id  the pool id.
  // @return  pointer to a new slab of memory.
  Slab* makeNewSlab(PoolId id);

  // frees a used slab back to the slab allocator.
  //
  // @throw throws std::runtime_error if the slab is invalid
  void freeSlab(Slab* slab);

  // returns the number of slabs that the cache can hold.
  unsigned int getNumUsableSlabs() const noexcept;

  // returns the SlabHeader for the memory address or nullptr if the memory
  // is invalid. Hotly accessed for getting alloc info
  FOLLY_ALWAYS_INLINE SlabHeader* getSlabHeader(
      const void* memory) const noexcept {
    const auto* slab = getSlabForMemory(memory);
    if (LIKELY(isValidSlab(slab))) {
      const auto slabIndex = static_cast<SlabIdx>(slab - slabMemoryStart_);
      return getSlabHeader(slabIndex);
    }
    return nullptr;
  }

  // return the SlabHeader for the given slab or nullptr if the slab is
  // invalid
  SlabHeader* getSlabHeader(const Slab* const slab) const noexcept;

  // returns ture if ptr points to memory in the slab and the slab is a valid
  // slab, false otherwise.
  bool isMemoryInSlab(const void* ptr, const Slab* slab) const noexcept;

  // true if the slab is a valid allocated slab in the memory belonging to this
  // allocator.
  FOLLY_ALWAYS_INLINE bool isValidSlab(const Slab* slab) const noexcept {
    // suppress TSAN race error, this is harmless because nextSlabAllocation_
    // cannot go backwards and slab can't become invalid once it is valid
    // folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
    return slab >= slabMemoryStart_ && slab < nextSlabAllocation_ &&
           getSlabForMemory(static_cast<const void*>(slab)) == slab;
  }

  // returns the slab in which the memory resides, irrespective of the
  // validity of the memory. The caller can use isValidSlab to check if the
  // returned slab is valid.
  FOLLY_ALWAYS_INLINE const Slab* getSlabForMemory(
      const void* memory) const noexcept {
    // returns the closest slab boundary for the memory address.
    return reinterpret_cast<const Slab*>(reinterpret_cast<uintptr_t>(memory) &
                                         kAddressMask);
  }

  using SlabIdx = uint32_t;

  // returns the index of the slab from the start of the slab memory
  SlabIdx slabIdx(const Slab* const slab) const noexcept {
    if (slab == nullptr) {
      return kNullSlabIdx;
    }
    // We should never be querying for a slab that is not valid or beyond
    // nextSlabAllocation_.
    XDCHECK(slab == nextSlabAllocation_ || isValidSlab(slab));
    return static_cast<SlabIdx>(slab - slabMemoryStart_);
  }

  // returns the slab corresponding to the idx, irrespective of the validity of
  // the memory. The caller can use isValidSlab to check if the returned slab is
  // valid.
  Slab* getSlabForIdx(const SlabIdx idx) const noexcept {
    if (idx == kNullSlabIdx) {
      return nullptr;
    }
    return &slabMemoryStart_[idx];
  }

 private:
  // null Slab* presenttation. With 4M Slab size, a valid slab index would never
  // reach 2^16 - 1;
  static constexpr SlabIdx kNullSlabIdx = std::numeric_limits<SlabIdx>::max();

  // returns first byte after the end of memory region we own.
  const Slab* getSlabMemoryEnd() const noexcept {
    return reinterpret_cast<Slab*>(reinterpret_cast<uint8_t*>(slabMemoryStart_) +
                                   slabMemorySize_);
  }

  // returns true if we have slabbed all the memory that is available to us.
  // false otherwise.
  bool allMemorySlabbed() const noexcept {
    return nextSlabAllocation_ == getSlabMemoryEnd();
  }

  FOLLY_ALWAYS_INLINE SlabHeader* getSlabHeader(
      unsigned int slabIndex) const noexcept {
    return reinterpret_cast<SlabHeader*>(headerMemoryStart_) + slabIndex;
  }

  // implementation of makeNewSlab that takes care of locking, free list and
  // carving out new slabs.
  // @return  pointer to slab or nullptr if no more slabs can be allocated.
  Slab* makeNewSlabImpl();

  // Initialize the header for the given slab and pool
  void initializeHeader(Slab* slab, PoolId id);

  // shutsdown the memory locker if it is still running.
  void stopMemoryLocker();

  // lock serializing access to nextSlabAllocation_, freeSlabs_.
  mutable std::mutex lock_;

  // the current sizes of different memory pools from the slab allocator's
  // perspective. This is bumped up during makeNewSlab based on the poolId and
  // bumped down when the slab is released through freeSlab.
  std::array<std::atomic<size_t>, std::numeric_limits<PoolId>::max()>
      memoryPoolSize_{{}};

  // list of allocated slabs that are not in use.
  std::vector<Slab*> freeSlabs_;

  // start of the slab header memory region
  void* const headerMemoryStart_{nullptr};

  // size of the slab header memory region
  const size_t headerMemorySize_;

  // beginning of the slab memory region
  Slab* const slabMemoryStart_{nullptr};

  // size of memory aligned to slab size
  const size_t slabMemorySize_;

  // the memory address up to which we have converted into slabs.
  Slab* nextSlabAllocation_{nullptr};

  // boolean atomic that represents whether the allocator can allocate any
  // more slabs without holding any locks.
  std::atomic<bool> canAllocate_{true};

  // thread that does back-ground job of paging in and locking the memory if
  // enabled.
  std::thread memoryLocker_;

  // signals the locker thread to stop if we need to shutdown this instance.
  std::atomic<bool> stopLocking_{false};

  // amount of time to sleep in between each step to spread out the page
  // faults over a period of time.
  static constexpr unsigned int kLockSleepMS = 100;

  // number of pages to touch in eash step.
  static constexpr size_t kPagesPerStep = 10000;

  static_assert((Slab::kSize & (Slab::kSize - 1)) == 0,
                "Slab size is not power of two");

  // mask for all addresses belonging to slab aligned to Slab::kSize;
  static constexpr uint64_t kAddressMask =
      std::numeric_limits<uint64_t>::max() -
      (static_cast<uint64_t>(1) << Slab::kNumSlabBits) + 1;
};
} // namespace cachelib
} // namespace facebook
