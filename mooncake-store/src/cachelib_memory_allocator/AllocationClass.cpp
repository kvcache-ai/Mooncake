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

#include "AllocationClass.h"

#include "SlabAllocator.h"
#include "common/Exceptions.h"
#include "common/Throttler.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <stdexcept>
#include <thread>

using namespace facebook::cachelib;

constexpr unsigned int AllocationClass::kFreeAllocsPruneLimit;
constexpr unsigned int AllocationClass::kFreeAllocsPruneSleepMicroSecs;
constexpr unsigned int AllocationClass::kForEachAllocPrefetchOffset;

AllocationClass::AllocationClass(ClassId classId,
                                 PoolId poolId,
                                 uint32_t allocSize,
                                 const SlabAllocator& s)
    : classId_(classId),
      poolId_(poolId),
      allocationSize_(allocSize),
      slabAlloc_(s) {
  checkState();
}

void AllocationClass::checkState() const {
  // classId_ and allocationSize_ must be valid.
  if (classId_ < 0) {
    throw std::invalid_argument(
        fmt::format("Invalid Class Id {}", classId_));
  }

  if (allocationSize_ > Slab::kSize) {
    throw std::invalid_argument(
        fmt::format("Invalid alloc size {}", allocationSize_));
  }

  const auto header = slabAlloc_.getSlabHeader(currSlab_);
  if (currSlab_ != nullptr && header == nullptr) {
    throw std::invalid_argument(fmt::format(
        "Could not locate header for our current slab {}", (void*) currSlab_));
  }

  if (header != nullptr && header->classId != classId_) {
    throw std::invalid_argument(fmt::format(
        "ClassId of currSlab {} is not the same as our classId {}",
        header->classId, classId_));
  }

  if (currSlab_ != nullptr &&
      std::find(allocatedSlabs_.begin(), allocatedSlabs_.end(), currSlab_) ==
          allocatedSlabs_.end()) {
    throw std::invalid_argument(fmt::format(
        "Current allocation slab {} is not in allocated slabs list",
        (void*) currSlab_));
  }
}

void AllocationClass::addSlabLocked(Slab* slab) {
  canAllocate_ = true;
  auto header = slabAlloc_.getSlabHeader(slab);
  header->classId = classId_;
  header->allocSize = allocationSize_;
  freeSlabs_.push_back(slab);
}

void AllocationClass::addSlab(Slab* slab) {
  XDCHECK_NE(nullptr, slab);
  std::unique_lock l(lock_);
  ([this, slab]() { addSlabLocked(slab); })();
}

void* AllocationClass::addSlabAndAllocate(Slab* slab) {
  XDCHECK_NE(nullptr, slab);
  std::unique_lock l(lock_);
  return ([this, slab]() {
    addSlabLocked(slab);
    return allocateLocked();
  })();
}

void* AllocationClass::allocateFromCurrentSlabLocked() noexcept {
  XDCHECK(canAllocateFromCurrentSlabLocked());
  void* ret = currSlab_->memoryAtOffset(currOffset_);
  currOffset_ += allocationSize_;
  return ret;
}

bool AllocationClass::canAllocateFromCurrentSlabLocked() const noexcept {
  return (currSlab_ != nullptr) &&
         ((currOffset_ + allocationSize_) <= Slab::kSize);
}

void* AllocationClass::allocate() {
  if (!canAllocate_) {
    return nullptr;
  }
  std::unique_lock l(lock_);
  return ([this]() -> void* { return allocateLocked(); })();
}

void* AllocationClass::allocateLocked() {
  // fast path for case when the cache is mostly full.
  if (freedAllocations_.empty() && freeSlabs_.empty() &&
      !canAllocateFromCurrentSlabLocked()) {
    canAllocate_ = false;
    return nullptr;
  }

  XDCHECK(canAllocate_);

  // grab from the free list if possible.
  if (!freedAllocations_.empty()) {
    void* ret = freedAllocations_.front();
    XDCHECK(ret != nullptr);
    freedAllocations_.pop_front();
    return reinterpret_cast<void*>(ret);
  }

  // see if we have an active slab that is being used to carve the
  // allocations.
  if (canAllocateFromCurrentSlabLocked()) {
    return allocateFromCurrentSlabLocked();
  }

  XDCHECK(canAllocate_);
  XDCHECK(!freeSlabs_.empty());
  setupCurrentSlabLocked();
  // grab a free slab and make it current.
  return allocateFromCurrentSlabLocked();
}

void AllocationClass::setupCurrentSlabLocked() {
  XDCHECK(!freeSlabs_.empty());
  auto slab = freeSlabs_.back();
  freeSlabs_.pop_back();
  currSlab_ = slab;
  currOffset_ = 0;
  allocatedSlabs_.push_back(slab);
}

const Slab* AllocationClass::getSlabForReleaseLocked() const noexcept {
  if (!freeSlabs_.empty()) {
    return freeSlabs_.front();
  } else if (!allocatedSlabs_.empty()) {
    std::random_device rd;
    std::mt19937 gen(rd());
    auto idx =
        std::uniform_int_distribution<uint32_t>(0, static_cast<uint32_t>(allocatedSlabs_.size() - 1))(gen);
    return allocatedSlabs_[idx];
  }
  return nullptr;
}

SlabReleaseContext AllocationClass::startSlabRelease(
    SlabReleaseMode mode, const void* hint, SlabReleaseAbortFn shouldAbortFn) {
  using LockHolder = std::unique_lock<std::mutex>;
  LockHolder startSlabReleaseLockHolder(startSlabReleaseLock_);
  const auto* hintSlab = slabAlloc_.getSlabForMemory(hint);
  if (hint != nullptr && !slabAlloc_.isValidSlab(hintSlab)) {
    throw std::invalid_argument(
        fmt::format("Invalid hint {} for slab release {}", hint, (void*) hintSlab));
  }

  const Slab* slab;
  SlabHeader* header;
  {
    std::unique_lock l(lock_);
    // if a hint is provided, use it. If not, try to get a free/allocated slab.
    slab = hint == nullptr ? getSlabForReleaseLocked() : hintSlab;
    if (slab == nullptr) {
      throw std::invalid_argument("Can not figure out a slab for release");
    }

    header = slabAlloc_.getSlabHeader(slab);
    // slab header must be valid and NOT marked for release
    if (header == nullptr || header->classId != getId() ||
        header->poolId != getPoolId() || header->isMarkedForRelease()) {
      throw std::invalid_argument(fmt::format(
          "Slab Header {} is in invalid state for release. id = {}, "
          "markedForRelease = {}, classId = {}",
          (void*) header, header == nullptr ? Slab::kInvalidClassId : header->classId,
          header == nullptr ? false : header->isMarkedForRelease(), getId()));
    }

    // if its is a free slab, get it off the freeSlabs_ and return context
    auto freeIt = std::find(freeSlabs_.begin(), freeSlabs_.end(), slab);
    if (freeIt != freeSlabs_.end()) {
      *freeIt = freeSlabs_.back();
      freeSlabs_.pop_back();
      header->classId = Slab::kInvalidClassId;
      header->allocSize = 0;
      return SlabReleaseContext{slab, header->poolId, header->classId, mode};
    }

    // The slab is actively used, so we create a new release alloc map
    // and mark the slab for release
    header->setMarkedForRelease(true);
    createSlabReleaseAllocMapLocked(slab);

    // remove this slab from the allocatedSlab_ if it exists.
    auto allocIt =
        std::find(allocatedSlabs_.begin(), allocatedSlabs_.end(), slab);
    if (allocIt == allocatedSlabs_.end()) {
      // not a part of free slabs and not part of allocated slab. This is an
      // error, return to caller. This should not happen. throw a run time
      // error.
      throw std::runtime_error(
          fmt::format("Slab {} belongs to class {}. But its not present in "
                      "the free list or "
                      "allocated list.",
                      (void*) slab, getId()));
    }
    *allocIt = allocatedSlabs_.back();
    allocatedSlabs_.pop_back();

    // if slab is being carved currently, then update slabReleaseAllocMap
    // allocState with free Allocs info, and then reset it
    if (currSlab_ == slab) {
      const auto it = slabReleaseAllocMap_.find(getSlabPtrValue(slab));
      auto& allocState = it->second;
      XDCHECK_EQ(allocState.size(), getAllocsPerSlab());
      for (size_t i = currOffset_ / allocationSize_; i < allocState.size();
           i++) {
        allocState[i] = true;
      }

      currSlab_ = nullptr;
      currOffset_ = 0;
    }
  } // alloc lock scope

  auto results = pruneFreeAllocs(slab, shouldAbortFn);
  if (results.first) {
    {
      std::unique_lock l(lock_);
      ([&]() {
        header->setMarkedForRelease(false);
        slabReleaseAllocMap_.erase(getSlabPtrValue(slab));
      })();
    }
    throw exception::SlabReleaseAborted(
        fmt::format("Slab Release aborted "
                    "during pruning free allocs. Slab address: {}",
                    (void*) slab));
  }
  std::vector<void*> activeAllocations = std::move(results.second);
  std::unique_lock l(lock_);
  return ([&]() {
    if (activeAllocations.empty()) {
      header->classId = Slab::kInvalidClassId;
      header->allocSize = 0;
      header->setMarkedForRelease(false);
      // no active allocations to be freed back. We can consider this slab as
      // released from this AllocationClass. This means we also do not need
      // to keep the slabFreeState
      slabReleaseAllocMap_.erase(getSlabPtrValue(slab));
      return SlabReleaseContext{slab, header->poolId, header->classId, mode};
    } else {
      ++activeReleases_;
      return SlabReleaseContext{slab, header->poolId, header->classId,
                                std::move(activeAllocations), mode};
    }
  })();
}

void* AllocationClass::getAllocForIdx(const Slab* slab, size_t idx) const {
  if (idx >= getAllocsPerSlab()) {
    throw std::invalid_argument(fmt::format("Invalid index {}", idx));
  }
  return slab->memoryAtOffset(idx * allocationSize_);
}

size_t AllocationClass::getAllocIdx(const Slab* slab,
                                    void* alloc) const noexcept {
  const size_t offset = reinterpret_cast<uintptr_t>(alloc) -
                        reinterpret_cast<uintptr_t>(slab->memoryAtOffset(0));
  XDCHECK_EQ(0u, offset % allocationSize_);
  XDCHECK_LT(offset, Slab::kSize);
  return offset / allocationSize_;
}

void AllocationClass::partitionFreeAllocs(const Slab* slab,
                                          FreeList& freeAllocs,
                                          FreeList& inSlab,
                                          FreeList& notInSlab) {
  for (unsigned int i = 0; i < kFreeAllocsPruneLimit && !freeAllocs.empty();
       ++i) {
    auto alloc = freeAllocs.front();
    freeAllocs.pop_front();
    if (slabAlloc_.isMemoryInSlab(reinterpret_cast<void*>(alloc), slab)) {
      inSlab.push_front(alloc);
    } else {
      notInSlab.push_front(alloc);
    }
  }
}

std::pair<bool, std::vector<void*>> AllocationClass::pruneFreeAllocs(
    const Slab* slab, SlabReleaseAbortFn shouldAbortFn) {
  // Find free allocations that belong to this active slab. If part of
  // allocated slab, release any freed allocations belonging to this slab.
  // Set the bit to true if the corresponding allocation is freed, false
  // otherwise.
  FreeList freeAllocs{};
  FreeList notInSlab{};
  FreeList inSlab{};

  {
    std::unique_lock l(lock_);
    ([&]() {
      // Take the allocation class free list offline
      // This is because we need to process this free list in batches
      // in order not to stall threads that need to allocate memory
      std::swap(freeAllocs, freedAllocations_);

      // Check up to kFreeAllocsPruneLimit while holding the lock. This limits
      // the amount of time we hold the lock while also having a good chance
      // of freedAllocations_ not being completely empty once we unlock.
      partitionFreeAllocs(slab, freeAllocs, inSlab, notInSlab);
    })();
  }

  bool shouldAbort = false;
  do {
    if (shouldAbortFn()) {
      shouldAbort = true;
      break;
    }
    {
      std::unique_lock l(lock_);
      ([&]() {
        // Put back allocs we checked while not holding the lock.
        if (!notInSlab.empty()) {
          freedAllocations_.splice(freedAllocations_.begin(), std::move(notInSlab));
          canAllocate_ = true;
        }

        auto& allocState = getSlabReleaseAllocMapLocked(slab);
        // Mark allocs we found while not holding the lock as freed.
        while (!inSlab.empty()) {
          auto alloc = inSlab.front();
          inSlab.pop_front();
          XDCHECK(
              slabAlloc_.isMemoryInSlab(reinterpret_cast<void*>(alloc), slab));
          const auto idx = getAllocIdx(slab, reinterpret_cast<void*>(alloc));
          XDCHECK_LT(idx, allocState.size());
          allocState[idx] = true;
        }
      })(); // alloc lock scope
    }

    if (freeAllocs.empty()) {
      XDCHECK(notInSlab.empty());
      XDCHECK(inSlab.empty());
      break;
    }

    // Scan the copied free list outside of lock. We place allocs into either
    // 'inSlab' or 'notInSlab' to be dealt with next time we have the lock.
    // NOTE: we limit to kFreeAllocsPruneLimit iterations so we can periodically
    // return allocs from 'notInSlab' to 'freedAllocations_'.
    partitionFreeAllocs(slab, freeAllocs, inSlab, notInSlab);

    // Let other threads do some work since we will process lots of batches
    /* sleep override */ std::this_thread::sleep_for(
        std::chrono::microseconds(kFreeAllocsPruneSleepMicroSecs));
  } while (true);

  // Collect the list of active allocations
  std::vector<void*> activeAllocations;

  // If shutdown is in progress, undo everything we did above and return
  // empty active allocations
  if (shouldAbort) {
    std::unique_lock l(lock_);
    ([&]() {
      if (!notInSlab.empty()) {
        freeAllocs.splice(freeAllocs.begin(), std::move(notInSlab));
      }
      if (!inSlab.empty()) {
        freeAllocs.splice(freeAllocs.begin(), std::move(inSlab));
      }
      freedAllocations_.splice(freedAllocations_.begin(), std::move(freeAllocs));
    })();
    return {shouldAbort, activeAllocations};
  }

  // reserve the maximum space for active allocations so we don't
  // malloc under the lock later
  activeAllocations.reserve(Slab::kSize / allocationSize_);
  std::unique_lock l(lock_);
  ([&]() {
    const auto& allocState = getSlabReleaseAllocMapLocked(slab);

    // Iterating through a vector<bool>. Up to 65K iterations if
    // the alloc size is 64 bytes in a 4MB slab. This should be
    // done fast enough since we're iterating a vector.
    size_t offset = 0;
    for (const auto freed : allocState) {
      if (!freed) {
        activeAllocations.push_back(slab->memoryAtOffset(offset));
      }
      offset += allocationSize_;
    }
  })(); // alloc lock scope

  return {shouldAbort, activeAllocations};
}

bool AllocationClass::allFreed(const Slab* slab) const {
  std::unique_lock l(lock_);
  return ([this, slab]() {
    const auto it = slabReleaseAllocMap_.find(getSlabPtrValue(slab));
    if (it == slabReleaseAllocMap_.end()) {
      throw std::runtime_error(fmt::format(
          "Slab {} is not in the active slab release allocation map.", (void*) slab));
    }

    const auto& allocState = it->second;
    for (const auto freed : allocState) {
      if (!freed) {
        return false;
      }
    }
    return true;
  })();
}

void AllocationClass::waitUntilAllFreed(const Slab* slab) {
  util::Throttler t{util::Throttler::Config{
      1000, /* sleepTime. milliseconds */
      10,   /* sleepInterval. milliseconds */
  }};
  while (!allFreed(slab)) {
    if (t.throttle() && t.numThrottles() % 128) {
      XLOG(WARN, "still waiting for all slabs released");
    }
  }
}

void AllocationClass::abortSlabRelease(const SlabReleaseContext& context) {
  if (context.isReleased()) {
    throw std::invalid_argument(fmt::format("context is already released"));
  }
  auto slab = context.getSlab();
  const auto slabPtrVal = getSlabPtrValue(slab);
  auto header = slabAlloc_.getSlabHeader(slab);

  std::unique_lock l(lock_);
  ([&]() {
    const auto it = slabReleaseAllocMap_.find(getSlabPtrValue(slab));
    bool inserted = false;
    if (it != slabReleaseAllocMap_.end()) {
      const auto& allocState = it->second;
      for (size_t idx = 0; idx < allocState.size(); idx++) {
        if (allocState[idx]) {
          auto alloc = getAllocForIdx(slab, idx);
          freedAllocations_.push_front(alloc);
          inserted = true;
        }
      }
    }
    if (inserted) {
      canAllocate_ = true;
    }
    slabReleaseAllocMap_.erase(slabPtrVal);
    allocatedSlabs_.push_back(const_cast<Slab*>(slab));
    // restore the classId and allocSize
    header->classId = classId_;
    header->allocSize = allocationSize_;
    header->setMarkedForRelease(false);
    --activeReleases_;
  })();
}

void AllocationClass::completeSlabRelease(const SlabReleaseContext& context) {
  if (context.isReleased()) {
    // nothing to be done here since the slab is already released
    return;
  }

  auto slab = context.getSlab();
  auto header = slabAlloc_.getSlabHeader(slab);

  {
    std::unique_lock l(lock_);
    ([&]() {
      // slab header must be valid and marked for release
      if (header == nullptr || header->classId != getId() ||
          !header->isMarkedForRelease()) {
        throw std::runtime_error(
            fmt::format("The slab at {} with header at {} is invalid",
                        (void*)slab, (void*)header));
      }
    })();
  }

  // release the lock and check that all the allocs are freed back to the
  // allocator for this slab.
  // TODO consider completeSlabRelease not sleep forever and provide the
  // context back to the caller for the list of non-free allocations after
  // waiting for a while.
  waitUntilAllFreed(slab);

  const auto slabPtrVal = getSlabPtrValue(slab);
  std::unique_lock l(lock_);
  ([&]() {
    slabReleaseAllocMap_.erase(slabPtrVal);

    header->classId = Slab::kInvalidClassId;
    header->allocSize = 0;
    header->setMarkedForRelease(false);
    --activeReleases_;
    XDCHECK_GE(activeReleases_.load(), 0);
  })();
}

void AllocationClass::checkSlabInRelease(const SlabReleaseContext& ctx,
                                         const void* memory) const {
  const auto* header = slabAlloc_.getSlabHeader(memory);
  if (header == nullptr || header->classId != classId_) {
    throw std::invalid_argument(fmt::format(
        "trying to check memory {} (with ClassId {}), not belonging to this "
        "AllocationClass (ClassID {})",
        memory,
        header ? header->classId : Slab::kInvalidClassId,
        classId_));
  }
  if (!header->isMarkedForRelease()) {
    throw std::invalid_argument(fmt::format(
        "trying whether memory at {} (with ClassID {}) is freed, but header is "
        "not marked for release",
        memory,
        header->classId));
  }
  const auto* slab = slabAlloc_.getSlabForMemory(memory);
  if (slab != ctx.getSlab()) {
    throw std::invalid_argument(fmt::format(
        "trying to check memory {} (with ClassId {}), against an invalid slab "
        "release context (ClassID {})",
        memory,
        header ? header->classId : Slab::kInvalidClassId,
        ctx.getClassId()));
  }
}

bool AllocationClass::isAllocFreedLocked(const SlabReleaseContext& /*ctx*/,
                                         void* memory) const {
  const auto* slab = slabAlloc_.getSlabForMemory(memory);
  const auto slabPtrVal = getSlabPtrValue(slab);
  const auto it = slabReleaseAllocMap_.find(slabPtrVal);

  // this should not happen
  if (it == slabReleaseAllocMap_.end()) {
    throw std::runtime_error(
        fmt::format("Invalid slabReleaseAllocMap "
                       "state when checking if memory is freed. Memory: {}",
                       memory));
  }

  const auto& allocState = it->second;
  const auto idx = getAllocIdx(slab, memory);
  return allocState[idx];
}

bool AllocationClass::isAllocFreed(const SlabReleaseContext& ctx,
                                   void* memory) const {
  checkSlabInRelease(ctx, memory);
  std::unique_lock l(lock_);
  return (
      [this, &ctx, memory]() { return isAllocFreedLocked(ctx, memory); })();
}

void AllocationClass::processAllocForRelease(
    const SlabReleaseContext& ctx,
    void* memory,
    const std::function<void(void*)>& callback) const {
  checkSlabInRelease(ctx, memory);
  std::unique_lock l(lock_);
  ([this, &ctx, memory, &callback]() {
    if (!isAllocFreedLocked(ctx, memory)) {
      callback(memory);
    }
  })();
}

void AllocationClass::free(void* memory) {
  const auto* header = slabAlloc_.getSlabHeader(memory);
  auto* slab = slabAlloc_.getSlabForMemory(memory);
  if (header == nullptr || header->classId != classId_) {
    throw std::invalid_argument(fmt::format(
        "trying to free memory {} (with ClassId {}), not belonging to this "
        "AllocationClass (ClassId {})",
        memory, header ? header->classId : Slab::kInvalidClassId, classId_));
  }

  const auto slabPtrVal = getSlabPtrValue(slab);
  std::unique_lock l(lock_);
  ([this, header, slab, memory, slabPtrVal]() {
    // check under the lock we actually add the allocation back to the free list
    if (header->isMarkedForRelease()) {
      auto it = slabReleaseAllocMap_.find(slabPtrVal);

      // this should not happen.
      if (it == slabReleaseAllocMap_.end()) {
        throw std::runtime_error(fmt::format(
            "Invalid slabReleaseAllocMap "
            "state when attempting to free an allocation. Memory: {}",
            memory));
      }

      auto& allocState = it->second;
      const auto idx = getAllocIdx(slab, memory);
      if (allocState[idx]) {
        throw std::invalid_argument(
            fmt::format("Allocation {} is already marked as free", memory));
      }
      allocState[idx] = true;
      return;
    }

    // TODO add checks here to ensure that we dont double free in debug mode.
    freedAllocations_.push_front(memory);
    canAllocate_ = true;
  })();
}

void AllocationClass::createSlabReleaseAllocMapLocked(const Slab* slab) {
  // Initialize slab free state
  // Each bit represents whether or not an alloc has already been freed
  const auto slabPtrVal = getSlabPtrValue(slab);
  std::vector<bool> allocState(getAllocsPerSlab(), false);
  const auto res =
      slabReleaseAllocMap_.insert({slabPtrVal, std::move(allocState)});
  if (!res.second) {
    // this should never happen. we must always be able to insert.
    throw std::runtime_error(
        fmt::format("failed to insert allocState map for slab {}", (void*) slab));
  }
}

std::vector<bool>& AllocationClass::getSlabReleaseAllocMapLocked(
    const Slab* slab) {
  const auto slabPtrVal = getSlabPtrValue(slab);
  return slabReleaseAllocMap_.at(slabPtrVal);
}
