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

#include "SlabAllocator.h"

#include <stdexcept>

using namespace facebook::cachelib;

namespace {
unsigned int numSlabs(size_t memorySize) noexcept {
  return static_cast<unsigned int>(memorySize / sizeof(Slab));
}
} // namespace

// definitions to avoid ODR violation.
constexpr uint64_t SlabAllocator::kAddressMask;
constexpr unsigned int SlabAllocator::kLockSleepMS;
constexpr size_t SlabAllocator::kPagesPerStep;

SlabAllocator::SlabAllocator(void* headerMemoryStart,
                             size_t headerMemorySize,
                             void* slabMemoryStart,
                             size_t slabMemorySize)
    : headerMemoryStart_(headerMemoryStart),
      headerMemorySize_(headerMemorySize),
      slabMemoryStart_(reinterpret_cast<Slab*>(slabMemoryStart)),
      slabMemorySize_(slabMemorySize),
      nextSlabAllocation_(slabMemoryStart_) {
  static_assert(!(sizeof(Slab) & (sizeof(Slab) - 1)),
                "slab size must be power of two");

  if (headerMemoryStart_ == nullptr ||
      headerMemorySize_ <= sizeof(SlabHeader) * numSlabs(slabMemorySize_)) {
    throw std::invalid_argument(
        fmt::format("Invalid memory spec. headerMemoryStart = {}, size = {}",
                    headerMemoryStart_,
                    headerMemorySize_));
  }

  if (slabMemoryStart_ == nullptr ||
      reinterpret_cast<uintptr_t>(slabMemoryStart) % sizeof(Slab)) {
    throw std::invalid_argument(
        fmt::format("Invalid slabMemoryStart_ {}", (void*)slabMemoryStart_));
  }

  if (slabMemorySize_ % sizeof(Slab)) {
    throw std::invalid_argument(
        fmt::format("Invalid slabMemorySize_ {}, sizeof(Slab) {}", slabMemorySize_, sizeof(Slab)));
  }
}

SlabAllocator::~SlabAllocator() {
  stopMemoryLocker();
}

void SlabAllocator::stopMemoryLocker() {
  if (memoryLocker_.joinable()) {
    stopLocking_ = true;
    memoryLocker_.join();
  }
}

unsigned int SlabAllocator::getNumUsableSlabs() const noexcept {
  return static_cast<unsigned int>(getSlabMemoryEnd() - slabMemoryStart_);
}

Slab* SlabAllocator::makeNewSlabImpl() {
  // early return without any locks.
  if (!canAllocate_) {
    return nullptr;
  }

  LockHolder l(lock_);
  // grab a free slab if it exists.
  if (!freeSlabs_.empty()) {
    auto slab = freeSlabs_.back();
    freeSlabs_.pop_back();
    return slab;
  }

  XDCHECK_EQ(0u,
             reinterpret_cast<uintptr_t>(nextSlabAllocation_) % sizeof(Slab));

  // check if we have any more memory left.
  if (allMemorySlabbed()) {
    // free list is empty and we have slabbed all the memory.
    canAllocate_ = false;
    return nullptr;
  }

  // allocate a new slab.
  return nextSlabAllocation_++;
}

// This does not hold the lock since the expectation is that its used with
// new/free slabs which are not in active use.
void SlabAllocator::initializeHeader(Slab* slab, PoolId id) {
  auto* header = getSlabHeader(slab);
  XDCHECK(header != nullptr);
  header = new (header) SlabHeader(id);
}

Slab* SlabAllocator::makeNewSlab(PoolId id) {
  Slab* slab = makeNewSlabImpl();
  if (slab == nullptr) {
    return nullptr;
  }

  memoryPoolSize_[id] += sizeof(Slab);
  // initialize the header for the slab.
  initializeHeader(slab, id);
  return slab;
}

void SlabAllocator::freeSlab(Slab* slab) {
  // find the header for the slab.
  auto* header = getSlabHeader(slab);
  XDCHECK(header != nullptr);
  if (header == nullptr) {
    throw std::runtime_error(fmt::format("Invalid Slab {}", (void*) slab));
  }

  memoryPoolSize_[header->poolId] -= sizeof(Slab);
  // grab the lock
  LockHolder l(lock_);
  freeSlabs_.push_back(slab);
  canAllocate_ = true;
  header->resetAllocInfo();
}

SlabHeader* SlabAllocator::getSlabHeader(
    const Slab* const slab) const noexcept {
  if ([&] {
        // TODO(T79149875): Fix data race exposed by TSAN.
        // folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
        return isValidSlab(slab);
      }()) {
    return [&] {
      // TODO(T79149875): Fix data race exposed by TSAN.
      // folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
      return getSlabHeader(slabIdx(slab));
    }();
  }
  return nullptr;
}

bool SlabAllocator::isMemoryInSlab(const void* ptr,
                                   const Slab* slab) const noexcept {
  if (!isValidSlab(slab)) {
    return false;
  }
  return getSlabForMemory(ptr) == slab;
}
