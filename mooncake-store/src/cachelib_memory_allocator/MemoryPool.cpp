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

#include "MemoryPool.h"

#include <algorithm>
#include <string>

#include "AllocationClass.h"
#include "SlabAllocator.h"

using namespace facebook::cachelib;
using LockHolder = std::unique_lock<std::mutex>;

MemoryPool::MemoryPool(PoolId id,
                       size_t poolSize,
                       SlabAllocator& alloc,
                       const std::set<uint32_t>& allocSizes)
    : id_(id),
      maxSize_{poolSize},
      slabAllocator_(alloc),
      acSizes_(allocSizes.begin(), allocSizes.end()),
      ac_(createAllocationClasses()) {
  checkState();
}

void MemoryPool::checkState() const {
  if (id_ < 0) {
    throw std::invalid_argument(
        fmt::format("Invaild MemoryPool id {}", id_));
  }

  const size_t currAlloc = currAllocSize_;
  const size_t currSlabAlloc = currSlabAllocSize_;
  if (currAlloc > currSlabAlloc) {
    throw std::invalid_argument(
        fmt::format("Alloc size {} is more than total slab alloc size {}",
                       currAlloc,
                       currSlabAlloc));
  }

  if (acSizes_.empty() || ac_.empty()) {
    throw std::invalid_argument("Empty alloc sizes");
  }

  if (acSizes_.size() != ac_.size()) {
    throw std::invalid_argument(fmt::format(
        "Allocation classes are not setup correctly. acSize.size = {}, but "
        "ac.size() = {}",
        acSizes_.size(), ac_.size()));
  }

  if (!std::is_sorted(acSizes_.begin(), acSizes_.end())) {
    throw std::invalid_argument("Allocation sizes are not sorted.");
  }

  const auto firstDuplicate =
      std::adjacent_find(acSizes_.begin(), acSizes_.end());
  if (firstDuplicate != acSizes_.end()) {
    throw std::invalid_argument(
        fmt::format("Duplicate allocation size: {}", *firstDuplicate));
  }

  for (size_t i = 0; i < acSizes_.size(); i++) {
    if (acSizes_[i] != ac_[i]->getAllocSize() ||
        acSizes_[i] < Slab::kMinAllocSize || acSizes_[i] > Slab::kSize) {
      throw std::invalid_argument(fmt::format(
          "Allocation Class with id {} and size {}, does not match the "
          "allocation size we expect {}",
          ac_[i]->getId(), ac_[i]->getAllocSize(), acSizes_[i]));
    }
  }

  for (const auto slab : freeSlabs_) {
    if (!slabAllocator_.isValidSlab(slab)) {
      throw std::invalid_argument(fmt::format("Invalid free slab {}", (void*) slab));
    }
  }
}

MemoryPool::ACVector MemoryPool::createAllocationClasses() const {
  ACVector ac;
  ClassId id = 0;
  for (const auto size : acSizes_) {
    if (size < Slab::kMinAllocSize || size > Slab::kSize) {
      throw std::invalid_argument(
          fmt::format("Invalid allocation class size {}", size));
    }
    ac.emplace_back(new AllocationClass(id++, getId(), size, slabAllocator_));
  }
  XDCHECK(std::is_sorted(ac.begin(),
                         ac.end(),
                         [](const std::unique_ptr<AllocationClass>& a,
                            const std::unique_ptr<AllocationClass>& b) {
                           return a->getAllocSize() < b->getAllocSize();
                         }));

  return ac;
}

size_t MemoryPool::getCurrentUsedSize() const noexcept {
  LockHolder l(lock_);
  return currSlabAllocSize_ + freeSlabs_.size() * Slab::kSize;
}

AllocationClass& MemoryPool::getAllocationClassFor(uint32_t size) const {
  const auto classId = getAllocationClassId(size);
  return *ac_[classId];
}

AllocationClass& MemoryPool::getAllocationClassFor(void* memory) const {
  const auto classId = getAllocationClassId(memory);
  return *ac_[classId];
}

AllocationClass& MemoryPool::getAllocationClassFor(ClassId cid) const {
  if (cid < static_cast<ClassId>(ac_.size())) {
    XDCHECK(ac_[cid] != nullptr);
    return *ac_[cid];
  }
  throw std::invalid_argument(fmt::format("Invalid classId {}", cid));
}

const AllocationClass& MemoryPool::getAllocationClass(ClassId cid) const {
  return getAllocationClassFor(cid);
}

ClassId MemoryPool::getAllocationClassId(uint32_t size) const {
  if (size > acSizes_.back() || size == 0) {
    throw std::invalid_argument(
        fmt::format("Invalid size for alloc {} ", size));
  }

  // can operate without holding the mutex since the vector does not change.
  const auto it = std::lower_bound(acSizes_.begin(), acSizes_.end(), size);

  // we already checked for the bounds.
  XDCHECK(it != acSizes_.end());

  const auto idx = std::distance(acSizes_.begin(), it);
  XDCHECK_LT(static_cast<size_t>(idx), ac_.size());
  return static_cast<ClassId>(idx);
}

ClassId MemoryPool::getAllocationClassId(const void* memory) const {
  // find the slab for this allocation and the header for the slab. None of
  // the following needs to be serialized with the mutex.
  const auto* header = slabAllocator_.getSlabHeader(memory);

  // unallocated slab or slab not allocated to this pool or slab that is
  // allocated to this pool but not any allocation class is a failure.
  if (header == nullptr || header->poolId != id_) {
    throw std::invalid_argument(fmt::format(
        "Memory {} [PoolId = {}] does not belong to this pool with id {}",
        memory,
        header ? header->poolId : Slab::kInvalidPoolId,
        id_));
  } else if (header->classId == Slab::kInvalidClassId) {
    throw std::invalid_argument("Memory does not belong to any valid class Id");
  }

  const auto classId = header->classId;
  if (classId >= static_cast<ClassId>(ac_.size()) || classId < 0) {
    // at this point, the slab indicates that it belongs to a bogus classId and
    // things are corrupt and the caller cant do anything about it. so throw an
    // exception to abort.
    throw std::runtime_error(fmt::format(
        "corrupt slab header/memory pool with class id {}", classId));
  }

  return classId;
}

Slab* MemoryPool::getSlabLocked() noexcept {
  {
    // check again after getting the lock.
    if (allSlabsAllocated()) {
      return nullptr;
    }

    // increment the size under lock to serialize. This ensures that only one
    // thread can possibly go above the limit and fetch it from free list or
    // the slab allocator. If we dont get it from the free list or slab
    // allocator, we bump it down.
    currSlabAllocSize_ += Slab::kSize;

    if (!freeSlabs_.empty()) {
      auto slab = freeSlabs_.back();
      freeSlabs_.pop_back();
      return slab;
    }
  }

  auto slab = slabAllocator_.makeNewSlab(id_);
  // if slab allocator failed to allocate, decrement the size.
  if (slab == nullptr) {
    currSlabAllocSize_ -= Slab::kSize;
  }
  return slab;
}

void* MemoryPool::allocate(uint32_t size) {
  auto& ac = getAllocationClassFor(size);

  auto alloc = ac.allocate();
  const auto allocSize = ac.getAllocSize();
  XDCHECK_GE(allocSize, size);

  if (alloc != nullptr) {
    currAllocSize_ += allocSize;
    return alloc;
  }

  // atomically see if we can acquire a slab by checking if we have
  // reached the limit by size. If not, then they can be acquired from
  // either the slab allocator or our free list. It is important to check
  // this before we grab it from the slab allocator or free list. Things
  // that release slab, bump down the currSlabAllocSize_ after actually
  // releasing and adding it to free list or slab allocator.
  if (allSlabsAllocated()) {
    return nullptr;
  }

  // TODO: introduce a new sharded lock by allocation class id for this slow
  // path Currently this would also serialize the slow paths of two different
  // allocation class ids that need slab to initiate an allocation.
  LockHolder l(lock_);
  alloc = ac.allocate();
  if (alloc != nullptr) {
    currAllocSize_ += allocSize;
    return alloc;
  }

  // see if we have a slab to add to the allocation class.
  auto slab = getSlabLocked();
  if (slab == nullptr) {
    // out of memory
    return nullptr;
  }

  // add it to the allocation class and try to allocate.
  alloc = ac.addSlabAndAllocate(slab);
  XDCHECK_NE(nullptr, alloc);

  currAllocSize_ += allocSize;
  return alloc;
}

void MemoryPool::free(void* alloc) {
  auto& ac = getAllocationClassFor(alloc);
  ac.free(alloc);
  currAllocSize_ -= ac.getAllocSize();
}

void MemoryPool::releaseSlab(SlabReleaseMode mode,
                             const Slab* slab,
                             ClassId receiverClassId) {
  // If we are doing a resize, we need to release the slab back to the
  // allocator since the pool is being resized. But, if we are doing a
  // rebalance, we are resizing the allocation classes in the pool. Hence we
  // need to retain the slabs within the pool.
  switch (mode) {
  case SlabReleaseMode::kResize:
    slabAllocator_.freeSlab(const_cast<Slab*>(slab));
    // decrement after actually releasing the slab.
    currSlabAllocSize_ -= Slab::kSize;
    ++nSlabResize_;
    break;

  case SlabReleaseMode::kRebalance:
    if (receiverClassId != Slab::kInvalidClassId) {
      // Pool's current size does not change since this slab is
      // given to another allocation class within the same pool
      auto& receiverAC = getAllocationClassFor(receiverClassId);
      receiverAC.addSlab(const_cast<Slab*>(slab));
    } else {
      {
        LockHolder l(lock_);
        freeSlabs_.push_back(const_cast<Slab*>(slab));
      }

      // decrememnt after adding to the free list and not before. This ensures
      // that threads which observe the result of this atomic can always grab
      // it from the free list.
      currSlabAllocSize_ -= Slab::kSize;
    }
    ++nSlabRebalance_;
    break;
  }
}

SlabReleaseContext MemoryPool::releaseFromFreeSlabs() {
  LockHolder l(lock_);
  if (freeSlabs_.empty()) {
    throw std::invalid_argument(
        "Pool does not have any free slabs outside of allocation class ");
  }

  auto slab = freeSlabs_.back();
  freeSlabs_.pop_back();
  return SlabReleaseContext{slab, id_, Slab::kInvalidClassId,
                            SlabReleaseMode::kResize};
}

SlabReleaseContext MemoryPool::startSlabRelease(
    ClassId victim,
    ClassId receiver,
    SlabReleaseMode mode,
    const void* hint,
    SlabReleaseAbortFn shouldAbortFn) {
  if (receiver != Slab::kInvalidClassId &&
      mode != SlabReleaseMode::kRebalance) {
    throw std::invalid_argument(fmt::format(
        "A valid receiver {} is specified but the rebalancing mode is "
        "not SlabReleaseMode::kRebalance",
        receiver));
  }

  if (victim == Slab::kInvalidClassId && mode != SlabReleaseMode::kResize) {
    throw std::invalid_argument(
        "can not obtain from free slab pool when not using resizing mode");
  }

  auto context = victim == Slab::kInvalidClassId
                     ? releaseFromFreeSlabs()
                     : getAllocationClassFor(victim).startSlabRelease(
                           mode, hint, shouldAbortFn);
  context.setReceiver(receiver);

  // if the context is already in the released state, add it to the free
  // slabs. the caller should not have to call completeSlabRelease()
  if (context.isReleased()) {
    XDCHECK(context.getActiveAllocations().empty());
    // The caller does not need to call completeSlabRelease.
    releaseSlab(context.getMode(), context.getSlab(), receiver);
  }
  return context;
}

void MemoryPool::abortSlabRelease(const SlabReleaseContext& context) {
  auto classId = context.getClassId();
  auto& allocClass = getAllocationClassFor(classId);

  // abort the slab release process.
  allocClass.abortSlabRelease(context);
  nSlabReleaseAborted_++;
}

void MemoryPool::completeSlabRelease(const SlabReleaseContext& context) {
  if (context.isReleased()) {
    // the slab release is already completed.
    return;
  }

  if (context.getReceiverClassId() != Slab::kInvalidClassId &&
      context.getMode() != SlabReleaseMode::kRebalance) {
    throw std::invalid_argument(fmt::format(
        "A valid receiver {} is specified but the rebalancing mode is "
        "not SlabReleaseMode::kRebalance",
        context.getReceiverClassId()));
  }

  auto slab = context.getSlab();
  auto mode = context.getMode();
  auto classId = context.getClassId();
  auto& allocClass = getAllocationClassFor(classId);

  // complete the slab release process.
  allocClass.completeSlabRelease(context);

  XDCHECK_EQ(slabAllocator_.getSlabHeader(slab)->poolId, getId());
  XDCHECK_EQ(slabAllocator_.getSlabHeader(slab)->classId,
             Slab::kInvalidClassId);
  XDCHECK_EQ(slabAllocator_.getSlabHeader(slab)->allocSize, 0u);

  releaseSlab(mode, slab, context.getReceiverClassId());
}
