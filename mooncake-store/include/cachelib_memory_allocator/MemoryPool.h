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
#include <cstddef>
#include <mutex>
#include <set>
#include <vector>

#include "AllocationClass.h"
#include "Slab.h"

namespace facebook {
namespace cachelib {

class SlabAllocator;

// memory pool corresponds to a pool set up by the MemoryPoolManager. All
// allocations for a given memory pool go through this. It consists of a bunch
// of allocation classes and talks to the slab allocator for grabbing slabs. The
// active memory pool size indicates the amount of memory actively being used
// by the memory pool. The sum of active memory and freed slabs and freed
// allocations per allocation class should amount for the total memory footprint
// of this memory pool from the slab allocator's perspective.
class MemoryPool {
 public:
  // creates a pool with the id and size.
  //
  // @param  id         the unique pool id.
  // @param  poolSize   max size of the pool.
  // @param  alloc      the slab allocator for requesting the slabs.
  // @param  allocSizes the set of allocation class sizes for this pool,
  //                    sorted in increasing order. The largest size should be
  //                    less than Slab::kSize.
  // @throw std::invalid_argument if allocSizes is invalid
  MemoryPool(PoolId id,
             size_t poolSize,
             SlabAllocator& alloc,
             const std::set<uint32_t>& allocSizes);

  MemoryPool(const MemoryPool&) = delete;
  MemoryPool& operator=(const MemoryPool&) = delete;

  // returns the poolId of this memory pool.
  PoolId getId() const noexcept { return id_; }

  // the configured size of the pool.
  size_t getPoolSize() const noexcept { return maxSize_; }

  // return the current size of the pool that has been already advised. Note
  // include the configured advised size if advising has not
  // caught up.
  size_t getPoolAdvisedSize() const noexcept {
    return curSlabsAdvised_ * Slab::kSize;
  }

  // return the usable size of the pool.
  // Usable pool size is configured pool size minus advised away size
  size_t getPoolUsableSize() const noexcept {
    auto advisedSize = getPoolAdvisedSize();
    return maxSize_ <= advisedSize ? 0 : maxSize_ - advisedSize;
  }

  // returns the allocation class sizes as configured in this pool
  const std::vector<uint32_t>& getAllocSizes() const noexcept {
    return acSizes_;
  }

  // returns true if the memory pools has more memory allocated than the
  // current size. This is possible because we allow resizing the pool
  // dynamically.
  bool overLimit() const noexcept {
    auto getCurrentUsedAndAdvisedSize =
        getCurrentUsedSize() + getPoolAdvisedSize();
    return getCurrentUsedAndAdvisedSize > maxSize_;
  }

  // returns the size of memory currently unallocated in this pool
  size_t getUnAllocatedSlabMemory() const noexcept {
    auto totalAllocSize = currSlabAllocSize_ + getPoolAdvisedSize();
    return totalAllocSize > maxSize_ ? 0 : maxSize_ - totalAllocSize;
  }

  // returns the current memory that is allocated in this memory pool.
  size_t getCurrentAllocSize() const noexcept { return currAllocSize_; }

  // returns current memory used by this memory pool, including slabs
  // in the free list.
  size_t getCurrentUsedSize() const noexcept;

  // returns true if the pool has allocated all the slabs it can for some
  // allocation class. This does not mean that the pool is full since the
  // allocation class corresponding to some allocation class can still have
  // free memory available.
  bool allSlabsAllocated() const noexcept {
    auto currAdvisedSize = getPoolAdvisedSize();
    return (currSlabAllocSize_ + currAdvisedSize + Slab::kSize) > maxSize_;
  }

  // allocates memory of at least _size_ bytes.
  //
  // @param size  size of the allocation.
  // @return pointer to allocation or nullptr on failure to allocate.
  // @throw  std::invalid_argument if size is invalid.
  void* allocate(uint32_t size);

  // Allocate a slab with zeroed memory
  //
  // @return pointer to allocation or nullptr on failure to allocate.
  // @throw  std::invalid_argument if requestedSize is invalid.
  void* allocateZeroedSlab();

  // frees the memory back to the pool. throws an exception if the memory does
  // not belong to this pool.
  //
  // @param  memory pointer to the memory to be freed
  //
  // throws the following exceptions in cases where either the caller freed
  // the wrong allocations to this pool or if there is an internal corruption
  // of data structures
  // @throw std::invalid_argument if the memory does not belong to this pool.
  // @throw std::run_time_error if the slab class information is corrupted.
  void free(void* memory);

  // resize the memory pool. This only adjusts the Pool size. It does not
  // release the slabs back to the SlabAllocator if the new size is less than
  // the current size. The caller is responsible for doing that through
  // startSlabRelease() calls.
  //
  // @param size_t size  the new size for this pool.
  void resize(size_t size) noexcept { maxSize_ = size; }

  // Start the process of releasing a slab from this allocation class id and
  // pool id. The release could be for a pool resizing or allocation class
  // rebalancing. If a valid context is returned, the caller needs to free the
  // active allocations in the valid context and call completeSlabRelease. A
  // null context indicates that a slab was successfully released. throws on
  // any other error.
  //
  // @param victim    the allocation class id in the pool. if invalid, we try
  //                  to pick from the free slabs if available
  // @param receiver  the allocation class that will get a slab
  // @param mode  the mode for slab release (rebalance/resize)
  // @param hint  hint referring to the slab. this can be an allocation that
  //              the user knows to exist in the slab. If this is nullptr, a
  //              random slab is selected from the pool and allocation class.
  // @param shouldAbortFn  invoked in the code to see if this release slab
  //                       process should be aborted
  //
  // @return  a valid context. If the slab is already released, then the
  //          caller needs to do nothing. If it is not released, then the caller
  //          needs to free the allocations and call completeSlabRelease with
  //          the same context.
  //
  // @throw std::invalid_argument if the hint is invalid or if the pid or cid
  //        is invalid. Or if the mode is set to kResize but the receiver is
  //        also specified. Receiver class id can only be specified if the mode
  //        is set to kRebalance.
  // @throw exception::SlabReleaseAborted if slab release is aborted due to
  //        shouldAbortFn returning true.
  SlabReleaseContext startSlabRelease(
      ClassId victim,
      ClassId receiver,
      SlabReleaseMode mode,
      const void* hint,
      SlabReleaseAbortFn shouldAbortFn = []() { return false; });

  // Aborts the slab release process when there were active allocations in
  // the slab. This should be called with the same non-null context that was
  // created using startSlabRelease and after the user FAILS to free all the
  // active allocations in the context. The state of the allocation class may
  // not exactly same as pre-startSlabRelease state because freed allocations
  // while trying to release the slab are not restored.
  //
  // @param context         context returned by startSlabRelease
  //
  // @throw std::invalid_argument if the context is invalid or
  //        context is already released or all allocs in the context are
  //        free
  void abortSlabRelease(const SlabReleaseContext& context);

  // completes the slab release process when there were active allocations in
  // the slab. This should be called with the same non-null context that was
  // created using startSlabRelease and after the user frees all the active
  // allocations in the context. After this, the slab is released
  // appropriately. This will block until all the allocations are returned to
  // the allocator.
  //
  // @param context         context returned by startSlabRelease
  //
  // @throw std::invalid_argument if the context is invalid.
  //        Or if the mode is set to kResize but the receiver is
  //        also specified. Receiver class id can only be specified if the mode
  //        is set to kRebalance.
  void completeSlabRelease(const SlabReleaseContext& context);

  // fetch the ClassId corresponding to the allocation class from this memory
  // pool
  //
  // @param  size  the allocation size
  // @return the allocations class id corresponding to the alloc size
  // @throw  std::invalid_argument if the size does not correspond to any
  //         allocation class.
  ClassId getAllocationClassId(uint32_t size) const;

  // fetch the ClassId for the memory.
  //
  // @param  memory  pointer to allocated memory from this pool.
  // @return the allocation class id for the memory
  // @throw  std::invalid_argument if the memory does not belong to this pool
  //         or a valid allocation class.
  ClassId getAllocationClassId(const void* memory) const;

  // fetch the allocation class for inspection. This is merely to read the
  // info about the allocation class.
  //
  // @param  cid    the allocation class id that we are looking for.
  // @return    pointer to the AllocationClass. guaranteed to be valid
  //            allocation class.
  // @throw std::invalid_argument if the ClassId is invalid.
  const AllocationClass& getAllocationClass(ClassId cid) const;

  // return the number of  allocation ClassIds for this pool based on the
  // allocation sizes that it was configured with. All allocations from this
  // pool will have ClassId from [0 .. numClassId - 1] (inclusive).
  unsigned int getNumClassId() const noexcept {
    return static_cast<unsigned int>(acSizes_.size());
  }

  // Gets allocation class for a given class id and calls forEachAllocation on
  // that allocation class.
  //
  // @param callback   Callback to be executed on each allocation
  //
  // @return           SlabIterationStatus
  template <typename AllocTraversalFn>
  SlabIterationStatus forEachAllocation(ClassId classId,
                                        Slab* slab,
                                        AllocTraversalFn&& callback) {
    auto& allocClass = getAllocationClassFor(classId);
    return allocClass.forEachAllocation(
        slab, std::forward<AllocTraversalFn>(callback));
  }

 private:
  // container for storing a vector of AllocationClass.
  using ACVector = std::vector<std::unique_ptr<AllocationClass>>;

  // intended to be used by the constructor to verify the state of the memory
  // pool, specifically when we deserialize from a serialized state
  // @throw std::invalid_argument if any of the state is invalid.
  void checkState() const;

  // get a slab for use based on the activeSize and maxSize. returns nullptr
  // if out of slab memory.
  Slab* getSlabLocked() noexcept;

  // create allocation classes corresponding to the pool's configuration.
  ACVector createAllocationClasses() const;

  // @return  AllocationClass corresponding to the memory, if it
  //          belongs to an AllocationClass
  //
  // @throw std::invalid_argument if the memory does not belong to this pool
  //        or is invalid.
  AllocationClass& getAllocationClassFor(void* memory) const;

  // fetch the allocation class corresponding to the allocation size. returns
  //
  // @param size  the allocation size requested.
  // @return  allocation class.
  // @throw   std::invalid_argument if the allocation size is out of range.
  AllocationClass& getAllocationClassFor(uint32_t size) const;

  // fetch the allocation class corresponding to the class id.
  //
  // @param cid   the allocation class id
  // @return      the allocation class
  // @throw std::invalid_argument if the class id is invalid.
  AllocationClass& getAllocationClassFor(ClassId cid) const;

  // helper function to release a slab back to either the slab allocator or to
  // our free pool.
  // @param mode            the mode of the release operation
  // @param slab            the slab to be released.
  // @param receiverClassId optional AC to receive this slab
  void releaseSlab(SlabReleaseMode mode,
                   const Slab* slab,
                   ClassId receiverClassId);

  // create a slab release context from the free slabs if possible.
  //
  // @throw std::invalid_argument if there are no free slabs available.
  SlabReleaseContext releaseFromFreeSlabs();

  // mutex for serializing access to freeSlabs_ and the currSlabAllocSize_.
  mutable std::mutex lock_;

  // the id for this memory pool
  const PoolId id_{-1};

  // the current max size of the memory pool.
  std::atomic<size_t> maxSize_{0};

  // the current size of all the slab memory we have allocated for this pool
  // that actively belong to one of its AllocationClasses. This does not
  // include the memory under freeSlabs_.
  std::atomic<size_t> currSlabAllocSize_{0};

  // the current size of all allocations from this memory pool.
  // currAllocSize_ <= currSlabSize_ <= maxSize_
  std::atomic<size_t> currAllocSize_{0};

  // the allocator for slabs.
  SlabAllocator& slabAllocator_;

  // slabs allocated from the slab allocator for this memory pool, that are
  // not currently in use.
  std::vector<Slab*> freeSlabs_;

  // sorted vector of allocation class sizes
  const std::vector<uint32_t> acSizes_;

  // vector of allocation classes for this pool, sorted by their allocation
  // sizes and indexed by their class id. This vector does not change once it
  // is initialized inside the constructor. so this can be accessed without
  // grabbing the mutex.
  const ACVector ac_;

  // Current configuration of advised away Slabs in the pool
  std::atomic<uint64_t> curSlabsAdvised_{0};

  // number of slabs we released for resizes and rebalances
  std::atomic<unsigned int> nSlabResize_{0};
  std::atomic<unsigned int> nSlabRebalance_{0};
  std::atomic<unsigned int> nSlabReleaseAborted_{0};
};
} // namespace cachelib
} // namespace facebook
