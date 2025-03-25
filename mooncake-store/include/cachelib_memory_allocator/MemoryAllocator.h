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

#include <limits>

#include "AllocationClass.h"
#include "MemoryPool.h"
#include "MemoryPoolManager.h"
#include "Slab.h"
#include "SlabAllocator.h"

namespace facebook {
namespace cachelib {

/* The following is a brief overview of the different hierarchies in the
 * implementation.
 *
 * MemoryAllocator -- provides allocation by any size up to Slab::kSize.  It
 * consists of a set of MemoryPools. To make an allocation from a pool, the
 * corresponding pool id is  to be used. The memory allocator uses the slab
 * allocator to make allocations of Slab::kSize and divides that into smaller
 * allocations. It also takes care of dividing the available memory into
 * different pools at the granularity of a slab.
 *
 * MemoryPool -- deals with memory allocation for a given pool. It contains a
 * collection of AllocationClass instances to actually handle allocations of any
 * size.  MemoryPools are configured to grow up to a given size by the
 * MemoryAllocator that owns it.
 *
 * AllocationClass -- creates allocations of a particular size from slabs
 * belonging to a given memory pool.
 *
 * SlabAllocator -- divides up a contiguous piece of memory into slabs. A slab
 * is a contiguous piece of memory of a pre-defined size (Slab::kSize).
 * Allocated slabs are distributed to different memory pools. The slab
 * allocator maintains the memory required for the slab headers and provides
 * an interface to fetch the header for given slab.
 *
 */

// uses the slab allocator and slab memory pool to actually allocate the memory.
// Read the description at the beginning of the file for more info
class MemoryAllocator {
   public:
    // maximum number of allocation classes that we support.
    static constexpr unsigned int kMaxClasses = 1 << 7;
    static constexpr ClassId kMaxClassId = kMaxClasses - 1;

    // maximum number of memory pools that we support.
    static constexpr unsigned int kMaxPools = MemoryPoolManager::kMaxPools;
    static constexpr PoolId kMaxPoolId = kMaxPools - 1;
    // default of 8 byte aligned.
    static constexpr uint32_t kAlignment = sizeof(void*);

    // config for the slab memory allocator.
    struct Config {
        Config() {}
        Config(std::set<uint32_t> sizes) : allocSizes(std::move(sizes)) {}

        // Hint to determine the allocation class sizes
        std::set<uint32_t> allocSizes;
    };

    // See Feishu document.
    MemoryAllocator(Config config, void* headerMemoryStart,
                    size_t headerMemorySize, void* slabMemoryStart,
                    size_t slabMemorySize);

    MemoryAllocator(const MemoryAllocator&) = delete;
    MemoryAllocator& operator=(const MemoryAllocator&) = delete;

    // allocate memory of corresponding size.
    //
    // @param id    the pool id to be used for this allocation.
    // @param size  the size for the allocation.
    // @return pointer to the memory corresponding to the allocation. nullptr if
    // memory is not available.
    //
    // @throw std::invalid_argument if the poolId is invalid or the size is
    //        invalid.
    void* allocate(PoolId id, uint32_t size);

    // free the memory back to the allocator.
    //
    // @throw std::invalid_argument if the memory does not belong to any active
    //        allocation handed out by this allocator.
    void free(void* memory);

    // Memory pool interface. The memory pools must be established before the
    // first allocation happens. Currently we dont support adding / removing
    // pools dynamically.
    //
    // @param name      the name of the pool
    // @param size      the size of the pool
    // @param allocSize the set of allocation sizes for this memory pool,
    //                  if empty, a default one will be used
    // @param ensureProvisionable   ensures that the size of the pool is enough
    //                              to provision one slab to each allocation
    //                              class
    //
    // @return a valid pool id that the caller can use on successful return.
    //
    // @throws std::invalid_argument if the name or size is inappropriate or
    //         if there is not enough space left for this pool.
    //         std::logic_error if we have run out the allowed number of pools.
    PoolId addPool(std::string name, size_t size,
                   const std::set<uint32_t>& allocSizes = {},
                   bool ensureProvisionable = false);

    // shrink the existing pool by _bytes_ .
    // @param id     the id for the pool
    // @param bytes  the number of bytes to be taken away from the pool
    // @return  true if the operation succeeded. false if the size of the pool
    // is
    //          smaller than _bytes_
    // @throw   std::invalid_argument if the poolId is invalid.
    bool shrinkPool(PoolId pid, size_t bytes) {
        return memoryPoolManager_.shrinkPool(pid, bytes);
    }

    // grow an existing pool by _bytes_. This will fail if there is no
    // available memory across all the pools to provide for this pool
    // @param id     the pool id to be grown.
    // @param bytes  the number of bytes to be added to the pool.
    // @return    true if the pool was grown. false if the necessary number of
    //            bytes were not available.
    // @throw     std::invalid_argument if the poolId is invalid.
    bool growPool(PoolId pid, size_t bytes) {
        return memoryPoolManager_.growPool(pid, bytes);
    }

    // move bytes from one pool to another. The source pool should be at least
    // _bytes_ in size.
    //
    // @param src     the pool to be sized down and giving the memory.
    // @param dest    the pool receiving the memory.
    // @param bytes   the number of bytes to move from src to dest.
    // @param   true if the resize succeeded. false if src does does not have
    //          correct size to do the transfer.
    // @throw   std::invalid_argument if src or dest is invalid pool
    bool resizePools(PoolId src, PoolId dest, size_t bytes) {
        return memoryPoolManager_.resizePools(src, dest, bytes);
    }

    // Start the process of releasing a slab from this allocation class id and
    // pool id. The release could be for a pool resizing or allocation class
    // rebalancing. If a valid context is returned, the caller needs to free the
    // active allocations in the valid context and call completeSlabRelease. A
    // null context indicates that a slab was successfully released. throws on
    // any other error.
    //
    // @param pid       the pool id
    // @param victim    the allocation class id in the pool. if invalid, we try
    //                  to pick any free slab that is available from the pool.
    // @param receiver  the allocation class that will get a slab
    // @param mode  the mode for slab release (rebalance/resize)
    // @param hint  hint referring to the slab. this can be an allocation that
    //              the user knows to exist in the slab. If this is nullptr, a
    //              random slab is selected from the pool and allocation class.
    // @param  shouldAbortFn invoked in the code to see if this release slab
    //         process should be aborted
    //
    // @return  a valid context. If the slab is already released, then the
    //          caller needs to do nothing. If it is not released, then the
    //          caller needs to free the allocations and call
    //          completeSlabRelease with the same context.
    //
    // @throw std::invalid_argument if the hint is invalid or if the pid or cid
    //        is invalid. Or if the mode is set to kResize but the receiver is
    //        also specified. Receiver class id can only be specified if the
    //        mode is set to kRebalance.
    // @throw exception::SlabReleaseAborted if slab release is aborted due to
    //        shouldAbortFn returning true.
    SlabReleaseContext startSlabRelease(
        PoolId pid, ClassId victim, ClassId receiver, SlabReleaseMode mode,
        const void* hint = nullptr,
        SlabReleaseAbortFn shouldAbortFn = []() { return false; });

    // Check if an alloc is free (during slab release)
    //
    // @param ctx     SlabReleaseContext to enforce that this is only called
    //                during slab release.
    // @param memory  alloc being checked.
    //
    // @return true  if the alloc is free.
    //
    // @throws std::invalid_argument  if the memory does not belong to a slab of
    //         this slab class, or if the slab is not actively being released,
    //         or if the context belongs to a different slab.
    bool isAllocFreed(const SlabReleaseContext& ctx, void* memory) const;

    // Check if the slab has all its active allocations freed.
    //
    // @param ctx context returned by startSlabRelease.
    // @return    true if all allocs have been freed back to the allocator
    //            false otherwise
    //
    // @throw std::invalid_argument if the pool id or allocation class id
    //        associated with the context is invalid.
    //
    //        std::runtime_error if the slab associatec with the context
    //        does not have the allocStateMap entry.
    bool allAllocsFreed(const SlabReleaseContext& ctx) const;

    // See AllocationClass::processAllocForRelease
    void processAllocForRelease(
        const SlabReleaseContext& ctx, void* memory,
        const std::function<void(void*)>& callback) const;

    // Aborts the slab release process when there were active allocations in
    // the slab. This should be called with the same non-null context that was
    // created using startSlabRelease and after the user FAILS to free all the
    // active allocations in the context. The state of the allocation class may
    // not exactly same as pre-startSlabRelease state because freed allocations
    // while trying to release the slab are not restored.
    //
    // @param context  the context returned by startSlabRelease
    //
    // @throw std::invalid_argument if the context is invalid or
    //        context is already released or all allocs in the context are
    //        free
    void abortSlabRelease(const SlabReleaseContext& context);

    // completes the slab release process when there were active allocations in
    // the slab. This should be called with the same non-null context that was
    // created using startSlabRelease and after the user frees all the active
    // allocations in the context. After this, the slab is released
    // appropriately. Calling this with a context that has the slab already
    // released is a no-op. This will block until all the active allocations are
    // completely returned to the allocator.
    //
    // @param context  a valid context
    // @throw std::invalid_argument if the context is invalid.
    //        Or if the mode is set to kResize but the receiver is
    //        also specified. Receiver class id can only be specified if the
    //        mode is set to kRebalance.
    void completeSlabRelease(const SlabReleaseContext& context);

    // get the PoolId corresponding to the pool name.
    //
    // @param name  the name of the pool
    // @return  poold id corresponding to the name if it exists or
    //          kInvalidPoolId if name is not a recognized pool.
    PoolId getPoolId(const std::string& name) const noexcept;

    // get the pool name corresponding to its PoolId
    //
    // @param id  the id of the pool
    // @return    pool name of this pool
    // @throw std::logic_error if the pool id is invalid.
    std::string getPoolName(PoolId id) const {
        return memoryPoolManager_.getPoolNameById(id);
    }

    // return the usable size in bytes for this allocator.
    size_t getMemorySize() const noexcept {
        return slabAllocator_.getNumUsableSlabs() * Slab::kSize;
    }

    size_t getUnreservedMemorySize() const noexcept {
        return memoryPoolManager_.getBytesUnReserved();
    }

    // return the total memory advised away
    size_t getAdvisedMemorySize() const noexcept {
        return memoryPoolManager_.getAdvisedMemorySize();
    }

    // return the list of pool ids for this allocator.
    std::set<PoolId> getPoolIds() const {
        return memoryPoolManager_.getPoolIds();
    }

    // fetches the memory pool for the id if one exists. This is purely to get
    // information out of the pool.
    //
    // @return const reference to memory pool for the id if one exists.
    // @throw std::invalid_argument if the pool id is invalid.
    const MemoryPool& getPool(PoolId id) const {
        return memoryPoolManager_.getPoolById(id);
    }

    // obtain list of pools that are currently occupying more memory than their
    // current limit.
    std::set<PoolId> getPoolsOverLimit() const {
        return memoryPoolManager_.getPoolsOverLimit();
    }

    // return true if all the memory for the allocator is allocated to some
    // pool.
    // this is leveraged by pool rebalancers to determine if the rebalancing has
    // to start.
    bool allSlabsAllocated() const noexcept {
        return slabAllocator_.allSlabsAllocated();
    }

    // returns true if all the slab memory for the pool is accounted for in some
    // allocation class belonging to the pool.
    //
    // @throw std::invalid_argument if the pool id does not belong to a valid
    // pool.
    bool allSlabsAllocated(PoolId pid) const {
        return getPool(pid).allSlabsAllocated();
    }

    // fetch the pool and allocation class information for the memory
    // corresponding to a memory allocation from the allocator. Caller is
    // expected to supply a memory that is valid and allocated from this
    // allocator.
    //
    // @param memory  the memory belonging to the slab allocator
    // @return        pair of poolId and classId of the memory
    // @throw std::invalid_argument if the memory doesn't belong to allocator
    FOLLY_ALWAYS_INLINE AllocInfo getAllocInfo(const void* memory) const {
        const auto* header = slabAllocator_.getSlabHeader(memory);
        if (!header) {
            throw std::invalid_argument(
                fmt::format("invalid header for slab memory addr: {}", memory));
        }
        return AllocInfo{header->poolId, header->classId, header->allocSize};
    }

    // fetch the allocation size for the pool id and class id.
    //
    // @param pid  the pool id
    // @param cid  the allocation class id
    //
    // @return the allocation size corresponding to this pair.
    // @throw std::invalid_argument if the ids are invalid.
    uint32_t getAllocSize(PoolId pid, ClassId cid) const {
        const auto& pool = getPool(pid);
        const auto& allocClass = pool.getAllocationClass(cid);
        return allocClass.getAllocSize();
    }

    // return the default allocation sizes for this allocator.
    const std::set<uint32_t>& getAllocSizes() const noexcept {
        return config_.allocSizes;
    }

    // fetch the allocation class info corresponding to a given size in a pool.
    //
    // @param poolId  the pool to be allocated from
    // @param nBytes  the allocation size
    // @return        a valid class id on success
    // @throw   std::invalid_argument if the poolId is invalid or the size is
    //          outside of the allocation sizes for the memory pool.
    ClassId getAllocationClassId(PoolId poolId, uint32_t nBytes) const;

    // Traverse each slab and call user defined callback on each allocation
    // within the slab. Callback will be invoked if the slab is not advised,
    // marked for release or currently being moved. Callbacks will be invoked
    // irrespective of whether the slab is allocated for free.
    //
    // @param callback   Callback to be executed on each allocation
    // @return           The number of slabs skipped
    //                   Slab can be skipped because it is being released or
    //                   already released but not yet assigned to another pool
    //                   or allocation class.
    template <typename AllocTraversalFn>
    uint64_t forEachAllocation(AllocTraversalFn&& callback) {
        uint64_t slabSkipped = 0;
        for (unsigned int idx = 0; idx < slabAllocator_.getNumUsableSlabs();
             ++idx) {
            Slab* slab = slabAllocator_.getSlabForIdx(idx);
            const auto slabHdr = slabAllocator_.getSlabHeader(slab);
            if (!slabHdr) {
                continue;
            }
            auto classId = slabHdr->classId;
            auto poolId = slabHdr->poolId;
            if (poolId == Slab::kInvalidPoolId ||
                classId == Slab::kInvalidClassId || slabHdr->isAdvised() ||
                slabHdr->isMarkedForRelease()) {
                ++slabSkipped;
                continue;
            }
            auto& pool = memoryPoolManager_.getPoolById(poolId);
            auto slabIterationStatus = pool.forEachAllocation(
                classId, slab, std::forward<AllocTraversalFn>(callback));
            if (slabIterationStatus ==
                SlabIterationStatus::kSkippedCurrentSlabAndContinue) {
                ++slabSkipped;
            } else if (slabIterationStatus ==
                       SlabIterationStatus::kAbortIteration) {
                return slabSkipped;
            }
        }
        return slabSkipped;
    }

    // returns a default set of allocation sizes with given size range and
    // factor.
    //
    // @param factor      the factor by which the alloc sizes grow.
    // @param maxSize     the maximum allowed allocation size
    // @param minSize     the minimum allowed allocation size
    // @param reduceFragmentation if true chunk sizes will be increased to the
    //                            maximum size that maintains the number of
    //                            chunks per slab as determined using factor.
    //
    // @return    std::set of allocation sizes that all fit within maxSize.
    //
    // @throw std::invalid_argument if the maxSize is more than the slab size.
    // @throw std::invalid_argument if the factor is <= 1.0
    // @throw std::invalid_argument if the factor is not incrementing large
    //                              enough when reduceFragmentation is enabled

    static std::set<uint32_t> generateAllocSizes(
        double factor = 1.25, uint32_t maxSize = Slab::kSize,
        uint32_t minSize = 72, bool reduceFragmentation = false);

   private:
    // @param memory    pointer to the memory.
    // @return          the MemoryPool corresponding to the memory.
    // @throw std::invalid_argument if the memory does not belong to any active
    //        allocation handed out by this allocator.
    MemoryPool& getMemoryPool(const void* memory) const;

    // the config for the allocator.
    const Config config_;

    // the instance of slab allocator we will use to allocate slabs.
    SlabAllocator slabAllocator_;

    // the instance used for book keeping information about the memory pools
    // configuration.
    MemoryPoolManager memoryPoolManager_;
};
}  // namespace cachelib
}  // namespace facebook
