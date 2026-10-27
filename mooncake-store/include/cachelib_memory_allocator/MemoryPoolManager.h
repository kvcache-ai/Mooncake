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

#include <array>
#include <map>
#include <shared_mutex>
#include <unordered_map>

#include "MemoryPool.h"
#include "Slab.h"
#include "SlabAllocator.h"

namespace facebook {
namespace cachelib {

// used to organize the available memory into pools and identify them by a
// string name or pool id.
class MemoryPoolManager {
 public:
  // maximum number of pools that we support.
  static constexpr unsigned int kMaxPools = 64;

  // creates a memory pool manager for this slabAllocator.
  // @param slabAlloc  the slab allocator to be used for the memory pools.
  explicit MemoryPoolManager(SlabAllocator& slabAlloc);

  MemoryPoolManager(const MemoryPoolManager&) = delete;
  MemoryPoolManager& operator=(const MemoryPoolManager&) = delete;

  // adding a pool
  // @param name   the string name representing the pool.
  // @param size   the size of the memory pool.
  // @param allocSizes  set of allocation sizes sorted in increasing
  //                    order. This will be used to create the corresponding
  //                    AllocationClasses.
  //
  // @return on success, returns id of the new memory pool.
  // @throw  std::invalid_argument if the name/size/allcoSizes are invalid or
  //         std::logic_error if we have run out the allowed number of pools.
  PoolId createNewPool(std::string name,
                       size_t size,
                       const std::set<uint32_t>& allocSizes);

  // shrink the existing pool by _bytes_ .
  // @param bytes  the number of bytes to be taken away from the pool
  // @return  true if the operation succeeded. false if the size of the pool is
  //          smaller than _bytes_
  // @throw   std::invalid_argument if the poolId is invalid.
  bool shrinkPool(PoolId pid, size_t bytes);

  // grow an existing pool by _bytes_. This will fail if there is no
  // available memory across all the pools to provide for this pool
  // @param bytes  the number of bytes to be added to the pool.
  // @return    true if the pool was grown. false if the necessary number of
  //            bytes were not available.
  // @throw     std::invalid_argument if the poolId is invalid.
  bool growPool(PoolId pid, size_t bytes);

  // move bytes from one pool to another. The source pool should be at least
  // _bytes_ in size.
  //
  // @param src     the pool to be sized down and giving the memory.
  // @param dest    the pool receiving the memory.
  // @param bytes   the number of bytes to move from src to dest.
  //
  // @return  true if the resize succeeded. False if the src pool does not have
  //          enough memory to make the resize.
  // @throw std::invalid_argument if src or dest is invalid pool
  bool resizePools(PoolId src, PoolId dest, size_t bytes);

  // Fetch the list of pools that are above their current limit due to a
  // recent resize.
  //
  // @return list of pools that are over limit.
  std::set<PoolId> getPoolsOverLimit() const;

  // access the memory pool by its name and id.
  // @returns returns a valid MemoryPool.
  // @throw std::invalid_argument if the name or id is invalid.
  MemoryPool& getPoolByName(const std::string& name) const;
  MemoryPool& getPoolById(PoolId id) const;

  // returns the pool's name by its pool ID
  // @throw std::logic_error if the pool ID not existed.
  const std::string& getPoolNameById(PoolId id) const;

  // returns the current pool ids that are being used.
  std::set<PoolId> getPoolIds() const;

  // size in bytes of the remaining size that is not reserved for any pools.
  size_t getBytesUnReserved() const {
    std::shared_lock l(lock_);
    return getRemainingSizeLocked();
  }

  // return total memory currently advised away
  size_t getAdvisedMemorySize() const noexcept {
    size_t sum = 0;
    std::unique_lock l(lock_);
    for (PoolId id = 0; id < nextPoolId_; id++) {
      sum += pools_[id]->getPoolAdvisedSize();
    }
    return sum;
  }

 private:
  // obtain the remaining size in bytes that is not reserved by taking into
  // account the total available memory in the slab allocator and the size of
  // all the pools we manage.
  size_t getRemainingSizeLocked() const noexcept;

  // rw lock serializing the access to poolsByName_ and pool creation.
  mutable std::shared_mutex lock_;

  // array of pools by Id. The valid pools are up to (nextPoolId_ - 1). This
  // is to ensure that we can fetch pools by Id without holding any locks as
  // long as the pool Id is valid.
  std::array<std::unique_ptr<MemoryPool>, kMaxPools> pools_;

  // pool name -> pool Id mapping.
  std::map<std::string, PoolId> poolsByName_;

  // the next available pool id.
  std::atomic<PoolId> nextPoolId_{0};

  // slab allocator for the pools
  SlabAllocator& slabAlloc_;
};
} // namespace cachelib
} // namespace facebook
