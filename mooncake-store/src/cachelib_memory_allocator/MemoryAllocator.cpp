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

#include "MemoryAllocator.h"

using namespace facebook::cachelib;

namespace {
// helper function.
void checkConfig(const MemoryAllocator::Config& config) {
  if (config.allocSizes.size() > MemoryAllocator::kMaxClasses) {
    throw std::invalid_argument("Too many allocation classes");
  }
}
} // namespace

MemoryAllocator::MemoryAllocator(Config config,
                                 void* headerMemoryStart,
                                 size_t headerMemorySize,
                                 void* slabMemoryStart,
                                 size_t slabMemorySize)
    : config_(std::move(config)),
      slabAllocator_(headerMemoryStart,
                     headerMemorySize,
                     slabMemoryStart,
                     slabMemorySize),
      memoryPoolManager_(slabAllocator_) {
  checkConfig(config_);
}

void* MemoryAllocator::allocate(PoolId id, uint32_t size) {
  auto& mp = memoryPoolManager_.getPoolById(id);
  return mp.allocate(size);
}

PoolId MemoryAllocator::addPool(std::string name,
                                size_t size,
                                const std::set<uint32_t>& allocSizes,
                                bool ensureProvisionable) {
  const std::set<uint32_t>& poolAllocSizes =
      allocSizes.empty() ? config_.allocSizes : allocSizes;

  if (poolAllocSizes.size() > MemoryAllocator::kMaxClasses) {
    throw std::invalid_argument("Too many allocation classes");
  }

  if (ensureProvisionable &&
      cachelib::Slab::kSize * poolAllocSizes.size() > size) {
    throw std::invalid_argument(fmt::format(
        "Pool {} cannot have at least one slab for each allocation class. "
        "{} bytes required, {} bytes given.",
        name,
        cachelib::Slab::kSize * poolAllocSizes.size(),
        size));
  }

  return memoryPoolManager_.createNewPool(name, size, poolAllocSizes);
}

PoolId MemoryAllocator::getPoolId(const std::string& name) const noexcept {
  try {
    const auto& mp = memoryPoolManager_.getPoolByName(name);
    return mp.getId();
  } catch (const std::invalid_argument&) {
    return Slab::kInvalidPoolId;
  }
}

void MemoryAllocator::free(void* memory) {
  auto& mp = getMemoryPool(memory);
  mp.free(memory);
}

MemoryPool& MemoryAllocator::getMemoryPool(const void* memory) const {
  const auto* header = slabAllocator_.getSlabHeader(memory);
  if (header == nullptr) {
    throw std::invalid_argument("not recognized by this allocator");
  }
  auto poolId = header->poolId;
  return memoryPoolManager_.getPoolById(poolId);
}

ClassId MemoryAllocator::getAllocationClassId(PoolId poolId,
                                              uint32_t size) const {
  const auto& pool = memoryPoolManager_.getPoolById(poolId);
  return pool.getAllocationClassId(size);
}

SlabReleaseContext MemoryAllocator::startSlabRelease(
    PoolId pid,
    ClassId victim,
    ClassId receiver,
    SlabReleaseMode mode,
    const void* hint,
    SlabReleaseAbortFn shouldAbortFn) {
  auto& pool = memoryPoolManager_.getPoolById(pid);
  return pool.startSlabRelease(victim, receiver, mode, hint, shouldAbortFn);
}

bool MemoryAllocator::isAllocFreed(const SlabReleaseContext& ctx,
                                   void* memory) const {
  const auto& pool = memoryPoolManager_.getPoolById(ctx.getPoolId());
  const auto& ac = pool.getAllocationClass(ctx.getClassId());
  return ac.isAllocFreed(ctx, memory);
}

bool MemoryAllocator::allAllocsFreed(const SlabReleaseContext& ctx) const {
  const auto& pool = memoryPoolManager_.getPoolById(ctx.getPoolId());
  const auto& ac = pool.getAllocationClass(ctx.getClassId());
  return ac.allFreed(ctx.getSlab());
}

void MemoryAllocator::processAllocForRelease(
    const SlabReleaseContext& ctx,
    void* memory,
    const std::function<void(void*)>& callback) const {
  const auto& pool = memoryPoolManager_.getPoolById(ctx.getPoolId());
  const auto& ac = pool.getAllocationClass(ctx.getClassId());
  return ac.processAllocForRelease(ctx, memory, callback);
}

void MemoryAllocator::completeSlabRelease(const SlabReleaseContext& context) {
  auto pid = context.getPoolId();
  auto& pool = memoryPoolManager_.getPoolById(pid);
  pool.completeSlabRelease(context);
}

void MemoryAllocator::abortSlabRelease(const SlabReleaseContext& context) {
  auto pid = context.getPoolId();
  auto& pool = memoryPoolManager_.getPoolById(pid);
  pool.abortSlabRelease(context);
}
std::set<uint32_t> MemoryAllocator::generateAllocSizes(
    double factor,
    uint32_t maxSize,
    uint32_t minSize,
    bool reduceFragmentation) {
  if (maxSize > Slab::kSize) {
    throw std::invalid_argument(
        fmt::format("maximum alloc size {} is more than the slab size {}",
                       maxSize, Slab::kSize));
  }

  if (factor <= 1.0) {
    throw std::invalid_argument(fmt::format("invalid factor {}", factor));
  }

  // Returns the next chunk size. Uses the previous size and factor to select a
  // size that increases the number of chunks per slab by at least one to reduce
  // slab wastage. Also increases the chunk size to the maximum that maintains
  // the same number of chunks per slab (for example: if slabs are 1 MB then a
  // chunk size of 300 KB can be upgraded to 333 KB while maintaining 3 chunks
  // per 1 MB slab).
  auto nextSize = [=](uint32_t prevSize, double incFactor) {
    // Increment by incFactor until we have a new number of chunks per slab.
    uint32_t newSize = prevSize;
    do {
      auto tmpPrevSize = newSize;
      newSize = util::getAlignedSize(static_cast<uint32_t>(newSize * incFactor),
                                     kAlignment);
      if (newSize == tmpPrevSize) {
        throw std::invalid_argument(
            fmt::format("invalid incFactor {}", incFactor));
      }

      if (newSize > Slab::kSize) {
        return newSize;
      }
    } while (Slab::kSize / newSize == Slab::kSize / prevSize);
    // Now make sure we're selecting the maximum chunk size while maintaining
    // the number of chunks per slab.
    const uint32_t perSlab = static_cast<uint32_t>(Slab::kSize) / newSize;
    XDCHECK_GT(perSlab, 0ULL);
    const uint32_t maxChunkSize = static_cast<uint32_t>(Slab::kSize) / perSlab;
    // Align down to maintain perslab
    newSize = maxChunkSize - maxChunkSize % kAlignment;
    XDCHECK_EQ(newSize % kAlignment, 0ULL);
    XDCHECK_EQ(static_cast<uint32_t>(Slab::kSize) / newSize, perSlab);
    return newSize;
  };

  std::set<uint32_t> allocSizes;

  auto size = minSize;
  while (size < maxSize) {
    const auto nPerSlab = Slab::kSize / size;
    // if we can not make more than one alloc per slab, we just default to the
    // max alloc size.
    if (nPerSlab <= 1) {
      break;
    }
    allocSizes.insert(size);

    if (reduceFragmentation) {
      size = nextSize(size, factor);
    } else {
      auto prevSize = size;
      size = util::getAlignedSize(static_cast<uint32_t>(size * factor),
                                  kAlignment);
      if (prevSize == size) {
        throw std::invalid_argument(
            fmt::format("invalid incFactor {}", factor));
      }
    }
    if (allocSizes.size() + 1 >= kMaxClasses)
      break;
  }

  allocSizes.insert(util::getAlignedSize(maxSize, kAlignment));
  return allocSizes;
}
