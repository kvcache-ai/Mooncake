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

#include "MemoryPoolManager.h"

using namespace facebook::cachelib;

constexpr unsigned int MemoryPoolManager::kMaxPools;

MemoryPoolManager::MemoryPoolManager(SlabAllocator& slabAlloc)
    : slabAlloc_(slabAlloc) {}

size_t MemoryPoolManager::getRemainingSizeLocked() const noexcept {
  const size_t totalSize =
      slabAlloc_.getNumUsableSlabs() * Slab::kSize;
  // check if there is enough space left.
  size_t sum = 0;
  for (PoolId id = 0; id < nextPoolId_; id++) {
    sum += pools_[id]->getPoolSize();
  }
  XDCHECK_LE(sum, totalSize);
  return totalSize - sum;
}

PoolId MemoryPoolManager::createNewPool(std::string name,
                                        size_t poolSize,
                                        const std::set<uint32_t>& allocSizes) {
  std::unique_lock l(lock_);
  if (poolsByName_.find(name) != poolsByName_.end()) {
    throw std::invalid_argument("Duplicate pool");
  }

  if (nextPoolId_ == kMaxPools) {
    throw std::logic_error("All pools exhausted");
  }

  const size_t remaining = getRemainingSizeLocked();
  if (remaining < poolSize) {
    // not enough memory to create a new pool.
    throw std::invalid_argument(fmt::format(
        "Not enough memory ({} bytes) to create a new pool of size {} bytes",
        remaining,
        poolSize));
  }

  const PoolId id = nextPoolId_;
  pools_[id] =
      std::make_unique<MemoryPool>(id, poolSize, slabAlloc_, allocSizes);
  poolsByName_.insert({name, id});
  nextPoolId_++;
  return id;
}

MemoryPool& MemoryPoolManager::getPoolByName(const std::string& name) const {
  std::shared_lock l(lock_);
  auto it = poolsByName_.find(name);
  if (it == poolsByName_.end()) {
    throw std::invalid_argument(fmt::format("Invalid pool name {}", name));
  }

  auto poolId = it->second;
  XDCHECK_LT(poolId, nextPoolId_.load());
  XDCHECK_GE(poolId, 0);
  XDCHECK(pools_[poolId] != nullptr);
  return *pools_[poolId];
}

MemoryPool& MemoryPoolManager::getPoolById(PoolId id) const {
  // does not need to grab the lock since nextPoolId_ is atomic and we always
  // bump it up after setting everything up..
  if (id < nextPoolId_ && id >= 0) {
    XDCHECK(pools_[id] != nullptr);
    return *pools_[id];
  }
  throw std::invalid_argument(fmt::format("Invalid pool id {}", id));
}

const std::string& MemoryPoolManager::getPoolNameById(PoolId id) const {
  std::shared_lock l(lock_);
  for (const auto& pair : poolsByName_) {
    if (pair.second == id) {
      return pair.first;
    }
  }
  throw std::invalid_argument(fmt::format("Invali pool id {}", id));
}

std::set<PoolId> MemoryPoolManager::getPoolIds() const {
  std::set<PoolId> ret;
  for (PoolId id = 0; id < nextPoolId_; ++id) {
    ret.insert(id);
  }
  return ret;
}

bool MemoryPoolManager::resizePools(PoolId src, PoolId dest, size_t bytes) {
  auto& srcPool = getPoolById(src);
  auto& destPool = getPoolById(dest);

  std::unique_lock l(lock_);
  if (srcPool.getPoolSize() < bytes) {
    return false;
  }

  // move the memory.
  srcPool.resize(srcPool.getPoolSize() - bytes);
  destPool.resize(destPool.getPoolSize() + bytes);
  return true;
}

bool MemoryPoolManager::shrinkPool(PoolId pid, size_t bytes) {
  auto& pool = getPoolById(pid);

  std::unique_lock l(lock_);
  if (pool.getPoolSize() < bytes) {
    return false;
  }
  pool.resize(pool.getPoolSize() - bytes);
  return true;
}

bool MemoryPoolManager::growPool(PoolId pid, size_t bytes) {
  auto& pool = getPoolById(pid);

  std::unique_lock l(lock_);
  const auto remaining = getRemainingSizeLocked();
  if (remaining < bytes) {
    return false;
  }

  pool.resize(pool.getPoolSize() + bytes);
  return true;
}

std::set<PoolId> MemoryPoolManager::getPoolsOverLimit() const {
  std::set<PoolId> res;
  std::shared_lock l(lock_);
  for (const auto& kv : poolsByName_) {
    const auto poolId = kv.second;
    const auto& pool = getPoolById(poolId);
    if (pool.overLimit()) {
      res.insert(poolId);
    }
  }
  return res;
}
