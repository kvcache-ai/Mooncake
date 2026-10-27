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

#include <folly/Likely.h>
#include <folly/logging/xlog.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <type_traits>
#include <vector>

#include "common/CompilerUtils.h"

namespace facebook {
namespace cachelib {

/**
 * A Slab is a contiguous Slab::kSize bytes of memory. An allocated slab can
 * belong to one unique <memory pool, allocation class> pair, and used to
 * carve out allocations corresponding to the pool and class size. Hence the
 * following are true:
 *
 * 1. All carved allocations in a slab are of fixed size depending on the
 * allocation class  it belongs to.
 * 2. All carved allocations in a slab belong to a single memory pool.
 *
 * This is to ensure that when we have to relinquish memory we have the
 * granularity of allocation class or memory pools by picking an individual
 * slab.
 *
 * The info about which memory pool and allocation class a slab is being used
 * for is stored in the slab header. The slab header is not located within the
 * slab, but is managed and the mapping of Slab to its header is maintained
 * independently by the SlabAllocator.
 */

// identifier for the memory tier
using TierId = int8_t;
// identifier for the memory pool
using PoolId = int8_t;
// identifier for the allocation class
using ClassId = int8_t;
// slab release abort function to determine if slab release should be aborted
using SlabReleaseAbortFn = std::function<bool(void)>;

struct AllocInfo {
    const PoolId poolId;
    const ClassId classId;
    // the allocation size configured for this PoolId, ClassId pair.
    const size_t allocSize;
};

// slabs that are aligned by kSize.
class CACHELIB_PACKED_ATTR Slab {
   public:
    // used to represent the fact that the slab does not belong to any
    // AllocationClass
    static constexpr ClassId kInvalidClassId = -1;

    // used to represent the fact that the slab does not belong to any
    // MemoryPool
    static constexpr PoolId kInvalidPoolId = -1;

    // size of the slab in bytes.
    static constexpr unsigned int kNumSlabBits = 24;

    // minimum of 64 byte allocations.
    static constexpr unsigned int kMinAllocPower = 6;
    static constexpr size_t kMinAllocSize = 1 << kMinAllocPower;

    static constexpr size_t kSize = 1 << kNumSlabBits;

    // returns pointer to the memory at the offset inside the slab memory.
    char* memoryAtOffset(size_t offset) const noexcept {
        XDCHECK_LT(offset, Slab::kSize);
        return dataStart() + offset;
    }

   private:
    // returns the pointer to the start of the slab memory.
    char* dataStart() const noexcept { return &data_[0]; }

    // available memory in this slab.
    mutable char data_[kSize];
};

static_assert(std::is_standard_layout<Slab>::value,
              "Slab is not standard layout");

enum class SlabHeaderFlag : uint8_t {
    IS_MARKED_FOR_RELEASE = 0,
    IS_ADVISED = 1,
    SH_FLAG_2 = 2,
    SH_FLAG_3 = 3,
    SH_FLAG_4 = 4,
    SH_FLAG_5 = 5,
    SH_FLAG_6 = 6,
    SH_FLAG_7 = 7
};

// one per slab. This is not colocated with the slab. But could be if there is
// trailing space based on the slab's allocation size.
struct CACHELIB_PACKED_ATTR SlabHeader {
    constexpr SlabHeader() noexcept = default;
    explicit SlabHeader(PoolId pid) : poolId(pid) {}
    SlabHeader(PoolId pid, ClassId cid, uint32_t size)
        : poolId(pid), classId(cid), allocSize(size) {}

    // This doesn't reset the flags. That's done explicitly by calling
    // setFlag/unsetFlag above.
    void resetAllocInfo() {
        poolId = Slab::kInvalidPoolId;
        classId = Slab::kInvalidClassId;
        allocSize = 0;
    }

    bool isAdvised() const noexcept {
        return isFlagSet(SlabHeaderFlag::IS_ADVISED);
    }

    void setAdvised(bool value) {
        value ? setFlag(SlabHeaderFlag::IS_ADVISED)
              : unSetFlag(SlabHeaderFlag::IS_ADVISED);
    }

    bool isMarkedForRelease() const noexcept {
        return isFlagSet(SlabHeaderFlag::IS_MARKED_FOR_RELEASE);
    }

    void setMarkedForRelease(bool value) {
        value ? setFlag(SlabHeaderFlag::IS_MARKED_FOR_RELEASE)
              : unSetFlag(SlabHeaderFlag::IS_MARKED_FOR_RELEASE);
    }

    // id of the pool that this slab currently belongs to.
    PoolId poolId{Slab::kInvalidPoolId};

    // the allocation class id that this slab currently belongs to
    ClassId classId{Slab::kInvalidClassId};

    // whether the slab is currently being released or not.
    uint8_t flags{0};

    // the allocation size of the allocation class. Useful for pointer
    // compression. the current size of this struct is 1 + 1 + 1 + 4 = 7 bytes.
    // This allocSize is accessed on every decompression of the
    // compressed pointer. If the offset of this changes, use the benchmark to
    // figure out if it moves the needle by a big margin.
    uint32_t allocSize{0};

   private:
    void setFlag(SlabHeaderFlag flag) noexcept {
        const uint8_t bitmask =
            static_cast<uint8_t>(1u << static_cast<unsigned int>(flag));
        // FIXME:
        // https://fb.workplace.com/groups/cachelibusers/posts/2345418462311949/
        // #pragma clang diagnostic push
        // #pragma clang diagnostic ignored "-Watomic-implicit-seq-cst"
        __sync_or_and_fetch(&flags, bitmask);
        // #pragma clang diagnostic pop
    }

    void unSetFlag(SlabHeaderFlag flag) noexcept {
        const uint8_t bitmask =
            static_cast<uint8_t>(std::numeric_limits<uint8_t>::max() -
                                 (1u << static_cast<unsigned int>(flag)));
        __sync_fetch_and_and(&flags, bitmask);
    }

    bool isFlagSet(SlabHeaderFlag flag) const noexcept {
        return flags & (1u << static_cast<unsigned int>(flag));
    }
};

// Definition for slab based resizing and rebalancing.
enum class SlabReleaseMode {
    kResize,     // Resize the pool
    kRebalance,  // Rebalance away a slab from one pool to another
};

// Used to denote store the context for releasing a slab.  This is created
// using a startSlabRelease call and needs to be passed on to the
// completeSlabRelease call to finalize the slab release process if the
// context is in a state where the slab is not released(isReleased())
class SlabReleaseContext {
   public:
    // non copyable
    SlabReleaseContext(const SlabReleaseContext&) = delete;
    SlabReleaseContext& operator=(const SlabReleaseContext&) = delete;

    // movable
    SlabReleaseContext(SlabReleaseContext&&) = default;
    SlabReleaseContext& operator=(SlabReleaseContext&&) = delete;

    // create a context where the slab is already released.
    SlabReleaseContext(const Slab* slab, PoolId pid, ClassId cid,
                       SlabReleaseMode m)
        : SlabReleaseContext(slab, pid, cid, {}, m) {}

    // create a context where the slab needs the user to free up some active
    // allocations for slab release.
    SlabReleaseContext(const Slab* slab, PoolId pid, ClassId cid,
                       std::vector<void*> allocations, SlabReleaseMode m)
        : slab_(slab),
          pid_(pid),
          victim_(cid),
          activeAllocations_(std::move(allocations)),
          mode_(m) {}

    // create a context where the slab is already released.
    //
    // also specify the receiver to receive the slab
    SlabReleaseContext(const Slab* slab, PoolId pid, ClassId victim,
                       ClassId receiver)
        : SlabReleaseContext(slab, pid, victim, {}, receiver) {}

    // create a context where the slab needs the user to free up some active
    // allocations for slab release.
    //
    // also specify the receiver to receive the slab.
    SlabReleaseContext(const Slab* slab, PoolId pid, ClassId victim,
                       std::vector<void*> allocations, ClassId receiver)
        : slab_(slab),
          pid_(pid),
          victim_(victim),
          activeAllocations_(std::move(allocations)),
          receiver_(receiver),
          mode_(SlabReleaseMode::kRebalance) {}

    // @return  true if the slab has already been released and there are no
    // active allocations to be freed.
    bool isReleased() const noexcept { return activeAllocations_.empty(); }

    // @return  true if the slab release context specifies a receiver to receive
    // the released slab
    bool hasValidReceiver() const noexcept {
        return receiver_ != Slab::kInvalidClassId;
    }

    PoolId getPoolId() const noexcept { return pid_; }
    ClassId getClassId() const noexcept { return victim_; }

    ClassId getReceiverClassId() const noexcept { return receiver_; }

    // @return  returns a list of active allocations. If the vector is empty
    //          it means no active allocations are associated with this slab.
    const std::vector<void*>& getActiveAllocations() const noexcept {
        return activeAllocations_;
    }

    // @return  pointer to slab marked for release
    const Slab* getSlab() const noexcept { return slab_; }

    // @return the mode for this slab release context.
    SlabReleaseMode getMode() const noexcept { return mode_; }

   private:
    // Slab about to be released.
    const Slab* const slab_;

    // the pool and the class id of the slab. If the slab is already released,
    // the classId is invalid.
    const PoolId pid_;
    const ClassId victim_;

    // Active allocations in this slab. Non-zero for a slab that is marked for
    // release.
    const std::vector<void*> activeAllocations_;

    // Optional receiver that will receive the slab being released
    ClassId receiver_{Slab::kInvalidClassId};

    // the mode for this slab release.
    const SlabReleaseMode mode_;

    void setReceiver(ClassId receiver) noexcept { receiver_ = receiver; }

    friend class MemoryPool;
};
}  // namespace cachelib
}  // namespace facebook
