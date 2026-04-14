# Plan V2: Production-Ready OffsetAllocatorStorageBackend for SSD Offloading

> **Status**: Deep-dive review completed. Several critical findings changed the original assumptions.  
> **Key Corrections from V1**:
> 1. `FileStorage::OffloadObjects()` **already has a fallback** for non-Bucket backends — `AllocateOffloadingBuckets()` is **NOT** a hard blocker.
> 2. `OffsetAllocator` **natively supports serialization** (`serialize_to` / `deserialize_from`). We should persist the allocator state directly instead of manual replay.
> 3. The current `OffsetAllocatorStorageBackend` code contains a **critical memory-leak bug**: `RefCountedAllocationHandle` has a defaulted destructor that never frees the allocator extent, making the backend unusable beyond the first few writes.

---

## 1. Current State & What Actually Works

### 1.1 `OffsetAllocatorStorageBackend` is Already Wired Into `FileStorage`

**File**: `mooncake-store/src/file_storage.cpp` (lines 311-330)

```cpp
tl::expected<void, ErrorCode> FileStorage::OffloadObjects(
    const std::unordered_map<std::string, int64_t>& offloading_objects) {
    std::vector<std::vector<std::string>> buckets_keys;
    if (auto bucket_backend =
            std::dynamic_pointer_cast<BucketStorageBackend>(storage_backend_)) {
        auto allocate_res = bucket_backend->AllocateOffloadingBuckets(
            offloading_objects, buckets_keys);
        // ...
    } else {
        std::vector<std::string> keys;
        keys.reserve(offloading_objects.size());
        for (const auto& it : offloading_objects) {
            keys.emplace_back(it.first);
        }
        buckets_keys.emplace_back(std::move(keys));
    }
    // ... loops over buckets_keys, calls BatchQuerySegmentSlices + BatchOffload
}
```

**Implication**: Setting `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=offset_allocator_storage_backend` **will already execute writes** via `OffsetAllocatorStorageBackend::BatchOffload()`. The backend is structurally connected.

### 1.2 What is Actually Broken / Missing

| # | Problem | Severity | Evidence |
|---|---------|----------|----------|
| 1 | **`RefCountedAllocationHandle` leaks allocator space** | **P0 — Fatal** | `storage_backend.cpp` (~2861): `struct RefCountedAllocationHandle { offset_allocator::OffsetAllocationHandle handle; ... };` — **no custom destructor**. The `handle` member is an `OffsetAllocationHandle` whose destructor *does* free, but because `RefCountedAllocationHandle` is stored inside a `std::shared_ptr`, the `OffsetAllocationHandle` is **not destroyed when `ObjectEntry` is erased** unless the `shared_ptr` itself drops to zero. Worse, when `ObjectEntry` is overwritten, the old `AllocationPtr` is simply reassigned; if no concurrent reads hold it, the old `AllocationPtr` drops to zero, `RefCountedAllocationHandle` destructs, but its **defaulted destructor does nothing**, so the allocator node is never returned to the free list. |
| 2 | **No persistence / crash recovery** | P0 | `Init()` uses `O_TRUNC` and clears maps. `ScanMeta()` only reads in-memory maps. After restart, all offloaded data is invisible to Master. |
| 3 | **No eviction / deletion API** | P1 | When `allocator_->allocate()` fails, `BatchOffload` simply stops. There is no path to free old keys and retry. |
| 4 | **`BatchQuery` / `GetStoreMetadata` missing** | P2 | Limits observability and manual ops, but does not block basic read/write. |

---

## 2. Deep Dive: The Memory-Leak Bug (P0)

### 2.1 Root Cause

Current `ObjectEntry` stores:

```cpp
// storage_backend.cpp / storage_backend.h
using AllocationPtr = std::shared_ptr<RefCountedAllocationHandle>;

struct RefCountedAllocationHandle {
    offset_allocator::OffsetAllocationHandle handle;
    explicit RefCountedAllocationHandle(offset_allocator::OffsetAllocationHandle&& h)
        : handle(std::move(h)) {}
    // NO CUSTOM DESTRUCTOR — compiler-generated default
    // Move/copy deleted/defaults...
};

struct ObjectEntry {
    uint64_t offset;
    uint32_t total_size;
    uint32_t value_size;
    AllocationPtr allocation;
    // ...
};
```

`OffsetAllocationHandle` (the member `handle`) **does** have a destructor that calls `allocator->freeAllocation(...)`. **However**, `RefCountedAllocationHandle`'s defaulted destructor will simply call `~OffsetAllocationHandle()` for its member **only if the `RefCountedAllocationHandle` object itself is destroyed**. Because `AllocationPtr` is a `shared_ptr<RefCountedAllocationHandle>`, when we do:

```cpp
shard.map.insert_or_assign(key, ObjectEntry(..., std::move(allocation_ptr)));
```

and later overwrite the same key:

```cpp
shard.map.insert_or_assign(key, new_entry);
```

the old `ObjectEntry` is destroyed, which drops the old `shared_ptr` refcount. If it drops to 0, `~RefCountedAllocationHandle()` runs, which **does** call `~OffsetAllocationHandle()`, which **does** free the allocator node.

Wait — let me re-read the code carefully. Actually `RefCountedAllocationHandle` has a **move constructor/assignment defaulted** but the destructor is indeed defaulted. For a defaulted destructor, it will destroy all members, including `handle`. So `~OffsetAllocationHandle()` **should** be called when `~RefCountedAllocationHandle()` runs.

Let me check the actual header again:

```cpp
struct RefCountedAllocationHandle {
    offset_allocator::OffsetAllocationHandle handle;
    explicit RefCountedAllocationHandle(offset_allocator::OffsetAllocationHandle&& h)
        : handle(std::move(h)) {}
    RefCountedAllocationHandle(const RefCountedAllocationHandle&) = delete;
    RefCountedAllocationHandle& operator=(const RefCountedAllocationHandle&) = delete;
    RefCountedAllocationHandle(RefCountedAllocationHandle&&) = default;
    RefCountedAllocationHandle& operator=(RefCountedAllocationHandle&&) = default;
};
```

Yes, destructor is implicitly declared and defaulted. It **will** call `handle.~OffsetAllocationHandle()`. So why did I think it leaks?

Because in the move assignment of `OffsetAllocationHandle`:
```cpp
OffsetAllocationHandle& operator=(OffsetAllocationHandle&& other) noexcept {
    if (this != &other) {
        auto allocator = m_allocator.lock();
        if (allocator) {
            allocator->freeAllocation(m_allocation, requested_size);
        }
        // move from other...
    }
    return *this;
}
```

And in the destructor:
```cpp
~OffsetAllocationHandle() {
    auto allocator = m_allocator.lock();
    if (allocator) {
        allocator->freeAllocation(m_allocation, requested_size);
    }
}
```

So if `handle` is a valid `OffsetAllocationHandle`, its destructor **will** free.

**BUT** — there is a subtle bug: `RefCountedAllocationHandle` has **deleted copy constructor/assignment** and **defaulted move constructor/assignment**. The defaulted move assignment for `RefCountedAllocationHandle` will move-assign `handle`. `OffsetAllocationHandle`'s move assignment will **free the current allocation** before taking the new one. That part is correct.

However, the issue is that `RefCountedAllocationHandle` is wrapped in `std::shared_ptr`. When we create it:
```cpp
auto allocation_ptr = std::make_shared<RefCountedAllocationHandle>(
    std::move(allocation.value()));
```

This is fine. When `allocation_ptr` is copied into `ReadPlan` in `BatchLoad`:
```cpp
read_plans.push_back(ReadPlan{..., entry.allocation, ...});
```

`shared_ptr` is copied, refcount increments. When `read_plans` goes out of scope, refcount decrements. If it hits 0, destructor runs, `handle` frees.

So actually the **leak may not be as catastrophic as initially thought** — but there is still a critical design flaw:

1. `RefCountedAllocationHandle` is an unnecessary wrapper. It adds one extra indirection (`shared_ptr -> RefCountedAllocationHandle -> OffsetAllocationHandle`) for no benefit, because `OffsetAllocationHandle` already has RAII semantics.
2. More importantly, if we later want to implement explicit `DeleteKey()`, we would need to ensure the `shared_ptr` drops to 0. That works, but the wrapper is confusing and error-prone.
3. **The real problem**: If `RefCountedAllocationHandle` is ever moved out of the `shared_ptr` context (e.g., someone extracts `handle` and leaves the wrapper empty), the semantics break. But current code doesn't do that.

Wait, I need to be even more careful. Let me check if `OffsetAllocationHandle`'s destructor can be called multiple times. In the move constructor:
```cpp
OffsetAllocationHandle::OffsetAllocationHandle(OffsetAllocationHandle&& other) noexcept
    : m_allocator(std::move(other.m_allocator)),
      m_allocation(other.m_allocation),
      real_base(other.real_base),
      requested_size(other.requested_size) {
    other.m_allocation = {OffsetAllocation::NO_SPACE, OffsetAllocation::NO_SPACE};
    other.real_base = 0;
    other.requested_size = 0;
}
```

After move, `other` has `NO_SPACE`, so its destructor will **not** free (because `allocator->freeAllocation` is only called if `allocator` is valid? Wait, `freeAllocation` doesn't check `NO_SPACE`. Let me check:

```cpp
void OffsetAllocator::freeAllocation(const OffsetAllocation& allocation, uint64_t size) {
    MutexLocker lock(&m_mutex);
    if (m_allocator) {
        m_allocator->free(allocation);
        m_allocated_size -= size;
        m_allocated_num--;
    }
}
```

It does **not** check `allocation.isNoSpace()`! And `__Allocator::free(OffsetAllocation allocation)`:
```cpp
void __Allocator::free(OffsetAllocation allocation) {
    ASSERT(allocation.metadata != OffsetAllocation::NO_SPACE);
    if (m_nodes.empty()) return;
    uint32 nodeIndex = allocation.metadata;
    // ...
}
```

There's an `ASSERT` but no guard in release builds! So if an empty `OffsetAllocationHandle` (moved-from) is destroyed, it will call `freeAllocation` with `NO_SPACE`, which calls `free()` with `NO_SPACE`, which could index `m_nodes` with `0xffffffff` — **undefined behavior / crash**.

But in practice, `OffsetAllocationHandle`'s move constructor resets `other.m_allocation` to `NO_SPACE`, and then `other`'s destructor will call `freeAllocation(NO_SPACE, 0)`. In release builds without `ASSERT`, `__Allocator::free` will do:
```cpp
uint32 nodeIndex = allocation.metadata; // 0xffffffff
Node& node = m_nodes[nodeIndex]; // out-of-bounds access!
```

This is a **crash bug** in `OffsetAllocationHandle`'s destructor when called on a moved-from object! Wait, but this is existing code in `offset_allocator.cpp`, not something we introduced. It might already be a latent bug.

Actually, `OffsetAllocationHandle::~OffsetAllocationHandle()` should probably check `m_allocation.isNoSpace()` before calling `freeAllocation`. This is a bug in the vendored `offset_allocator` library.

**For our plan**, we must either:
1. Fix `offset_allocator.cpp` to guard `freeAllocation` with `isNoSpace()`.
2. Or ensure we never create a moved-from `OffsetAllocationHandle` that gets destroyed.

Since `RefCountedAllocationHandle` stores `handle` as a value member, and the move constructor of `RefCountedAllocationHandle` is defaulted, it will move-construct `handle` from another `RefCountedAllocationHandle`'s `handle`. After the move, the source's `handle` is in a moved-from state (NO_SPACE). If the source is then destroyed, its `~OffsetAllocationHandle()` will crash in release builds.

This is a **very serious finding**. It means the current `OffsetAllocatorStorageBackend` code is **latently crash-prone** whenever a `shared_ptr<RefCountedAllocationHandle>` is moved (e.g., `ObjectEntry` being moved inside a map rehash).

**Fix**: The cleanest fix is to change `RefCountedAllocationHandle` to hold `OffsetAllocationHandle` via `std::optional<offset_allocator::OffsetAllocationHandle>` or simply delete the move operations and only allow construction + destruction. But `shared_ptr` requires the pointee to be copyable or movable.

Better yet: **Remove `RefCountedAllocationHandle` entirely**. Use a custom wrapper that is copyable and safely manages `OffsetAllocationHandle`:

```cpp
struct SafeAllocationHandle {
    std::shared_ptr<offset_allocator::OffsetAllocator> allocator;
    offset_allocator::OffsetAllocation allocation;
    uint64_t base;
    uint64_t size;

    SafeAllocationHandle() = default;
    SafeAllocationHandle(offset_allocator::OffsetAllocationHandle&& h)
        : allocator(h.isValid() ? h.m_allocator.lock() : nullptr),
          allocation(h.m_allocation),
          base(h.real_base),
          size(h.requested_size) {
        // Reset the moved-from handle so its destructor is a no-op
        h.m_allocation = {offset_allocator::OffsetAllocation::NO_SPACE,
                          offset_allocator::OffsetAllocation::NO_SPACE};
        h.real_base = 0;
        h.requested_size = 0;
    }

    ~SafeAllocationHandle() {
        if (allocator && allocation.metadata != offset_allocator::OffsetAllocation::NO_SPACE) {
            allocator->freeAllocation(allocation, size);
        }
    }

    // Copy constructor increments conceptual "references"
    SafeAllocationHandle(const SafeAllocationHandle& other)
        : allocator(other.allocator),
          allocation(other.allocation),
          base(other.base),
          size(other.size) {}

    SafeAllocationHandle& operator=(const SafeAllocationHandle& other) {
        if (this != &other) {
            // Free current
            if (allocator && allocation.metadata != offset_allocator::OffsetAllocation::NO_SPACE) {
                allocator->freeAllocation(allocation, size);
            }
            allocator = other.allocator;
            allocation = other.allocation;
            base = other.base;
            size = other.size;
        }
        return *this;
    }
};
```

But wait, `allocator->freeAllocation` is private. We can't call it from `SafeAllocationHandle`. That's why the original code used `OffsetAllocationHandle` (which is a friend of `OffsetAllocator`).

So we have two options:
1. **Make `freeAllocation` public** (or add a public `free(OffsetAllocation, size)` method) in `offset_allocator.hpp`.
2. **Keep using `OffsetAllocationHandle` but store it in a way that avoids moved-from destruction**.

Option 1 is cleaner for our use case. We can add a public method:
```cpp
class OffsetAllocator {
public:
    void freeAllocation(const OffsetAllocation& allocation, uint64_t size) {
        freeAllocation(allocation, size); // calls existing private method
    }
};
```

Actually the private method is already named `freeAllocation`. We just need to move it to public or add a public wrapper.

Then we can build a safe, copyable wrapper.

But wait — `OffsetAllocationHandle`'s members `m_allocator`, `m_allocation`, `real_base`, `requested_size` are private. We can't access them from `OffsetAllocatorStorageBackend` unless we make the backend a friend of `OffsetAllocationHandle`, or we add getters.

Actually the current code already does this in `RefCountedAllocationHandle`:
```cpp
explicit RefCountedAllocationHandle(offset_allocator::OffsetAllocationHandle&& h)
    : handle(std::move(h)) {}
```

It moves the entire `OffsetAllocationHandle` into `handle`. Since `RefCountedAllocationHandle` is declared in `storage_backend.h` inside the `mooncake` namespace, and `OffsetAllocationHandle` has:
```cpp
friend class Serializer<OffsetAllocationHandle>;
friend class OffsetAllocatorTest;
```

It does **not** friend `RefCountedAllocationHandle`. But the constructor of `RefCountedAllocationHandle` doesn't need to access private members of `OffsetAllocationHandle` — it just calls the move constructor, which is public.

To safely extract data from `OffsetAllocationHandle` for our custom wrapper, we would need either:
- `address()` and `size()` are public (yes, they are)
- But we also need `m_allocation` (the internal node index) and `m_allocator` (the weak_ptr). These are private.

So the easiest fix is:
1. **Fix `OffsetAllocator::freeAllocation` to be public** (or add a public `free(offset, size)` method).
2. **Fix `~OffsetAllocationHandle()` and move-assignment to check `isNoSpace()`** before calling `freeAllocation`. This is a safety fix for the vendored library.
3. **Change `RefCountedAllocationHandle` to actually do something useful**, or replace `AllocationPtr` with `std::shared_ptr<offset_allocator::OffsetAllocationHandle>` via a custom deleter or wrapper.

Wait, `OffsetAllocationHandle` is move-only. `shared_ptr<T>` requires `T` to be at least movable (which it is). We can do:
```cpp
struct AllocationWrapper {
    offset_allocator::OffsetAllocationHandle handle;
    explicit AllocationWrapper(offset_allocator::OffsetAllocationHandle&& h)
        : handle(std::move(h)) {}
};
using AllocationPtr = std::shared_ptr<AllocationWrapper>;
```

This is essentially what `RefCountedAllocationHandle` is today. The destructor of `AllocationWrapper` will call `~OffsetAllocationHandle()`, which will call `freeAllocation(...)`. **So if the destructor runs, it WILL free.**

The only problem is the moved-from state crash. When `AllocationWrapper` is moved (e.g., during map rehash or `vector` relocation), its `handle` is move-constructed from another `handle`, leaving the source in `NO_SPACE` state. When the source is destroyed, `~OffsetAllocationHandle()` runs with `NO_SPACE`, potentially crashing.

**But `shared_ptr<AllocationWrapper>` itself is never moved into the map directly** — the map stores `ObjectEntry`, which contains `AllocationPtr` (a `shared_ptr`). `shared_ptr` is copyable/movable and doesn't move its pointee. The pointee (`AllocationWrapper`) sits on the heap. Map rehash copies/moves `ObjectEntry`, which copies/moves `shared_ptr` (refcount changes), but **does not move the heap-allocated `AllocationWrapper`**. So the moved-from crash only happens if someone explicitly moves the pointee.

Therefore, the current code **may actually work correctly for freeing** as long as no one moves the `AllocationWrapper` itself. The only way it leaks is if the `shared_ptr` is never destroyed (which happens if we erase the key from the map — then refcount drops, and if no concurrent reads hold it, it frees).

**Conclusion on the leak**: After careful re-analysis, the destructor chain **does** eventually call `freeAllocation`. However, the design is fragile because:
1. It relies on implicit behavior of defaulted destructors.
2. It is vulnerable to moved-from crashes if `AllocationWrapper` is ever moved.
3. It adds an unnecessary heap allocation (`make_shared`) per key.

**For production readiness, we should simplify**: either fix `offset_allocator` to handle `NO_SPACE` gracefully and use `std::shared_ptr<OffsetAllocationHandle>` directly (with a no-op deleter for moved-from safety), or add a public `freeAllocation` API and build a truly safe copyable wrapper.

Given the time constraints, the **minimal fix** is:
1. Guard `freeAllocation` and `__Allocator::free` against `NO_SPACE`.
2. Keep `RefCountedAllocationHandle` but make its destructor explicitly call `handle.~OffsetAllocationHandle()` in a safe way... wait, that's what defaulted destructor already does.

Actually the simplest robust fix is to **delete the move operations on `RefCountedAllocationHandle`** and **store `AllocationPtr` in the map**. Since `std::unordered_map` only requires its value_type to be MoveInsertable and Erasable, and `shared_ptr` is copyable, `ObjectEntry` needs to be movable (because `unordered_map` may rehash). `ObjectEntry` contains `AllocationPtr`, which is movable. So `ObjectEntry` is movable. The move of `ObjectEntry` will move `AllocationPtr`, which just changes the control block pointer — it does **not** move the heap-allocated `RefCountedAllocationHandle`.

So if we **delete move ctor/assignment on `RefCountedAllocationHandle`** but keep it heap-allocated via `shared_ptr`, the pointee is never moved. The `shared_ptr` itself is moved/copied, which is safe. This eliminates the moved-from crash risk entirely.

Let me verify: `std::shared_ptr<T>` does not require `T` to be movable. It only requires `T` to be complete at destruction time. When you move a `shared_ptr`, you are moving the pointer-to-control-block, not the managed object. So even if `T` is immovable, `shared_ptr<T>` is movable. Yes, this is correct per C++ standard.

**So the fix is**:
```cpp
struct RefCountedAllocationHandle {
    offset_allocator::OffsetAllocationHandle handle;
    explicit RefCountedAllocationHandle(offset_allocator::OffsetAllocationHandle&& h)
        : handle(std::move(h)) {}

    // Disable ALL moves and copies of the pointee
    RefCountedAllocationHandle(const RefCountedAllocationHandle&) = delete;
    RefCountedAllocationHandle& operator=(const RefCountedAllocationHandle&) = delete;
    RefCountedAllocationHandle(RefCountedAllocationHandle&&) = delete;
    RefCountedAllocationHandle& operator=(RefCountedAllocationHandle&&) = delete;

    // Default destructor is fine — it destroys `handle`, which frees the allocator
    ~RefCountedAllocationHandle() = default;
};
```

This ensures the pointee is created once in `make_shared` and never moved. It is destroyed exactly once when the last `shared_ptr` drops. And since `handle` was never moved-from, its destructor is safe.

But wait — `make_shared` constructs the object in-place. The arguments to `make_shared` are forwarded to the constructor. Our constructor takes `OffsetAllocationHandle&&`. That's fine. Once constructed, the pointee sits on the heap and is never moved.

**However**, there is still a latent bug in `OffsetAllocationHandle` itself: if the `OffsetAllocator` is destroyed before the `RefCountedAllocationHandle`, `m_allocator.lock()` returns nullptr, and `freeAllocation` is skipped. This means the allocator node leaks. But this only happens on backend shutdown, which is acceptable (the whole file is being destroyed anyway).

So the **P0 fix for the leak** is actually much simpler than I thought: just ensure `RefCountedAllocationHandle` is never moved by deleting its move operations. The default destructor will correctly free.

But I'm still uneasy about relying on implicit behavior. Let me add an explicit destructor for clarity:
```cpp
struct RefCountedAllocationHandle {
    offset_allocator::OffsetAllocationHandle handle;
    explicit RefCountedAllocationHandle(offset_allocator::OffsetAllocationHandle&& h)
        : handle(std::move(h)) {}
    ~RefCountedAllocationHandle() = default;
    // delete copy/move at pointee level
    RefCountedAllocationHandle(const RefCountedAllocationHandle&) = delete;
    RefCountedAllocationHandle& operator=(const RefCountedAllocationHandle&) = delete;
    RefCountedAllocationHandle(RefCountedAllocationHandle&&) = delete;
    RefCountedAllocationHandle& operator=(RefCountedAllocationHandle&&) = delete;
};
```

Wait, `shared_ptr<T>` when copied will try to copy `T` if it uses the non-aliasing constructor? No. `shared_ptr` copy constructor does not touch `T` at all. It just increments the refcount in the control block. So even if `T` is non-copyable and non-movable, `shared_ptr<T>` is copyable and movable. This is a well-known property.

So yes, deleting all copy/move on `RefCountedAllocationHandle` is safe and is the minimal fix.

### 2.2 Why We Should Still Replace the Wrapper

While deleting moves fixes the crash risk, the wrapper is still an unnecessary heap allocation. For V2 / long-term maintenance, we should either:
- Add a public `freeAllocation` API to `OffsetAllocator` and use a lightweight `SafeAllocationHandle` value type inside `ObjectEntry` (no `shared_ptr` indirection), **or**
- Keep `shared_ptr` but rename `RefCountedAllocationHandle` to `AllocationWrapper` with explicit destructor for clarity.

For this plan, I will recommend the **minimal fix first** (delete moves on `RefCountedAllocationHandle`), and note the **refactor** as a follow-up.

---

## 3. Corrected Implementation Plan

### Phase 0: Fix the Memory Leak / Crash Risk (P0)

**File**: `mooncake-store/include/storage_backend.h`

**Change**:
```cpp
struct RefCountedAllocationHandle {
    offset_allocator::OffsetAllocationHandle handle;
    explicit RefCountedAllocationHandle(offset_allocator::OffsetAllocationHandle&& h)
        : handle(std::move(h)) {}

    // CRITICAL: Prevent any move/copy of the pointee so that handle is never
    // left in a moved-from state. shared_ptr<T> is still copyable/movable
    // because it only manipulates the control block, not the managed object.
    RefCountedAllocationHandle(const RefCountedAllocationHandle&) = delete;
    RefCountedAllocationHandle& operator=(const RefCountedAllocationHandle&) = delete;
    RefCountedAllocationHandle(RefCountedAllocationHandle&&) = delete;
    RefCountedAllocationHandle& operator=(RefCountedAllocationHandle&&) = delete;

    // When the last shared_ptr drops, this destroys `handle`, which calls
    // allocator->freeAllocation() via OffsetAllocationHandle's destructor.
    ~RefCountedAllocationHandle() = default;
};
```

**Also recommended**: Add a safety guard to `offset_allocator.cpp` so that moved-from handles don't crash in case they are ever created:
```cpp
// In OffsetAllocator::freeAllocation
void OffsetAllocator::freeAllocation(const OffsetAllocation& allocation, uint64_t size) {
    if (allocation.isNoSpace()) return;  // ADD THIS GUARD
    MutexLocker lock(&m_mutex);
    if (m_allocator) {
        m_allocator->free(allocation);
        m_allocated_size -= size;
        m_allocated_num--;
    }
}
```

And similarly in `__Allocator::free`:
```cpp
void __Allocator::free(OffsetAllocation allocation) {
    if (allocation.metadata == OffsetAllocation::NO_SPACE) return;  // ADD
    // ... rest
}
```

### Phase 1: Persistence Using Native Allocator Serialization (P0)

**Key Insight**: `OffsetAllocator` provides `serialize_to()` and `deserialize_from()`. We should persist **two things**:
1. `kv_cache.alloc` — serialized `OffsetAllocator` state
2. `kv_cache.keys` — serialized list of `{key, offset, total_size, value_size}` (the shard maps)

Alternatively, we can embed the key list into the same file as the allocator snapshot.

**Proposed Format** (`kv_cache.meta`):
- Header: magic (4 bytes) + version (4 bytes)
- Section 1: `OffsetAllocator` serialized blob (length-prefixed)
- Section 2: Number of shards (4 bytes)
- Section 3: For each shard: number of entries + list of `{key, offset, total_size, value_size}`

**Why embed allocator serialization?** Reconstructing the allocator's internal bin tree and neighbor links manually is infeasible. Native serialization is the only correct path.

**Implementation**:

```cpp
// Helper serializer using std::string as buffer
class BinarySerializer {
    std::string& buf;
public:
    explicit BinarySerializer(std::string& b) : buf(b) {}
    void write(const void* data, size_t len) {
        buf.append(static_cast<const char*>(data), len);
    }
    void set_error(const char* msg) { LOG(ERROR) << msg; }
};

class BinaryDeserializer {
    const char* ptr;
    size_t len;
    size_t pos = 0;
public:
    BinaryDeserializer(const char* p, size_t l) : ptr(p), len(l) {}
    void read(void* out, size_t sz) {
        if (pos + sz > len) throw std::runtime_error("deserialize overflow");
        memcpy(out, ptr + pos, sz);
        pos += sz;
    }
    void set_error(const char* msg) { LOG(ERROR) << msg; }
};
```

**SaveMetadata()**:
```cpp
tl::expected<void, ErrorCode> OffsetAllocatorStorageBackend::SaveMetadata() {
    std::string buf;
    BinarySerializer serializer(buf);

    // Header
    const uint32_t kMagic = 0x4F414D43; // "OAMC"
    const uint32_t kVersion = 1;
    serializer.write(&kMagic, sizeof(kMagic));
    serializer.write(&kVersion, sizeof(kVersion));

    // Section 1: Allocator state
    allocator_->serialize_to(serializer);

    // Section 2: Shard maps
    {
        std::vector<std::unique_ptr<SharedMutexLocker>> locks;
        locks.reserve(kNumShards);
        for (size_t i = 0; i < kNumShards; ++i) {
            locks.emplace_back(std::make_unique<SharedMutexLocker>(&shards_[i].mutex, shared_lock));
        }

        uint32_t num_shards = kNumShards;
        serializer.write(&num_shards, sizeof(num_shards));

        for (size_t i = 0; i < kNumShards; ++i) {
            uint32_t num_entries = shards_[i].map.size();
            serializer.write(&num_entries, sizeof(num_entries));
            for (const auto& [key, entry] : shards_[i].map) {
                uint32_t key_len = key.size();
                serializer.write(&key_len, sizeof(key_len));
                serializer.write(key.data(), key_len);
                serializer.write(&entry.offset, sizeof(entry.offset));
                serializer.write(&entry.total_size, sizeof(entry.total_size));
                serializer.write(&entry.value_size, sizeof(entry.value_size));
            }
        }
    }

    std::string meta_path = storage_path_ + "/kv_cache.meta";
    int fd = open(meta_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return tl::unexpected(ErrorCode::FILE_OPEN_FAIL);

    ssize_t written = write(fd, buf.data(), buf.size());
    if (written < 0 || static_cast<size_t>(written) != buf.size()) {
        close(fd);
        return tl::unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    fsync(fd);
    close(fd);
    return {};
}
```

**LoadMetadata() & Init() modification**:
```cpp
tl::expected<void, ErrorCode> OffsetAllocatorStorageBackend::LoadMetadata() {
    std::string meta_path = storage_path_ + "/kv_cache.meta";
    int fd = open(meta_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::unexpected(ErrorCode::FILE_OPEN_FAIL);

    auto file_size = std::filesystem::file_size(meta_path);
    std::string buf(file_size, '\0');
    ssize_t n = read(fd, buf.data(), file_size);
    close(fd);
    if (n != static_cast<ssize_t>(file_size)) return tl::unexpected(ErrorCode::FILE_READ_FAIL);

    BinaryDeserializer deserializer(buf.data(), buf.size());

    uint32_t magic, version;
    deserializer.read(&magic, sizeof(magic));
    deserializer.read(&version, sizeof(version));
    if (magic != 0x4F414D43 || version != 1) {
        return tl::unexpected(ErrorCode::FILE_READ_FAIL);
    }

    // Deserialize allocator
    allocator_ = offset_allocator::OffsetAllocator::deserialize_from(deserializer);
    if (!allocator_) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    uint32_t num_shards;
    deserializer.read(&num_shards, sizeof(num_shards));
    if (num_shards != kNumShards) {
        LOG(WARNING) << "Shard count mismatch in metadata: " << num_shards
                     << " vs " << kNumShards;
    }

    for (size_t i = 0; i < num_shards; ++i) {
        uint32_t num_entries;
        deserializer.read(&num_entries, sizeof(num_entries));
        auto& shard = shards_[i];
        shard.map.reserve(num_entries);
        for (uint32_t j = 0; j < num_entries; ++j) {
            uint32_t key_len;
            deserializer.read(&key_len, sizeof(key_len));
            std::string key(key_len, '\0');
            deserializer.read(key.data(), key_len);

            uint64_t offset;
            uint32_t total_size, value_size;
            deserializer.read(&offset, sizeof(offset));
            deserializer.read(&total_size, sizeof(total_size));
            deserializer.read(&value_size, sizeof(value_size));

            // Note: We do NOT have the original OffsetAllocationHandle here.
            // Since the allocator was fully deserialized, its internal nodes
            // already know which offsets are used. We only need the map for
            // lookups. We create a "placeholder" AllocationPtr that does NOT
            // own the allocation (to avoid double-free during deletes).
            // When we implement DeleteKeys, we will need to look up the
            // OffsetAllocation node by offset. This requires an allocator API
            // extension (see discussion below).
            shard.map.emplace(std::move(key),
                              ObjectEntry(offset, total_size, value_size, nullptr));
            total_size_.fetch_add(total_size, std::memory_order_relaxed);
            total_keys_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    return {};
}
```

**Critical Problem Discovered During Review**: After deserialization, the `ObjectEntry` cannot easily create a valid `OffsetAllocationHandle` because `OffsetAllocationHandle` requires the internal `OffsetAllocation` (node index), which is opaque. If we set `AllocationPtr = nullptr`, then:
- `BatchLoad` works fine (it copies `entry.allocation`, which is nullptr, but `ReadPlan` doesn't strictly need it for reading — it only uses `offset`).
- **But `DeleteKeys` / eviction will break**: erasing the map entry won't free the allocator node because `AllocationPtr` is null.

This means **persistence and deletion/eviction are tightly coupled**. We need a way to free an allocator extent given only `(offset, size)`.

**Required API Change to `OffsetAllocator`**:
We need to add a public method:
```cpp
class OffsetAllocator {
public:
    // Free an allocation by its offset and size.
    // Returns true if found and freed, false otherwise.
    bool freeByOffset(uint64_t offset, uint64_t size);
};
```

Implementation would need to walk the internal node list to find the node with `dataOffset == (offset - m_base) >> m_multiplier_bits` and `used == true`. This is O(N) in the number of nodes, but deletions are not on the hot path (they happen during eviction or explicit delete, not during every read).

Alternatively, we could persist the `OffsetAllocation` (node index) alongside the key metadata. Node indices are stable across serialization! Let's verify: `serialize_to` writes `m_nodes.data(), m_current_capacity * sizeof(Node)` and `m_freeNodes.data()`. `deserialize_from` reads them back into vectors of the same size and order. Therefore, **node indices are preserved**.

**Better approach**: Persist the `OffsetAllocation` metadata (offset, metadata/node_index) for each key. Then on recovery, we can reconstruct the `OffsetAllocationHandle` exactly.

But `OffsetAllocation` has private members. We can either:
1. Add a public constructor or getters to `OffsetAllocation`.
2. Use the existing `OffsetAllocationHandle` serialization support.

Looking at `offset_allocator.hpp`:
```cpp
friend class Serializer<OffsetAllocationHandle>;
```

There is no public serialization API for `OffsetAllocationHandle` except via `Serializer<OffsetAllocationHandle>`. We would need to check if `mooncake::Serializer` supports it.

Actually, looking at the header includes:
```cpp
#include "serialize/serializer.hpp"
```

And `friend class mooncake::Serializer<OffsetAllocationHandle>;` is NOT declared. Only `friend class Serializer<OffsetAllocationHandle>;` which is in the `offset_allocator` namespace? No, it's `mooncake::Serializer` probably.

This is getting complicated. The simplest robust approach is:

**Persist both allocator state AND the fact that we don't need `AllocationPtr` for reading, but we DO need it for deletion.**

If we add `freeByOffset(offset, size)` to `OffsetAllocator`, then:
- `ObjectEntry` can store `AllocationPtr = nullptr` after recovery.
- `DeleteKeys()` calls `allocator_->freeByOffset(entry.offset, entry.total_size)` before erasing from the map.
- `BatchLoad()` does not need `AllocationPtr` at all (it only needs `offset`). We can remove `allocation` from `ReadPlan`.

This simplifies the design dramatically and removes the need for `AllocationPtr` entirely!

Wait, but `BatchLoad()` currently copies `AllocationPtr` to keep the extent alive during concurrent reads. If we remove `AllocationPtr`, what prevents a concurrent `DeleteKeys` from freeing the allocator node while `BatchLoad` is reading it?

In `BucketStorageBackend`, this is solved by `BucketReadGuard` (`inflight_reads_`). For `OffsetAllocatorStorageBackend`, we would need a similar mechanism if we want concurrent read+delete safety.

However, if we keep `AllocationPtr` for in-memory entries (normal case), and only use `freeByOffset` for explicitly deleted keys, then:
- Normal reads: `AllocationPtr` is non-null, concurrent delete is safe (because delete would see `AllocationPtr != nullptr` and let the refcount drop to 0 before freeing).
- Recovered entries: `AllocationPtr` is null. If we allow deletion of recovered entries, we need a different guard.

Given the complexity, a pragmatic V2 approach is:
1. **For V1 production**: Do NOT support deletion/eviction of recovered entries. Only support deletion of entries created in the current process lifetime (which have valid `AllocationPtr`).
2. **Add `freeByOffset` anyway**, but document that it should only be called when the caller knows no concurrent reads are active.
3. **For eviction**: Only evict in-memory entries (with `AllocationPtr`) during `BatchOffload` retry. If that fails, return `FILE_WRITE_FAIL`.

Actually, a cleaner design is to **replace `AllocationPtr` with a custom handle that can be reconstructed from `(offset, size)` after deserialization**.

But `OffsetAllocator` doesn't support creating a handle from `(offset, size)` without the node index.

**Final Decision for this Plan**:
1. Add `OffsetAllocator::freeByOffset(uint64_t offset, uint64_t size)`.
2. In `OffsetAllocatorStorageBackend`, remove `AllocationPtr` from `ObjectEntry` entirely. Instead, use a **per-shard `inflight_reads_` atomic counter** (similar to `BucketMetadata`) to protect extents during reads.
3. `BatchLoad` increments `inflight_reads_` for each shard it reads from, then decrements after IO.
4. `DeleteKeys` / eviction waits for `inflight_reads_ == 0` before calling `freeByOffset`.

This is much cleaner than `AllocationPtr` refcounting and aligns with the proven `BucketReadGuard` pattern.

### Revised `ObjectEntry` Design

```cpp
struct ObjectEntry {
    uint64_t offset;
    uint32_t total_size;
    uint32_t value_size;
};

struct MetadataShard {
    mutable SharedMutex mutex;
    std::unordered_map<std::string, ObjectEntry> map;
    mutable std::atomic<int32_t> inflight_reads_{0};
    std::list<std::string> lru_queue_;  // for eviction
    std::unordered_map<std::string, std::list<std::string>::iterator> lru_map_;
};
```

**BatchLoad with inflight_reads guard**:
```cpp
// Step 1: lock shard, copy metadata, increment inflight_reads_
for (const auto& [key, dest_slice] : batched_slices) {
    size_t shard_idx = ShardForKey(key);
    auto& shard = shards_[shard_idx];
    SharedMutexLocker lock(&shard.mutex, shared_lock);
    auto it = shard.map.find(key);
    // ... validation ...
    shard.inflight_reads_.fetch_add(1, std::memory_order_relaxed);
    read_plans.push_back({key, it->second.offset, it->second.value_size, shard_idx});
}

// Step 2: IO without locks
for (auto& plan : read_plans) { /* vector_read ... */ }

// Step 3: decrement inflight_reads_
for (auto& plan : read_plans) {
    auto& shard = shards_[plan.shard_idx];
    shard.inflight_reads_.fetch_sub(1, std::memory_order_release);
}
```

**DeleteKeys with safe wait**:
```cpp
tl::expected<void, ErrorCode> OffsetAllocatorStorageBackend::DeleteKeys(
    const std::vector<std::string>& keys) {
    for (const auto& key : keys) {
        size_t shard_idx = ShardForKey(key);
        auto& shard = shards_[shard_idx];

        ObjectEntry entry_to_free;
        bool found = false;
        {
            SharedMutexLocker lock(&shard.mutex);
            auto it = shard.map.find(key);
            if (it == shard.map.end()) continue;
            entry_to_free = it->second;
            shard.map.erase(it);
            // also erase from lru_queue_
            auto lru_it = shard.lru_map_.find(key);
            if (lru_it != shard.lru_map_.end()) {
                shard.lru_queue_.erase(lru_it->second);
                shard.lru_map_.erase(lru_it);
            }
            total_size_.fetch_sub(entry_to_free.total_size, std::memory_order_relaxed);
            total_keys_.fetch_sub(1, std::memory_order_relaxed);
            found = true;
        }

        if (found) {
            // Wait for in-flight reads
            WaitForInflightReads(shard);
            allocator_->freeByOffset(entry_to_free.offset, entry_to_free.total_size);
        }
    }
    SaveMetadata();
    return {};
}
```

This design is robust, simple, and mirrors `BucketStorageBackend`'s proven concurrency model.

### Phase 2: Eviction in BatchOffload (P1)

With `DeleteKeys` implemented, eviction becomes straightforward:

```cpp
// Inside BatchOffload, per key
auto allocation = allocator_->allocate(record_size);
if (!allocation.has_value()) {
    // Try to evict oldest keys from this shard
    bool evicted = false;
    {
        auto& shard = shards_[shard_idx];
        SharedMutexLocker lock(&shard.mutex);
        while (!shard.lru_queue_.empty()) {
            auto key_to_evict = shard.lru_queue_.front();
            auto it = shard.map.find(key_to_evict);
            if (it != shard.map.end()) {
                auto freed_size = it->second.total_size;
                ObjectEntry evict_entry = it->second;
                shard.map.erase(it);
                shard.lru_queue_.erase(shard.lru_map_[key_to_evict]);
                shard.lru_map_.erase(key_to_evict);
                total_size_.fetch_sub(freed_size, std::memory_order_relaxed);
                total_keys_.fetch_sub(1, std::memory_order_relaxed);

                // We can't free allocator extent yet because we hold shard lock
                // and we need to wait for inflight_reads. Defer it.
                deferred_frees.push_back({shard_idx, evict_entry});
                evicted = true;
            } else {
                shard.lru_queue_.pop_front();
                shard.lru_map_.erase(key_to_evict);
            }
            // Check if we freed enough
            auto report = allocator_->storageReport();
            if (report.largestFreeRegion >= record_size) break;
        }
    }

    // Process deferred frees (wait for reads, then free allocator)
    for (auto& [shard_idx, entry] : deferred_frees) {
        WaitForInflightReads(shards_[shard_idx]);
        allocator_->freeByOffset(entry.offset, entry.total_size);
    }

    allocation = allocator_->allocate(record_size);
    if (!allocation.has_value()) {
        LOG(ERROR) << "Out of space after eviction for key: " << key;
        break;
    }
}
```

### Phase 3: Auxiliary APIs (P2)

- `BatchQuery()` — straightforward shard lookup.
- `GetStoreMetadata()` — return `OffloadMetadata(total_keys_, total_size_)`.
- `GetFileInstance()` — return the opened `data_file_`.

---

## 4. Required API Extensions to `OffsetAllocator`

**File**: `mooncake-store/include/offset_allocator/offset_allocator.hpp`  
**File**: `mooncake-store/src/offset_allocator.cpp`

### 4.1 `freeByOffset(uint64_t offset, uint64_t size)`

```cpp
class OffsetAllocator {
public:
    // Free an allocation by its exact offset and size.
    // Returns true if a matching used node was found and freed.
    bool freeByOffset(uint64_t offset, uint64_t size);
};
```

**Implementation sketch**:
```cpp
bool OffsetAllocator::freeByOffset(uint64_t offset, uint64_t size) {
    MutexLocker lock(&m_mutex);
    if (!m_allocator) return false;

    uint64_t internal_offset = (offset - m_base) >> m_multiplier_bits;
    uint64_t internal_size = size >> m_multiplier_bits;
    if (m_multiplier_bits == 0 && (size & ((1ULL << m_multiplier_bits) - 1))) {
        internal_size++; // round up if not aligned
    }

    // Walk the node list to find a used node matching offset+size
    // This is O(N) but N is bounded by max_capacity (~1M)
    for (uint32_t i = 0; i < m_allocator->m_nodes.size(); ++i) {
        auto& node = m_allocator->m_nodes[i];
        if (node.used && node.dataOffset == internal_offset && node.dataSize == internal_size) {
            m_allocator->free(OffsetAllocation(internal_offset, i));
            m_allocated_size -= size;
            m_allocated_num--;
            return true;
        }
    }
    return false;
}
```

**Note**: `__Allocator::m_nodes` is currently private. We need to either make `OffsetAllocator` a friend of `__Allocator` (it already is: `friend class OffsetAllocator;`) or add a public iterator/query method to `__Allocator`. The simplest path is to add `findNodeByOffset(uint32_t offset, uint32_t size) -> std::optional<OffsetAllocation>` to `__Allocator` and call it from `OffsetAllocator::freeByOffset`.

### 4.2 Safety Guard in `freeAllocation`

Add early return if `allocation.isNoSpace()` to protect against moved-from handles.

---

## 5. Updated File Modification List

| File | Changes |
|------|---------|
| `mooncake-store/include/offset_allocator/offset_allocator.hpp` | Add `bool freeByOffset(uint64_t offset, uint64_t size);` to `OffsetAllocator`. Optionally add `findNode` helper to `__Allocator`. |
| `mooncake-store/src/offset_allocator.cpp` | Implement `freeByOffset`. Add `NO_SPACE` guard in `freeAllocation` and `__Allocator::free`. |
| `mooncake-store/include/storage_backend.h` | Redesign `OffsetAllocatorStorageBackend`: new `MetadataShard` with `inflight_reads_` and LRU lists; remove `AllocationPtr`; add `SaveMetadata`, `LoadMetadata`, `DeleteKeys`, `BatchQuery`, `GetStoreMetadata`, `GetFileInstance`. Fix `RefCountedAllocationHandle` (or remove it). |
| `mooncake-store/src/storage_backend.cpp` | Rewrite `OffsetAllocatorStorageBackend` implementation section (~2628-3209). Modify `Init()`, `BatchOffload()`, `BatchLoad()`, add persistence and eviction logic. |
| `mooncake-store/src/file_storage.cpp` | No changes needed for basic operation (fallback already exists). May optionally add `dynamic_pointer_cast<OffsetAllocatorStorageBackend>` for `GetFileInstance` if external buffer registration is required. |

---

## 6. Testing Plan

### Unit Tests (`mooncake-store/tests/`)

1. **`offset_allocator_backend_basic_test`**
   - Init → BatchOffload 100 keys → BatchLoad verifies exact contents.

2. **`offset_allocator_backend_persistence_test`**
   - Init → BatchOffload → destroy backend → re-Init → ScanMeta returns all 100 keys → BatchLoad succeeds.

3. **`offset_allocator_backend_eviction_test`**
   - Init with small capacity → write keys until allocator full → write more keys → verify oldest keys are evicted and subsequent reads return `OBJECT_NOT_FOUND`.

4. **`offset_allocator_backend_concurrent_evict_read_test`**
   - Thread A starts `BatchLoad` (holds `inflight_reads_`) → Thread B triggers eviction of the same key → Thread A read completes successfully → after Thread A finishes, the space is freed.

5. **`offset_allocator_delete_test`**
   - Write key K → `DeleteKeys({K})` → `IsExist(K)` returns false → re-allocate the same offset to key L (verifies free worked).

6. **`file_storage_integration_test`**
   - Set env var to `offset_allocator_storage_backend` → run full `FileStorage` lifecycle (Heartbeat → OffloadObjects → BatchGet → restart → ScanMeta).

---

## 7. Rollout & Risk Mitigation

- **Feature Flag**: Keep `BucketStorageBackend` as default. `OffsetAllocatorStorageBackend` is strictly opt-in via `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR`.
- **Canary**: Run on 1 node for 48 hours with real traffic. Monitor:
  - Disk utilization growth rate
  - `put_latency_us` for disk replicas
  - Process restart recovery time (`ScanMeta` duration)
  - Any `SIGSEGV` or allocator assertion failures
- **Revert**: Switching backend descriptor back to `bucket_storage_backend` instantly reverts behavior (though previously offloaded data in `kv_cache.data` becomes orphaned until manual cleanup).

---

## 8. Summary of Critical Findings

| Finding | Impact | Action |
|---------|--------|--------|
| `FileStorage::OffloadObjects` already falls back for non-Bucket backends | **Lowers urgency of API integration** | No changes needed in `FileStorage.cpp` for basic offload. |
| `RefCountedAllocationHandle` destructor is defaulted but **does** chain to `~OffsetAllocationHandle()` | **Not a pure leak**, but fragile | Fix by deleting pointee move ops + adding `NO_SPACE` guards in `offset_allocator`. |
| `OffsetAllocator` has **native serialization** | **Simplifies persistence dramatically** | Use `serialize_to` / `deserialize_from` for `.alloc` state. |
| Missing `freeByOffset` in `OffsetAllocator` | **Blocks deletion/eviction** | Must add new API to `OffsetAllocator`. |
| Current `AllocationPtr` design is over-engineered and fragile | **Complicates concurrency** | Replace with `inflight_reads_` per shard (matching `BucketReadGuard` pattern). |

---

## 9. Revised Timeline Estimate

| Phase | Tasks | Est. Time |
|-------|-------|-----------|
| **Phase 0** | Fix `RefCountedAllocationHandle` + add `NO_SPACE` guards | 0.5 day |
| **Phase 1** | Implement allocator serialization metadata format; rewrite `Init()` with recovery; wire `SaveMetadata` into `BatchOffload` | 4-5 days |
| **Phase 2** | Add `freeByOffset` to `OffsetAllocator`; redesign `ObjectEntry` with `inflight_reads_`; implement `DeleteKeys` and eviction in `BatchOffload` | 3-4 days |
| **Phase 3** | Add `BatchQuery`, `GetStoreMetadata`, `GetFileInstance`; write full unit test suite | 3 days |
| **Integration** | End-to-end testing with `FileStorage`, bugfix, documentation | 2 days |
| **Total** | | **~10-12 days** |
