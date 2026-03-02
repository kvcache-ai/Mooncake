# Local Hot Cache: Zero-Copy get_buffer Fast Path & LRU Lock Optimization

## Overview

This change resolves two performance issues from the PR #1226 review:

1. **LRU lock contention**: All cache reads were serialized by a global exclusive lock.
2. **Synchronous memcpy on cache miss**: Each cache miss triggered up to 16MB memcpy (user buffer -> cache block).

Solutions:
- **Issue 1**: Replace exclusive locks on read path with shared locks + deferred batch LRU touches.
- **Issue 2**: Integrate a zero-copy hot cache fast path into `get_buffer_internal()`, so all Python APIs (`get`, `get_tensor`, `get_buffer`) benefit automatically.

The original `Get()` API is **unchanged**.

---

## Issue 1: LRU Lock Optimization

### Root Cause

`GetHotKey()`, `ReleaseHotKey()`, and `TouchHotKey()` all used `std::unique_lock` (exclusive). Since `GetHotKey()` called `touchLRU()` to move list nodes, every cache read required write access, serializing all concurrent reads.

### Solution

| Method | Before | After |
|---|---|---|
| `GetHotKey()` | `unique_lock` + `touchLRU()` | `shared_lock` + `accessed.store(true)` |
| `ReleaseHotKey()` | `unique_lock` | `shared_lock` |
| `TouchHotKey()` | `unique_lock` + `touchLRU()` | `shared_lock` + `accessed.store(true)` |
| `PutHotKey()` | `unique_lock` | `unique_lock` + `drainDeferredTouches()` |
| `GetFreeBlock()` | `unique_lock` | `unique_lock` + `drainDeferredTouches()` |

Core mechanism — **deferred LRU touch**:

- `HotMemBlock` has `std::atomic<bool> accessed` flag.
- Read-path methods set `accessed = true` under shared lock (no list mutation).
- `drainDeferredTouches()` runs under exclusive lock in write-path methods (`PutHotKey`, `GetFreeBlock`), splicing all `accessed=true` nodes to the front.

This ensures LRU order is approximately correct before eviction decisions, without blocking concurrent reads.

### Changed Files

- `mooncake-store/include/local_hot_cache.h` — `HotMemBlock` gets `std::atomic<bool> accessed`; `LocalHotCache` gets private `drainDeferredTouches()`.
- `mooncake-store/src/local_hot_cache.cpp` — Read-path locks changed to `shared_lock`; `drainDeferredTouches()` implemented.

---

## Issue 2: Zero-Copy Fast Path in `get_buffer_internal`

### Root Cause

The original `Get()` cache-miss flow:

```
Remote ──TransferRead──→ User Buffer ──memcpy──→ Cache Block
```

Each miss triggered a synchronous memcpy (up to 16MB/block) in the `SubmitPutTask` path, increasing `Get()` latency.

### Solution

A hot cache fast path is integrated into `RealClient::get_buffer_internal()`:

```
Cache hit:   Cache Block → BufferHandle (view mode, zero-copy)
Cache miss:  Falls through to existing Get() + ProcessSlicesAsync path
```

On cache hit, `GetHotKey()` increments `ref_count` and returns a `HotMemBlock*`. A `BufferHandle` is created in **view mode** (`BufferHandle(void* ptr, size_t size, release_fn)`), with `release_fn` capturing the `HotMemBlock*` directly and decrementing `ref_count` on destruction. This eliminates the TOCTOU bug present in the old `CacheBlockHandle` design (which used key-string lookup to release).

Because this is done inside `get_buffer_internal`, all Python-facing APIs (`get`, `get_tensor`, `get_buffer`) benefit automatically with zero code changes upstream.

### Design Details

- `release_fn` captures `HotMemBlock*` directly (not key string) — no TOCTOU
- Captures `shared_ptr<LocalHotCache>` to keep cache alive while handle exists
- Cache miss falls through to existing `Client::Get()` + `ProcessSlicesAsync` path

### Why not modify `Get()` directly?

The original `Get()` API is left unchanged because:

- `Get()` supports all replica types (memory + disk); the cache fast path only applies to memory replicas already in the cache.
- `Get()` has no size limit; cache blocks are fixed-size.
- `Get()` copies data into user-owned buffers (caller controls lifetime); the view mode returns a read-only handle (cache manages lifetime).
- Modifying `Get()`'s async cache fill path would risk use-after-free: worker threads would read user buffers after `Get()` returns, but the user may have already freed them.

### Changed Files

- `mooncake-store/src/real_client.cpp` — Hot cache fast path added to `get_buffer_internal()`.
- `mooncake-store/include/local_hot_cache.h` — `CacheBlockHandle` class and `PutHotKeyWithRef()` removed (no longer needed).
- `mooncake-store/src/local_hot_cache.cpp` — `CacheBlockHandle` and `PutHotKeyWithRef()` implementations removed.
- `mooncake-store/include/client_service.h` — `GetZeroCopy()` declarations removed.
- `mooncake-store/src/client_service.cpp` — `GetZeroCopy()` implementations removed.

---

## Tests

All tests are in `client_local_hot_cache_test.cpp`.

| Test | Description |
|---|---|
| `GetBufferInternalHotCacheZeroCopy` | Verifies BufferHandle view mode with hot cache ref_count lifecycle |
| `DeferredLRUTouchOrdering` | Accessed blocks survive eviction |
| `ConcurrentGetHotKeySharedLock` | 8 threads x 100 reads, no crashes |

```bash
cd build && cmake .. && make -j$(nproc)
./mooncake-store/tests/client_local_hot_cache_test
```

---

## Data Flow Comparison

```
Get() hit:           Cache Block ──TransferRead──→ User Buffer (TE performs memcpy)
Get() miss:          Remote ──TransferRead──→ User Buffer ──memcpy──→ Cache Block (async)

get_buffer() hit:    Cache Block → BufferHandle view mode (zero-copy)
get_buffer() miss:   Falls through to Get() path above
```
