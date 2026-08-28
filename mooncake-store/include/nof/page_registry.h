#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>

#include "types.h"  // ErrorCode

namespace mooncake {

// Bookkeeping for the process-global DMA translation table (#3131), factored
// out of the SPDK initiator so the policy is unit-testable without SPDK. The
// two memory (un)registration calls are injected: the SPDK initiator wires
// spdk_mem_register/spdk_mem_unregister, tests wire fakes.
//
// SPDK v23.01.1 semantics (lib/env_dpdk/memory.c) this mirrors:
//   - register/unregister require (vaddr, len) 2MB-aligned, else -EINVAL;
//   - registering an already-registered page fails with -EBUSY (e.g. DPDK
//     memseg memory from spdk_zmalloc, registered by the memseg walk);
//   - unmapping a sub-range of a region fails with -ERANGE — this registry
//     always registers/unregisters single pages, each its own region.
//
// Ownership model: every registration is charged to an explicit owner token
// (the registering initiator instance). Per-page refcounts are process-wide,
// while the set of registered ranges is tracked per owner:
//   - two owners registering the same buffer each add a page reference, so
//     one owner's Unregister never unmaps pages the other still relies on;
//   - re-registering the same (owner, ptr) with a size already covered is an
//     idempotent no-op, while a larger size extends coverage to the newly
//     covered pages (previously this was silently treated as registered);
//   - UnregisterAll(owner) releases everything an owner holds (teardown
//     backstop).
//
// Normalization registers whole pages containing the buffer; this grants no
// extra DMA reach beyond the buffer's own pages.
class NofPageRegistry {
   public:
    using MemRegisterFn = int (*)(void* addr, size_t len);
    using MemUnregisterFn = int (*)(void* addr, size_t len);

    NofPageRegistry(MemRegisterFn register_fn, MemUnregisterFn unregister_fn);

    NofPageRegistry(const NofPageRegistry&) = delete;
    NofPageRegistry& operator=(const NofPageRegistry&) = delete;

    ErrorCode Register(void* owner, void* ptr, size_t size);
    ErrorCode Unregister(void* owner, void* ptr);
    ErrorCode UnregisterAll(void* owner);

   private:
    static constexpr uintptr_t kPageSize = 2ULL << 20;  // 2MB hugepage

    struct PageReg {
        uint32_t count = 0;
        // Registered by the DPDK memseg walk (-EBUSY): never unregister.
        bool external = false;
    };

    // Drops this registration's page references, unregistering pages whose
    // last reference goes away. Caller must hold mutex_.
    void ReleaseRangeLocked(void* ptr, size_t size);

    MemRegisterFn register_fn_;
    MemUnregisterFn unregister_fn_;
    std::map<uint64_t, PageReg> page_regs_;  // key: 2MB-aligned page base
    // owner token -> (user ptr -> registered size)
    std::map<void*, std::map<void*, size_t>> owner_regs_;
    std::mutex mutex_;
};

}  // namespace mooncake
