#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>

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
//   - spdk_mem_register marks a range BEFORE its notify callbacks and does
//     not roll back on failure; -EINVAL on unregister covers both "nothing
//     marked" and "partially translated", and the two cannot be told apart.
//
// Ownership model: registrations are charged to an explicit owner token (the
// registering initiator instance). Per-page refcounts are process-wide, so
// one owner's Unregister never unmaps pages another owner still relies on.
// Each owner record is {valid_size, pending}:
//   - valid_size: whole-page prefix that is validly registered. OK is only
//     returned for fully registered ranges.
//   - pending: pages still charged whose backend cleanup failed or could not
//     be confirmed. Retries touch only pending pages plus the valid prefix —
//     never references owned by someone else. A pending page re-registered
//     by another owner must stay registered for the new owner.
//
// Register failure rolls back this call's pages and cleans up the failing
// page; anything whose cleanup is unconfirmed moves to pending and returns
// NOF_REGISTRATION_STUCK (callers must quarantine, never munmap without a
// successful Unregister). Unregister failure likewise moves pages to pending
// and propagates INTERNAL_ERROR. Normalization registers whole pages
// containing the buffer; this grants no extra DMA reach beyond the buffer's
// own pages.
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

    struct OwnerReg {
        size_t valid_size = 0;       // validly registered whole-page prefix
        std::set<uint64_t> pending;  // pages owed a backend cleanup
    };

    // Drops one owner reference on `page`. Returns false when the backend
    // unregistration fails: the page stays charged (count 1) and pending.
    bool ReleasePageRefLocked(uint64_t page, std::set<uint64_t>& pending);

    // Retries every pending page; true only when the backlog is empty after.
    bool RetryPendingLocked(OwnerReg& rec);

    // Releases the valid prefix and the pending backlog of one record; true
    // only when nothing remains charged.
    bool UnregisterRecordLocked(void* ptr, OwnerReg& rec);

    MemRegisterFn register_fn_;
    MemUnregisterFn unregister_fn_;
    std::map<uint64_t, PageReg> page_regs_;  // key: 2MB-aligned page base
    // owner token -> (user ptr -> registration record)
    std::map<void*, std::map<void*, OwnerReg>> owner_regs_;
    std::mutex mutex_;
};

}  // namespace mooncake
