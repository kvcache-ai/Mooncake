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
//   - unmapping a sub-range of a region fails with -ERANGE — this registry
//     always registers/unregisters single pages, each its own region.
//
// Ownership model: every registration is charged to an explicit owner token
// (the registering initiator instance). Per-page refcounts are process-wide,
// while per-owner records track each registration's state:
//   - two owners registering the same buffer each add a page reference, so
//     one owner's Unregister never unmaps pages the other still relies on;
//   - re-registering the same (owner, ptr) with a size already covered by the
//     record's VALID prefix is an idempotent no-op, while a larger size
//     extends coverage to the newly covered pages;
//   - UnregisterAll(owner) releases everything an owner holds (teardown
//     backstop).
//
// Each owner record is {valid_size, pending}:
//   - valid_size: bytes from the range start that are VALIDLY registered
//     (always a whole-page prefix). OK is only ever returned for a range
//     fully covered by valid registrations — "OK means fully registered".
//   - pending: page bases still charged to this registration whose backend
//     cleanup failed or could not be confirmed (cleanup-pending state).
//     Retries only ever touch pending pages plus the valid prefix — never
//     pages whose references belong to other owners.
//
// Failure semantics (SPDK v23.01.1 can leave partial state behind):
//   - Register failure: pages this call bumped are rolled back and the
//     failing page itself is cleaned up via unregister_fn_. If any of those
//     unregisters cannot be confirmed (failing page or a rollback page
//     alike), the affected pages move into the record's pending set and
//     NOF_REGISTRATION_STUCK is returned — callers must treat the range as
//     still registered (quarantine; never munmap without a successful
//     Unregister). A retry of a stuck range first retries the pending
//     cleanup, then registers the still-missing coverage for real; it never
//     re-calls register_fn_ on a page whose state is unknown (a leftover
//     translation would answer -EBUSY and be misclassified as external).
//   - Unregister failure: pages that fail to unregister move into pending,
//     the owner record is retained for retry, and INTERNAL_ERROR is
//     propagated so callers can quarantine instead of munmapping.
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

    struct OwnerReg {
        // Bytes from the range start that are validly registered (whole-page
        // prefix). Zero for a purely cleanup-pending record.
        size_t valid_size = 0;
        // Page bases still charged to this registration whose backend
        // cleanup failed or could not be confirmed.
        std::set<uint64_t> pending;
    };

    // Drops one owner reference on `page`. Returns true when nothing more is
    // owed for it; on backend unregister failure the page stays charged
    // (count 1) and is kept in `pending`. Caller must hold mutex_.
    bool ReleasePageRefLocked(uint64_t page, std::set<uint64_t>& pending);

    // Retries every pending page of the record. Returns true only when the
    // backlog is empty afterwards. Caller must hold mutex_.
    bool RetryPendingLocked(OwnerReg& rec);

    // Releases the valid prefix and the pending backlog of one record.
    // Returns true only when the record has nothing charged afterwards
    // (pending empty and valid_size reset to 0). Caller must hold mutex_.
    bool UnregisterRecordLocked(void* ptr, OwnerReg& rec);

    MemRegisterFn register_fn_;
    MemUnregisterFn unregister_fn_;
    std::map<uint64_t, PageReg> page_regs_;  // key: 2MB-aligned page base
    // owner token -> (user ptr -> registration record)
    std::map<void*, std::map<void*, OwnerReg>> owner_regs_;
    std::mutex mutex_;
};

}  // namespace mooncake
