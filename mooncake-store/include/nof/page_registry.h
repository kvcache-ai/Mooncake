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
//   - spdk_mem_register marks a range BEFORE its notify callbacks and does
//     not roll back on failure; -EINVAL on unregister covers both "nothing
//     marked" and "partially translated" — the two cannot be told apart.
//
// Ownership: registrations are charged to an owner token (a unique,
// never-recycled id per initiator instance). Per-page refcounts are
// process-wide, so one owner's Unregister never unmaps pages another owner
// still relies on.
//
// Failure handling is fail-stop (the baseline SpdkWrapper contract): a
// failed registration is cleaned up once; anything whose cleanup cannot be
// confirmed is QUARANTINED — terminally pinned, never unregistered, never
// re-registered, never munmapped (the kernel reclaims it at process exit).
// A quarantined page is unknown translation state, NOT usable coverage: any
// registration whose range contains one reports NOF_REGISTRATION_STUCK, and
// teardown fails to release it, keeping the mapping pinned.
//
// Each owner record is {size, charged_from}: the covered range is
// [charged_from ?: first_page, page_align(begin + size)). Charges are
// contiguous up to the first quarantined page, and a failed unregister
// shrinks charged_from to the first unreleasable page, so a retry
// immediately hits it again and can never touch pages whose remaining
// references belong to other owners. In the deepest cascade (a redo of a
// shrunk record failing again before the retained suffix), charges may form
// two blocks; the gap is skipped safely on teardown, and pages past a
// quarantine point stay registered — leaked but pinned, by design.
class NofPageRegistry {
   public:
    using MemRegisterFn = int (*)(void* addr, size_t len);
    using MemUnregisterFn = int (*)(void* addr, size_t len);

    NofPageRegistry(MemRegisterFn register_fn, MemUnregisterFn unregister_fn);

    NofPageRegistry(const NofPageRegistry&) = delete;
    NofPageRegistry& operator=(const NofPageRegistry&) = delete;

    ErrorCode Register(uint64_t owner, void* ptr, size_t size);
    ErrorCode Unregister(uint64_t owner, void* ptr);
    ErrorCode UnregisterAll(uint64_t owner);

   private:
    static constexpr uintptr_t kPageSize = 2ULL << 20;  // 2MB hugepage

    struct PageReg {
        uint32_t count = 0;
        // Registered by the DPDK memseg walk (-EBUSY): never unregister.
        bool external = false;
        // Cleanup unconfirmed: terminally pinned, never unregistered.
        bool quarantined = false;
    };

    struct OwnerRange {
        // Requested range size; coverage ends at page_align(begin + size).
        size_t size = 0;
        // First page still charged (0 = the whole prefix is charged). Set
        // to the first unreleasable page when an unregister fails.
        uintptr_t charged_from = 0;
        // The covered range includes a quarantined page: the range is not
        // usable DMA coverage and Register must report STUCK for it.
        bool has_quarantined = false;
    };

    // Releases this registration's charged pages starting at charged_from,
    // stopping at the first unreleasable page and shrinking charged_from to
    // it. Returns true only when nothing remains charged.
    // Caller must hold mutex_.
    bool ReleaseRangeLocked(void* ptr, OwnerRange& rec);

    MemRegisterFn register_fn_;
    MemUnregisterFn unregister_fn_;
    std::map<uint64_t, PageReg> page_regs_;  // key: 2MB-aligned page base
    // owner token -> (user ptr -> registration record)
    std::map<uint64_t, std::map<void*, OwnerRange>> owner_regs_;
    std::mutex mutex_;
};

}  // namespace mooncake
