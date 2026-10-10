#include "nof/page_registry.h"

#include <glog/logging.h>

#include <cerrno>
#include <vector>

namespace mooncake {

NofPageRegistry::NofPageRegistry(MemRegisterFn register_fn,
                                 MemUnregisterFn unregister_fn)
    : register_fn_(register_fn), unregister_fn_(unregister_fn) {}

ErrorCode NofPageRegistry::Register(void* owner, void* ptr, size_t size) {
    const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t first_page = begin & ~(kPageSize - 1);
    const uintptr_t end_page =
        (begin + size + kPageSize - 1) & ~(kPageSize - 1);

    std::lock_guard<std::mutex> lock(mutex_);
    auto& registrations = owner_regs_[owner];

    // Same-owner re-registration of ptr: no-op when the existing range
    // already covers the request; otherwise extend. Same ptr means the same
    // first page, so the extension is always a suffix of pages.
    uintptr_t bump_from = first_page;
    auto existing = registrations.find(ptr);
    if (existing != registrations.end()) {
        const uintptr_t covered_end =
            (begin + existing->second + kPageSize - 1) & ~(kPageSize - 1);
        if (end_page <= covered_end) {
            return ErrorCode::OK;
        }
        bump_from = covered_end;
    }

    std::vector<uintptr_t> touched;  // pages whose count this call bumped
    for (uintptr_t page = bump_from; page < end_page; page += kPageSize) {
        auto& reg = page_regs_[page];
        if (reg.count == 0 && !reg.external) {
            int rc = register_fn_(reinterpret_cast<void*>(page), kPageSize);
            if (rc == -EBUSY) {
                // Already registered via DPDK's memseg walk — not ours to
                // unregister.
                reg.external = true;
            } else if (rc != 0) {
                LOG(ERROR) << "page register failed: page="
                           << reinterpret_cast<void*>(page) << " rc=" << rc;
                // Roll back the pages this call bumped.
                for (uintptr_t p : touched) {
                    auto& r = page_regs_[p];
                    if (--r.count == 0) {
                        unregister_fn_(reinterpret_cast<void*>(p), kPageSize);
                        page_regs_.erase(p);
                    }
                }
                // The backend may have partially changed state for the
                // FAILING page itself: spdk_mem_register() marks the range in
                // g_mem_reg_map before running its notify callbacks and does
                // not roll that back on failure (SPDK v23.01.1
                // lib/env_dpdk/memory.c:370-384), and in iova=va it can
                // already have installed the IOMMU mapping when a later step
                // fails. Try to clean the failing page up.
                if (unregister_fn_(reinterpret_cast<void*>(page), kPageSize) ==
                    0) {
                    // Cleanup confirmed: drop the entry created above and
                    // report a plain failure — the caller may munmap safely.
                    if (reg.count == 0 && !reg.external) {
                        page_regs_.erase(page);
                    }
                    return ErrorCode::INTERNAL_ERROR;
                }
                // Cleanup cannot be confirmed (-EINVAL covers both "nothing
                // was marked" and "marked but not fully translated", and the
                // two cannot be told apart). Keep explicit bookkeeping: the
                // page stays charged to us and the owner range is recorded,
                // so a later Unregister/UnregisterAll retries the cleanup and
                // the shm quarantine paths retain the mapping instead of
                // munmapping a possibly-live translation. A same-range retry
                // hits the "already covered" fast path above and does not
                // re-call register_fn_ — deliberate: re-registering a page in
                // this unknown state could return -EBUSY from the leftover
                // translation and be misclassified as externally owned.
                reg.count = 1;
                registrations[ptr] = size;
                return ErrorCode::NOF_REGISTRATION_STUCK;
            }
        }
        if (!reg.external) {
            reg.count++;
            touched.push_back(page);
        }
    }
    registrations[ptr] = size;
    return ErrorCode::OK;
}

ErrorCode NofPageRegistry::Unregister(void* owner, void* ptr) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto owner_it = owner_regs_.find(owner);
    if (owner_it == owner_regs_.end()) {
        return ErrorCode::OK;  // this owner registered nothing: no-op
    }
    auto it = owner_it->second.find(ptr);
    if (it == owner_it->second.end()) {
        return ErrorCode::OK;  // not registered by this owner: no-op
    }
    if (!ReleaseRangeLocked(ptr, it->second)) {
        // Some pages could not be released: keep the owner record so the
        // caller's quarantine path can retry, and propagate the failure —
        // munmap is NOT safe while the backend may still hold a translation.
        return ErrorCode::INTERNAL_ERROR;
    }
    owner_it->second.erase(it);
    if (owner_it->second.empty()) {
        owner_regs_.erase(owner_it);
    }
    return ErrorCode::OK;
}

ErrorCode NofPageRegistry::UnregisterAll(void* owner) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto owner_it = owner_regs_.find(owner);
    if (owner_it == owner_regs_.end()) {
        return ErrorCode::OK;
    }
    bool all_released = true;
    for (auto it = owner_it->second.begin(); it != owner_it->second.end();) {
        if (ReleaseRangeLocked(it->first, it->second)) {
            it = owner_it->second.erase(it);
        } else {
            // Keep this range's record for a later retry.
            all_released = false;
            ++it;
        }
    }
    if (owner_it->second.empty()) {
        owner_regs_.erase(owner_it);
    }
    return all_released ? ErrorCode::OK : ErrorCode::INTERNAL_ERROR;
}

// Returns true only when every page of the range was released. Pages whose
// backend unregistration fails keep their record (count stays 1) so a later
// retry still owns them.
bool NofPageRegistry::ReleaseRangeLocked(void* ptr, size_t size) {
    const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t first_page = begin & ~(kPageSize - 1);
    const uintptr_t end_page =
        (begin + size + kPageSize - 1) & ~(kPageSize - 1);
    bool all_released = true;
    for (uintptr_t page = first_page; page < end_page; page += kPageSize) {
        auto pit = page_regs_.find(page);
        if (pit == page_regs_.end() || pit->second.external) {
            continue;
        }
        if (pit->second.count > 1) {
            --pit->second.count;
            continue;
        }
        // Last reference: single-page region == legal single-page unmap
        // (NOTIFY_START semantics).
        int rc = unregister_fn_(reinterpret_cast<void*>(page), kPageSize);
        if (rc == 0) {
            page_regs_.erase(pit);
        } else {
            LOG(ERROR) << "page unregister failed: page="
                       << reinterpret_cast<void*>(page) << " rc=" << rc;
            all_released = false;
        }
    }
    return all_released;
}

}  // namespace mooncake
