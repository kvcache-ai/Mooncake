#include "nof/page_registry.h"

#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <vector>

namespace mooncake {

namespace {
constexpr uintptr_t kPageMask = (2ULL << 20) - 1;
uintptr_t AlignUpToPage(uintptr_t v) { return (v + kPageMask) & ~kPageMask; }
}  // namespace

NofPageRegistry::NofPageRegistry(MemRegisterFn register_fn,
                                 MemUnregisterFn unregister_fn)
    : register_fn_(register_fn), unregister_fn_(unregister_fn) {}

bool NofPageRegistry::ReleasePageRefLocked(uint64_t page,
                                           std::set<uint64_t>& pending) {
    auto pit = page_regs_.find(page);
    if (pit == page_regs_.end()) {
        pending.erase(page);
        return true;
    }
    auto& reg = pit->second;
    if (reg.external) {
        // Registered by the DPDK memseg walk: not ours to release.
        pending.erase(page);
        return true;
    }
    if (reg.count > 1) {
        // Other owners still hold the page: dropping this reference never
        // touches the backend. (For a pending page this also cancels the
        // owed cleanup — the page must stay registered for the new owner.)
        --reg.count;
        pending.erase(page);
        return true;
    }
    // count == 1: last reference. Single-page region == legal single-page
    // unmap (NOTIFY_START semantics).
    if (unregister_fn_(reinterpret_cast<void*>(page), kPageSize) == 0) {
        page_regs_.erase(pit);
        pending.erase(page);
        return true;
    }
    LOG(ERROR) << "page unregister failed: page="
               << reinterpret_cast<void*>(page);
    // Keep the record charged and the page pending: the backend may still
    // hold the translation, and a later retry must find it here rather than
    // re-registering it (a leftover translation answers -EBUSY and would be
    // misclassified as external).
    pending.insert(page);
    return false;
}

bool NofPageRegistry::RetryPendingLocked(OwnerReg& rec) {
    // Snapshot: ReleasePageRefLocked mutates the set.
    const std::set<uint64_t> backlog = rec.pending;
    for (uint64_t page : backlog) {
        if (!ReleasePageRefLocked(page, rec.pending)) {
            return false;
        }
    }
    return rec.pending.empty();
}

bool NofPageRegistry::UnregisterRecordLocked(void* ptr, OwnerReg& rec) {
    const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t first_page = begin & ~kPageMask;
    const uintptr_t covered_end = AlignUpToPage(begin + rec.valid_size);

    bool all_released = true;
    // Release the valid prefix.
    for (uintptr_t page = first_page; page < covered_end; page += kPageSize) {
        all_released &= ReleasePageRefLocked(page, rec.pending);
    }
    rec.valid_size = 0;
    // Release the pending backlog (snapshot: ReleasePageRefLocked mutates
    // the set).
    const std::set<uint64_t> backlog = rec.pending;
    for (uint64_t page : backlog) {
        all_released &= ReleasePageRefLocked(page, rec.pending);
    }
    return all_released && rec.pending.empty();
}

ErrorCode NofPageRegistry::Register(void* owner, void* ptr, size_t size) {
    const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t first_page = begin & ~kPageMask;
    const uintptr_t end_page = AlignUpToPage(begin + size);

    std::lock_guard<std::mutex> lock(mutex_);
    auto& registrations = owner_regs_[owner];
    auto existing = registrations.find(ptr);

    // Cleanup-pending record: retry the backlog first. OK is only returned
    // for fully registered ranges, so a stuck record must be cleaned and
    // then genuinely (re)registered before OK can come back.
    if (existing != registrations.end() && !existing->second.pending.empty() &&
        !RetryPendingLocked(existing->second)) {
        return ErrorCode::NOF_REGISTRATION_STUCK;
    }

    // Coverage from the VALID prefix only: a stuck record's valid_size was
    // never extended past what is actually registered.
    uintptr_t bump_from = first_page;
    if (existing != registrations.end()) {
        const uintptr_t covered_end =
            AlignUpToPage(begin + existing->second.valid_size);
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
                // Roll back the pages this call bumped. A backend unregister
                // failure here is the same unconfirmed-state problem as the
                // failing page itself (SPDK v23.01.1 does not roll back a
                // failed spdk_mem_register, and -EINVAL on unregister covers
                // both "nothing marked" and "partially translated"), so move
                // the page into pending instead of erasing it.
                std::set<uint64_t> newly_pending;
                bool rollback_clean = true;
                for (uintptr_t p : touched) {
                    auto& r = page_regs_[p];
                    if (r.count > 1) {
                        --r.count;  // other owners still hold the page
                        continue;
                    }
                    if (unregister_fn_(reinterpret_cast<void*>(p), kPageSize) ==
                        0) {
                        page_regs_.erase(p);
                    } else {
                        LOG(ERROR) << "rollback unregister failed: page="
                                   << reinterpret_cast<void*>(p);
                        newly_pending.insert(p);  // record kept, count stays 1
                        rollback_clean = false;
                    }
                }
                // The failing page itself may hold partial state (see
                // above): try to clean it up.
                const bool failing_clean =
                    unregister_fn_(reinterpret_cast<void*>(page), kPageSize) ==
                    0;
                if (failing_clean) {
                    // Cleanup confirmed: drop the entry created above.
                    if (reg.count == 0 && !reg.external) {
                        page_regs_.erase(page);
                    }
                } else {
                    // Cleanup unconfirmed: keep the page charged and pending.
                    reg.count = 1;
                    newly_pending.insert(page);
                }
                if (failing_clean && rollback_clean) {
                    // Everything this call touched is confirmed clean —
                    // plain failure, the caller may munmap safely. An
                    // existing record keeps its previous valid prefix.
                    return ErrorCode::INTERNAL_ERROR;
                }
                // Something could not be confirmed clean. Record the backlog
                // so a later retry resumes exactly where it stopped and the
                // shm quarantine paths retain the mapping instead of
                // munmapping a possibly-live translation. The valid prefix
                // is NOT extended: coverage must be restored by a retry.
                auto& rec = registrations[ptr];
                rec.pending.insert(newly_pending.begin(), newly_pending.end());
                return ErrorCode::NOF_REGISTRATION_STUCK;
            }
        }
        if (!reg.external) {
            reg.count++;
            touched.push_back(page);
        }
    }
    registrations[ptr].valid_size =
        std::max(registrations[ptr].valid_size, size);
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
    if (!UnregisterRecordLocked(ptr, it->second)) {
        // Some pages could not be released: the record (with its pending
        // backlog) is retained so the caller's quarantine path can retry —
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
        if (UnregisterRecordLocked(it->first, it->second)) {
            it = owner_it->second.erase(it);
        } else {
            // Keep this record (and its pending backlog) for a later retry.
            all_released = false;
            ++it;
        }
    }
    if (owner_it->second.empty()) {
        owner_regs_.erase(owner_it);
    }
    return all_released ? ErrorCode::OK : ErrorCode::INTERNAL_ERROR;
}

}  // namespace mooncake
