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

ErrorCode NofPageRegistry::Register(uint64_t owner, void* ptr, size_t size) {
    if (ptr == nullptr || size == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t first_page = begin & ~kPageMask;
    const uintptr_t end_page = AlignUpToPage(begin + size);

    std::lock_guard<std::mutex> lock(mutex_);
    auto& registrations = owner_regs_[owner];

    // [skip_from, skip_end): the retained suffix of a record shrunk by a
    // failed unregister — still charged, must not be re-bumped. A redo walks
    // up to skip_end even when the new size is smaller, so no
    // covered-but-uncharged hole is left in the prefix.
    uintptr_t bump_from = first_page;
    uintptr_t skip_from = 0, skip_end = 0;
    auto existing = registrations.find(ptr);
    if (existing != registrations.end()) {
        const uintptr_t covered_end =
            AlignUpToPage(begin + existing->second.size);
        if (existing->second.charged_from == 0) {
            if (end_page <= covered_end) {
                // Quarantine means unknown translation state, not usable DMA
                // coverage.
                return existing->second.has_quarantined
                           ? ErrorCode::NOF_REGISTRATION_STUCK
                           : ErrorCode::OK;
            }
            bump_from = covered_end;
        } else {
            bump_from = first_page;
            skip_from = existing->second.charged_from;
            skip_end = covered_end;
        }
    }
    const uintptr_t walk_end =
        skip_end != 0 ? std::max(end_page, skip_end) : end_page;

    std::vector<uintptr_t> touched;  // pages whose count this call bumped
    bool saw_quarantined = false;
    for (uintptr_t page = bump_from; page < walk_end; page += kPageSize) {
        if (page >= skip_from && page < skip_end) {
            continue;  // still charged by this record
        }
        auto& reg = page_regs_[page];
        // External pages are pinned and never (un)registered by us; a
        // quarantined page additionally makes the whole range report STUCK
        // (unknown translation state is not usable DMA coverage).
        if (reg.external) {
            continue;
        }
        if (reg.quarantined) {
            // Unknown translation state: the range is not usable DMA
            // coverage, so this registration reports STUCK, not OK.
            LOG(WARNING) << "Register over a quarantined NoF page " << page
                         << " (translation state unknown; page stays pinned)";
            saw_quarantined = true;
            continue;
        }
        if (reg.count == 0) {
            int rc = register_fn_(reinterpret_cast<void*>(page), kPageSize);
            if (rc == -EBUSY) {
                // Already registered via DPDK's memseg walk.
                reg.external = true;
                continue;
            }
            if (rc != 0) {
                LOG(ERROR) << "page register failed: page="
                           << reinterpret_cast<void*>(page) << " rc=" << rc;
                // SPDK does not roll back a failed spdk_mem_register: try to
                // clean up the failing page itself, once; otherwise
                // quarantine it and keep this call's charges.
                if (unregister_fn_(reinterpret_cast<void*>(page), kPageSize) !=
                    0) {
                    reg.count = 1;
                    reg.quarantined = true;
                    auto& rec = registrations[ptr];
                    rec.size = std::max(
                        rec.size,
                        static_cast<size_t>((page + kPageSize) - begin));
                    rec.charged_from = 0;
                    rec.has_quarantined = true;
                    return ErrorCode::NOF_REGISTRATION_STUCK;
                }
                page_regs_.erase(page);
                // The failing page is confirmed clean: roll back this call's
                // pages in reverse order and STOP at the first rollback
                // failure, quarantining that page — the remaining charges
                // stay a contiguous prefix.
                for (auto it = touched.rbegin(); it != touched.rend(); ++it) {
                    auto& r = page_regs_[*it];
                    if (r.count > 1) {
                        --r.count;  // other owners still hold the page
                        continue;
                    }
                    if (unregister_fn_(reinterpret_cast<void*>(*it),
                                       kPageSize) == 0) {
                        page_regs_.erase(*it);
                        continue;
                    }
                    LOG(ERROR) << "rollback unregister failed: page="
                               << reinterpret_cast<void*>(*it)
                               << "; quarantining page";
                    r.quarantined = true;  // count stays 1, terminal
                    auto& rec = registrations[ptr];
                    rec.size = std::max(
                        rec.size,
                        static_cast<size_t>((*it + kPageSize) - begin));
                    rec.charged_from = 0;
                    rec.has_quarantined = true;
                    return ErrorCode::NOF_REGISTRATION_STUCK;
                }
                // Everything this call touched is confirmed clean.
                return ErrorCode::INTERNAL_ERROR;
            }
        }
        reg.count++;
        touched.push_back(page);
    }
    auto& rec = registrations[ptr];
    rec.size = std::max(rec.size, size);
    rec.charged_from = 0;  // full coverage restored
    if (saw_quarantined) {
        rec.has_quarantined = true;
    }
    // Quarantined pages in coverage mean the range is not usable DMA
    // coverage; only a range with none may report OK.
    return rec.has_quarantined ? ErrorCode::NOF_REGISTRATION_STUCK
                               : ErrorCode::OK;
}

bool NofPageRegistry::ReleaseRangeLocked(void* ptr, OwnerRange& rec) {
    const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t first_page = begin & ~kPageMask;
    const uintptr_t end_page = AlignUpToPage(begin + rec.size);
    // Start where the remaining charges start.
    const uintptr_t start =
        rec.charged_from != 0 ? rec.charged_from : first_page;
    for (uintptr_t page = start; page < end_page; page += kPageSize) {
        auto pit = page_regs_.find(page);
        if (pit == page_regs_.end() || pit->second.external) {
            continue;
        }
        if (pit->second.quarantined) {
            // Terminal: never unregistered, the mapping must stay pinned.
            rec.charged_from = page;
            rec.has_quarantined = true;
            return false;
        }
        if (pit->second.count > 1) {
            --pit->second.count;
            continue;
        }
        if (unregister_fn_(reinterpret_cast<void*>(page), kPageSize) == 0) {
            page_regs_.erase(pit);
            continue;
        }
        LOG(ERROR) << "page unregister failed: page="
                   << reinterpret_cast<void*>(page) << "; quarantining page";
        pit->second.quarantined = true;  // count stays 1, terminal
        rec.charged_from = page;
        rec.has_quarantined = true;
        return false;
    }
    return true;
}

ErrorCode NofPageRegistry::Unregister(uint64_t owner, void* ptr) {
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
        // Record retained (shrunk to the first unreleasable page): the
        // caller's quarantine path keeps the mapping pinned.
        return ErrorCode::INTERNAL_ERROR;
    }
    owner_it->second.erase(it);
    if (owner_it->second.empty()) {
        owner_regs_.erase(owner_it);
    }
    return ErrorCode::OK;
}

ErrorCode NofPageRegistry::UnregisterAll(uint64_t owner) {
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
            // Record retained so the caller's mapping stays quarantined.
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
