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

ErrorCode NofPageRegistry::Register(void* owner, void* ptr, size_t size) {
    const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t first_page = begin & ~kPageMask;
    const uintptr_t end_page = AlignUpToPage(begin + size);

    std::lock_guard<std::mutex> lock(mutex_);
    auto& registrations = owner_regs_[owner];

    // Same-owner re-registration of ptr: no-op when the existing record
    // covers the request; a larger size extends from the covered prefix.
    uintptr_t bump_from = first_page;
    auto existing = registrations.find(ptr);
    if (existing != registrations.end()) {
        const uintptr_t covered_end = AlignUpToPage(begin + existing->second);
        if (end_page <= covered_end) {
            return ErrorCode::OK;
        }
        bump_from = covered_end;
    }

    std::vector<uintptr_t> touched;  // pages whose count this call bumped
    for (uintptr_t page = bump_from; page < end_page; page += kPageSize) {
        auto& reg = page_regs_[page];
        // External and quarantined pages are treated alike: pinned, never
        // (un)registered by us. A quarantined page's translation state is
        // unknown; the coverage counts so teardown keeps the mapping pinned.
        if (reg.external) {
            continue;
        }
        if (reg.quarantined) {
            LOG(WARNING) << "Register over a quarantined NoF page " << page
                         << " (translation state unknown; page stays pinned)";
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
                // clean up the failing page itself, once.
                const bool clean = unregister_fn_(reinterpret_cast<void*>(page),
                                                  kPageSize) == 0;
                if (clean) {
                    page_regs_.erase(page);
                    // Confirmed-clean failure: roll back this call's pages;
                    // the caller may munmap safely.
                    for (uintptr_t p : touched) {
                        auto& r = page_regs_[p];
                        if (--r.count == 0) {
                            unregister_fn_(reinterpret_cast<void*>(p),
                                           kPageSize);
                            page_regs_.erase(p);
                        }
                    }
                    return ErrorCode::INTERNAL_ERROR;
                }
                // Cleanup unconfirmed: quarantine the failing page and keep
                // this call's charges (per-page refcounts already partition
                // owners). Record coverage up to the failing page so a later
                // re-registration extends over the untouched tail.
                reg.count = 1;
                reg.quarantined = true;
                const size_t covered =
                    static_cast<size_t>((page + kPageSize) - begin);
                size_t& rec = registrations[ptr];
                rec = std::max(rec, covered);
                return ErrorCode::NOF_REGISTRATION_STUCK;
            }
        }
        reg.count++;
        touched.push_back(page);
    }
    size_t& rec = registrations[ptr];
    rec = std::max(rec, size);
    return ErrorCode::OK;
}

bool NofPageRegistry::ReleaseRangeLocked(void* ptr, size_t size) {
    const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t first_page = begin & ~kPageMask;
    const uintptr_t end_page = AlignUpToPage(begin + size);
    bool all_released = true;
    for (uintptr_t page = first_page; page < end_page; page += kPageSize) {
        auto pit = page_regs_.find(page);
        if (pit == page_regs_.end() || pit->second.external) {
            continue;
        }
        if (pit->second.quarantined) {
            // Terminal: never unregistered, the mapping must stay pinned.
            all_released = false;
            continue;
        }
        if (pit->second.count > 1) {
            --pit->second.count;
            continue;
        }
        if (unregister_fn_(reinterpret_cast<void*>(page), kPageSize) == 0) {
            page_regs_.erase(pit);
        } else {
            LOG(ERROR) << "page unregister failed: page="
                       << reinterpret_cast<void*>(page)
                       << "; quarantining page";
            pit->second.quarantined = true;  // count stays 1, terminal
            all_released = false;
        }
    }
    return all_released;
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
        // Record retained: the caller's quarantine path keeps the mapping
        // pinned instead of munmapping a possibly-live translation.
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
