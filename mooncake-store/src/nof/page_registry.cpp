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
                return ErrorCode::INTERNAL_ERROR;
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
    ReleaseRangeLocked(ptr, it->second);
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
    for (const auto& [ptr, size] : owner_it->second) {
        ReleaseRangeLocked(ptr, size);
    }
    owner_regs_.erase(owner_it);
    return ErrorCode::OK;
}

void NofPageRegistry::ReleaseRangeLocked(void* ptr, size_t size) {
    const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t first_page = begin & ~(kPageSize - 1);
    const uintptr_t end_page =
        (begin + size + kPageSize - 1) & ~(kPageSize - 1);
    for (uintptr_t page = first_page; page < end_page; page += kPageSize) {
        auto pit = page_regs_.find(page);
        if (pit == page_regs_.end() || pit->second.external) {
            continue;
        }
        if (--pit->second.count == 0) {
            // Single-page region == legal single-page unmap
            // (NOTIFY_START semantics).
            int rc = unregister_fn_(reinterpret_cast<void*>(page), kPageSize);
            if (rc != 0) {
                LOG(ERROR) << "page unregister failed: page="
                           << reinterpret_cast<void*>(page) << " rc=" << rc;
            }
            page_regs_.erase(pit);
        }
    }
}

}  // namespace mooncake
