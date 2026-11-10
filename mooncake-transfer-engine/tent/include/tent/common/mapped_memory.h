#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "tent/common/status.h"

namespace mooncake::tent {

struct EgmNumaInfo {
    int numa_node;
    size_t granularity;
};

// The mapping is kept alive by caller strong references, separate from
// transport caching and lifetime.
// The caller must stop external GPU work before releasing the last reference.
class MappedMemory : public std::enable_shared_from_this<MappedMemory> {
   public:
    MappedMemory(uint64_t address, size_t length,
                 std::function<Status()> cleanup)
        : address_(address), length_(length), cleanup_(std::move(cleanup)) {}
    MappedMemory(const MappedMemory&) = delete;
    MappedMemory& operator=(const MappedMemory&) = delete;
    ~MappedMemory() {
        if (cleanup_) (void)cleanup_();
    }

    uint64_t address() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return address_;
    }
    size_t length() const { return length_; }

    // Explicit close can report failure and retry; the destructor is best
    // effort.
    Status release() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!cleanup_) return Status::OK();
        if (weak_from_this().use_count() > 1)
            return Status::InvalidArgument(
                "mapping still held by an external table/lookup");
        // Once release starts, acquire may no longer reuse it, even if only
        // half of the unmap completed.
        address_ = 0;
        auto status = cleanup_();
        if (status.ok()) cleanup_ = {};
        return status;
    }

   private:
    uint64_t address_;
    size_t length_;
    std::function<Status()> cleanup_;
    mutable std::mutex mutex_;
};

}  // namespace mooncake::tent
