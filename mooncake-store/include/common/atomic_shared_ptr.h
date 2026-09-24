#pragma once

#include <atomic>
#include <memory>
#include <version>

namespace mooncake {

// Atomic load/store of a shared_ptr, for state that a lock holder replaces
// while readers copy it concurrently.
//
// std::atomic<std::shared_ptr<T>> only reached libstdc++ in GCC 12, and the
// project still builds on GCC 11 (Ubuntu 22.04). There the C++11 free
// functions give the same semantics on a plain shared_ptr; they are
// deprecated exactly where the standard specialization takes over, so no
// translation unit ever sees both.
#ifdef __cpp_lib_atomic_shared_ptr

template <typename T>
using AtomicSharedPtr = std::atomic<std::shared_ptr<T>>;

#else

template <typename T>
class AtomicSharedPtr {
   public:
    AtomicSharedPtr() noexcept = default;
    AtomicSharedPtr(std::shared_ptr<T> desired) noexcept
        : value_(std::move(desired)) {}
    AtomicSharedPtr(const AtomicSharedPtr&) = delete;
    AtomicSharedPtr& operator=(const AtomicSharedPtr&) = delete;

    [[nodiscard]] std::shared_ptr<T> load(
        std::memory_order order = std::memory_order_seq_cst) const noexcept {
        return std::atomic_load_explicit(&value_, order);
    }

    void store(std::shared_ptr<T> desired,
               std::memory_order order = std::memory_order_seq_cst) noexcept {
        std::atomic_store_explicit(&value_, std::move(desired), order);
    }

   private:
    std::shared_ptr<T> value_;
};

#endif

}  // namespace mooncake
