#pragma once

#include <atomic>
#include <cstddef>

namespace mooncake::testing {

// Linker wrappers used only by the MLU test executables. No production hooks.
extern std::atomic<bool> fail_mlu_h2d;
extern std::atomic<bool> fail_mlu_d2h;
extern std::atomic<bool> fail_mlu_pinned_allocation;
extern std::atomic<size_t> mlu_injected_copy_failures;
extern std::atomic<size_t> mlu_pinned_allocations;
extern std::atomic<size_t> mlu_pinned_frees;
extern std::atomic<size_t> mlu_pinned_free_errors;

class ScopedMluFailure {
   public:
    explicit ScopedMluFailure(std::atomic<bool>& flag) : flag_(flag) {
        flag_.store(true);
    }
    ~ScopedMluFailure() { flag_.store(false); }
    ScopedMluFailure(const ScopedMluFailure&) = delete;
    ScopedMluFailure& operator=(const ScopedMluFailure&) = delete;

   private:
    std::atomic<bool>& flag_;
};

}  // namespace mooncake::testing
