#include "mlu_test_hooks.h"

#include <cnrt.h>
#include <thread>

namespace mooncake::testing {
std::atomic<bool> fail_mlu_h2d{false};
std::atomic<bool> fail_mlu_d2h{false};
std::atomic<bool> fail_mlu_pinned_allocation{false};
std::atomic<size_t> mlu_injected_copy_failures{0};
std::atomic<size_t> mlu_pinned_allocations{0};
std::atomic<size_t> mlu_pinned_frees{0};
std::atomic<size_t> mlu_pinned_free_errors{0};
std::atomic<bool> pause_mlu_h2d{false};
std::atomic<size_t> mlu_h2d_inflight{0};
std::atomic<size_t> mlu_h2d_worker_id{0};
}  // namespace mooncake::testing

extern "C" {
cnrtRet_t __real_cnrtMemcpy(void*, void*, size_t, cnrtMemTransDir_t);
cnrtRet_t __real_cnrtHostMalloc(void**, size_t);
cnrtRet_t __real_cnrtFreeHost(void*);

cnrtRet_t __wrap_cnrtMemcpy(void* dst, void* src, size_t size,
                            cnrtMemTransDir_t direction) {
    using namespace mooncake::testing;
    if ((direction == cnrtMemcpyHostToDev && fail_mlu_h2d.load()) ||
        (direction == cnrtMemcpyDevToHost && fail_mlu_d2h.load())) {
        ++mlu_injected_copy_failures;
        return cnrtErrorCndrvFuncCall;
    }
    if (direction == cnrtMemcpyHostToDev) {
        mlu_h2d_worker_id.store(
            std::hash<std::thread::id>{}(std::this_thread::get_id()));
        ++mlu_h2d_inflight;
        while (pause_mlu_h2d.load()) pause_mlu_h2d.wait(true);
        auto result = __real_cnrtMemcpy(dst, src, size, direction);
        --mlu_h2d_inflight;
        return result;
    }
    return __real_cnrtMemcpy(dst, src, size, direction);
}

cnrtRet_t __wrap_cnrtHostMalloc(void** ptr, size_t size) {
    using namespace mooncake::testing;
    if (fail_mlu_pinned_allocation.load()) return cnrtErrorSysNoMem;
    auto result = __real_cnrtHostMalloc(ptr, size);
    if (result == cnrtSuccess && *ptr) ++mlu_pinned_allocations;
    return result;
}

cnrtRet_t __wrap_cnrtFreeHost(void* ptr) {
    using namespace mooncake::testing;
    auto result = __real_cnrtFreeHost(ptr);
    if (result == cnrtSuccess) {
        if (ptr) ++mlu_pinned_frees;
    } else {
        ++mlu_pinned_free_errors;
    }
    return result;
}
}
