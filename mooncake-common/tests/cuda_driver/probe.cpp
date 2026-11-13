#include "cuda_driver.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <type_traits>
#include <vector>

static void check(bool condition, const char* message) {
    if (!condition) {
        std::fprintf(stderr, "FAIL: %s\n", message);
        std::exit(1);
    }
}

static CUresult initialize() { return MOONCAKE_CUDA_SYMBOL(cuInit)(0); }

static void checkErrorMessages(CUresult expected) {
    const char* name = nullptr;
    const char* message = nullptr;
    check(MOONCAKE_CUDA_SYMBOL(cuGetErrorName)(expected, &name) == expected,
          "error-name failure code");
    check(
        MOONCAKE_CUDA_SYMBOL(cuGetErrorString)(expected, &message) == expected,
        "error-string failure code");
    check(name && *name && message && *message, "safe fallback error messages");
    check(MOONCAKE_CUDA_SYMBOL(cuGetErrorString)(expected, nullptr) == expected,
          "null error-string output is safe");
}

int main(int argc, char** argv) {
    check(argc >= 2, "scenario argument");
    if (std::strcmp(argv[1], "import") == 0) {
        check(argc == 3, "import library argument");
        void* library = dlopen(argv[2], RTLD_NOW | RTLD_LOCAL);
        check(library != nullptr,
              "consumer shared library imports without CUDA");
        dlclose(library);
        return 0;
    }
    if (std::strcmp(argv[1], "unavailable") == 0) {
        for (int i = 0; i < 10; ++i) {
            check(initialize() == CUDA_ERROR_SHARED_OBJECT_INIT_FAILED,
                  "unavailable driver returns a typed error");
        }
        checkErrorMessages(CUDA_ERROR_SHARED_OBJECT_INIT_FAILED);
        return 0;
    }
    if (std::strcmp(argv[1], "missing") == 0) {
        check(initialize() == CUDA_SUCCESS, "available symbol still works");
        CUdeviceptr base = 0;
        size_t size = 0;
        for (int i = 0; i < 10; ++i) {
            check(
                MOONCAKE_CUDA_SYMBOL(cuMemGetAddressRange)(&base, &size, 32) ==
                    CUDA_ERROR_SHARED_OBJECT_SYMBOL_NOT_FOUND,
                "missing v2 never falls back to legacy ABI");
        }
        checkErrorMessages(CUDA_ERROR_SHARED_OBJECT_SYMBOL_NOT_FOUND);
        return 0;
    }
    check(std::strcmp(argv[1], "complete") == 0, "known scenario");
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([&] {
            ++ready;
            while (!start.load()) std::this_thread::yield();
            for (int j = 0; j < 100; ++j) {
                check(initialize() == CUDA_SUCCESS,
                      "concurrent initialization");
            }
        });
    }
    while (ready.load() != 16) std::this_thread::yield();
    start = true;
    for (auto& thread : threads) thread.join();

    auto address_range = MOONCAKE_CUDA_SYMBOL(cuMemGetAddressRange);
    static_assert(std::is_same<decltype(address_range),
                               decltype(&::cuMemGetAddressRange)>::value,
                  "preserve the exact header-selected ABI");
    CUdeviceptr base = 0;
    size_t size = 0;
    check(address_range(&base, &size, 64) == CUDA_SUCCESS && base == 48 &&
              size == 8192,
          "resolve v2 address-range symbol");

    auto create_context = MOONCAKE_CUDA_SYMBOL(cuCtxCreate);
    static_assert(
        std::is_same<decltype(create_context), decltype(&::cuCtxCreate)>::value,
        "preserve CUDA 12 and CUDA 13 context signatures");
    CUcontext context = nullptr;
#if CUDA_VERSION >= 13000
    check(create_context(&context, nullptr, 0, 0) == CUDA_SUCCESS,
          "resolve v4 context symbol");
#else
    check(create_context(&context, 0, 0) == CUDA_SUCCESS,
          "resolve v2 context symbol");
#endif
    check(context == reinterpret_cast<CUcontext>(0x1234), "context output");
    return 0;
}
