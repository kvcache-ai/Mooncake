// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MOONCAKE_CUDA_DRIVER_H_
#define MOONCAKE_CUDA_DRIVER_H_

#include <cuda.h>
#include <dlfcn.h>

#include <cstdio>

namespace mooncake {
namespace cuda_driver_detail {

// Do not close this handle: cached function pointers can be used during static
// destruction. Including this header alone never loads the driver.
inline void* driverLibrary() {
    static void* const handle = []() -> void* {
        void* library = dlopen("libcuda.so.1", RTLD_NOW | RTLD_LOCAL);
        if (!library) {
            const char* error = dlerror();
            std::fprintf(stderr, "Mooncake: cannot load libcuda.so.1: %s\n",
                         error ? error : "unknown dynamic loader error");
        }
        return library;
    }();
    return handle;
}

template <typename Function, CUresult Error>
struct UnavailableFunction;

template <CUresult Error, typename... Args>
struct UnavailableFunction<CUresult (*)(Args...), Error> {
    static CUresult call(Args...) { return Error; }
};

// cuGetErrorName and cuGetErrorString have this signature. Existing callers
// often print their output without checking the return code, including while
// reporting a driver-load failure. Always give them a usable diagnostic.
template <CUresult Error>
struct UnavailableFunction<CUresult (*)(CUresult, const char**), Error> {
    static CUresult call(CUresult, const char** message) {
        if (message) {
            *message = Error == CUDA_ERROR_SHARED_OBJECT_INIT_FAILED
                           ? "CUDA driver library could not be loaded"
                           : "CUDA driver symbol could not be loaded";
        }
        return Error;
    }
};

template <typename Function>
inline Function resolve(const char* symbol) {
    void* library = driverLibrary();
    if (!library) {
        return &UnavailableFunction<Function,
                                    CUDA_ERROR_SHARED_OBJECT_INIT_FAILED>::call;
    }
    dlerror();
    void* address = dlsym(library, symbol);
    const char* error = dlerror();
    if (!address || error) {
        std::fprintf(stderr,
                     "Mooncake: cannot load CUDA driver symbol %s: %s\n",
                     symbol, error ? error : "null symbol address");
        return &UnavailableFunction<
            Function, CUDA_ERROR_SHARED_OBJECT_SYMBOL_NOT_FOUND>::call;
    }
    return reinterpret_cast<Function>(address);
}

}  // namespace cuda_driver_detail
}  // namespace mooncake

#define MOONCAKE_CUDA_STRINGIFY_IMPL(symbol) #symbol
#define MOONCAKE_CUDA_STRINGIFY(symbol) MOONCAKE_CUDA_STRINGIFY_IMPL(symbol)

// Expand CUDA's ABI aliases before stringifying (for example,
// cuMemGetAddressRange -> cuMemGetAddressRange_v2). Never fall back to an older
// symbol: it may have a different signature. Each call site caches its exact
// function-pointer type, with thread-safe initialization since C++11.
// Usage: MOONCAKE_CUDA_SYMBOL(cuInit)(0), or assign the result to an API table.
#define MOONCAKE_CUDA_SYMBOL(symbol)                                      \
    ([]() -> decltype(&::symbol) {                                        \
        static const auto function =                                      \
            ::mooncake::cuda_driver_detail::resolve<decltype(&::symbol)>( \
                MOONCAKE_CUDA_STRINGIFY(symbol));                         \
        return function;                                                  \
    }())

#endif  // MOONCAKE_CUDA_DRIVER_H_
