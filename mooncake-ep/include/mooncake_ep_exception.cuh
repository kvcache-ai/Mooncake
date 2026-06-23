#pragma once

#include <string>
#include <exception>

#ifndef EP_STATIC_ASSERT
#define EP_STATIC_ASSERT(cond, reason) static_assert(cond, reason)
#endif

class EPException : public std::exception {
   private:
    std::string message = {};

   public:
    explicit EPException(const char* name, const char* file, const int line,
                         const std::string& error) {
        message = std::string("Failed: ") + name + " error " + file + ":" +
                  std::to_string(line) + " '" + error + "'";
    }

    const char* what() const noexcept override { return message.c_str(); }
};

#ifndef CUDA_CHECK
#define CUDA_CHECK(cmd)                                   \
    do {                                                  \
        cudaError_t e = (cmd);                            \
        if (e != cudaSuccess) {                           \
            throw EPException("CUDA", __FILE__, __LINE__, \
                              cudaGetErrorString(e));     \
        }                                                 \
    } while (0)
#endif

#ifndef EP_HOST_ASSERT
#define EP_HOST_ASSERT(cond)                                           \
    do {                                                               \
        if (not(cond)) {                                               \
            throw EPException("Assertion", __FILE__, __LINE__, #cond); \
        }                                                              \
    } while (0)
#endif

#ifndef EP_DEVICE_ASSERT
#ifdef MOONCAKE_EP_USE_MUSA
// MUSA SDK 4.3.x can turn kernels that merely contain a device-side __trap()
// branch into illegal memory accesses, even when the assertion condition is
// true.  Keep these invariants as host/static checks on MUSA builds.
#define EP_DEVICE_ASSERT(cond) \
    do {                       \
        (void)sizeof(cond);    \
    } while (0)
#else
#define EP_DEVICE_ASSERT(cond)                                           \
    do {                                                                 \
        if (not(cond)) {                                                 \
            printf("Assertion failed: %s:%d, condition: %s\n", __FILE__, \
                   __LINE__, #cond);                                     \
            __trap();                                                    \
        }                                                                \
    } while (0)
#endif
#endif
