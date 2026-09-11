#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_ASSERT_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_ASSERT_CUH

#include <cstdio>

// Debug checks for device-side programming-contract violations. Conditions
// must not have side effects: they are not evaluated when NDEBUG is defined.
// Recoverable runtime failures such as transfer timeouts or unavailable routes
// must use explicit error handling instead.
#define PG_DETAIL_DEVICE_STRINGIFY_IMPL(value) #value
#define PG_DETAIL_DEVICE_STRINGIFY(value) PG_DETAIL_DEVICE_STRINGIFY_IMPL(value)

#if defined(NDEBUG) || defined(USE_MUSA) || defined(USE_MACA)
// Device diagnostics are disabled in release builds and on MUSA/MACA.
#define PG_DEVICE_ASSERT(condition) \
    do {                            \
        (void)sizeof(condition);    \
    } while (false)

#define PG_DEVICE_UNREACHABLE() \
    do {                        \
    } while (false)
#else
namespace mooncake::detail {

[[noreturn]] static __device__ __noinline__ void deviceAssertionFailure(
    const char* message) {
    printf("%s", message);
    __trap();
}

}  // namespace mooncake::detail

#define PG_DEVICE_ASSERT(condition)                             \
    do {                                                        \
        if (!(condition)) {                                     \
            ::mooncake::detail::deviceAssertionFailure(         \
                "PG device assertion failed: " __FILE__         \
                ":" PG_DETAIL_DEVICE_STRINGIFY(                 \
                    __LINE__) ", condition: " #condition "\n"); \
        }                                                       \
    } while (false)

#define PG_DEVICE_UNREACHABLE()                             \
    do {                                                    \
        ::mooncake::detail::deviceAssertionFailure(         \
            "PG device reached unreachable code: " __FILE__ \
            ":" PG_DETAIL_DEVICE_STRINGIFY(__LINE__) "\n"); \
    } while (false)
#endif

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_ASSERT_CUH
