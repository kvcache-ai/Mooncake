#ifndef MOONCAKE_PG_ASSERT_H
#define MOONCAKE_PG_ASSERT_H

#include <sstream>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace mooncake {

class PGAssertionException : public std::runtime_error {
   public:
    using std::runtime_error::runtime_error;
};

namespace detail {

template <typename... Args>
[[noreturn]] inline void throwPGAssertFailure(Args&&... args) {
    std::ostringstream message;
    if constexpr (sizeof...(Args) == 0) {
        message << "PG assertion failed";
    } else {
        (message << ... << std::forward<Args>(args));
    }
    throw PGAssertionException(message.str());
}

// error_types.h specializes this for PGResult without making device callers
// depend on the host error-handling types.
template <typename T>
struct IsPGResult : std::false_type {};

template <typename T>
using RemoveCVRef =
    typename std::remove_cv<typename std::remove_reference<T>::type>::type;

template <typename T>
inline constexpr bool is_pg_result_v = IsPGResult<RemoveCVRef<T>>::value;

}  // namespace detail
}  // namespace mooncake

#define PG_DETAIL_ASSERT_STRINGIFY_IMPL(value) #value
#define PG_DETAIL_ASSERT_STRINGIFY(value) PG_DETAIL_ASSERT_STRINGIFY_IMPL(value)

#if defined(NDEBUG) || ((defined(__CUDA_ARCH__) || defined(__MUSA_ARCH__)) && \
                        (defined(USE_MUSA) || defined(USE_MACA)))
// Diagnostics are disabled in release builds and on MUSA/MACA devices.
#define PG_DETAIL_ASSERT(condition, ...) \
    do {                                 \
        (void)sizeof(condition);         \
    } while (false)
#elif defined(__CUDA_ARCH__)
#include <cstdio>
#include <cuda_alike.h>

namespace mooncake::detail {

[[noreturn]] static __device__ __noinline__ void deviceAssertionFailure(
    const char* message) {
    printf("%s", message);
    __trap();
}

}  // namespace mooncake::detail

// Device diagnostics report the condition and source location. Optional
// stream-formatted messages are host-only and are not evaluated on device.
#define PG_DETAIL_ASSERT(condition, ...)                        \
    do {                                                        \
        if (!(condition)) {                                     \
            ::mooncake::detail::deviceAssertionFailure(         \
                "PG device assertion failed: " __FILE__         \
                ":" PG_DETAIL_ASSERT_STRINGIFY(                 \
                    __LINE__) ", condition: " #condition "\n"); \
        }                                                       \
    } while (false)
#else
#define PG_DETAIL_ASSERT(condition, ...)                           \
    do {                                                           \
        if (!(condition)) {                                        \
            ::mooncake::detail::throwPGAssertFailure(__VA_ARGS__); \
        }                                                          \
    } while (false)
#endif

// Debug checks for host/device programming-contract violations. Conditions
// and diagnostic arguments must not have side effects: NDEBUG disables their
// evaluation. Recoverable runtime failures require explicit error handling.
#define PG_ASSERT(condition, ...)                                       \
    do {                                                                \
        static_assert(                                                  \
            !::mooncake::detail::is_pg_result_v<decltype((condition))>, \
            "PG_ASSERT does not accept PGResult");                      \
        PG_DETAIL_ASSERT(condition, __VA_ARGS__);                       \
    } while (false)

#define PG_UNREACHABLE()                              \
    PG_ASSERT(false, "PG unreachable code: " __FILE__ \
                     ":" PG_DETAIL_ASSERT_STRINGIFY(__LINE__))

#endif  // MOONCAKE_PG_ASSERT_H
