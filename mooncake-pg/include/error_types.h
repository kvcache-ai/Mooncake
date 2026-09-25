#ifndef MOONCAKE_PG_ERROR_TYPES_H
#define MOONCAKE_PG_ERROR_TYPES_H

#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <ylt/util/expected.hpp>

namespace mooncake {

class PGAssertionException : public std::runtime_error {
   public:
    using std::runtime_error::runtime_error;
};

// Keep the order and values synchronized with mooncakePgResult_t.
enum class PGErrorCode : uint8_t {
    InvalidArgument = 1,
    InvalidState = 2,
    NotSupported = 3,
    Timeout = 4,
    ResourceBusy = 5,
    TransferEngineError = 6,
    RpcError = 7,
    SystemError = 8,
    InternalError = 9,
};

struct PGError {
    PGErrorCode code;
    std::string message;
};

template <typename T>
using PGResult = ylt::expected<T, PGError>;

namespace detail {

template <typename... Args>
[[noreturn]] inline void throwPGAssertFailure(Args&&... args) {
    std::ostringstream message;
    (message << ... << std::forward<Args>(args));
    throw PGAssertionException(message.str());
}

template <typename T>
struct IsPGResult : std::false_type {};

template <typename T>
struct IsPGResult<ylt::expected<T, PGError>> : std::true_type {};

template <typename T>
using RemoveCVRef =
    typename std::remove_cv<typename std::remove_reference<T>::type>::type;

template <typename T>
inline constexpr bool is_pg_result_v = IsPGResult<RemoveCVRef<T>>::value;

inline PGError addPGErrorContext(PGError error, std::string_view expression,
                                 std::string_view function,
                                 std::string_view file, int line) {
    const auto separator = file.find_last_of("/\\");
    if (separator != std::string_view::npos) {
        file.remove_prefix(separator + 1);
    }

    try {
        std::string context("\n  while ");
        context.append(expression);
        context.append(" [");
        context.append(function);
        context.append(" @ ");
        context.append(file);
        context.push_back(':');
        context.append(std::to_string(line));
        context.push_back(']');
        error.message.append(context);
    } catch (...) {
        // Preserve the original error when diagnostic enrichment fails.
    }
    return error;
}

}  // namespace detail

inline auto makePGError(PGError error) {
    return ylt::unexpected<PGError>{std::move(error)};
}

inline auto makePGError(PGErrorCode code, std::string message) {
    return makePGError(PGError{code, std::move(message)});
}

}  // namespace mooncake

#define PG_ASSERT(condition, ...)                                       \
    do {                                                                \
        static_assert(                                                  \
            !::mooncake::detail::is_pg_result_v<decltype((condition))>, \
            "PG_ASSERT does not accept PGResult");                      \
        if (!(condition)) {                                             \
            ::mooncake::detail::throwPGAssertFailure(__VA_ARGS__);      \
        }                                                               \
    } while (false)

#define PG_ASSERT_OK(expression)                                               \
    do {                                                                       \
        auto&& pg_result_internal = (expression);                              \
        static_assert(                                                         \
            std::is_same_v<                                                    \
                ::mooncake::detail::RemoveCVRef<decltype(pg_result_internal)>, \
                ::mooncake::PGResult<void>>,                                   \
            "PG_ASSERT_OK requires PGResult<void>");                           \
        if (!pg_result_internal.has_value()) {                                 \
            ::mooncake::detail::throwPGAssertFailure(                          \
                #expression, " failed: ", pg_result_internal.error().message); \
        }                                                                      \
    } while (false)

#define PG_ASSERT_CUDA(expression)                                          \
    do {                                                                    \
        const auto pg_cuda_error_internal = (expression);                   \
        PG_ASSERT(pg_cuda_error_internal == cudaSuccess, #expression,       \
                  " failed: ", cudaGetErrorString(pg_cuda_error_internal)); \
    } while (false)

#define PG_DETAIL_TRY(expression)                                       \
    do {                                                                \
        auto&& pg_result_internal = (expression);                       \
        if (!pg_result_internal.has_value()) {                          \
            return ::mooncake::makePGError(                             \
                ::mooncake::detail::addPGErrorContext(                  \
                    std::move(pg_result_internal).error(), #expression, \
                    __func__, __FILE__, __LINE__));                     \
        }                                                               \
    } while (false)

#define PG_DETAIL_CONCAT_IMPL(left, right) left##right
#define PG_DETAIL_CONCAT(left, right) PG_DETAIL_CONCAT_IMPL(left, right)

#define PG_DETAIL_TRY_ASSIGN_IMPL(result_name, lhs, expression,               \
                                  expression_text)                            \
    auto&& result_name = (expression);                                        \
    if (!result_name.has_value()) {                                           \
        return ::mooncake::makePGError(::mooncake::detail::addPGErrorContext( \
            std::move(result_name).error(), expression_text, __func__,        \
            __FILE__, __LINE__));                                             \
    }                                                                         \
    lhs = std::move(result_name).value()

#define PG_DETAIL_TRY_ASSIGN(lhs, ...)                                         \
    PG_DETAIL_TRY_ASSIGN_IMPL(PG_DETAIL_CONCAT(pg_result_internal_, __LINE__), \
                              lhs, (__VA_ARGS__), #__VA_ARGS__)

#define PG_DETAIL_FIRST(first, ...) first

// Propagate an error with PG_TRY(expression). PG_TRY(lhs, expression) also
// assigns the successful value to lhs, which may be a declaration such as
// `auto value`.
#define PG_TRY(first, ...)                             \
    PG_DETAIL_FIRST(__VA_OPT__(PG_DETAIL_TRY_ASSIGN, ) \
                        PG_DETAIL_TRY)(first __VA_OPT__(, ) __VA_ARGS__)

#define PG_TRY_TE(expression)                                               \
    do {                                                                    \
        const int pg_te_error_internal = (expression);                      \
        if (pg_te_error_internal != 0) {                                    \
            return ::mooncake::makePGError(                                 \
                ::mooncake::PGErrorCode::TransferEngineError,               \
                std::string(#expression) +                                  \
                    " failed, rc=" + std::to_string(pg_te_error_internal)); \
        }                                                                   \
    } while (false)

#define PG_TRY_CUDA(expression)                                                \
    do {                                                                       \
        const auto pg_cuda_error_internal = (expression);                      \
        if (pg_cuda_error_internal != cudaSuccess) {                           \
            return ::mooncake::makePGError(                                    \
                ::mooncake::PGErrorCode::SystemError,                          \
                std::string(#expression) +                                     \
                    " failed: " + cudaGetErrorString(pg_cuda_error_internal)); \
        }                                                                      \
    } while (false)

#define PG_VALIDATE_ARG(condition, message)                           \
    do {                                                              \
        if (!(condition)) {                                           \
            return ::mooncake::makePGError(                           \
                ::mooncake::PGErrorCode::InvalidArgument, (message)); \
        }                                                             \
    } while (false)

#define PG_VALIDATE_STATE(condition, message)                      \
    do {                                                           \
        if (!(condition)) {                                        \
            return ::mooncake::makePGError(                        \
                ::mooncake::PGErrorCode::InvalidState, (message)); \
        }                                                          \
    } while (false)

#endif  // MOONCAKE_PG_ERROR_TYPES_H
