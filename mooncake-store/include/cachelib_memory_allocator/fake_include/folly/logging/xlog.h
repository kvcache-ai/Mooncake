/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdlib>

#include <folly/Portability.h>

/*
 * This file contains the XLOG() and XLOGF() macros.
 *
 * These macros make it easy to use the logging library without having to
 * manually pick log category names.  All XLOG() and XLOGF() statements in a
 * given file automatically use a LogCategory based on the current file name.
 *
 * For instance, in src/foo/bar.cpp, the default log category name will be
 * "src.foo.bar"
 *
 * If desired, the log category name used by XLOG() in a .cpp file may be
 * overridden using XLOG_SET_CATEGORY_NAME() macro.
 */

/**
 * Log a message to this file's default log category.
 *
 * By default the log category name is automatically picked based on the
 * current filename.  In src/foo/bar.cpp the log category name "src.foo.bar"
 * will be used.  In "lib/stuff/foo.h" the log category name will be
 * "lib.stuff.foo"
 *
 * Note that the filename is based on the __FILE__ macro defined by the
 * compiler.  This is typically dependent on the filename argument that you
 * give to the compiler.  For example, if you compile src/foo/bar.cpp by
 * invoking the compiler inside src/foo and only give it "bar.cpp" as an
 * argument, the category name will simply be "bar".  In general XLOG() works
 * best if you always invoke the compiler from the root directory of your
 * project repository.
 */

/*
 * The global value of FOLLY_XLOG_MIN_LEVEL. All the messages logged to
 * XLOG(XXX) with severity less than FOLLY_XLOG_MIN_LEVEL will not be displayed.
 * If it can be determined at compile time that the message will not be printed,
 * the statement will be compiled out.
 * FOLLY_XLOG_MIN_LEVEL should be below FATAL.
 *
 *
 * Example: to strip out messages less than ERR, use the value of ERR below.
 */
#ifndef FOLLY_XLOG_MIN_LEVEL
#define FOLLY_XLOG_MIN_LEVEL MIN_LEVEL
#endif

#define XLOG(level, ...)                   \
  XLOG_IMPL(                               \
      ::folly::LogLevel::level,            \
      ::folly::LogStreamProcessor::APPEND, \
      ##__VA_ARGS__)

/**
 * Log a message if and only if the specified condition predicate evaluates
 * to true. Note that the condition is *only* evaluated if the log-level check
 * passes.
 */
#define XLOG_IF(level, cond, ...)          \
  XLOG_IF_IMPL(                            \
      ::folly::LogLevel::level,            \
      cond,                                \
      ::folly::LogStreamProcessor::APPEND, \
      ##__VA_ARGS__)
/**
 * Log a message to this file's default log category, using a format string.
 */
#define XLOGF(level, fmt, ...)             \
  XLOG_IMPL(                               \
      ::folly::LogLevel::level,            \
      ::folly::LogStreamProcessor::FORMAT, \
      fmt,                                 \
      ##__VA_ARGS__)

/**
 * Log a message using a format string if and only if the specified condition
 * predicate evaluates to true. Note that the condition is *only* evaluated
 * if the log-level check passes.
 */
#define XLOGF_IF(level, cond, fmt, ...)    \
  XLOG_IF_IMPL(                            \
      ::folly::LogLevel::level,            \
      cond,                                \
      ::folly::LogStreamProcessor::FORMAT, \
      fmt,                                 \
      ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except only log a message every @param ms
 * milliseconds.
 *
 * Note that this is threadsafe.
 */
#define XLOG_EVERY_MS(level, ms, ...)                                  \
  XLOG_IF(                                                             \
      level,                                                           \
      [__folly_detail_xlog_ms = ms] {                                  \
        static ::folly::logging::IntervalRateLimiter                   \
            folly_detail_xlog_limiter(                                 \
                1, std::chrono::milliseconds(__folly_detail_xlog_ms)); \
        return folly_detail_xlog_limiter.check();                      \
      }(),                                                             \
      ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except only log a message every @param ms
 * milliseconds and if the specified condition predicate evaluates to true.
 *
 * Note that this is threadsafe.
 */
#define XLOG_EVERY_MS_IF(level, cond, ms, ...)                               \
  XLOG_IF(                                                                   \
      level,                                                                 \
      (cond) &&                                                              \
          [__folly_detail_xlog_ms = ms] {                                    \
            static ::folly::logging::IntervalRateLimiter                     \
                folly_detail_xlog_limiter(                                   \
                    1, ::std::chrono::milliseconds(__folly_detail_xlog_ms)); \
            return folly_detail_xlog_limiter.check();                        \
          }(),                                                               \
      ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except log a message if the specified condition
 * predicate evaluates to true or every @param ms milliseconds
 *
 * Note that this is threadsafe.
 */
#define XLOG_EVERY_MS_OR(level, cond, ms, ...)                               \
  XLOG_IF(                                                                   \
      level,                                                                 \
      (cond) ||                                                              \
          [__folly_detail_xlog_ms = ms] {                                    \
            static ::folly::logging::IntervalRateLimiter                     \
                folly_detail_xlog_limiter(                                   \
                    1, ::std::chrono::milliseconds(__folly_detail_xlog_ms)); \
            return folly_detail_xlog_limiter.check();                        \
          }(),                                                               \
      ##__VA_ARGS__)

/**
 * Similar to XLOGF(...) except only log a message every @param ms
 * milliseconds and if the specified condition predicate evaluates to true.
 *
 * Note that this is threadsafe.
 */
#define XLOGF_EVERY_MS_IF(level, cond, ms, fmt, ...)                         \
  XLOGF_IF(                                                                  \
      level,                                                                 \
      (cond) &&                                                              \
          [__folly_detail_xlog_ms = ms] {                                    \
            static ::folly::logging::IntervalRateLimiter                     \
                folly_detail_xlog_limiter(                                   \
                    1, ::std::chrono::milliseconds(__folly_detail_xlog_ms)); \
            return folly_detail_xlog_limiter.check();                        \
          }(),                                                               \
      fmt,                                                                   \
      ##__VA_ARGS__)

/**
 * Similar to XLOGF(...) except only log a message every @param ms
 * milliseconds.
 *
 * Note that this is threadsafe.
 */
#define XLOGF_EVERY_MS(level, ms, fmt, ...) \
  XLOGF_EVERY_MS_IF(level, true, ms, fmt, ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except only log a message every @param n
 * invocations, approximately.
 *
 * The internal counter is process-global and threadsafe but, to
 * to avoid the performance degradation of atomic-rmw operations,
 * increments are non-atomic. Some increments may be missed under
 * contention, leading to possible over-logging or under-logging
 * effects.
 */
#define XLOG_EVERY_N(level, n, ...)                                       \
  XLOG_IF(                                                                \
      level,                                                              \
      [&] {                                                               \
        struct folly_detail_xlog_tag {};                                  \
        return ::folly::detail::xlogEveryNImpl<folly_detail_xlog_tag>(n); \
      }(),                                                                \
      ##__VA_ARGS__)

/**
 * Similar to XLOGF(...) except only log a message every @param n
 * invocations, approximately.
 *
 * The internal counter is process-global and threadsafe but, to
 * to avoid the performance degradation of atomic-rmw operations,
 * increments are non-atomic. Some increments may be missed under
 * contention, leading to possible over-logging or under-logging
 * effects.
 */
#define XLOGF_EVERY_N(level, n, fmt, ...)                                 \
  XLOGF_IF(                                                               \
      level,                                                              \
      [&] {                                                               \
        struct folly_detail_xlog_tag {};                                  \
        return ::folly::detail::xlogEveryNImpl<folly_detail_xlog_tag>(n); \
      }(),                                                                \
      fmt,                                                                \
      ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except only log a message every @param n
 * invocations, approximately, and if the specified condition predicate
 * evaluates to true.
 *
 * The internal counter is process-global and threadsafe but, to
 * to avoid the performance degradation of atomic-rmw operations,
 * increments are non-atomic. Some increments may be missed under
 * contention, leading to possible over-logging or under-logging
 * effects.
 */
#define XLOG_EVERY_N_IF(level, cond, n, ...)                                  \
  XLOG_IF(                                                                    \
      level,                                                                  \
      (cond) &&                                                               \
          [&] {                                                               \
            struct folly_detail_xlog_tag {};                                  \
            return ::folly::detail::xlogEveryNImpl<folly_detail_xlog_tag>(n); \
          }(),                                                                \
      ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except it logs a message if the condition predicate
 * evalutes to true or approximately every @param n invocations
 *
 * The internal counter is process-global and threadsafe but, to
 * to avoid the performance degradation of atomic-rmw operations,
 * increments are non-atomic. Some increments may be missed under
 * contention, leading to possible over-logging or under-logging
 * effects.
 */
#define XLOG_EVERY_N_OR(level, cond, n, ...)                                  \
  XLOG_IF(                                                                    \
      level,                                                                  \
      (cond) ||                                                               \
          [&] {                                                               \
            struct folly_detail_xlog_tag {};                                  \
            return ::folly::detail::xlogEveryNImpl<folly_detail_xlog_tag>(n); \
          }(),                                                                \
      ##__VA_ARGS__)

/**
 * Similar to XLOGF(...) except only log a message every @param n
 * invocations, approximately, and if the specified condition predicate
 * evaluates to true.
 *
 * The internal counter is process-global and threadsafe but, to
 * to avoid the performance degradation of atomic-rmw operations,
 * increments are non-atomic. Some increments may be missed under
 * contention, leading to possible over-logging or under-logging
 * effects.
 */
#define XLOGF_EVERY_N_IF(level, cond, n, fmt, ...)                            \
  XLOGF_IF(                                                                   \
      level,                                                                  \
      (cond) &&                                                               \
          [&] {                                                               \
            struct folly_detail_xlog_tag {};                                  \
            return ::folly::detail::xlogEveryNImpl<folly_detail_xlog_tag>(n); \
          }(),                                                                \
      fmt,                                                                    \
      ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except only log a message every @param n
 * invocations, exactly.
 *
 * The internal counter is process-global and threadsafe and
 * increments are atomic. The over-logging and under-logging
 * schenarios of XLOG_EVERY_N(...) are avoided, traded off for
 * the performance degradation of atomic-rmw operations.
 */
#define XLOG_EVERY_N_EXACT(level, n, ...)                                      \
  XLOG_IF(                                                                     \
      level,                                                                   \
      [&] {                                                                    \
        struct folly_detail_xlog_tag {};                                       \
        return ::folly::detail::xlogEveryNExactImpl<folly_detail_xlog_tag>(n); \
      }(),                                                                     \
      ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except only log a message every @param n
 * invocations per thread.
 *
 * The internal counter is thread-local, avoiding the contention
 * which the XLOG_EVERY_N variations which use a global counter
 * may suffer. If a given XLOG_EVERY_N or variation expansion is
 * encountered concurrently by multiple threads in a hot code
 * path and the global counter in the expansion is observed to
 * be contended, then switching to XLOG_EVERY_N_THREAD can help.
 *
 * Every thread that invokes this expansion has a counter for
 * this expansion. The internal counters are all stored in a
 * single thread-local map to control TLS overhead, at the cost
 * of a small runtime performance hit.
 */
#define XLOG_EVERY_N_THREAD(level, n, ...)                                   \
  XLOG_IF(                                                                   \
      level,                                                                 \
      [&] {                                                                  \
        struct folly_detail_xlog_tag {};                                     \
        return ::folly::detail::xlogEveryNThreadImpl<folly_detail_xlog_tag>( \
            n);                                                              \
      }(),                                                                   \
      ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except only log at most @param count messages
 * per @param ms millisecond interval.
 *
 * The internal counters are process-global and threadsafe.
 */
#define XLOG_N_PER_MS(level, count, ms, ...)               \
  XLOG_IF(                                                 \
      level,                                               \
      [] {                                                 \
        static ::folly::logging::IntervalRateLimiter       \
            folly_detail_xlog_limiter(                     \
                (count), ::std::chrono::milliseconds(ms)); \
        return folly_detail_xlog_limiter.check();          \
      }(),                                                 \
      ##__VA_ARGS__)

/**
 * Similar to XLOG(...) except only log a message the first n times, exactly.
 *
 * The internal counter is process-global and threadsafe and exchanges are
 * atomic.
 */
#define XLOG_FIRST_N(level, n, ...)                                            \
  XLOG_IF(                                                                     \
      level,                                                                   \
      [&] {                                                                    \
        struct folly_detail_xlog_tag {};                                       \
        return ::folly::detail::xlogFirstNExactImpl<folly_detail_xlog_tag>(n); \
      }(),                                                                     \
      ##__VA_ARGS__)

/**
 * FOLLY_XLOG_STRIP_PREFIXES can be defined to a string containing a
 * colon-separated list of directory prefixes to strip off from the filename
 * before using it to compute the log category name.
 *
 * If this is defined, use xlogStripFilename() to strip off directory prefixes;
 * otherwise just use __FILE__ literally.  xlogStripFilename() is a constexpr
 * expression so that this stripping can be performed fully at compile time.
 * (There is no guarantee that the compiler will evaluate it at compile time,
 * though.)
 */
#ifdef FOLLY_XLOG_STRIP_PREFIXES
#define XLOG_FILENAME        \
  (static_cast<char const*>( \
      ::folly::xlogStripFilename(__FILE__, FOLLY_XLOG_STRIP_PREFIXES)))
#else
#define XLOG_FILENAME (static_cast<char const*>(__FILE__))
#endif

#define XLOG_IMPL(level, type, ...) \
  XLOG_ACTUAL_IMPL(                 \
      level, true, ::folly::isLogLevelFatal(level), type, ##__VA_ARGS__)

#define XLOG_IF_IMPL(level, cond, type, ...) \
  XLOG_ACTUAL_IMPL(level, cond, false, type, ##__VA_ARGS__)

/**
 * Helper macro used to implement XLOG() and XLOGF()
 *
 * Beware that the level argument is evaluated twice.
 *
 * This macro is somewhat tricky:
 *
 * - In order to support streaming argument support (with the << operator),
 *   the macro must expand to a single ternary ? expression.  This is the only
 *   way we can avoid evaluating the log arguments if the log check fails,
 *   and still have the macro behave as expected when used as the body of an if
 *   or else statement.
 *
 * - We need to store some static-scope local state in order to track the
 *   LogCategory to use.  This is a bit tricky to do and still meet the
 *   requirements of being a single expression, but fortunately static
 *   variables inside a lambda work for this purpose.
 *
 *   Inside header files, each XLOG() statement defines two static variables:
 *   - the LogLevel for this category
 *   - a pointer to the LogCategory
 *
 *   If the __INCLUDE_LEVEL__ macro is available (both gcc and clang support
 *   this), then we we can detect when we are inside a .cpp file versus a
 *   header file.  If we are inside a .cpp file, we can avoid declaring these
 *   variables once per XLOG() statement, and instead we only declare one copy
 *   of these variables for the entire file.
 *
 * - We want to make sure this macro is safe to use even from inside static
 *   initialization code that runs before main.  We also want to make the log
 *   admittance check as cheap as possible, so that disabled debug logs have
 *   minimal overhead, and can be left in place even in performance senstive
 *   code.
 *
 *   In order to do this, we rely on zero-initialization of variables with
 *   static storage duration.  The LogLevel variable will always be
 *   0-initialized before any code runs.  Therefore the very first time an
 *   XLOG() statement is hit the initial log level check will always pass
 *   (since all level values are greater or equal to 0), and we then do a
 *   second check to see if the log level and category variables need to be
 *   initialized.  On all subsequent calls, disabled log statements can be
 *   skipped with just a single check of the LogLevel.
 */
#define XLOG_ACTUAL_IMPL(level, cond, always_fatal, type, ...) static_cast<void>(0)

/**
 * Check if an XLOG() statement with the given log level would be enabled.
 *
 * The level parameter must be an unqualified LogLevel enum value.
 */
#define XLOG_IS_ON(level) XLOG_IS_ON_IMPL(::folly::LogLevel::level)

/**
 * Expects a fully qualified LogLevel enum value.
 *
 * This helper macro invokes XLOG_IS_ON_IMPL_HELPER() to perform the real
 * log level check, with a couple additions:
 * - If the log level is less than FOLLY_XLOG_MIN_LEVEL it evaluates to false
 *   to allow the compiler to completely optimize out the check and log message
 *   if the level is less than this compile-time fixed constant.
 * - If the log level is fatal, this has an extra check at the end to ensure the
 *   compiler can detect that it always evaluates to true.  This helps the
 *   compiler detect that statements like XCHECK(false) never return.  Note that
 *   XLOG_IS_ON_IMPL_HELPER() must still be invoked first for fatal log levels
 *   in order to initialize folly::detail::custom::xlogFileScopeInfo.
 */
#define XLOG_IS_ON_IMPL(level)                              \
  ((((level) >= ::folly::LogLevel::FOLLY_XLOG_MIN_LEVEL) && \
    XLOG_IS_ON_IMPL_HELPER(level)) ||                       \
   ((level) >= ::folly::kMinFatalLogLevel))

/**
 * Helper macro to implement of XLOG_IS_ON()
 *
 * This macro is used in the XLOG() implementation, and therefore must be as
 * cheap as possible.  It stores the category's LogLevel as a local static
 * variable.  The very first time this macro is evaluated it will look up the
 * correct LogCategory and initialize the LogLevel.  Subsequent calls then
 * are only a single conditional log level check.
 *
 * The LogCategory object keeps track of this local LogLevel variable and
 * automatically keeps it up-to-date when the category's effective level is
 * changed.
 *
 * See XlogLevelInfo for the implementation details.
 */
#define XLOG_IS_ON_IMPL_HELPER(level)                           \
  ([] {                                                         \
    static ::folly::XlogLevelInfo<XLOG_IS_IN_HEADER_FILE>       \
        folly_detail_xlog_level;                                \
    constexpr auto* folly_detail_xlog_filename = XLOG_FILENAME; \
    constexpr folly::StringPiece folly_detail_xlog_category =   \
        ::folly::detail::custom::getXlogCategoryName(           \
            folly_detail_xlog_filename, 0);                     \
    return folly_detail_xlog_level.check(                       \
        (level),                                                \
        folly_detail_xlog_category,                             \
        ::folly::detail::custom::isXlogCategoryOverridden(0),   \
        &::folly::detail::custom::xlogFileScopeInfo);           \
  }())

/**
 * Get the name of the log category that will be used by XLOG() statements
 * in this file.
 */
#define XLOG_GET_CATEGORY_NAME()                                        \
  (::folly::detail::custom::isXlogCategoryOverridden(0)                 \
       ? ::folly::detail::custom::getXlogCategoryName(XLOG_FILENAME, 0) \
       : ::folly::getXlogCategoryNameForFile(XLOG_FILENAME))

/**
 * Get a pointer to the LogCategory that will be used by XLOG() statements in
 * this file.
 *
 * This is just a small wrapper around a LoggerDB::getCategory() call.
 * This must be implemented as a macro since it uses __FILE__, and that must
 * expand to the correct filename based on where the macro is used.
 */
#define XLOG_GET_CATEGORY() \
  (::folly::LoggerDB::get().getCategory(XLOG_GET_CATEGORY_NAME()))

/**
 * XLOG_SET_CATEGORY_NAME() can be used to explicitly define the log category
 * name used by all XLOG() and XLOGF() calls in this translation unit.
 *
 * This overrides the default behavior of picking a category name based on the
 * current filename.
 *
 * This should be used at the top-level scope in a .cpp file, before any XLOG()
 * or XLOGF() macros have been used in the file.
 *
 * XLOG_SET_CATEGORY_NAME() cannot be used inside header files.
 */
#ifdef __INCLUDE_LEVEL__
#define XLOG_SET_CATEGORY_CHECK \
  static_assert(                \
      __INCLUDE_LEVEL__ == 0,   \
      "XLOG_SET_CATEGORY_NAME() should not be used in header files");
#else
#define XLOG_SET_CATEGORY_CHECK
#endif

#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ < 9
// gcc 8.x crashes with an internal compiler error when trying to evaluate
// getXlogCategoryName() in a constexpr expression.  Keeping category name
// constexpr is important for performance, and XLOG_SET_CATEGORY_NAME() is
// fairly rarely used, so simply let it be a no-op if compiling with older
// versions of gcc.
#define XLOG_SET_CATEGORY_NAME(category)
#else
#define XLOG_SET_CATEGORY_NAME(category)                                     \
  namespace folly {                                                          \
  namespace detail {                                                         \
  namespace custom {                                                         \
  namespace {                                                                \
  struct xlog_correct_usage;                                                 \
  static_assert(                                                             \
      ::std::is_same<                                                        \
          xlog_correct_usage,                                                \
          ::folly::detail::custom::xlog_correct_usage>::value,               \
      "XLOG_SET_CATEGORY_NAME() should not be used within namespace scope"); \
  XLOG_SET_CATEGORY_CHECK                                                    \
  FOLLY_CONSTEVAL inline StringPiece getXlogCategoryName(StringPiece, int) { \
    return category;                                                         \
  }                                                                          \
  FOLLY_CONSTEVAL inline bool isXlogCategoryOverridden(int) {                \
    return true;                                                             \
  }                                                                          \
  }                                                                          \
  }                                                                          \
  }                                                                          \
  }
#endif

/**
 * Assert that a condition is true.
 *
 * This crashes the program with an XLOG(FATAL) message if the condition is
 * false.  Unlike assert() CHECK statements are always enabled, regardless of
 * the setting of NDEBUG.
 */
#define XCHECK(cond, ...)         \
  XLOG_IF(                        \
      FATAL,                      \
      FOLLY_UNLIKELY(!(cond)),    \
      "Check failed: " #cond " ", \
      ##__VA_ARGS__)

#define XCHECK_OP(op, arg1, arg2, ...) static_cast<void>(0)

/**
 * Assert a comparison relationship between two arguments.
 *
 * If the comparison check fails the values of both expressions being compared
 * will be included in the failure message.  This is the main benefit of using
 * these specific comparison macros over XCHECK().  XCHECK() will simply log
 * that the expression evaluated was false, while these macros include the
 * specific values being compared.
 */
#define XCHECK_EQ(arg1, arg2, ...) XCHECK_OP(==, arg1, arg2, ##__VA_ARGS__)
#define XCHECK_NE(arg1, arg2, ...) XCHECK_OP(!=, arg1, arg2, ##__VA_ARGS__)
#define XCHECK_LT(arg1, arg2, ...) XCHECK_OP(<, arg1, arg2, ##__VA_ARGS__)
#define XCHECK_GT(arg1, arg2, ...) XCHECK_OP(>, arg1, arg2, ##__VA_ARGS__)
#define XCHECK_LE(arg1, arg2, ...) XCHECK_OP(<=, arg1, arg2, ##__VA_ARGS__)
#define XCHECK_GE(arg1, arg2, ...) XCHECK_OP(>=, arg1, arg2, ##__VA_ARGS__)

/**
 * Assert that a condition is true in debug builds only.
 *
 * When NDEBUG is not defined this behaves like XCHECK().
 * When NDEBUG is defined XDCHECK statements are not evaluated and will never
 * log.
 *
 * You can use `XLOG_IF(DFATAL, condition)` instead if you want the condition to
 * be evaluated in release builds but log a message without crashing the
 * program.
 */
#define XDCHECK(cond, ...) \
  (!::folly::kIsDebug) ? static_cast<void>(0) : XCHECK(cond, ##__VA_ARGS__)

/*
 * It would be nice to rely solely on folly::kIsDebug here rather than NDEBUG.
 * However doing so would make the code substantially more complicated.  It is
 * much simpler to simply change the definition of XDCHECK_OP() based on NDEBUG.
 */
#ifdef NDEBUG
#define XDCHECK_OP(op, arg1, arg2, ...) \
  while (false)                         \
  XCHECK_OP(op, arg1, arg2, ##__VA_ARGS__)
#else
#define XDCHECK_OP(op, arg1, arg2, ...) XCHECK_OP(op, arg1, arg2, ##__VA_ARGS__)
#endif

/**
 * Assert a comparison relationship between two arguments in debug builds.
 *
 * When NDEBUG is not set these macros behaves like the corresponding
 * XCHECK_XX() versions (XCHECK_EQ(), XCHECK_NE(), etc).
 *
 * When NDEBUG is defined these statements are not evaluated and will never log.
 */
#define XDCHECK_EQ(arg1, arg2, ...) XDCHECK_OP(==, arg1, arg2, ##__VA_ARGS__)
#define XDCHECK_NE(arg1, arg2, ...) XDCHECK_OP(!=, arg1, arg2, ##__VA_ARGS__)
#define XDCHECK_LT(arg1, arg2, ...) XDCHECK_OP(<, arg1, arg2, ##__VA_ARGS__)
#define XDCHECK_GT(arg1, arg2, ...) XDCHECK_OP(>, arg1, arg2, ##__VA_ARGS__)
#define XDCHECK_LE(arg1, arg2, ...) XDCHECK_OP(<=, arg1, arg2, ##__VA_ARGS__)
#define XDCHECK_GE(arg1, arg2, ...) XDCHECK_OP(>=, arg1, arg2, ##__VA_ARGS__)

/**
 * XLOG_IS_IN_HEADER_FILE evaluates to false if we can definitively tell if we
 * are not in a header file.  Otherwise, it evaluates to true.
 */
#ifdef __INCLUDE_LEVEL__
#define XLOG_IS_IN_HEADER_FILE bool(__INCLUDE_LEVEL__ > 0)
#else
// Without __INCLUDE_LEVEL__ we canot tell if we are in a header file or not,
// and must pessimstically assume we are always in a header file.
#define XLOG_IS_IN_HEADER_FILE true
#endif
