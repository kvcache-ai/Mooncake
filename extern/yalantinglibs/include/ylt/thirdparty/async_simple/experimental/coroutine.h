// -*- C++ -*-
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef ASYNC_SIMPLE_EXPERIMENTAL_COROUTINE
#define ASYNC_SIMPLE_EXPERIMENTAL_COROUTINE

/**
    experimental/coroutine synopsis

// C++next

namespace std {
namespace experimental {
inline namespace coroutines_v1 {

  // 18.11.1 coroutine traits
template <typename R, typename... ArgTypes>
class coroutine_traits;
// 18.11.2 coroutine handle
template <typename Promise = void>
class coroutine_handle;
// 18.11.2.7 comparison operators:
bool operator==(coroutine_handle<> x, coroutine_handle<> y) _NOEXCEPT;
bool operator!=(coroutine_handle<> x, coroutine_handle<> y) _NOEXCEPT;
bool operator<(coroutine_handle<> x, coroutine_handle<> y) _NOEXCEPT;
bool operator<=(coroutine_handle<> x, coroutine_handle<> y) _NOEXCEPT;
bool operator>=(coroutine_handle<> x, coroutine_handle<> y) _NOEXCEPT;
bool operator>(coroutine_handle<> x, coroutine_handle<> y) _NOEXCEPT;
// 18.11.3 trivial awaitables
struct suspend_never;
struct suspend_always;
// 18.11.2.8 hash support:
template <class T> struct hash;
template <class P> struct hash<coroutine_handle<P>>;

} // namespace coroutines_v1
} // namespace experimental
} // namespace std

 */

#ifndef ASYNC_SIMPLE_USE_MODULES
#if __has_include(<version>)
// Use <version> to detect standard library. In case libstdc++ doesn't implement
// <version>, it shouldn't own <coroutine> too.
#include <version>
#endif

// clang couldn't compile <coroutine> in libstdc++. In this case,
// we could only use self-provided coroutine header.
// Note: the <coroutine> header in libc++ is available for both clang and gcc.
// And the outdated <experimental/coroutine> is available for clang only(need to
// exclude msvc).
#if (__cplusplus <= 201703L && !defined(_MSC_VER)) || \
    (defined(__clang__) && defined(__GLIBCXX__) && \
     (__GLIBCXX__ < 20210408))
#define USE_SELF_DEFINED_COROUTINE
#endif

#if __has_include(<coroutine>) && !defined(USE_SELF_DEFINED_COROUTINE)
#include <coroutine>
#define HAS_NON_EXPERIMENTAL_COROUTINE
#elif __has_include(<experimental/coroutine>)
#include <experimental/coroutine>
#else

#if defined(__cpp_lib_three_way_comparison) && \
    defined(__cpp_impl_three_way_comparison)
#define HAS_THREE_WAY_COMPARISON
#endif

#include <cassert>
#if defined(HAS_THREE_WAY_COMPARISON)
#include <compare>
#endif
#include <cstddef>
#include <functional>
#include <memory>  // for hash<T*>
#include <new>
#include <type_traits>

#if defined(__cpp_impl_coroutine) && __cpp_impl_coroutine >= 201902L
#define HAS_NON_EXPERIMENTAL_COROUTINE
#endif

#ifdef HAS_NON_EXPERIMENTAL_COROUTINE
#define STD_CORO std
#else
#define STD_CORO std::experimental
#endif

namespace async_simple {
namespace detail {

template <class>
struct __void_t {
    typedef void type;
};

}  // namespace detail
}  // namespace async_simple

namespace STD_CORO {

template <class _Tp, class = void>
struct __coroutine_traits_sfinae {};

template <class _Tp>
struct __coroutine_traits_sfinae<_Tp, typename async_simple::detail::__void_t<
                                          typename _Tp::promise_type>::type> {
    using promise_type = typename _Tp::promise_type;
};

template <typename _Ret, typename... _Args>
struct coroutine_traits : public __coroutine_traits_sfinae<_Ret> {};

template <typename _Promise = void>
class coroutine_handle;

template <>
class coroutine_handle<void> {
public:
    // [coroutine.handle.con], construct/reset
    constexpr coroutine_handle() noexcept = default;

    constexpr coroutine_handle(nullptr_t) noexcept {}

    coroutine_handle& operator=(nullptr_t) noexcept {
        __handle_ = nullptr;
        return *this;
    }

    // [coroutine.handle.export.import], export/import
    constexpr void* address() const noexcept { return __handle_; }

    static constexpr coroutine_handle from_address(void* __addr) noexcept {
        coroutine_handle __tmp;
        __tmp.__handle_ = __addr;
        return __tmp;
    }

    // [coroutine.handle.observers], observers
    constexpr explicit operator bool() const noexcept {
        return __handle_ != nullptr;
    }

    bool done() const {
        assert(__is_suspended() &&
               "done() can be called only on suspended coroutines");
        return __builtin_coro_done(__handle_);
    }

    // [coroutine.handle.resumption], resumption
    void operator()() const { resume(); }

    void resume() const {
        assert(__is_suspended() &&
               "resume() can be called only on suspended coroutines");
        assert(!done() &&
               "resume() has undefined behavior when the coroutine is done");
        __builtin_coro_resume(__handle_);
    }

    void destroy() const {
        assert(__is_suspended() &&
               "destroy() can be called only on suspended coroutines");
        __builtin_coro_destroy(__handle_);
    }

private:
    bool __is_suspended() const {
        // FIXME actually implement a check for if the coro is suspended.
        return __handle_ != nullptr;
    }

    void* __handle_ = nullptr;
};

// [coroutine.handle.compare]
#if defined(HAS_THREE_WAY_COMPARISON)
inline constexpr bool operator==(coroutine_handle<> __x,
                                 coroutine_handle<> __y) noexcept {
    return __x.address() == __y.address();
}
inline constexpr strong_ordering operator<=>(coroutine_handle<> __x,
                                             coroutine_handle<> __y) noexcept {
    return compare_three_way()(__x.address(), __y.address());
}
#else
inline bool operator==(coroutine_handle<> __x,
                       coroutine_handle<> __y) noexcept {
    return __x.address() == __y.address();
}
inline bool operator!=(coroutine_handle<> __x,
                       coroutine_handle<> __y) noexcept {
    return !(__x == __y);
}
inline bool operator<(coroutine_handle<> __x, coroutine_handle<> __y) noexcept {
    return std::less<void*>()(__x.address(), __y.address());
}
inline bool operator>(coroutine_handle<> __x, coroutine_handle<> __y) noexcept {
    return __y < __x;
}
inline bool operator<=(coroutine_handle<> __x,
                       coroutine_handle<> __y) noexcept {
    return !(__x > __y);
}
inline bool operator>=(coroutine_handle<> __x,
                       coroutine_handle<> __y) noexcept {
    return !(__x < __y);
}
#endif  // HAS_THREE_WAY_COMPARISON

template <typename _Promise>
class coroutine_handle {
public:
    // [coroutine.handle.con], construct/reset
    constexpr coroutine_handle() noexcept = default;

    constexpr coroutine_handle(nullptr_t) noexcept {}

    static coroutine_handle from_promise(_Promise& __promise) {
        using _RawPromise = typename remove_cv<_Promise>::type;
        coroutine_handle __tmp;
        __tmp.__handle_ = __builtin_coro_promise(
            std::addressof(const_cast<_RawPromise&>(__promise)),
            alignof(_Promise), true);
        return __tmp;
    }

    coroutine_handle& operator=(nullptr_t) noexcept {
        __handle_ = nullptr;
        return *this;
    }

    // [coroutine.handle.export.import], export/import
    constexpr void* address() const noexcept { return __handle_; }

    static constexpr coroutine_handle from_address(void* __addr) noexcept {
        coroutine_handle __tmp;
        __tmp.__handle_ = __addr;
        return __tmp;
    }

    // [coroutine.handle.conv], conversion
    constexpr operator coroutine_handle<>() const noexcept {
        return coroutine_handle<>::from_address(address());
    }

    // [coroutine.handle.observers], observers
    constexpr explicit operator bool() const noexcept {
        return __handle_ != nullptr;
    }

    bool done() const {
        assert(__is_suspended() &&
               "done() can be called only on suspended coroutines");
        return __builtin_coro_done(__handle_);
    }

    // [coroutine.handle.resumption], resumption
    void operator()() const { resume(); }

    void resume() const {
        assert(__is_suspended() &&
               "resume() can be called only on suspended coroutines");
        assert(!done() &&
               "resume() has undefined behavior when the coroutine is done");
        __builtin_coro_resume(__handle_);
    }

    void destroy() const {
        assert(__is_suspended() &&
               "destroy() can be called only on suspended coroutines");
        __builtin_coro_destroy(__handle_);
    }

    // [coroutine.handle.promise], promise access
    _Promise& promise() const {
        return *static_cast<_Promise*>(
            __builtin_coro_promise(this->__handle_, alignof(_Promise), false));
    }

private:
    bool __is_suspended() const {
        // FIXME actually implement a check for if the coro is suspended.
        return __handle_ != nullptr;
    }
    void* __handle_ = nullptr;
};

struct noop_coroutine_promise {};

template <>
class coroutine_handle<noop_coroutine_promise> {
public:
    // [coroutine.handle.noop.conv], conversion
    constexpr operator coroutine_handle<>() const noexcept {
        return coroutine_handle<>::from_address(address());
    }

    // [coroutine.handle.noop.observers], observers
    constexpr explicit operator bool() const noexcept { return true; }
    constexpr bool done() const noexcept { return false; }

    // [coroutine.handle.noop.resumption], resumption
    constexpr void operator()() const noexcept {}
    constexpr void resume() const noexcept {}
    constexpr void destroy() const noexcept {}

    // [coroutine.handle.noop.promise], promise access
    noop_coroutine_promise& promise() const noexcept {
        return *static_cast<noop_coroutine_promise*>(__builtin_coro_promise(
            this->__handle_, alignof(noop_coroutine_promise), false));
    }

    // [coroutine.handle.noop.address], address
    constexpr void* address() const noexcept { return __handle_; }

private:
    friend coroutine_handle<noop_coroutine_promise> noop_coroutine() noexcept;

#if __has_builtin(__builtin_coro_noop)
    coroutine_handle() noexcept { this->__handle_ = __builtin_coro_noop(); }

    void* __handle_ = nullptr;

#elif defined(__GNUC__) and !defined(__clang__)
    // GCC doesn't implement __builtin_coro_noop().
    // Construct the coroutine frame manually instead.
    struct __noop_coroutine_frame_ty_ {
        static void __dummy_resume_destroy_func() {}

        void (*__resume_)() = __dummy_resume_destroy_func;
        void (*__destroy_)() = __dummy_resume_destroy_func;
        struct noop_coroutine_promise __promise_;
    };

    static __noop_coroutine_frame_ty_ __noop_coroutine_frame_;

    void* __handle_ = &__noop_coroutine_frame_;

    coroutine_handle() noexcept = default;

#endif  // __has_builtin(__builtin_coro_noop)
};

using noop_coroutine_handle = coroutine_handle<noop_coroutine_promise>;

#if defined(__GNUC__) and !defined(__clang__)
inline noop_coroutine_handle::__noop_coroutine_frame_ty_
    noop_coroutine_handle::__noop_coroutine_frame_{};
#endif

// [coroutine.noop.coroutine]
inline noop_coroutine_handle noop_coroutine() noexcept {
    return noop_coroutine_handle();
}

// [coroutine.trivial.awaitables]
struct suspend_never {
    constexpr bool await_ready() const noexcept { return true; }
    constexpr void await_suspend(coroutine_handle<>) const noexcept {}
    constexpr void await_resume() const noexcept {}
};

struct suspend_always {
    constexpr bool await_ready() const noexcept { return false; }
    constexpr void await_suspend(coroutine_handle<>) const noexcept {}
    constexpr void await_resume() const noexcept {}
};

}  // namespace STD_CORO

namespace std {

template <class _Tp>
struct hash<STD_CORO::coroutine_handle<_Tp> > {
    using __arg_type = STD_CORO::coroutine_handle<_Tp>;

    size_t operator()(__arg_type const& __v) const noexcept {
        return hash<void*>()(__v.address());
    }
};
}  // namespace std
#undef STD_CORO
#endif

#if !defined(HAS_NON_EXPERIMENTAL_COROUTINE)

namespace std {
using std::experimental::coroutine_handle;
using std::experimental::coroutine_traits;
using std::experimental::noop_coroutine;
using std::experimental::noop_coroutine_handle;
using std::experimental::suspend_always;
using std::experimental::suspend_never;
}  // namespace std

#endif /* HAS_NON_EXPERIMENTAL_COROUTINE */
#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {
namespace coro {
template <typename T = void>
using CoroHandle = std::coroutine_handle<T>;
}
}  // namespace async_simple

#endif /* ASYNC_SIMPLE_EXPERIMENTAL_COROUTINE */
