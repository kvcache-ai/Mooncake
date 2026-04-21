/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
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

#ifndef ASYNC_SIMPLE_CORO_GENERATOR_H
#define ASYNC_SIMPLE_CORO_GENERATOR_H

#if defined(__clang__) && __clang_major__ == 15
// See: https://github.com/alibaba/async_simple/issues/372
#warning "Clang 15 is not supported for Generator due to some issues."
#endif

#if defined(__cpp_lib_generator) && __has_include(<generator>)

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <generator>
#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple::coro {

template <class Ref, class V = void, class Allocator = void>
using Generator = std::generator<Ref, V, Allocator>;

}  // namespace async_simple::coro

namespace async_simple::ranges {

template <class R, class Alloc = std::allocator<std::byte>>
using elements_of = std::ranges::elements_of<R, Alloc>;

}  // namespace async_simple::ranges

#else

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <cassert>
#include <concepts>
#include <cstddef>
#include <exception>
#include <ranges>
#include <type_traits>
#include <utility>

#include "async_simple/Common.h"
#include "async_simple/coro/PromiseAllocator.h"
#include "async_simple/experimental/coroutine.h"
#endif

namespace async_simple::ranges {

// For internal use only, for compatibility with lower version standard
// libraries
namespace internal {

// clang-format off
template <typename T>
concept range = requires(T& t) {
    std::ranges::begin(t);
    std::ranges::end(t);
};

template <class T>
using iterator_t = decltype(std::ranges::begin(std::declval<T&>()));

template <range R>
using sentinel_t = decltype(std::ranges::end(std::declval<R&>()));

template <range R>
using range_value_t = std::iter_value_t<iterator_t<R>>;

template <range R>
using range_reference_t = std::iter_reference_t<iterator_t<R>>;

template <class T>
concept input_range = range<T> && std::input_iterator<iterator_t<T>>;

}  // namespace internal

// clang-format on

#ifdef _MSC_VER
#define EMPTY_BASES __declspec(empty_bases)
#ifdef __clang__
#define NO_UNIQUE_ADDRESS
#else
#define NO_UNIQUE_ADDRESS [[msvc::no_unique_address]]
#endif
#else
#define EMPTY_BASES
#define NO_UNIQUE_ADDRESS [[no_unique_address]]
#endif

template <class R, class Alloc = std::allocator<std::byte>>
struct elements_of {
    NO_UNIQUE_ADDRESS R range;
    NO_UNIQUE_ADDRESS Alloc allocator{};
};

template <class R, class Alloc = std::allocator<std::byte>>
elements_of(R&&, Alloc = {}) -> elements_of<R&&, Alloc>;

}  // namespace async_simple::ranges

namespace async_simple::coro::detail {

template <class yield>
struct gen_promise_base;

}  // namespace async_simple::coro::detail

namespace async_simple::coro {

// clang-format off
/**
 * Implementation comes from [P2502R2]
 * (https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2022/p2502r2.pdf)
 *
 * Generator: Synchronous Coroutine Generator for Ranges.
 * Synchronous generators are an important use case for coroutines.
 * The Generator class represents a sequence of synchronously produced
 * values where the values are produced by a coroutine. Generator is a
 * move-only view which models input_range and has move-only iterators.
 * This is because the coroutine state is a unique resource (even if the
 * coroutine handle is copyable).
 *
 * Values are produced by using the 'co_yield' keyword.
 * Using the 'co_await' keyword is not allowed In the body of Generator.
 * The end of the sequence is indicated by executing 'co_return;' either
 * explicitly or by letting execution run off the end of the coroutine.
 * For example:
 *
 * Generator<int> answer() {
 *   co_yield 42;
 *   co_return;
 * }
 *
 * Generator has 3 template parameters: Generator<Ref, V = void, Allocator = void>
 * From Ref and V, we derive types:
 * using value     = conditional_t<is_void_v<V>, remove_cvref_t<Ref>, V>;
 * using reference = conditional_t<is_void_v<V>, Ref&&, Ref>;
 * using yielded   = conditional_t<is_reference_v<reference>, reference, const reference&>;
 *
 * • value is a cv-unqualified object type that specifies the value type of the 
 *   generator’s range and iterators
 * • reference specifies the reference type (not necessarily a core language 
 *   reference type) of the generator’s range and iterators, and it is the return 
 *   value of the opeartor* member function of the generator's iterators.
 * • yielded (necessarily a reference type) is the type of the parameter to the 
 *   primary overload of yield_value in the generator’s associated promise type.
 *
 * Generator<meow>
 * Our expectation is that 98% of use cases will need to specify only one 
 * parameter. The resulting Generator:
 * • has a value type of std::remove_cvref<meow>
 * • has a reference type of meow, if it is reference type, or meow&& otherwise
 * • has a yielded type of reference, because `std::is_reference_v<reference>` 
 *   must be established at this time, but users don't need to care about this.
 *  
 *  // Following examples show difference between:
    //
    //                                 If I co_yield a...                reference     value
    //                           X / X&&  | X&         | const X&
    //                        ------------+------------+-----------
    // - Generator<X>               (same as Generator<X&&>)              X&&           X
    // - Generator<const X&>   ref        | ref        | ref              const X&     const X
    // - Generator<X&&>        ref        | 1 copy     | 1 copy           X&&           X
    // - Generator<X&>         ill-formed | ref        | ill-formed       X&            X
    //
 *
 *  Generator<int> (same as Generator<int&&>)
 *  Generator<int&&> xvalue_example() {
 * [1]     co_yield 1;
 * [2]     int x{2};
 * [3]     co_yield x;             // well-formed: generated element is copy of lvalue
 * [4]     const int y{3};         
 * [5]     co_yield y;             // same as above
 * [6]     int z{4};
 * [7]     co_yield std::move(z);  // pass by rvalue reference.
 *  }
 *     
 *  // Generator<int>::iterator operator* return type: 
 *  //  -> Generator<int>::reference -> int&&
 * 
 *  for (auto&& i : xvalue_example()) { // auto -> int&&, no copy, no move
 *      std::cout << i << std::endl;    // The variable z can still be used 
 *                                      // after the sequence number 7
 *  }
 * 
 *  for (auto i : xvalue_example()) {   // auto -> int
 *      std::cout << i << std::endl;    // The variable z cannot be used 
 *                                      // after the sequence number 7, 
 *                                      // because it is moved. But the 
 *                                      // variable x can still be used 
 *                                      // after the sequence number 3,
 *                                      // because the value returned by
 *                                      // the iterator is the copied.
 *  }
 *  
 *  // Pass by reference, no copy.
 *  Generator<const int&> const_lvalue_example() {
 *      co_yield 1;             // OK
 *      const int x{2};
 *      co_yield x;             // OK
 *      co_yield std::move(x);  // OK: same as above
 *      int y{3};
 *      co_yield y;             // OK
 *  }
 * 
 *  Geneartor<int&> lvalue_example() {
 *      co_yield 1;             // ill-formed: prvalue -> non-const lvalue
 *      int x{2};
 *      co_yield x;             // OK
 *      co_yield std::move(x);  // ill-formed: xvalue -> non-const lvalue
 *      const int y{3};
 *      co_yield y;             // ill-formed: const lvalue -> non-const lvalue
 *  }
 * 
 * Generator<meow, woof>
 * For the rare user who needs generator to step outside the box and use a 
 * proxy reference type, or who needs to generate a range whose iterators 
 * yield prvalues for whatever reason, we have two-argument generator. 
 * If woof is void, this is generator<meow>. Otherwise, the resulting generator:
 * 
 * • has a value type of woof
 * • has a reference type of meow
 * // TODO: ...
 *  
 *  For example:
 *  // value_type = std::string_view
 *  // reference = std::string_view
 *  // Generator::iterator operator* return type: std::string_view
 *  // This can be expensive for types that are expensive to copy, 
 *  // but can provide a small performance win for types that are cheap to copy 
 *  // (like built-in integer types).
 *  Generator<std::string_view, std::string_view> string_views() {
 *      co_yield "foo";
 *      co_yield "bar";
 *  }
 *  
 *  // value_type = std::string
 *  // reference = std::string_view
 *  Generator<std::string_view, std::string> strings() {
 *      co_yield "start";
 *      std::string s;
 *      for (auto sv : string_views()) {
 *          s = sv;
 *          s.push_back('!');
 *          co_yield s;
 *      }
 *      co_yield "end";
 *  }
 * 
 *  // conversion to a vector of strings
 *  // If the value_type was string_view, it would convert to a vector of string_view,
 *  // which would lead to undefined behavior operating on elements of v that were
 *  // invalidated while iterating through the generator.
 *  auto v = std::ranges::to<vector>(strings());
 * 
 * Allocator support: See async_simple/coro/PromiseAllocator.h
 * 
 * Recursive generator
 * A "recursive generator" is a coroutine that supports the ability to directly co_yield 
 * a generator of the same type as a way of emitting the elements of that generator as 
 * elements of the current generator.
 * 
 * Example: A generator can co_yield other generators of the same type
 * 
 * Generator<const std::string&> delete_rows(std::string table, std::vector<int> ids) {
 *     for (int id : ids) {
 *         co_yield std::format("DELETE FROM {0} WHERE id = {1};", table, id);
 *     }
 * }
 * 
 * Generator<const std::string&> all_queries() {
 *     co_yield std::ranges::elements_of(delete_rows("user", {4, 7, 9 10}));
 *     co_yield std::ranges::elements_of(delete_rows("order", {11, 19}));
 * }
 * 
 *  Example: A generator can also be used recursively
 *  
 *  Generator<int, int> visit(TreeNode& tree) {
 *      if (tree.left)
 *          co_yield ranges::elements_of{visit(*tree.left)};
 *      co_yield tree.value;
 *      if (tree.right)
 *          co_yield ranges::elements_of{visit(*tree.right)};
 *  }
 * 
 * In addition to being more concise, the ability to directly yield a nested generator 
 * has some performance benefits compared to iterating over the contents of the nested 
 * generator and manually yielding each of its elements.
 * 
 * Yielding a nested generator allows the consumer of the top-level coroutine to directly 
 * resume the current leaf generator when incrementing the iterator, whereas a solution 
 * that has each generator manually iterating over elements of the child generator requires 
 * O(depth) coroutine resumptions/suspensions per element of the sequence.
 * 
 * Example:  Non-recursive form incurs O(depth) resumptions/suspensions per element 
 * and is more cumbersome to write:
 * 
 *  Generator<int, int> slow_visit(TreeNode& tree) {
 *      if (tree.left) {
 *          for (int x : slow_visit(*tree.left))
 *              co_yield x;
 *      }
 *      co_yield tree.value;
 *      if (tree.right) {
 *          for (int x : slow_visit(*tree.right))
 *              co_yield x;
 *      }
 *  }
 * 
 */
// clang-format on
template <class Ref, class V = void, class Allocator = void>
class Generator {
private:
    using value =
        std::conditional_t<std::is_void_v<V>, std::remove_cvref_t<Ref>, V>;
    using reference = std::conditional_t<std::is_void_v<V>, Ref&&, Ref>;

    class iterator;

    // clang-format off
    static_assert(
        std::same_as<std::remove_cvref_t<value>, value> &&
            std::is_object_v<value>,
        "generator's value type must be a cv-unqualified object type");
    static_assert(std::is_reference_v<reference> ||
                      (std::is_object_v<reference> &&
                       std::same_as<std::remove_cv_t<reference>, reference> &&
                       std::copy_constructible<reference>),
                  "generator's second argument must be a reference type or a "
                  "cv-unqualified "
                  "copy-constructible object type");
    // clang-format on

public:
    class promise_type;

    using Handle = std::coroutine_handle<promise_type>;
    using yielded = std::conditional_t<std::is_reference_v<reference>,
                                       reference, const reference&>;

    Generator(Generator&& other) noexcept
        : _coro(std::exchange(other._coro, nullptr)) {}
    Generator& operator=(Generator&& other) noexcept;
    ~Generator();
    Generator(const Generator&) = delete;
    Generator& operator=(const Generator&) = delete;
    [[nodiscard]] iterator begin();
    [[nodiscard]] std::default_sentinel_t end() const noexcept { return {}; }

private:
    explicit Generator(Handle coro) noexcept;

private:
    Handle _coro = nullptr;

    template <class Yield>
    friend struct detail::gen_promise_base;
};

namespace detail {

template <class yielded>
struct gen_promise_base {
protected:
    struct NestInfo {
        std::exception_ptr except_;
        std::coroutine_handle<gen_promise_base> parent_;
        std::coroutine_handle<gen_promise_base> root_;
    };

    template <class R2, class V2, class Alloc2>
    struct NestedAwaiter {
        NestInfo nested_;
        Generator<R2, V2, Alloc2> gen_;

        explicit NestedAwaiter(Generator<R2, V2, Alloc2>&& gen) noexcept
            : gen_(std::move(gen)) {}
        bool await_ready() noexcept { return !gen_._coro; }

        template <class Promise>
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<Promise> handle) noexcept {
            auto target = std::coroutine_handle<gen_promise_base>::from_address(
                gen_._coro.address());
            nested_.parent_ =
                std::coroutine_handle<gen_promise_base>::from_address(
                    handle.address());
            auto& current = handle.promise();
            if (current.info_) {
                nested_.root_ = current.info_->root_;
            } else {
                nested_.root_ = nested_.parent_;
            }
            nested_.root_.promise().top_ = target;
            target.promise().info_ = std::addressof(nested_);
            return target;
        }

        void await_resume() {
            if (nested_.except_) {
                std::rethrow_exception(std::move(nested_.except_));
            }
        }
    };

    struct ElementAwaiter {
        std::remove_cvref_t<yielded> value_;
        constexpr bool await_ready() const noexcept { return false; }

        template <class Promise>
        constexpr void await_suspend(
            std::coroutine_handle<Promise> handle) noexcept {
            auto& current = handle.promise();
            current.value_ = std::addressof(value_);
        }

        constexpr void await_resume() const noexcept {}
    };

    struct FinalAwaiter {
        bool await_ready() const noexcept { return false; }

        template <class Promise>
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<Promise> handle) noexcept {
            auto& current = handle.promise();

            if (!current.info_) {
                return std::noop_coroutine();
            }

            auto previous = current.info_->parent_;
            current.info_->root_.promise().top_ = previous;
            current.info_ = nullptr;
            return previous;
        }

        void await_resume() noexcept {}
    };

    template <class Ref, class V, class Alloc>
    friend class async_simple::coro::Generator;

    std::add_pointer_t<yielded> value_ = nullptr;
    std::coroutine_handle<gen_promise_base> top_ =
        std::coroutine_handle<gen_promise_base>::from_promise(*this);
    NestInfo* info_ = nullptr;
};

}  // namespace detail

template <class Ref, class V, class Allocator>
class Generator<Ref, V, Allocator>::promise_type
    : public detail::gen_promise_base<yielded>,
      public detail::PromiseAllocator<Allocator> {
public:
    using Base = detail::gen_promise_base<yielded>;

    std::suspend_always initial_suspend() noexcept { return {}; }
    // When info is `nullptr`, equivalent to std::suspend_always
    auto final_suspend() noexcept { return typename Base::FinalAwaiter{}; }

    std::suspend_always yield_value(yielded val) noexcept {
        this->value_ = std::addressof(val);
        return {};
    }

    // clang-format off
    auto yield_value(const std::remove_reference_t<yielded>& lval) requires
        std::is_rvalue_reference_v<yielded> && std::constructible_from<
        std::remove_cvref_t<yielded>, const std::remove_reference_t<yielded>& > 
    { return typename Base::ElementAwaiter{lval}; }
    // clang-format on

    // clang-format off
    template <class R2, class V2, class Alloc2, class Unused>
    requires std::same_as<typename Generator<R2, V2, Alloc2>::yielded, yielded>
    auto yield_value(
        ranges::elements_of<Generator<R2, V2, Alloc2>&&, Unused> g) noexcept {
        // clang-format on
        return typename Base::template NestedAwaiter<R2, V2, Alloc2>{
            std::move(g.range)};
    }

    // clang-format off
    template <class R, class Alloc>
    requires std::convertible_to<ranges::internal::range_reference_t<R>, yielded>
    auto yield_value(ranges::elements_of<R, Alloc> r) noexcept {
        // clang-format on
        auto nested = [](std::allocator_arg_t, Alloc,
                         ranges::internal::iterator_t<R> i,
                         ranges::internal::sentinel_t<R> s)
            -> Generator<yielded, ranges::internal::range_value_t<R>, Alloc> {
            for (; i != s; ++i) {
                co_yield static_cast<yielded>(*i);
            }
        };
        return yield_value(ranges::elements_of{
            nested(std::allocator_arg, r.allocator, std::ranges::begin(r.range),
                   std::ranges::end(r.range))});
    }

    void return_void() noexcept {}
    void await_transform() = delete;
    void unhandled_exception() {
        if (this->info_) {
            this->info_->except_ = std::current_exception();
        } else {
            throw;
        }
    }

    Generator get_return_object() noexcept {
        return Generator(Generator::Handle::from_promise(*this));
    }
};

template <class Ref, class V, class Allocator>
class Generator<Ref, V, Allocator>::iterator {
public:
    using value_type = value;
    using difference_type = std::ptrdiff_t;

    explicit iterator(Handle coro) noexcept : _coro(coro) {}
    ~iterator() {
        if (_coro) {
            if (!_coro.done()) {
                // TODO: error log
            }
            _coro.destroy();
            _coro = nullptr;
        }
    }
    iterator(iterator&& rhs) noexcept
        : _coro(std::exchange(rhs._coro, nullptr)) {}

    iterator& operator=(iterator&& rhs) {
        logicAssert(!_coro, "Should not own a coroutine handle");
        _coro = std::exchange(rhs._coro, nullptr);
    }

    explicit operator bool() const noexcept { return _coro && !_coro.done(); }

    [[nodiscard]] bool operator==(std::default_sentinel_t) const {
        return !_coro || _coro.done();
    }

    reference operator*() const {
        logicAssert(
            this->operator bool(),
            "Should have a coroutine handle or the coroutine has not ended");
        assert(_coro.promise().top_.promise().value_ &&
               "value pointer is nullptr");
        return static_cast<yielded>(*_coro.promise().top_.promise().value_);
    }

    iterator& operator++() {
        logicAssert(this->operator bool(),
                    "Can't increment generator end iterator");
        _coro.promise().top_.resume();
        return *this;
    }

    void operator++(int) { ++(*this); }

private:
    Handle _coro = nullptr;
};

template <class Ref, class V, class Allocator>
Generator<Ref, V, Allocator>::Generator(Handle coro) noexcept : _coro(coro) {}

template <class Ref, class V, class Allocator>
Generator<Ref, V, Allocator>& Generator<Ref, V, Allocator>::operator=(
    Generator&& other) noexcept {
    if (_coro) {
        if (!_coro.done()) {
            // TODO: [Warning] the coroutine is not done!
        }
        _coro.destroy();
    }
    _coro = std::exchange(other._coro, nullptr);
    return *this;
}

template <class Ref, class V, class Allocator>
Generator<Ref, V, Allocator>::~Generator() {
    if (_coro) {
        if (!_coro.done()) {
            // TODO: log
        }
        _coro.destroy();
        _coro = nullptr;
    }
}

template <class Ref, class V, class Allocator>
typename Generator<Ref, V, Allocator>::iterator
Generator<Ref, V, Allocator>::begin() {
    logicAssert(!!_coro, "Can't call begin on moved-from generator");
    _coro.resume();
    return iterator(std::exchange(_coro, nullptr));
}

}  // namespace async_simple::coro

template <class Ref, class V, class Allocator>
inline constexpr bool
    std::ranges::enable_view<async_simple::coro::Generator<Ref, V, Allocator>> =
        true;

#endif  // __has_include(<generator>)

#endif  // ASYNC_SIMPLE_CORO_GENERATOR_H
