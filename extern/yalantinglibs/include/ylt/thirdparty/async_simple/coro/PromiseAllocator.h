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

#ifndef ASYNC_SIMPLE_CORO_PROMISEALLOCATOR_H
#define ASYNC_SIMPLE_CORO_PROMISEALLOCATOR_H

#ifndef ASYNC_SIMPLE_USE_MODULES
#include <algorithm>
#include <concepts>
#include <cstring>
#include <memory>
#include <new>
#include <type_traits>

// FIXME: Definition not found when clang compiles libstdcxx
// The reason is that clang does not define the macro __cpp_sized_deallocation
// The reason why `__cpp_sized_deallocation` is not enabled is that it will
// cause ABI breaking
#if defined(__clang__) && defined(__GLIBCXX__)
#endif  // ASYNC_SIMPLE_USE_MODULES

void operator delete[](void* p, std::size_t sz) noexcept;
#endif

namespace async_simple::coro::detail {

struct alignas(__STDCPP_DEFAULT_NEW_ALIGNMENT__) Aligned_block {
    unsigned char pad[__STDCPP_DEFAULT_NEW_ALIGNMENT__];
};

template <class Alloc>
using Rebind =
    typename std::allocator_traits<Alloc>::template rebind_alloc<Aligned_block>;

// clang-format off
template <class Alloc>
concept HasRealPointers = std::same_as<Alloc, void> ||
    std::is_pointer_v<typename std::allocator_traits<Alloc>::pointer>;
// clang-format on

/**
 * Implementation comes from [P2502R2]
 * (https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2022/p2502r2.pdf)
 *
 * This class can be used as the base class of the coroutine promise_type class.
 * It supports both stateless and stateful allocators.
 * The proposed interface requires that, if an allocator is provided,
 * it is the second argument to the coroutine function, immediately
 * preceded by an argument of type std::allocator_arg_t. This approach
 * is necessary to distinguish the allocator desired to allocate the coroutine
 * state from allocators whose purpose is to be used in the body of the
 * coroutine function.
 *
 * For Exmaple:
 * Generator<int> stateless_void_example(std::allocator_arg_t,
 *                                       std::allocator<std::byte>) {
 *     co_yield 42;
 * }
 *
 * stateless_void_example(std::allocator_arg, std::allocator<float>{});
 *
 */
template <class Allocator, bool no_except_new = false>
class PromiseAllocator {
private:
    using Alloc = Rebind<Allocator>;

    static void* Allocate(Alloc al, const std::size_t size) {
        if constexpr (std::default_initializable<Alloc> &&
                      std::allocator_traits<Alloc>::is_always_equal::value) {
            // do not store stateless allocator
            const size_t count =
                (size + sizeof(Aligned_block) - 1) / sizeof(Aligned_block);
            return al.allocate(count);
        } else {
            // store stateful allocator
            static constexpr size_t Align =
                (::std::max)(alignof(Alloc), sizeof(Aligned_block));
            const size_t count =
                (size + sizeof(Alloc) + Align - 1) / sizeof(Aligned_block);
            void* const ptr = al.allocate(count);
            const auto al_address =
                (reinterpret_cast<uintptr_t>(ptr) + size + alignof(Alloc) - 1) &
                ~(alignof(Alloc) - 1);
            ::new (reinterpret_cast<void*>(al_address)) Alloc(::std::move(al));
            return ptr;
        }
    }

public:
    // clang-format off
    static void* operator new(
        const size_t size) requires std::default_initializable<Alloc> {
        // clang-format on
        return Allocate(Alloc{}, size);
    }

    // clang-format off
    template <class Alloc2, class... Args>
    requires std::convertible_to<const Alloc2&, Allocator>
    static void* operator new(const size_t size, std::allocator_arg_t,
                              const Alloc2& al, const Args&...) {
        // clang-format on
        return Allocate(static_cast<Alloc>(static_cast<Allocator>(al)), size);
    }

    // clang-format off
    template <class This, class Alloc2, class... Args>
    requires std::convertible_to<const Alloc2&, Allocator>
    static void* operator new(const size_t size, const This&,
                              std::allocator_arg_t, const Alloc2& al,
                              const Args&...) {
        // clang-format on
        return Allocate(static_cast<Alloc>(static_cast<Allocator>(al)), size);
    }

    static void operator delete(void* const ptr, size_t size) noexcept {
        if constexpr (std::default_initializable<Alloc> &&
                      std::allocator_traits<Alloc>::is_always_equal::value) {
            // make stateless allocator
            Alloc al{};
            const size_t count =
                (size + sizeof(Aligned_block) - 1) / sizeof(Aligned_block);
            al.deallocate(static_cast<Aligned_block*>(ptr), count);
        } else {
            // retrieve stateful allocator
            const auto _Al_address =
                (reinterpret_cast<uintptr_t>(ptr) + size + alignof(Alloc) - 1) &
                ~(alignof(Alloc) - 1);
            auto& stored_al = *reinterpret_cast<Alloc*>(_Al_address);
            Alloc al{::std::move(stored_al)};
            stored_al.~Alloc();

            static constexpr size_t Align =
                (::std::max)(alignof(Alloc), sizeof(Aligned_block));
            const size_t _Count =
                (size + sizeof(Alloc) + Align - 1) / sizeof(Aligned_block);
            al.deallocate(static_cast<Aligned_block*>(ptr), _Count);
        }
    }
};

template <>
class PromiseAllocator<void, false> {  // type-erased allocator
private:
    using DeallocFn = void (*)(void*, size_t);

    template <class ProtoAlloc>
    static void* Allocate(const ProtoAlloc& proto, std::size_t size) {
        using Alloc = Rebind<ProtoAlloc>;
        auto al = static_cast<Alloc>(proto);

        if constexpr (std::default_initializable<Alloc> &&
                      std::allocator_traits<Alloc>::is_always_equal::value) {
            // don't store stateless allocator
            const DeallocFn dealloc = [](void* const ptr, const size_t size) {
                Alloc al{};
                const size_t count =
                    (size + sizeof(DeallocFn) + sizeof(Aligned_block) - 1) /
                    sizeof(Aligned_block);
                al.deallocate(static_cast<Aligned_block*>(ptr), count);
            };

            const size_t count =
                (size + sizeof(DeallocFn) + sizeof(Aligned_block) - 1) /
                sizeof(Aligned_block);
            void* const ptr = al.allocate(count);
            ::memcpy(static_cast<char*>(ptr) + size, &dealloc, sizeof(dealloc));
            return ptr;
        } else {
            // store stateful allocator
            static constexpr size_t Align =
                (::std::max)(alignof(Alloc), sizeof(Aligned_block));

            const DeallocFn dealloc = [](void* const ptr, size_t size) {
                size += sizeof(DeallocFn);
                const auto al_address = (reinterpret_cast<uintptr_t>(ptr) +
                                         size + alignof(Alloc) - 1) &
                                        ~(alignof(Alloc) - 1);
                auto& stored_al = *reinterpret_cast<const Alloc*>(al_address);
                Alloc al{::std::move(stored_al)};
                stored_al.~Alloc();

                const size_t count =
                    (size + sizeof(al) + Align - 1) / sizeof(Aligned_block);
                al.deallocate(static_cast<Aligned_block*>(ptr), count);
            };

            const size_t count =
                (size + sizeof(DeallocFn) + sizeof(al) + Align - 1) /
                sizeof(Aligned_block);
            void* const ptr = al.allocate(count);
            ::memcpy(static_cast<char*>(ptr) + size, &dealloc, sizeof(dealloc));
            size += sizeof(DeallocFn);
            const auto al_address =
                (reinterpret_cast<uintptr_t>(ptr) + size + alignof(Alloc) - 1) &
                ~(alignof(Alloc) - 1);
            ::new (reinterpret_cast<void*>(al_address)) Alloc{::std::move(al)};
            return ptr;
        }
    }

public:
    static void* operator new(const std::size_t size) {  // defailt: new/delete
        void* const ptr = ::operator new[](size + sizeof(DeallocFn));
        const DeallocFn dealloc = [](void* const ptr, const size_t size) {
            ::operator delete[](ptr, size + sizeof(DeallocFn));
        };
        ::memcpy(static_cast<char*>(ptr) + size, &dealloc, sizeof(DeallocFn));
        return ptr;
    }

    template <class Alloc, class... Args>
    static void* operator new(const std::size_t size, std::allocator_arg_t,
                              const Alloc& al, const Args&...) {
        static_assert(HasRealPointers<Alloc>,
                      "coroutine allocators must use true pointers");
        return Allocate(al, size);
    }

    template <class This, class Alloc, class... Args>
    static void* operator new(const std::size_t size, const This&,
                              std::allocator_arg_t, const Alloc& al,
                              const Args&...) {
        static_assert(HasRealPointers<Alloc>,
                      "coroutine allocators must be true pointers");
        return Allocate(al, size);
    }

    static void operator delete(void* const ptr, std::size_t size) noexcept {
        DeallocFn dealloc;
        ::memcpy(&dealloc, static_cast<const char*>(ptr) + size,
                 sizeof(DeallocFn));
        return dealloc(ptr, size);
    }
};

template <>
class PromiseAllocator<void, true> {  // type-erased allocator
private:
    using DeallocFn = void (*)(void*, size_t);

    template <class ProtoAlloc>
    static void* Allocate(const ProtoAlloc& proto, std::size_t size) noexcept {
        using Alloc = Rebind<ProtoAlloc>;
        auto al = static_cast<Alloc>(proto);

        if constexpr (std::default_initializable<Alloc> &&
                      std::allocator_traits<Alloc>::is_always_equal::value) {
            // don't store stateless allocator
            const DeallocFn dealloc = [](void* const ptr, const size_t size) {
                Alloc al{};
                const size_t count =
                    (size + sizeof(DeallocFn) + sizeof(Aligned_block) - 1) /
                    sizeof(Aligned_block);
                al.deallocate(static_cast<Aligned_block*>(ptr), count);
            };

            const size_t count =
                (size + sizeof(DeallocFn) + sizeof(Aligned_block) - 1) /
                sizeof(Aligned_block);
            void* ptr = nullptr;
            try {
                ptr = al.allocate(count);
            } catch (...) {
                return nullptr;
            }
            ::memcpy(static_cast<char*>(ptr) + size, &dealloc, sizeof(dealloc));
            return ptr;
        } else {
            // store stateful allocator
            static constexpr size_t Align =
                (::std::max)(alignof(Alloc), sizeof(Aligned_block));

            const DeallocFn dealloc = [](void* const ptr, size_t size) {
                size += sizeof(DeallocFn);
                const auto al_address = (reinterpret_cast<uintptr_t>(ptr) +
                                         size + alignof(Alloc) - 1) &
                                        ~(alignof(Alloc) - 1);
                auto& stored_al = *reinterpret_cast<const Alloc*>(al_address);
                Alloc al{::std::move(stored_al)};
                stored_al.~Alloc();

                const size_t count =
                    (size + sizeof(al) + Align - 1) / sizeof(Aligned_block);
                al.deallocate(static_cast<Aligned_block*>(ptr), count);
            };

            const size_t count =
                (size + sizeof(DeallocFn) + sizeof(al) + Align - 1) /
                sizeof(Aligned_block);
            void* ptr = nullptr;
            try {
                ptr = al.allocate(count);
            } catch (...) {
                return nullptr;
            }
            ::memcpy(static_cast<char*>(ptr) + size, &dealloc, sizeof(dealloc));
            size += sizeof(DeallocFn);
            const auto al_address =
                (reinterpret_cast<uintptr_t>(ptr) + size + alignof(Alloc) - 1) &
                ~(alignof(Alloc) - 1);
            ::new (reinterpret_cast<void*>(al_address)) Alloc{::std::move(al)};
            return ptr;
        }
    }

public:
    static void* operator new(
        const std::size_t size) noexcept {  // default: new/delete
        void* const ptr =
            ::operator new[](size + sizeof(DeallocFn), std::nothrow);
        if (ptr == nullptr) {
            return nullptr;
        }
        const DeallocFn dealloc = [](void* const ptr, const size_t size) {
            ::operator delete[](ptr, size + sizeof(DeallocFn));
        };
        ::memcpy(static_cast<char*>(ptr) + size, &dealloc, sizeof(DeallocFn));
        return ptr;
    }

    template <class Alloc, class... Args>
    static void* operator new(const std::size_t size, std::allocator_arg_t,
                              const Alloc& al, const Args&...) noexcept {
        static_assert(HasRealPointers<Alloc>,
                      "coroutine allocators must use true pointers");
        return Allocate(al, size);
    }

    template <class This, class Alloc, class... Args>
    static void* operator new(const std::size_t size, const This&,
                              std::allocator_arg_t, const Alloc& al,
                              const Args&...) noexcept {
        static_assert(HasRealPointers<Alloc>,
                      "coroutine allocators must be true pointers");
        return Allocate(al, size);
    }

    static void operator delete(void* const ptr, std::size_t size) noexcept {
        DeallocFn dealloc;
        ::memcpy(&dealloc, static_cast<const char*>(ptr) + size,
                 sizeof(DeallocFn));
        return dealloc(ptr, size);
    }
};

}  // namespace async_simple::coro::detail

#endif
