#pragma once
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>
#if __has_include(<stacktrace>) && __cplusplus >= 202302L
#include <stacktrace>
#else
#include "b_stacktrace.h"
#endif

namespace ylt::util {

#if defined _MSC_VER
#define FF_PTR_FORCE_INLINE __forceinline
#else
#define FF_PTR_FORCE_INLINE __attribute__((always_inline)) inline
#endif

enum class ff_ptr_error_type {
  heap_use_after_free = 0,
  double_free = 1,
  use_after_scope = 2,
};

inline std::string to_string(ff_ptr_error_type error) {
  switch (error) {
    case ff_ptr_error_type::heap_use_after_free:
      return "heap use after free";
    case ff_ptr_error_type::double_free:
      return "double free";
    case ff_ptr_error_type::use_after_scope:
      return "use after scope";
    default:
      return "unknown error";
  }
}

template <typename T>
class ff_ptr;
namespace detail {

template <typename T>
struct ff_ptr_impl {
  using type = ff_ptr<T>;
};

template <>
struct ff_ptr_impl<void> {
  using type = void*;
};

template <>
struct ff_ptr_impl<const void> {
  using type = const void*;
};

template <>
struct ff_ptr_impl<char> {
  using type = char*;
};

template <>
struct ff_ptr_impl<const char> {
  using type = const char*;
};

template <>
struct ff_ptr_impl<unsigned char> {
  using type = unsigned char*;
};

template <>
struct ff_ptr_impl<const unsigned char> {
  using type = const unsigned char*;
};

template <>
struct ff_ptr_impl<signed char*> {
  using type = signed char*;
};

template <>
struct ff_ptr_impl<const signed char*> {
  using type = const signed char*;
};

template <>
struct ff_ptr_impl<uint8_t*> {
  using type = uint8_t*;
};
template <>
struct ff_ptr_impl<const uint8_t*> {
  using type = const uint8_t*;
};

constexpr inline uint64_t magic_poison_value = 0xECFD'A8B9'6475'2031;
inline bool is_poisoned(const void* ptr) {
  if (ptr) [[likely]]
    return memcmp((char*)ptr, (char*)&magic_poison_value, sizeof(uint64_t)) ==
           0;
  else
    return false;
}
template <typename T>
inline void poisoned(T* ptr) {
  if (ptr) [[likely]] {
    if constexpr (sizeof(T) < 12) {
      memcpy((char*)ptr, (char*)&magic_poison_value, sizeof(uint64_t));
    }
    else {
      memcpy(((char*)ptr), (char*)&magic_poison_value, sizeof(uint64_t));
    }
  }
}

template <typename T>
inline void depoisoned(T* ptr) {
  if (ptr) [[likely]] {
    if constexpr (sizeof(T) < 12) {
      memcpy((char*)ptr, (char*)&magic_poison_value, sizeof(uint64_t));
    }
    else {
      memcpy(((char*)ptr), (char*)&magic_poison_value, sizeof(uint64_t));
    }
  }
}

inline static auto default_error_handler = [](ff_ptr_error_type error,
                                              void* raw_pointer) {
  std::cerr << "==========================================================\n"
               "ff_ptr: "
            << raw_pointer << "check failed:" << to_string(error) << "\n"
            << std::endl;
#if __cpp_lib_stacktrace && __cplusplus >= 202302L
  std::cerr << std::stacktrace::current() << std::endl;
#else
  auto str = b_stacktrace_get_string();
  std::cerr << str << std::endl;
  free(str);
#endif
};
inline std::shared_ptr<std::function<void(ff_ptr_error_type, void*)>>
get_error_handler(std::function<void(ff_ptr_error_type, void*)> handler =
                      default_error_handler) {
  static auto error_handler =
      std::make_shared<std::function<void(ff_ptr_error_type, void*)>>(
          std::move(handler));
  return error_handler;
}

template <typename T>
inline const void* get_pointer_offset(T* raw_pointer) noexcept {
  if constexpr (sizeof(T) >= 12) {
    return ((char*)raw_pointer);
  }
  return raw_pointer;
}
inline const void* get_pointer_offset(void* raw_pointer) noexcept {
  return raw_pointer;
}

template <typename T>
inline bool ptr_check(ff_ptr_error_type error, T* raw_pointer) noexcept {
  if constexpr (requires { sizeof(T); }) {
    if (is_poisoned(get_pointer_offset(raw_pointer))) [[unlikely]] {
      (*get_error_handler())(error, (void*)raw_pointer);
      return false;
    }
    return true;
  }
}

template <typename T>
struct dt {
  using type = T*;
};

template <typename T>
struct dt<ff_ptr<T>> {
  using type = typename dt<T>::type*;
};

template <typename T>
class ff_ptr_allocator_t {
 public:
  using value_type = T;

  ff_ptr_allocator_t() noexcept = default;

  template <typename U>
  ff_ptr_allocator_t(const ff_ptr_allocator_t<U>&) noexcept {}

  T* allocate(std::size_t n) {
    return static_cast<T*>(::operator new(n * sizeof(T)));
  }

  void deallocate(T* p, std::size_t n) noexcept { ::operator delete(p); }

  template <typename U, typename... Args>
  void construct(U* p, Args&&... args) {
    depoisoned(p);
    ::new ((void*)p) U(std::forward<Args>(args)...);
  }

  void destroy(T* p) {
    // avoid double check, we disable check here.
    // char* data = reinterpret_cast<char*>(p);
    // detail::ptr_check(ff_ptr_error_type::double_free, data);
    p->~T();
    // detail::poisoned(this);
  }
};

}  // namespace detail

inline void regist_ff_ptr_error_handler(
    std::function<void(ff_ptr_error_type, void*)> func) {
  detail::get_error_handler(std::move(func));
}

template <typename T>
class ff_ptr {
  T* raw_pointer = nullptr;

 public:
  ff_ptr(const std::unique_ptr<T>& ptr) noexcept { raw_pointer = ptr.get(); }
  ff_ptr(const std::shared_ptr<T>& ptr) noexcept { raw_pointer = ptr.get(); }
  ff_ptr(std::nullptr_t) noexcept {}
  ff_ptr() noexcept = default;
  ff_ptr(const ff_ptr&) noexcept = default;
  ff_ptr(T* raw_pointer) : raw_pointer(raw_pointer) {}
  ff_ptr(detail::dt<T> raw_pointer) : raw_pointer(raw_pointer) {}
  T* operator->() noexcept {
    detail::ptr_check(ff_ptr_error_type::heap_use_after_free, raw_pointer);
    return raw_pointer;
  }
  T* const operator->() const noexcept {
    detail::ptr_check(ff_ptr_error_type::heap_use_after_free, raw_pointer);
    return raw_pointer;
  }
  operator bool() const noexcept { return raw_pointer; }
  operator intptr_t() const noexcept { return raw_pointer; }
  operator uintptr_t() const noexcept { return raw_pointer; }
  operator T*() noexcept {
    detail::ptr_check(ff_ptr_error_type::heap_use_after_free, raw_pointer);
    return raw_pointer;
  }
  operator ff_ptr<const T>() const noexcept { return raw_pointer; }
  operator T* const() const noexcept {
    detail::ptr_check(ff_ptr_error_type::heap_use_after_free, raw_pointer);
    return raw_pointer;
  }
  template <typename U>
  explicit operator U*() noexcept {
    detail::ptr_check(ff_ptr_error_type::heap_use_after_free, raw_pointer);
    return (U*)raw_pointer;
  }
  template <typename U>
  explicit operator const U*() const noexcept {
    detail::ptr_check(ff_ptr_error_type::heap_use_after_free, raw_pointer);
    return (const U*)raw_pointer;
  }
  ff_ptr& operator+=(std::ptrdiff_t index) noexcept {
    raw_pointer += index;
    return *this;
  }
  ff_ptr& operator-=(std::ptrdiff_t index) noexcept {
    raw_pointer -= index;
    return *this;
  }
  ff_ptr operator+(std::ptrdiff_t index) const noexcept {
    return raw_pointer + index;
  }

  ff_ptr operator-(std::ptrdiff_t index) const noexcept {
    return raw_pointer - index;
  }
  ff_ptr& operator++() noexcept {
    raw_pointer++;
    return *this;
  }
  ff_ptr operator++(int) noexcept { return ++raw_pointer; }
  ff_ptr& operator--() noexcept {
    raw_pointer++;
    return *this;
  }
  ff_ptr operator--(int) noexcept { return --raw_pointer; }
  T* get() noexcept {
    detail::ptr_check(ff_ptr_error_type::heap_use_after_free, raw_pointer);
    return raw_pointer;
  }
  T* const get() const noexcept {
    detail::ptr_check(ff_ptr_error_type::heap_use_after_free, raw_pointer);
    return raw_pointer;
  }
  template <typename T1, typename T2>
  friend std::strong_ordering operator<=>(ff_ptr<T1> lhs,
                                          ff_ptr<T2> rhs) noexcept;
  template <typename T1, typename T2>
  friend bool operator==(ff_ptr<T1> lhs, ff_ptr<T2> rhs) noexcept;
  template <typename T1, typename T2>
  friend bool operator==(ff_ptr<T1> lhs, T2* rhs) noexcept;
  template <typename T1, typename T2>
  friend std::strong_ordering operator<=>(ff_ptr<T1> lhs, T2* rhs) noexcept;
  template <typename T1, typename T2>
  friend bool operator==(T1* lhs, ff_ptr<T2> rhs) noexcept;
  template <typename T1, typename T2>
  friend std::strong_ordering operator<=>(T1* lhs, ff_ptr<T2> rhs) noexcept;
};

template <typename T1, typename T2>
std::strong_ordering operator<=>(ff_ptr<T1> lhs, ff_ptr<T2> rhs) noexcept {
  return lhs.raw_pointer <=> rhs.raw_pointer;
}
template <typename T1, typename T2>
bool operator==(ff_ptr<T1> lhs, ff_ptr<T2> rhs) noexcept {
  return lhs.raw_pointer == rhs.raw_pointer;
}
template <typename T1, typename T2>
bool operator==(ff_ptr<T1> lhs, T2* rhs) noexcept {
  return lhs.raw_pointer == rhs;
}
template <typename T1, typename T2>
std::strong_ordering operator<=>(ff_ptr<T1> lhs, T2* rhs) noexcept {
  return lhs.raw_pointer <=> rhs;
}
template <typename T1, typename T2>
bool operator==(T1* lhs, ff_ptr<T2> rhs) noexcept {
  return lhs == rhs.raw_pointer;
}
template <typename T1, typename T2>
std::strong_ordering operator<=>(T1* lhs, ff_ptr<T2> rhs) noexcept {
  return lhs <=> rhs.raw_pointer;
}
template <typename Derived>
struct ff_Base {
  ff_Base() { depoisoned(this); }
  ~ff_Base() {
    if constexpr (sizeof(Derived) >= 8) {
      uint64_t* data = (uint64_t*)this;
      if (detail::ptr_check(ff_ptr_error_type::use_after_scope, data))
          [[unlikely]] {
        detail::poisoned(this);
      }
    }
  }
};

template <typename T>
void ff_delete(ff_ptr<T> p) {
  if constexpr (!std::is_base_of_v<ff_Base<T>, T> &&
                sizeof(T) >= sizeof(uint64_t)) {
    uint64_t* data = (uint64_t*)p.get();
    detail::ptr_check(ff_ptr_error_type::double_free, data);
    if (p) {
      p->~T();
      poisoned(p);
      std::free(p);
    }
  }
  else {
    delete p;
  }
}

template <typename T, typename... Args>
ff_ptr<T> ff_new(Args&&... args) {
  if constexpr (std::is_base_of_v<ff_Base<T>, T>) {
    return new T{std::forward<Args>(args)...};
  }
  else if constexpr (sizeof(T) >= sizeof(uint64_t)) {
    T* buf = std::malloc(sizeof(T));
    depoisoned(buf);
    return new (buf) T{std::forward<Args>(args)...};
  }
  else {
    return new T{std::forward<Args>(args)...};
  }
}

template <typename T>
struct ff_shared_ptr : public std::shared_ptr<T> {
  using std::shared_ptr<T>::shared_ptr;
  ff_shared_ptr(ff_ptr<T> p) : std::shared_ptr<T>(p.get()) {}
  template <typename Deleter>
  ff_shared_ptr(ff_ptr<T> p, Deleter d)
      : std::shared_ptr<T>(p.get(), std::move(d)) {}
  ff_shared_ptr(const std::shared_ptr<T>& p) : std::shared_ptr<T>(p) {}
  ff_shared_ptr(std::shared_ptr<T>&& p) : std::shared_ptr<T>(std::move(p)) {}
  ff_shared_ptr(const ff_shared_ptr& p) = default;
  ff_shared_ptr(ff_shared_ptr&& p) = default;
  ff_shared_ptr& operator=(const ff_shared_ptr& p) = default;
  ff_shared_ptr& operator=(ff_shared_ptr&& p) = default;
  T* operator->() noexcept { return this->get(); }
  T* const operator->() const noexcept { return this->get(); }

  operator T* const() const noexcept { return this->get(); }
  operator ff_ptr<T>() const noexcept { return this->get(); }
  template <typename U>
  explicit operator U*() noexcept {
    return (U*)this->get();
  }
  template <typename U>
  explicit operator const U*() const noexcept {
    return (const U*)this->get();
  }
  operator std::shared_ptr<T>() & {
    char* data = reinterpret_cast<char*>(this->get());
    if (detail::ptr_check(ff_ptr_error_type::heap_use_after_free, data))
        [[unlikely]] {
      detail::poisoned(this->get());
    }
    // TODO: add deleter check when convert to shared_ptr
    return std::shared_ptr<T>(*this);
  }
  operator std::shared_ptr<T>() && {
    char* data = reinterpret_cast<char*>(std::shared_ptr<T>::get());
    if (detail::ptr_check(ff_ptr_error_type::heap_use_after_free, data))
        [[unlikely]] {
      detail::poisoned(std::shared_ptr<T>::get());
    }
    return std::shared_ptr<T>(std::move(*this));
  }
  ff_ptr<T> get() const noexcept { return std::shared_ptr<T>::get(); }
  ~ff_shared_ptr() {
    if constexpr (!std::is_base_of_v<ff_Base<T>, T> &&
                  sizeof(T) >= sizeof(uint64_t)) {
      char* data = reinterpret_cast<char*>(std::shared_ptr<T>::get());
      if (detail::ptr_check(ff_ptr_error_type::double_free, data))
          [[unlikely]] {
        detail::poisoned(std::shared_ptr<T>::get());
      }
    }
  }
};

template <typename T, typename Deleter = void>
struct ff_unique_ptr : public std::unique_ptr<T, Deleter> {
  using std::unique_ptr<T, Deleter>::unique_ptr;
  ff_unique_ptr(std::unique_ptr<T, Deleter>&& p)
      : std::unique_ptr<T, Deleter>(std::move(p)) {}
  ff_unique_ptr(const ff_unique_ptr& p) = default;
  ff_unique_ptr(ff_unique_ptr&& p) = default;
  ff_unique_ptr& operator=(const ff_unique_ptr& p) = default;
  ff_unique_ptr& operator=(ff_unique_ptr&& p) = default;
  T* operator->() noexcept { return this->get(); }
  T* const operator->() const noexcept { return this->get(); }

  operator T*() noexcept { return this->get(); }
  operator ff_ptr<T>() const noexcept { return this->get(); }
  template <typename U>
  explicit operator U*() noexcept {
    return (U*)this->get();
  }
  template <typename U>
  explicit operator const U*() const noexcept {
    return (const U*)this->get();
  }
  ff_ptr<T> get() const noexcept { return std::unique_ptr<T, Deleter>::get(); }
  operator std::unique_ptr<T, Deleter>() & {
    char* data = reinterpret_cast<char*>(std::unique_ptr<T, Deleter>::get());
    if (detail::ptr_check(ff_ptr_error_type::heap_use_after_free, data))
        [[unlikely]] {
      detail::poisoned(std::unique_ptr<T, Deleter>::get());
    }
    return std::unique_ptr<T>(*this);
  }
  operator std::unique_ptr<T, Deleter>() && {
    char* data = reinterpret_cast<char*>(std::unique_ptr<T, Deleter>::get());
    if (detail::ptr_check(ff_ptr_error_type::heap_use_after_free, data))
        [[unlikely]] {
      detail::poisoned(std::unique_ptr<T, Deleter>::get());
    }
    return std::move(*this);
  }
  ~ff_unique_ptr() {
    if constexpr (!std::is_base_of_v<ff_Base<T>, T> &&
                  sizeof(T) >= sizeof(uint64_t)) {
      char* data = reinterpret_cast<char*>(std::unique_ptr<T, Deleter>::get());
      if (detail::ptr_check(ff_ptr_error_type::double_free, data))
          [[unlikely]] {
        detail::poisoned(std::unique_ptr<T, Deleter>::get());
      }
    }
  }
};

template <typename T, typename... Args>
ff_unique_ptr<T> ff_make_unique(Args&&... args) {
  if constexpr (std::is_base_of_v<ff_Base<T>, T>) {
    return std::make_unique<T>(std::forward<Args>(args)...);
  }
  else {
    auto mem = ::operator new(sizeof(T));
    detail::depoisoned(mem);
    ::new ((void*)mem) T(std::forward<Args>(args)...);
    return ff_unique_ptr<T>(mem);
  }
}

template <typename T, typename... Args>
ff_shared_ptr<T> ff_make_shared(Args&&... args) {
  if constexpr (std::is_base_of_v<ff_Base<T>, T>) {
    return std::make_shared<T>(std::forward<Args>(args)...);
  }
  else {
    return std::allocate_shared<T>(detail::ff_ptr_allocator_t<T>(),
                                   std::forward<Args>(args)...);
  }
}

}  // namespace ylt::util

namespace std {
template <typename T>
struct is_pointer<ylt::util::ff_ptr<T>> : public std::true_type {};
}  // namespace std

template <typename T>
using ff_ptr = typename ylt::util::detail::ff_ptr_impl<T>::type;
