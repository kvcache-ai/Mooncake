#pragma once

// HeapOptional<T>: an optional that keeps its value on the heap, so it costs
// one pointer while empty. For a field that is empty almost always and lives
// on every one of many objects, where std::optional<T> would reserve sizeof(T)
// for a value that is rarely there.
//
// It reads like std::optional<T> (has_value, *, ->, emplace, reset, assigning
// a T) and copies its value like one. Engaging it allocates.

#include <memory>
#include <type_traits>
#include <utility>

namespace mooncake {

template <typename T>
class HeapOptional {
   public:
    HeapOptional() = default;
    HeapOptional(const HeapOptional& other)
        : value_(other.value_ ? std::make_unique<T>(*other.value_) : nullptr) {}
    HeapOptional(HeapOptional&&) noexcept = default;
    HeapOptional& operator=(const HeapOptional& other) {
        if (this != &other) {
            value_ =
                other.value_ ? std::make_unique<T>(*other.value_) : nullptr;
        }
        return *this;
    }
    HeapOptional& operator=(HeapOptional&&) noexcept = default;

    template <typename U = T>
        requires std::is_constructible_v<T, U&&> &&
                 (!std::is_same_v<std::remove_cvref_t<U>, HeapOptional>)
    HeapOptional& operator=(U&& value) {
        if (value_) {
            *value_ = std::forward<U>(value);
        } else {
            value_ = std::make_unique<T>(std::forward<U>(value));
        }
        return *this;
    }

    template <typename... Args>
    T& emplace(Args&&... args) {
        value_ = std::make_unique<T>(std::forward<Args>(args)...);
        return *value_;
    }

    void reset() noexcept { value_.reset(); }

    [[nodiscard]] bool has_value() const noexcept { return value_ != nullptr; }
    explicit operator bool() const noexcept { return has_value(); }

    // The rest require has_value().
    T& operator*() noexcept { return *value_; }
    const T& operator*() const noexcept { return *value_; }
    T* operator->() noexcept { return value_.get(); }
    const T* operator->() const noexcept { return value_.get(); }

   private:
    std::unique_ptr<T> value_;
};

}  // namespace mooncake
