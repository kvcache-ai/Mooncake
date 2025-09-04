#pragma once

#include <optional>
#include <stdexcept>

namespace mooncake {

/**
 * @brief A template class that wraps std::optional to provide configurable
 * variables with setter/getter functionality and exception handling for unset
 * values.
 * @tparam T The type of the configurable variable
 */
template <typename T>
class RequiredParam {
   private:
    std::optional<T> value_;

   public:
    RequiredParam() : name_(nullptr) {}
    RequiredParam(const char* name) : name_(name) {}

    // Add copy constructor
    RequiredParam(const RequiredParam& other) { value_ = other.value_; }

    // Add copy assignment operator
    RequiredParam& operator=(const RequiredParam& other) {
        value_ = other.value_;
        return *this;
    }

    /**
     * @brief Assignment operator to set the value
     * @param value The value to set
     * @return Reference to this object
     */
    RequiredParam& operator=(const T& value) {
        value_ = value;
        return *this;
    }

    /**
     * @brief Set the value of the config variable
     * @param value The value to set
     */
    void Set(const T& value) { value_ = value; }

    /**
     * @brief Get the value of the config variable
     * @return The stored value
     * @throws std::runtime_error if the value has not been set
     */
    T Get() const {
        if (!value_.has_value()) {
            if (name_ == nullptr) {
                throw std::runtime_error("Required parameter has not been set");
            } else {
                throw std::runtime_error("Required parameter " +
                                         std::string(name_) +
                                         " has not been set");
            }
        }
        return value_.value();
    }

    /**
     * @brief Implicit conversion operator to type T
     * @return The stored value
     * @throws std::runtime_error if the value has not been set
     */
    operator T() const { return Get(); }

    /**
     * @brief Check if the value has been set
     * @return true if the value is set, false otherwise
     */
    bool IsSet() const { return value_.has_value(); }

    /**
     * @brief Get the value if set, otherwise return a default value
     * @param default_value The default value to return if not set
     * @return The stored value or the default value
     */
    T GetOrDefault(const T& default_value) const {
        return value_.value_or(default_value);
    }

    /**
     * @brief Clear the stored value
     */
    void Clear() { value_.reset(); }

   private:
    // The name of the parameter, used for error message
    const char* name_;
};

}  // namespace mooncake