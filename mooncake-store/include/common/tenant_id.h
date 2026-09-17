#pragma once

#include <cstddef>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

namespace mooncake {

class TenantId final {
   public:
    static constexpr std::string_view kDefaultValue = "default";

    TenantId() : value_(kDefaultValue) {}

    explicit TenantId(std::string raw)
        : value_(raw.empty() ? std::string(kDefaultValue) : std::move(raw)) {}

    static const TenantId& Default() {
        static const TenantId kDefaultTenant;
        return kDefaultTenant;
    }

    const std::string& value() const noexcept { return value_; }

    bool IsDefault() const noexcept { return value_ == kDefaultValue; }

    bool IsValid() const noexcept {
        if (value_.empty() || value_.front() == kReservedPrefix) {
            return false;
        }
        for (unsigned char c : value_) {
            if (c < kFirstPrintableAscii || c == kDeleteAscii) {
                return false;
            }
        }
        return true;
    }

    std::string MakeScopedKey(std::string_view local_key) const {
        std::string scoped_key;
        scoped_key.reserve(value_.size() + local_key.size() + 1);
        scoped_key.append(value_);
        scoped_key.push_back(kScopedKeySeparator);
        scoped_key.append(local_key);
        return scoped_key;
    }

    static std::pair<TenantId, std::string> ParseScopedKey(
        std::string_view scoped_key) {
        const auto separator = scoped_key.find(kScopedKeySeparator);
        if (separator == std::string_view::npos) {
            return {TenantId::Default(), std::string(scoped_key)};
        }
        return {TenantId(std::string(scoped_key.substr(0, separator))),
                std::string(scoped_key.substr(separator + 1))};
    }

    friend bool operator==(const TenantId&, const TenantId&) = default;

    friend bool operator<(const TenantId& lhs, const TenantId& rhs) noexcept {
        return lhs.value_ < rhs.value_;
    }

    friend std::ostream& operator<<(std::ostream& os,
                                    const TenantId& tenant_id) {
        return os << tenant_id.value_;
    }

   private:
    static constexpr char kReservedPrefix = '_';
    static constexpr unsigned char kFirstPrintableAscii = 0x20;
    static constexpr unsigned char kDeleteAscii = 0x7f;
    static constexpr char kScopedKeySeparator = '\0';

    std::string value_;
};

struct TenantIdHash {
    size_t operator()(const TenantId& tenant_id) const noexcept {
        return std::hash<std::string>{}(tenant_id.value());
    }
};

}  // namespace mooncake
