#pragma once
#include <charconv>
#include <optional>
#include <string>
#include <string_view>

namespace mooncake {
// An opaque physical record name inside the ordinary tenant scope. Old and
// new incarnations coexist; neither a same-size rewrite nor a stale query can
// redirect a read to another incarnation's bytes.
inline std::string MakeOffloadStorageKey(std::string_view key,
                                         std::string_view generation) {
    if (generation.empty() || generation.size() > 64) return {};
    return "@mcssd2:" + std::to_string(generation.size()) + ":" +
           std::string(generation) + std::string(key);
}
struct OffloadStorageIdentity {
    std::string key;
    std::string generation;
};
inline std::optional<OffloadStorageIdentity> ParseOffloadStorageKey(
    std::string_view physical) {
    constexpr std::string_view prefix = "@mcssd2:";
    if (!physical.starts_with(prefix)) return {};
    physical.remove_prefix(prefix.size());
    const auto separator = physical.find(':');
    if (separator == std::string_view::npos) return {};
    size_t length = 0;
    const auto parsed =
        std::from_chars(physical.data(), physical.data() + separator, length);
    if (parsed.ec != std::errc{} || parsed.ptr != physical.data() + separator ||
        !length || length > 64)
        return {};
    physical.remove_prefix(separator + 1);
    if (physical.size() < length) return {};
    return OffloadStorageIdentity{std::string(physical.substr(length)),
                                  std::string(physical.substr(0, length))};
}
}  // namespace mooncake
