#pragma once

#include <charconv>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace mooncake {

struct KvEventKeyInfo {
    std::string cache_prefix;
    std::string model_name;
    std::string block_hash;
    std::optional<uint64_t> seq_hash;
    std::optional<int64_t> group_id;
    std::optional<int64_t> tp_rank;
    std::optional<int64_t> head_or_tp_rank;
    std::optional<int64_t> pcp_rank;
    std::optional<int64_t> dcp_rank;
    std::optional<int64_t> pp_rank;
    std::optional<int64_t> layer_id;
};

namespace detail {

inline std::vector<std::string_view> SplitKey(std::string_view key) {
    std::vector<std::string_view> parts;
    size_t begin = 0;
    while (begin <= key.size()) {
        const size_t end = key.find('@', begin);
        if (end == std::string_view::npos) {
            parts.emplace_back(key.substr(begin));
            break;
        }
        parts.emplace_back(key.substr(begin, end - begin));
        begin = end + 1;
    }
    return parts;
}

inline bool ParseNonNegativeInt(std::string_view value, int64_t* result) {
    if (value.empty()) {
        return false;
    }
    int64_t parsed = 0;
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
    if (error != std::errc{} || end != value.data() + value.size() ||
        parsed < 0) {
        return false;
    }
    *result = parsed;
    return true;
}

inline bool ParseLabeledInt(std::string_view part, std::string_view label,
                            int64_t* result) {
    return part.starts_with(label) &&
           ParseNonNegativeInt(part.substr(label.size()), result);
}

inline std::optional<uint64_t> ParseLow64Hex(std::string_view value) {
    if (value.starts_with("0x") || value.starts_with("0X")) {
        value.remove_prefix(2);
    }
    if (value.empty()) {
        return std::nullopt;
    }
    for (const char c : value) {
        const bool hex_digit = (c >= '0' && c <= '9') ||
                               (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        if (!hex_digit) {
            return std::nullopt;
        }
    }

    // vLLM converts byte hashes to KV Event integers by retaining the low
    // 64 bits. Connector keys carry the full hash as hexadecimal text.
    if (value.size() > 16) {
        value.remove_prefix(value.size() - 16);
    }
    uint64_t parsed = 0;
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed, 16);
    if (error != std::errc{} || end != value.data() + value.size()) {
        return std::nullopt;
    }
    return parsed;
}

inline std::string JoinParts(const std::vector<std::string_view>& parts,
                             size_t begin, size_t end) {
    std::string result;
    for (size_t i = begin; i < end; ++i) {
        if (!result.empty()) {
            result.push_back('@');
        }
        result.append(parts[i]);
    }
    return result;
}

}  // namespace detail

// Parses Mooncake Store keys emitted by the vLLM and vLLM Ascend connectors.
// Unknown keys return nullopt and remain available to consumers as object_key.
inline std::optional<KvEventKeyInfo> ParseKvEventKey(
    const std::string& object_key) {
    const auto parts = detail::SplitKey(object_key);
    if (parts.size() < 6) {
        return std::nullopt;
    }

    // vLLM:
    // [namespace@]model@tp_rank:N@pcpN@dcpN@pp_rank:N[@group:N]@hash
    for (size_t i = 1; i < parts.size(); ++i) {
        int64_t tp_rank = 0;
        if (!detail::ParseLabeledInt(parts[i], "tp_rank:", &tp_rank)) {
            continue;
        }
        const bool has_group = i + 6 == parts.size();
        const bool no_group = i + 5 == parts.size();
        if (!has_group && !no_group) {
            continue;
        }
        int64_t pcp_rank = 0;
        int64_t dcp_rank = 0;
        int64_t pp_rank = 0;
        int64_t group_id = 0;
        if (!detail::ParseLabeledInt(parts[i + 1], "pcp", &pcp_rank) ||
            !detail::ParseLabeledInt(parts[i + 2], "dcp", &dcp_rank) ||
            !detail::ParseLabeledInt(parts[i + 3], "pp_rank:", &pp_rank) ||
            (has_group &&
             !detail::ParseLabeledInt(parts[i + 4], "group:", &group_id))) {
            continue;
        }
        const size_t hash_index = i + (has_group ? 5 : 4);
        if (parts[i - 1].empty() || parts[hash_index].empty()) {
            return std::nullopt;
        }
        KvEventKeyInfo info;
        // cache_prefix and model_name are both unescaped. The connector puts
        // model_name immediately before tp_rank, which is the only stable
        // boundary available to the publisher.
        if (i > 1) {
            info.cache_prefix = detail::JoinParts(parts, 0, i - 1);
        }
        info.model_name = std::string(parts[i - 1]);
        info.block_hash = std::string(parts[hash_index]);
        info.seq_hash = detail::ParseLow64Hex(parts[hash_index]);
        info.tp_rank = tp_rank;
        info.pcp_rank = pcp_rank;
        info.dcp_rank = dcp_rank;
        info.pp_rank = pp_rank;
        if (has_group) {
            info.group_id = group_id;
        }
        return info;
    }

    // vLLM Ascend normal:
    // model@pcpN@dcpN@head_or_tp_rank:N@pp_rank:N@hash
    // vLLM Ascend layerwise:
    // model@pcpN@dcpN@head_or_tp_rank:N@hash@layer_id
    for (size_t i = 1; i < parts.size(); ++i) {
        int64_t pcp_rank = 0;
        if (!detail::ParseLabeledInt(parts[i], "pcp", &pcp_rank)) {
            continue;
        }
        int64_t dcp_rank = 0;
        int64_t head_or_tp_rank = 0;
        if (i + 4 >= parts.size() ||
            !detail::ParseLabeledInt(parts[i + 1], "dcp", &dcp_rank) ||
            !detail::ParseLabeledInt(parts[i + 2],
                                     "head_or_tp_rank:", &head_or_tp_rank)) {
            continue;
        }

        size_t hash_index = 0;
        int64_t pp_rank = 0;
        int64_t layer_id = 0;
        if (i + 5 == parts.size() &&
            detail::ParseLabeledInt(parts[i + 3], "pp_rank:", &pp_rank)) {
            hash_index = i + 4;
        } else if (i + 5 == parts.size() &&
                   detail::ParseNonNegativeInt(parts[i + 4], &layer_id)) {
            hash_index = i + 3;
        } else {
            continue;
        }
        if (parts[0].empty() || parts[hash_index].empty()) {
            return std::nullopt;
        }
        KvEventKeyInfo info;
        info.model_name = detail::JoinParts(parts, 0, i);
        info.block_hash = std::string(parts[hash_index]);
        info.seq_hash = detail::ParseLow64Hex(parts[hash_index]);
        info.head_or_tp_rank = head_or_tp_rank;
        info.pcp_rank = pcp_rank;
        info.dcp_rank = dcp_rank;
        if (hash_index == i + 4) {
            info.pp_rank = pp_rank;
        } else {
            info.layer_id = layer_id;
        }
        return info;
    }
    return std::nullopt;
}

// Parses object keys encoded as decimal or 0x-prefixed hex u64 hashes.
inline std::optional<uint64_t> ParseSeqHashFromObjectKey(
    const std::string& object_key) {
    if (object_key.empty()) {
        return std::nullopt;
    }
    std::string_view encoded = object_key;
    int base = 10;
    if (encoded.size() >= 2 && encoded[0] == '0' &&
        (encoded[1] == 'x' || encoded[1] == 'X')) {
        encoded.remove_prefix(2);
        base = 16;
    }
    if (encoded.empty()) {
        return std::nullopt;
    }

    uint64_t value = 0;
    const auto [end, error] = std::from_chars(
        encoded.data(), encoded.data() + encoded.size(), value, base);
    if (error != std::errc{} || end != encoded.data() + encoded.size()) {
        return std::nullopt;
    }
    return value;
}

}  // namespace mooncake
