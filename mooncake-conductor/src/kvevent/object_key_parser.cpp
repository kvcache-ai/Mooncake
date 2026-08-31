#include "conductor/kvevent/object_key_parser.h"

#include <charconv>
#include <cctype>
#include <string_view>
#include <vector>

namespace conductor {
namespace kvevent {
namespace {

bool IsHex(char value) {
    return (value >= '0' && value <= '9') ||
           (value >= 'a' && value <= 'f') ||
           (value >= 'A' && value <= 'F');
}

char LowerHex(char value) {
    return value >= 'A' && value <= 'F'
               ? static_cast<char>(value - 'A' + 'a')
               : value;
}

std::vector<std::string_view> SplitAt(std::string_view value, char separator) {
    std::vector<std::string_view> parts;
    size_t begin = 0;
    while (begin <= value.size()) {
        const size_t end = value.find(separator, begin);
        if (end == std::string_view::npos) {
            parts.emplace_back(value.substr(begin));
            break;
        }
        parts.emplace_back(value.substr(begin, end - begin));
        begin = end + 1;
    }
    return parts;
}

std::string JoinParts(const std::vector<std::string_view>& parts,
                      size_t begin, size_t end) {
    std::string result;
    for (size_t index = begin; index < end; ++index) {
        if (!result.empty()) result.push_back('@');
        result.append(parts[index]);
    }
    return result;
}

bool HasLabel(std::string_view value, std::string_view label) {
    return value.starts_with(label) && value.size() > label.size();
}

bool IsNonNegativeDecimal(std::string_view value) {
    if (value.empty()) return false;
    uint64_t parsed = 0;
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    return error == std::errc{} && end == value.data() + value.size();
}

bool HasDecimalLabel(std::string_view value, std::string_view label) {
    return value.starts_with(label) &&
           IsNonNegativeDecimal(value.substr(label.size()));
}

bool IsHashText(std::string_view value) {
    if (value.size() < 16 || value.size() % 2 != 0) return false;
    for (char character : value) {
        if (!IsHex(character)) return false;
    }
    return true;
}

std::string_view StripOptionalHexPrefix(std::string_view value) {
    if (value.starts_with("0x") || value.starts_with("0X")) {
        value.remove_prefix(2);
    }
    return value;
}

bool IsVllmHashText(std::string_view value) {
    return IsHashText(StripOptionalHexPrefix(value));
}

std::string FillProjectedHash(std::string_view hash,
                              ParsedSglangObjectKey* result,
                              bool first_half) {
    hash = StripOptionalHexPrefix(hash);
    if (!IsHashText(hash)) return "object key hash is not valid hex";
    result->full_hash.clear();
    result->full_hash.reserve(hash.size());
    for (char character : hash) result->full_hash.push_back(LowerHex(character));
    result->prefix.value = 0;
    const size_t offset = first_half ? 0 : result->full_hash.size() - 16;
    for (size_t index = offset; index < offset + 16; ++index) {
        const char character = result->full_hash[index];
        const uint64_t nibble = character <= '9'
                                    ? static_cast<uint64_t>(character - '0')
                                    : static_cast<uint64_t>(character - 'a' + 10);
        result->prefix.value = (result->prefix.value << 4) | nibble;
    }
    return "";
}

}  // namespace

std::string ParseSglangObjectKey(const std::string& object_key,
                                 ParsedSglangObjectKey* result) {
    if (result == nullptr) {
        return "result is null";
    }
    *result = {};
    constexpr size_t kSha256HexLength = 64;
    size_t hash_start = std::string::npos;
    for (size_t start = 0; start + kSha256HexLength <= object_key.size();
         ++start) {
        if (start != 0 && IsHex(object_key[start - 1])) {
            continue;
        }
        bool valid = true;
        for (size_t index = 0; index < kSha256HexLength; ++index) {
            if (!IsHex(object_key[start + index])) {
                valid = false;
                break;
            }
        }
        if (!valid || (start + kSha256HexLength < object_key.size() &&
                       IsHex(object_key[start + kSha256HexLength]))) {
            continue;
        }
        hash_start = start;
        break;
    }
    if (hash_start == std::string::npos) {
        return "SGLang object_key does not contain an isolated 64-hex hash";
    }

    const size_t hash_end = hash_start + kSha256HexLength;
    if (hash_start > 0 && object_key[hash_start - 1] != '_') {
        return "SGLang hash must be separated from its namespace by '_'";
    }
    if (hash_end < object_key.size() && object_key[hash_end] != '_') {
        return "SGLang hash must be followed by a component suffix";
    }

    result->full_hash.reserve(kSha256HexLength);
    for (size_t index = hash_start; index < hash_end; ++index) {
        result->full_hash.push_back(LowerHex(object_key[index]));
    }
    result->namespace_prefix = object_key.substr(0, hash_start);
    if (!result->namespace_prefix.empty() &&
        result->namespace_prefix.back() == '_') {
        result->namespace_prefix.pop_back();
    }
    result->logical_key = object_key.substr(0, hash_end);
    result->component_suffix = object_key.substr(hash_end);

    uint64_t projected = 0;
    const auto [end, error] = std::from_chars(
        result->full_hash.data(), result->full_hash.data() + 16, projected, 16);
    if (error != std::errc{} || end != result->full_hash.data() + 16) {
        return "failed to project SGLang hash";
    }
    result->prefix.value = projected;
    return "";
}

std::string ParseVllmObjectKey(const std::string& object_key,
                               ParsedSglangObjectKey* result) {
    if (result == nullptr) return "result is null";
    *result = {};
    const auto parts = SplitAt(object_key, '@');

    // Current vLLM-Ascend layerwise GVA keys use a compact layout without
    // field labels: model@hash@head_or_tp_rank, or
    // model@group_id@hash@head_or_tp_rank for multi-group models.
    // Handle this before the labelled layouts, which have six or more parts.
    if (parts.size() == 3 || parts.size() == 4) {
        const size_t hash_index = parts.size() == 3 ? 1 : 2;
        const size_t rank_index = parts.size() - 1;
        const bool valid_group =
            parts.size() == 3 || IsNonNegativeDecimal(parts[1]);
        if (!parts[0].empty() && valid_group &&
            IsVllmHashText(parts[hash_index]) &&
            IsNonNegativeDecimal(parts[rank_index])) {
            result->logical_key = object_key;
            result->namespace_prefix =
                parts.size() == 3 ? std::string(parts[0])
                                  : JoinParts(parts, 0, hash_index);
            return FillProjectedHash(parts[hash_index], result, false);
        }
    }

    if (parts.size() < 6) {
        return "vLLM object_key has too few fields";
    }

    // vLLM keys put the connector hash last.  Newer connectors may append
    // cache namespace metadata (group/cache_role/cache_family) before it;
    // consume those labels when present while retaining the older layout.
    for (size_t index = 1; index < parts.size(); ++index) {
        if (!HasDecimalLabel(parts[index], "tp_rank:")) {
            continue;
        }
        const size_t hash_index = parts.size() - 1;
        if (index + 3 >= hash_index ||
            !HasDecimalLabel(parts[index + 1], "pcp") ||
            !HasDecimalLabel(parts[index + 2], "dcp") ||
            !HasDecimalLabel(parts[index + 3], "pp_rank:")) {
            continue;
        }
        size_t cursor = index + 4;
        if (cursor < hash_index &&
            HasDecimalLabel(parts[cursor], "group:")) {
            ++cursor;
        }
        if (cursor < hash_index && HasLabel(parts[cursor], "cache_role:")) {
            ++cursor;
        }
        if (cursor < hash_index && HasLabel(parts[cursor], "cache_family:")) {
            ++cursor;
        }
        if (cursor != hash_index || !IsVllmHashText(parts[hash_index])) {
            continue;
        }
        result->logical_key = object_key;
        result->namespace_prefix = index > 1 ? JoinParts(parts, 0, index - 1)
                                             : "";
        return FillProjectedHash(parts[hash_index], result, false);
    }

    // vLLM-Ascend keys have pcp/dcp/head_or_tp_rank, followed by optional
    // pp/group/cache metadata.  Layerwise keys either use the current
    // `layer_id:N@hash` form or the historical `hash@N` form.
    for (size_t index = 1; index < parts.size(); ++index) {
        if (!HasDecimalLabel(parts[index], "pcp") ||
            index + 2 >= parts.size() ||
            !HasDecimalLabel(parts[index + 1], "dcp") ||
            !HasDecimalLabel(parts[index + 2], "head_or_tp_rank:")) {
            continue;
        }
        const size_t final_part = parts.size() - 1;
        auto matches_layout = [&](size_t hash_index) {
            if (hash_index <= index + 2 ||
                !IsVllmHashText(parts[hash_index])) {
                return false;
            }
            size_t cursor = index + 3;
            if (cursor < hash_index &&
                HasDecimalLabel(parts[cursor], "pp_rank:")) {
                ++cursor;
            }
            if (cursor < hash_index &&
                HasDecimalLabel(parts[cursor], "group:")) {
                ++cursor;
            }
            if (cursor < hash_index &&
                HasLabel(parts[cursor], "cache_role:")) {
                ++cursor;
            }
            if (cursor < hash_index &&
                HasLabel(parts[cursor], "cache_family:")) {
                ++cursor;
            }
            if (cursor < hash_index &&
                HasDecimalLabel(parts[cursor], "layer_id:")) {
                ++cursor;
            }
            return cursor == hash_index;
        };

        size_t hash_index = final_part;
        if (!matches_layout(hash_index) && final_part > index + 3 &&
            IsNonNegativeDecimal(parts[final_part]) &&
            matches_layout(final_part - 1)) {
            hash_index = final_part - 1;
        }
        if (hash_index == final_part && !matches_layout(hash_index)) {
            continue;
        }
        result->logical_key = object_key;
        result->namespace_prefix = std::string(parts[0]);
        return FillProjectedHash(parts[hash_index], result, false);
    }

    return "unrecognized vLLM/vLLM-Ascend object_key";
}

}  // namespace kvevent
}  // namespace conductor
