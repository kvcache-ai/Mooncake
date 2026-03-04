#include "utils/type_util.h"

#include <cctype>
#include <charconv>
#include <string>
#include <string_view>

namespace mooncake {

bool TypeUtil::ParseBool(std::string_view value, bool& out) {
    std::string lower;
    lower.reserve(value.size());
    for (char ch : value) {
        lower.push_back(
            static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    if (lower == "1" || lower == "true" || lower == "yes") {
        out = true;
        return true;
    }
    if (lower == "0" || lower == "false" || lower == "no") {
        out = false;
        return true;
    }
    return false;
}

bool TypeUtil::ParseUint64(std::string_view value, uint64_t& out) {
    const char* begin = value.data();
    const char* end = begin + value.size();
    if (begin == end) {
        return false;
    }
    auto result = std::from_chars(begin, end, out);
    return result.ec == std::errc{} && result.ptr == end;
}

bool TypeUtil::ParseInt64(std::string_view value, int64_t& out) {
    const char* begin = value.data();
    const char* end = begin + value.size();
    if (begin == end) {
        return false;
    }
    auto result = std::from_chars(begin, end, out);
    return result.ec == std::errc{} && result.ptr == end;
}

}  // namespace mooncake
