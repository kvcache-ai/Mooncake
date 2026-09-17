#pragma once

#include <cstdint>
#include <string>

namespace mooncake {
namespace base64 {

// Base64 encoding for binary payload.
// JsonCpp treats strings as UTF-8, so we must encode binary data.
inline std::string Encode(const std::string& data) {
    static const char base64_chars[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string result;
    result.reserve(((data.size() + 2) / 3) * 4);

    size_t i = 0;
    size_t data_len = data.size();

    // Process 3 bytes at a time
    while (i + 2 < data_len) {
        uint32_t octet_a = static_cast<unsigned char>(data[i++]);
        uint32_t octet_b = static_cast<unsigned char>(data[i++]);
        uint32_t octet_c = static_cast<unsigned char>(data[i++]);

        uint32_t triple = (octet_a << 16) | (octet_b << 8) | octet_c;

        result.push_back(base64_chars[(triple >> 18) & 0x3F]);
        result.push_back(base64_chars[(triple >> 12) & 0x3F]);
        result.push_back(base64_chars[(triple >> 6) & 0x3F]);
        result.push_back(base64_chars[triple & 0x3F]);
    }

    // Handle remaining bytes
    size_t remaining = data_len - i;
    if (remaining > 0) {
        uint32_t octet_a = static_cast<unsigned char>(data[i++]);
        uint32_t octet_b =
            (remaining > 1) ? static_cast<unsigned char>(data[i++]) : 0;
        uint32_t octet_c = 0;

        uint32_t triple = (octet_a << 16) | (octet_b << 8) | octet_c;

        result.push_back(base64_chars[(triple >> 18) & 0x3F]);
        result.push_back(base64_chars[(triple >> 12) & 0x3F]);
        result.push_back((remaining > 1) ? base64_chars[(triple >> 6) & 0x3F]
                                         : '=');
        result.push_back('=');
    }

    return result;
}

// Base64 decoding for binary payload.
inline std::string Decode(const std::string& encoded) {
    static const unsigned char decode_table[256] = {
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63, 52, 53, 54, 55, 56, 57,
        58, 59, 60, 61, 64, 64, 64, 64, 64, 64, 64, 0,  1,  2,  3,  4,  5,  6,
        7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 64, 64, 64, 64, 64, 64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
        37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64};

    std::string result;
    result.reserve((encoded.size() * 3) / 4);

    size_t i = 0;
    while (i < encoded.size()) {
        // Skip whitespace and invalid chars
        while (i < encoded.size() &&
               (encoded[i] == ' ' || encoded[i] == '\n' || encoded[i] == '\r' ||
                encoded[i] == '\t')) {
            i++;
        }
        if (i >= encoded.size()) break;

        uint32_t sextet_a =
            decode_table[static_cast<unsigned char>(encoded[i++])];
        if (i >= encoded.size() || sextet_a == 64) break;

        uint32_t sextet_b =
            decode_table[static_cast<unsigned char>(encoded[i++])];
        if (sextet_b == 64) break;

        uint32_t sextet_c =
            (i < encoded.size())
                ? decode_table[static_cast<unsigned char>(encoded[i++])]
                : 64;
        uint32_t sextet_d =
            (i < encoded.size())
                ? decode_table[static_cast<unsigned char>(encoded[i++])]
                : 64;

        uint32_t triple = (sextet_a << 18) | (sextet_b << 12) |
                          ((sextet_c != 64) ? (sextet_c << 6) : 0) |
                          ((sextet_d != 64) ? sextet_d : 0);

        result.push_back(static_cast<char>((triple >> 16) & 0xFF));
        if (sextet_c != 64) {
            result.push_back(static_cast<char>((triple >> 8) & 0xFF));
        }
        if (sextet_d != 64) {
            result.push_back(static_cast<char>(triple & 0xFF));
        }
    }

    return result;
}

}  // namespace base64
}  // namespace mooncake
