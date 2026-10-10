#pragma once

// Signing helpers for object storage adapters, used by the S3 adapter. They
// are protocol-neutral: S3 and OSS V4 both use HMAC-SHA256 request signing
// over a canonical request and differ only in header names, scope and
// canonicalization details, which the adapters own.

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <cctype>
#include <chrono>
#include <ctime>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mooncake::object_storage_signing {

inline std::string UriEncode(std::string_view value,
                             bool preserve_slash = false) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string result;
    result.reserve(value.size() * 3);
    for (unsigned char c : value) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' ||
            (preserve_slash && c == '/')) {
            result.push_back(static_cast<char>(c));
        } else {
            result.push_back('%');
            result.push_back(hex[c >> 4]);
            result.push_back(hex[c & 0x0f]);
        }
    }
    return result;
}

inline std::string Hex(const unsigned char* data, size_t size) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string result(size * 2, '\0');
    for (size_t i = 0; i < size; ++i) {
        result[2 * i] = hex[data[i] >> 4];
        result[2 * i + 1] = hex[data[i] & 0x0f];
    }
    return result;
}

inline std::vector<unsigned char> HmacSha256(const void* key, size_t key_size,
                                             std::string_view data) {
    std::vector<unsigned char> result(EVP_MAX_MD_SIZE);
    unsigned int result_size = 0;
    HMAC(EVP_sha256(), key, static_cast<int>(key_size),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(),
         result.data(), &result_size);
    result.resize(result_size);
    return result;
}

inline std::string Sha256Hex(std::string_view data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(),
           hash);
    return Hex(hash, sizeof(hash));
}

// Returns {"YYYYMMDDTHHMMSSZ", "YYYYMMDD"} for the current UTC time.
inline std::pair<std::string, std::string> SigningTimestamp() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t value = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&value, &tm);
    char timestamp[17];
    char date[9];
    std::strftime(timestamp, sizeof(timestamp), "%Y%m%dT%H%M%SZ", &tm);
    std::strftime(date, sizeof(date), "%Y%m%d", &tm);
    return {timestamp, date};
}

// Sorted, URI-encoded query string. OSS V4 writes a parameter with an empty
// value as "name"; AWS Signature V4 requires "name=".
inline std::string CanonicalQuery(
    const std::map<std::string, std::string>& query,
    bool equals_for_empty_value = false) {
    std::map<std::string, std::string> encoded;
    for (const auto& [name, value] : query) {
        encoded.emplace(UriEncode(name), UriEncode(value));
    }
    std::string result;
    for (const auto& [name, value] : encoded) {
        if (!result.empty()) result.push_back('&');
        result += name;
        if (!value.empty() || equals_for_empty_value) result += "=" + value;
    }
    return result;
}

}  // namespace mooncake::object_storage_signing
