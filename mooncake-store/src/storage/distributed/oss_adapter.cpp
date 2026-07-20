#include "storage/distributed/oss_adapter.h"

#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>

#include <glog/logging.h>

namespace mooncake {
namespace {

std::once_flag curl_init_once;

std::string GetEnv(const char* name, const char* fallback = nullptr) {
    const char* value = std::getenv(name);
    if ((!value || !*value) && fallback) value = std::getenv(fallback);
    return value ? value : "";
}

bool GetEnvBool(const char* name, bool default_value) {
    auto value = GetEnv(name);
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (value.empty()) return default_value;
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

std::string Trim(std::string value) {
    auto not_space = [](unsigned char c) { return !std::isspace(c); };
    value.erase(value.begin(),
                std::find_if(value.begin(), value.end(), not_space));
    value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(),
                value.end());
    return value;
}

std::string Lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return value;
}

std::string UriEncode(std::string_view value, bool preserve_slash = false) {
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

int FromHex(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::string UriDecode(std::string_view value) {
    std::string result;
    result.reserve(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '%' && i + 2 < value.size()) {
            int hi = FromHex(value[i + 1]);
            int lo = FromHex(value[i + 2]);
            if (hi >= 0 && lo >= 0) {
                result.push_back(static_cast<char>((hi << 4) | lo));
                i += 2;
                continue;
            }
        }
        result.push_back(value[i]);
    }
    return result;
}

std::string XmlDecode(std::string value) {
    const std::array<std::pair<std::string_view, std::string_view>, 5>
        entities = {{{"&amp;", "&"},
                     {"&lt;", "<"},
                     {"&gt;", ">"},
                     {"&quot;", "\""},
                     {"&apos;", "'"}}};
    for (const auto& [encoded, decoded] : entities) {
        size_t pos = 0;
        while ((pos = value.find(encoded, pos)) != std::string::npos) {
            value.replace(pos, encoded.size(), decoded);
            pos += decoded.size();
        }
    }
    return value;
}

std::string XmlValue(std::string_view xml, std::string_view tag,
                     size_t start = 0) {
    const std::string open = "<" + std::string(tag) + ">";
    const std::string close = "</" + std::string(tag) + ">";
    size_t begin = xml.find(open, start);
    if (begin == std::string_view::npos) return {};
    begin += open.size();
    size_t end = xml.find(close, begin);
    if (end == std::string_view::npos) return {};
    return XmlDecode(std::string(xml.substr(begin, end - begin)));
}

std::string Hex(const unsigned char* data, size_t size) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string result(size * 2, '\0');
    for (size_t i = 0; i < size; ++i) {
        result[2 * i] = hex[data[i] >> 4];
        result[2 * i + 1] = hex[data[i] & 0x0f];
    }
    return result;
}

std::vector<unsigned char> HmacSha256(const void* key, size_t key_size,
                                      std::string_view data) {
    std::vector<unsigned char> result(EVP_MAX_MD_SIZE);
    unsigned int result_size = 0;
    HMAC(EVP_sha256(), key, static_cast<int>(key_size),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(),
         result.data(), &result_size);
    result.resize(result_size);
    return result;
}

std::string Sha256Hex(std::string_view data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(),
           hash);
    return Hex(hash, sizeof(hash));
}

std::pair<std::string, std::string> OssTimestamp() {
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

std::string CanonicalQuery(const std::map<std::string, std::string>& query) {
    std::map<std::string, std::string> encoded;
    for (const auto& [name, value] : query) {
        encoded.emplace(UriEncode(name), UriEncode(value));
    }
    std::string result;
    for (const auto& [name, value] : encoded) {
        if (!result.empty()) result.push_back('&');
        result += name;
        if (!value.empty()) result += "=" + value;
    }
    return result;
}

size_t BodyCallback(char* data, size_t size, size_t count, void* user_data) {
    auto* body = static_cast<std::string*>(user_data);
    body->append(data, size * count);
    return size * count;
}

size_t HeaderCallback(char* data, size_t size, size_t count, void* user_data) {
    auto* headers = static_cast<std::map<std::string, std::string>*>(user_data);
    std::string_view line(data, size * count);
    const size_t colon = line.find(':');
    if (colon != std::string_view::npos) {
        std::string name = Lower(std::string(line.substr(0, colon)));
        std::string value = Trim(std::string(line.substr(colon + 1)));
        (*headers)[std::move(name)] = std::move(value);
    }
    return size * count;
}

bool IsSuccess(long status) { return status >= 200 && status < 300; }

}  // namespace

OssFileSystemAdapter::~OssFileSystemAdapter() { Shutdown(); }

tl::expected<void, ErrorCode> OssFileSystemAdapter::Init(
    const std::string& mount_path) {
    endpoint_ = GetEnv("MOONCAKE_OSS_ENDPOINT", "OSS_ENDPOINT");
    bucket_ = GetEnv("MOONCAKE_OSS_BUCKET", "OSS_BUCKET");
    region_ = GetEnv("MOONCAKE_OSS_REGION", "OSS_REGION");
    access_key_id_ = GetEnv("MOONCAKE_OSS_ACCESS_KEY_ID", "OSS_ACCESS_KEY_ID");
    access_key_secret_ =
        GetEnv("MOONCAKE_OSS_ACCESS_KEY_SECRET", "OSS_ACCESS_KEY_SECRET");
    security_token_ =
        GetEnv("MOONCAKE_OSS_SECURITY_TOKEN", "OSS_SESSION_TOKEN");
    path_style_ = GetEnvBool("MOONCAKE_OSS_PATH_STYLE", false);
    anonymous_ = GetEnvBool("MOONCAKE_OSS_ANONYMOUS", false);

    while (!endpoint_.empty() && endpoint_.back() == '/') endpoint_.pop_back();
    mount_path_ = mount_path;
    while (mount_path_.size() > 1 && mount_path_.back() == '/')
        mount_path_.pop_back();

    if (endpoint_.empty() || bucket_.empty() || region_.empty()) {
        LOG(ERROR) << "OSS requires MOONCAKE_OSS_ENDPOINT, "
                      "MOONCAKE_OSS_BUCKET and MOONCAKE_OSS_REGION";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!anonymous_ && (access_key_id_.empty() || access_key_secret_.empty())) {
        LOG(ERROR)
            << "OSS credentials are missing; set MOONCAKE_OSS_ACCESS_KEY_ID "
               "and MOONCAKE_OSS_ACCESS_KEY_SECRET";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::call_once(curl_init_once,
                   [] { curl_global_init(CURL_GLOBAL_DEFAULT); });
    initialized_ = true;
    LOG(INFO) << "OSS adapter initialized: endpoint=" << endpoint_
              << ", bucket=" << bucket_ << ", region=" << region_
              << ", root=" << mount_path_;
    return {};
}

tl::expected<void, ErrorCode> OssFileSystemAdapter::Shutdown() {
    initialized_ = false;
    return {};
}

std::string OssFileSystemAdapter::PathToKey(const std::string& path) const {
    const size_t first = path.find_first_not_of('/');
    return first == std::string::npos ? std::string() : path.substr(first);
}

std::string OssFileSystemAdapter::BuildUrl(
    const std::string& key,
    const std::map<std::string, std::string>& query) const {
    std::string base = endpoint_;
    if (path_style_) {
        base += "/" + UriEncode(bucket_);
    } else {
        const size_t scheme = base.find("://");
        if (scheme == std::string::npos) {
            base = bucket_ + "." + base;
        } else {
            base.insert(scheme + 3, bucket_ + ".");
        }
    }
    base += "/" + UriEncode(key, true);
    const auto canonical_query = CanonicalQuery(query);
    if (!canonical_query.empty()) base += "?" + canonical_query;
    return base;
}

std::string OssFileSystemAdapter::BuildAuthorization(
    const std::string& method, const std::string& key,
    const std::map<std::string, std::string>& query,
    const std::string& timestamp) const {
    const std::string date = timestamp.substr(0, 8);
    const std::string canonical_uri =
        UriEncode("/" + bucket_ + "/" + key, true);
    std::string canonical_headers = "x-oss-content-sha256:UNSIGNED-PAYLOAD\n";
    canonical_headers += "x-oss-date:" + timestamp + "\n";
    if (!security_token_.empty()) {
        canonical_headers +=
            "x-oss-security-token:" + Trim(security_token_) + "\n";
    }
    const std::string canonical_request =
        method + "\n" + canonical_uri + "\n" + CanonicalQuery(query) + "\n" +
        canonical_headers + "\n\nUNSIGNED-PAYLOAD";
    const std::string scope = date + "/" + region_ + "/oss/aliyun_v4_request";
    const std::string string_to_sign = "OSS4-HMAC-SHA256\n" + timestamp + "\n" +
                                       scope + "\n" +
                                       Sha256Hex(canonical_request);

    const std::string secret = "aliyun_v4" + access_key_secret_;
    auto date_key = HmacSha256(secret.data(), secret.size(), date);
    auto region_key = HmacSha256(date_key.data(), date_key.size(), region_);
    auto service_key = HmacSha256(region_key.data(), region_key.size(), "oss");
    auto signing_key =
        HmacSha256(service_key.data(), service_key.size(), "aliyun_v4_request");
    auto signature =
        HmacSha256(signing_key.data(), signing_key.size(), string_to_sign);
    return "OSS4-HMAC-SHA256 Credential=" + access_key_id_ + "/" + scope +
           ",Signature=" + Hex(signature.data(), signature.size());
}

tl::expected<OssFileSystemAdapter::Response, ErrorCode>
OssFileSystemAdapter::Request(const std::string& method, const std::string& key,
                              const std::map<std::string, std::string>& query,
                              const char* body, size_t body_size,
                              const std::string& range) const {
    if (!initialized_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    CURL* curl = curl_easy_init();
    if (!curl) return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    Response response;
    curl_slist* headers = nullptr;
    auto add_header = [&](const std::string& header) {
        headers = curl_slist_append(headers, header.c_str());
    };

    const std::string timestamp = OssTimestamp().first;
    add_header("x-oss-content-sha256: UNSIGNED-PAYLOAD");
    add_header("x-oss-date: " + timestamp);
    if (!security_token_.empty())
        add_header("x-oss-security-token: " + security_token_);
    if (!anonymous_)
        add_header("Authorization: " +
                   BuildAuthorization(method, key, query, timestamp));
    add_header("Expect:");
    add_header("Content-Type:");
    if (!range.empty()) add_header("Range: " + range);

    const std::string url = BuildUrl(key, query);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, BodyCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response.body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response.headers);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 5000L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 120000L);
    if (method == "HEAD") curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    if (method == "PUT") {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body ? body : "");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
                         static_cast<curl_off_t>(body_size));
    }

    const CURLcode result = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.status);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (result != CURLE_OK) {
        LOG(ERROR) << "OSS " << method
                   << " request failed: " << curl_easy_strerror(result);
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (!IsSuccess(response.status) && response.status != 404) {
        LOG(ERROR) << "OSS " << method << " returned HTTP " << response.status
                   << ": " << response.body.substr(0, 512);
    }
    return response;
}

tl::expected<size_t, ErrorCode> OssFileSystemAdapter::WriteFile(
    const std::string& path, std::span<const char> data) {
    auto response =
        Request("PUT", PathToKey(path), {}, data.data(), data.size());
    if (!response) return tl::make_unexpected(response.error());
    if (!IsSuccess(response->status))
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return data.size();
}

tl::expected<size_t, ErrorCode> OssFileSystemAdapter::ReadFile(
    const std::string& path, void* buf, size_t len) {
    if (!buf && len > 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    std::string range;
    if (len > 0) range = "bytes=0-" + std::to_string(len - 1);
    auto response = Request("GET", PathToKey(path), {}, nullptr, 0, range);
    if (!response) return tl::make_unexpected(response.error());
    if (response->status == 404)
        return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
    if (!IsSuccess(response->status) || response->body.size() != len)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    if (len > 0) std::memcpy(buf, response->body.data(), len);
    return len;
}

tl::expected<size_t, ErrorCode> OssFileSystemAdapter::VectorWriteFile(
    const std::string& path, const iovec* iov, int iovcnt, off_t offset) {
    if (offset != 0 || iovcnt < 0 || (!iov && iovcnt > 0))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    size_t total = 0;
    for (int i = 0; i < iovcnt; ++i) {
        if ((!iov[i].iov_base && iov[i].iov_len > 0) ||
            iov[i].iov_len > std::numeric_limits<size_t>::max() - total)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        total += iov[i].iov_len;
    }
    std::string data;
    data.reserve(total);
    for (int i = 0; i < iovcnt; ++i) {
        if (iov[i].iov_len > 0) {
            data.append(static_cast<const char*>(iov[i].iov_base),
                        iov[i].iov_len);
        }
    }
    return WriteFile(path, std::span<const char>(data.data(), data.size()));
}

tl::expected<size_t, ErrorCode> OssFileSystemAdapter::VectorReadFile(
    const std::string& path, const iovec* iov, int iovcnt, off_t offset) {
    if (offset < 0 || iovcnt < 0 || (!iov && iovcnt > 0))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    size_t total = 0;
    for (int i = 0; i < iovcnt; ++i) {
        if ((!iov[i].iov_base && iov[i].iov_len > 0) ||
            iov[i].iov_len > std::numeric_limits<size_t>::max() - total)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        total += iov[i].iov_len;
    }
    std::string range;
    if (total > 0) {
        range = "bytes=" + std::to_string(offset) + "-" +
                std::to_string(static_cast<uint64_t>(offset) + total - 1);
    }
    auto response = Request("GET", PathToKey(path), {}, nullptr, 0, range);
    if (!response) return tl::make_unexpected(response.error());
    if (response->status == 404)
        return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
    if (!IsSuccess(response->status) || response->body.size() != total)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    size_t copied = 0;
    for (int i = 0; i < iovcnt; ++i) {
        if (iov[i].iov_len > 0) {
            std::memcpy(iov[i].iov_base, response->body.data() + copied,
                        iov[i].iov_len);
        }
        copied += iov[i].iov_len;
    }
    return total;
}

tl::expected<void, ErrorCode> OssFileSystemAdapter::DeleteFile(
    const std::string& path) {
    auto response = Request("DELETE", PathToKey(path));
    if (!response) return tl::make_unexpected(response.error());
    if (!IsSuccess(response->status) && response->status != 404)
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

tl::expected<bool, ErrorCode> OssFileSystemAdapter::FileExists(
    const std::string& path) {
    auto response = Request("HEAD", PathToKey(path));
    if (!response) return tl::make_unexpected(response.error());
    if (response->status == 404) return false;
    if (!IsSuccess(response->status))
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    return true;
}

tl::expected<size_t, ErrorCode> OssFileSystemAdapter::GetFileSize(
    const std::string& path) {
    auto response = Request("HEAD", PathToKey(path));
    if (!response) return tl::make_unexpected(response.error());
    if (response->status == 404)
        return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
    if (!IsSuccess(response->status))
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    auto it = response->headers.find("content-length");
    if (it == response->headers.end())
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    try {
        return static_cast<size_t>(std::stoull(it->second));
    } catch (...) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
}

tl::expected<std::vector<FileInfo>, ErrorCode>
OssFileSystemAdapter::ListFilesInternal(const std::string& dir) const {
    std::string prefix = PathToKey(dir);
    if (!prefix.empty() && prefix.back() != '/') prefix.push_back('/');
    std::vector<FileInfo> files;
    std::string token;
    do {
        std::map<std::string, std::string> query{{"delimiter", "/"},
                                                 {"encoding-type", "url"},
                                                 {"list-type", "2"},
                                                 {"max-keys", "1000"},
                                                 {"prefix", prefix}};
        if (!token.empty()) query["continuation-token"] = token;
        auto response = Request("GET", "", query);
        if (!response) return tl::make_unexpected(response.error());
        if (!IsSuccess(response->status))
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);

        size_t pos = 0;
        while ((pos = response->body.find("<Contents>", pos)) !=
               std::string::npos) {
            const size_t end = response->body.find("</Contents>", pos);
            if (end == std::string::npos) break;
            const std::string_view item(response->body.data() + pos,
                                        end + 11 - pos);
            std::string key = UriDecode(XmlValue(item, "Key"));
            const std::string size_value = XmlValue(item, "Size");
            if (key.rfind(prefix, 0) == 0) {
                std::string name = key.substr(prefix.size());
                if (!name.empty() && name.find('/') == std::string::npos) {
                    try {
                        files.push_back(
                            {std::move(name), std::stoull(size_value)});
                    } catch (...) {
                        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                    }
                }
            }
            pos = end + 11;
        }
        token = UriDecode(XmlValue(response->body, "NextContinuationToken"));
        const std::string truncated =
            Lower(XmlValue(response->body, "IsTruncated"));
        if (truncated != "true") token.clear();
        if (truncated == "true" && token.empty())
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    } while (!token.empty());
    return files;
}

tl::expected<std::vector<std::string>, ErrorCode>
OssFileSystemAdapter::ListFiles(const std::string& dir) {
    auto infos = ListFilesInternal(dir);
    if (!infos) return tl::make_unexpected(infos.error());
    std::vector<std::string> files;
    files.reserve(infos->size());
    for (auto& info : *infos) files.push_back(std::move(info.name));
    return files;
}

tl::expected<std::vector<FileInfo>, ErrorCode>
OssFileSystemAdapter::ListFilesWithInfo(const std::string& dir) {
    return ListFilesInternal(dir);
}

}  // namespace mooncake
