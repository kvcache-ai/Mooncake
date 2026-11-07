#include "adapter.h"

#include <curl/curl.h>

#include <algorithm>
#include <charconv>
#include <cctype>
#include <limits>
#include <map>
#include <memory>
#include <string_view>

namespace mooncake::cuobject_prototype {
namespace {
struct Reply {
    std::map<std::string, std::string> headers;
    size_t body_bytes = 0;
};

size_t Header(char* data, size_t size, size_t count, void* user) {
    const size_t length = size * count;
    auto& reply = *static_cast<Reply*>(user);
    std::string_view line(data, length);
    if (line.starts_with("HTTP/")) reply.headers.clear();
    auto colon = line.find(':');
    if (colon == std::string_view::npos) return length;
    std::string name(line.substr(0, colon));
    std::transform(name.begin(), name.end(), name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    auto value = line.substr(colon + 1);
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front())))
        value.remove_prefix(1);
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back())))
        value.remove_suffix(1);
    // Reject duplicate protocol headers instead of silently trusting the last.
    if (!reply.headers.emplace(name, value).second &&
        name.starts_with("x-amz-rdma"))
        return 0;
    return length;
}

size_t Body(char*, size_t size, size_t count, void* user) {
    auto& reply = *static_cast<Reply*>(user);
    const size_t length = size * count;
    if (length > 64 * 1024 - reply.body_bytes) return 0;
    reply.body_bytes += length;
    return length;
}
}  // namespace

tl::expected<size_t, ErrorCode> Adapter::Transfer(const std::string& key,
                                                  void* ptr, size_t size,
                                                  bool upload) {
    if (!initialized_ || !rdma_ || !ptr || size == 0 ||
        size > std::numeric_limits<size_t>::max() -
                   reinterpret_cast<uintptr_t>(ptr))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto token = rdma_->Token(ptr, size, upload);
    if (token.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(),
                                                             curl_easy_cleanup);
    if (!curl) {
        rdma_->Release(token);
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    const auto physical_key = LogicalToPhysicalKey(key);
    const std::string method = upload ? "PUT" : "GET";
    const std::string range =
        upload ? "" : "bytes=0-" + std::to_string(size - 1);
    auto lines = BuildSignedHeaders(method, physical_key, {}, range);
    lines.push_back("x-amz-rdma-token: " + token);
    lines.push_back("Expect:");
    if (!range.empty()) lines.push_back("Range: " + range);
    curl_slist* headers = nullptr;
    for (const auto& line : lines) {
        auto* next = curl_slist_append(headers, line.c_str());
        if (!next) {
            curl_slist_free_all(headers);
            rdma_->Release(token);
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        headers = next;
    }
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> owned_headers(
        headers, curl_slist_free_all);
    Reply reply;
    const auto url = BuildUrl(physical_key, {});
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_PATH_AS_IS, 1L);
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl.get(), CURLOPT_CONNECTTIMEOUT_MS, 10000L);
    curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT_MS, 120000L);
    curl_easy_setopt(curl.get(), CURLOPT_HEADERFUNCTION, Header);
    curl_easy_setopt(curl.get(), CURLOPT_HEADERDATA, &reply);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, Body);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &reply);
    if (upload) {
        curl_easy_setopt(curl.get(), CURLOPT_CUSTOMREQUEST, "PUT");
        curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, "");
        curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE_LARGE,
                         curl_off_t{0});
    }
    const auto result = curl_easy_perform(curl.get());
    long status = 0;
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status);
    // Strict experimental protocol: explicit RDMA acknowledgment for GET and
    // PUT. Never report an ordinary S3 empty PUT as successful data transfer.
    const auto& acknowledgment = reply.headers["x-amz-rdma-reply"];
    bool valid =
        result == CURLE_OK && reply.body_bytes == 0 &&
        (upload ? status == 200 : (status == 200 || status == 206)) &&
        (acknowledgment == "200" || (!upload && acknowledgment == "206"));
    if (!upload) {
        // The pinned MinIO RDMA fork uses the older x-amz-rdma-bytes name.
        // Require an exact count with either spelling and reject conflicts.
        const auto modern = reply.headers.find("x-amz-rdma-bytes-transferred");
        const auto legacy = reply.headers.find("x-amz-rdma-bytes");
        const auto& bytes = modern != reply.headers.end()
                                ? modern->second
                                : reply.headers["x-amz-rdma-bytes"];
        valid = valid && (modern == reply.headers.end() ||
                          legacy == reply.headers.end() ||
                          modern->second == legacy->second);
        size_t transferred = 0;
        auto parsed = std::from_chars(bytes.data(), bytes.data() + bytes.size(),
                                      transferred);
        valid = valid && parsed.ec == std::errc{} &&
                parsed.ptr == bytes.data() + bytes.size() &&
                transferred == size;
    }
    if (!valid) {
        rdma_->Abort(token);
        return tl::make_unexpected(upload ? ErrorCode::FILE_WRITE_FAIL
                                          : ErrorCode::FILE_READ_FAIL);
    }
    rdma_->Release(token);
    return size;
}

tl::expected<void, ErrorCode> Adapter::Put(const std::string& key,
                                           std::span<const char> data) {
    auto result =
        Transfer(key, const_cast<char*>(data.data()), data.size(), true);
    if (!result) return tl::make_unexpected(result.error());
    return {};
}

tl::expected<void, ErrorCode> Adapter::PutV(const std::string& key,
                                            const iovec* iov, int count) {
    // Preserve atomic whole-object PUT without introducing a coalescing copy.
    if (!iov || count != 1)
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    return Put(key,
               {static_cast<const char*>(iov[0].iov_base), iov[0].iov_len});
}

tl::expected<size_t, ErrorCode> Adapter::Get(const std::string& key, void* ptr,
                                             size_t size) {
    return Transfer(key, ptr, size, false);
}

std::vector<tl::expected<void, ErrorCode>> Adapter::PutBatch(
    const std::vector<ObjectPutRequest>& requests) {
    return ObjectStorageAdapter::PutBatch(requests);
}

std::vector<tl::expected<size_t, ErrorCode>> Adapter::GetBatch(
    const std::vector<ObjectGetRequest>& requests) {
    return ObjectStorageAdapter::GetBatch(requests);
}

}  // namespace mooncake::cuobject_prototype
