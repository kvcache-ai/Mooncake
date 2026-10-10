#include "storage/distributed/rest_object_storage_adapter.h"

#include <curl/curl.h>

#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <ctime>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>

#include <glog/logging.h>

#include "random.h"
#include "storage/distributed/object_storage_signing.h"
#include "storage/distributed/object_storage_xml.h"

namespace mooncake {
namespace {

using object_storage_signing::CanonicalQuery;
using object_storage_signing::UriEncode;

std::once_flag curl_init_once;

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

struct DownloadContext {
    std::string* body = nullptr;
    char* buffer = nullptr;
    size_t capacity = 0;
    size_t transferred = 0;
    bool overflow = false;
};

size_t DownloadCallback(char* data, size_t size, size_t count,
                        void* user_data) {
    auto* context = static_cast<DownloadContext*>(user_data);
    const size_t bytes = size * count;
    if (context->buffer) {
        if (bytes > context->capacity - context->transferred) {
            context->overflow = true;
            return 0;
        }
        std::memcpy(context->buffer + context->transferred, data, bytes);
    } else {
        context->body->append(data, bytes);
    }
    context->transferred += bytes;
    return bytes;
}

size_t HeaderCallback(char* data, size_t size, size_t count, void* user_data) {
    auto* headers = static_cast<std::map<std::string, std::string>*>(user_data);
    const size_t length = size * count;
    std::string_view line(data, length);
    if (line.starts_with("HTTP/")) {
        headers->clear();
        return length;
    }
    const size_t colon = line.find(':');
    if (colon != std::string_view::npos) {
        std::string name(line.substr(0, colon));
        std::string value(line.substr(colon + 1));
        boost::algorithm::to_lower(name);
        boost::algorithm::trim(value);
        (*headers)[std::move(name)] = std::move(value);
    }
    return length;
}

tl::expected<size_t, ErrorCode> GetIovecSize(const iovec* iov, int iovcnt) {
    if (iovcnt < 0 || (!iov && iovcnt > 0))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    size_t total = 0;
    for (int i = 0; i < iovcnt; ++i) {
        if ((!iov[i].iov_base && iov[i].iov_len > 0) ||
            iov[i].iov_len > std::numeric_limits<size_t>::max() - total) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total += iov[i].iov_len;
    }
    return total;
}

struct IovecUploadContext {
    const iovec* iov = nullptr;
    int iovcnt = 0;
    int index = 0;
    size_t offset = 0;
};

size_t IovecUploadCallback(char* buffer, size_t size, size_t count,
                           void* user_data) {
    auto* context = static_cast<IovecUploadContext*>(user_data);
    const size_t capacity = size * count;
    size_t copied = 0;
    while (copied < capacity && context->index < context->iovcnt) {
        const iovec& current = context->iov[context->index];
        const size_t available = current.iov_len - context->offset;
        const size_t chunk = std::min(capacity - copied, available);
        if (chunk > 0) {
            std::memcpy(
                buffer + copied,
                static_cast<const char*>(current.iov_base) + context->offset,
                chunk);
        }
        copied += chunk;
        context->offset += chunk;
        if (context->offset == current.iov_len) {
            ++context->index;
            context->offset = 0;
        }
    }
    return copied;
}

int IovecSeekCallback(void* user_data, curl_off_t offset, int origin) {
    if (origin != SEEK_SET || offset != 0) return CURL_SEEKFUNC_CANTSEEK;
    auto* context = static_cast<IovecUploadContext*>(user_data);
    context->index = 0;
    context->offset = 0;
    return CURL_SEEKFUNC_OK;
}

bool IsSuccess(long status) { return status >= 200 && status < 300; }

// Returns "Code: Message" from an XML error body, without echoing the
// rest of the body (S3 errors can include the access key ID and the
// string to sign).
std::string ErrorSummary(std::string_view body) {
    std::string summary = object_storage_xml::ErrorCodeAndMessage(body);
    return summary.empty() ? "(no error code)" : summary;
}

// Attempts per request, counting the first. Transient failures are retried,
// as the AWS SDKs do: connection set-up (a dropped SYN alone can take a
// connect past its timeout), resets and timeouts, throttling and 5xx.
constexpr int kMaxAttempts = 3;

bool IsTransientCurlError(CURLcode code) {
    switch (code) {
        case CURLE_COULDNT_RESOLVE_HOST:
        case CURLE_COULDNT_CONNECT:
        case CURLE_OPERATION_TIMEDOUT:
        case CURLE_SSL_CONNECT_ERROR:
        case CURLE_SEND_ERROR:
        case CURLE_RECV_ERROR:
        case CURLE_GOT_NOTHING:
        case CURLE_PARTIAL_FILE:
            return true;
        default:
            return false;
    }
}

bool IsTransientStatus(long status) {
    return status == 429 || status == 500 || status == 502 || status == 503 ||
           status == 504;
}

// A GET whose error body does not fit the caller's buffer ends in a local
// write error; the HTTP status then decides, so a 503 on a small object is
// still retried. A timeout is retried only while the connection was being set
// up (TCP and TLS), before anything was sent: a request that had been sent
// and then ran into the overall request timeout is not retried, so a stalled
// server does not hold the caller for several request timeouts.
//
// Retry policy for writes. A PUT that timed out after it was sent may still
// land on the server; it is reported as failed and not resent, so the caller
// can briefly see the previous object (or none) rather than have a second,
// overlapping write of the same key race the first. A PUT that failed with a
// reset or a 5xx is resent: the same bytes to the same key, an idempotent
// overwrite.
bool IsTransientFailure(CURLcode result, long status, bool overflow,
                        CURL* curl) {
    if (result == CURLE_OK || (result == CURLE_WRITE_ERROR && overflow)) {
        return IsTransientStatus(status);
    }
    if (result == CURLE_OPERATION_TIMEDOUT) {
#if LIBCURL_VERSION_NUM >= 0x073d00  // CURLINFO_PRETRANSFER_TIME_T: 7.61.0.
        curl_off_t pretransfer = 0;
        curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME_T, &pretransfer);
#else
        double pretransfer = 0;
        curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME, &pretransfer);
#endif
        return pretransfer == 0;
    }
    return IsTransientCurlError(result);
}

// Sleeps before attempt 2, 3, ...: 100 ms, then 200 ms, each plus up to 50%
// jitter so that a throttled batch does not retry in lockstep.
void BackoffBeforeAttempt(int attempt) {
    const int base_ms = 100 << (attempt - 2);
    std::this_thread::sleep_for(
        std::chrono::milliseconds(base_ms + randomUniform(0, base_ms / 2)));
}

}  // namespace

struct RestObjectStorageAdapter::BatchRequest {
    bool upload = false;
    std::string logical_key;
    const iovec* upload_iov = nullptr;
    int upload_iovcnt = 0;
    void* download_buffer = nullptr;
    size_t size = 0;
};

struct RestObjectStorageAdapter::RequestContext {
    RequestContext() = default;
    RequestContext(const RequestContext&) = delete;
    RequestContext& operator=(const RequestContext&) = delete;
    RequestContext(RequestContext&&) = delete;
    RequestContext& operator=(RequestContext&&) = delete;

    ~RequestContext() {
        if (curl) {
            if (multi) curl_multi_remove_handle(multi, curl);
            curl_easy_cleanup(curl);
        }
        curl_slist_free_all(headers);
    }

    size_t index = 0;
    bool upload = false;
    size_t expected_size = 0;
    CURL* curl = nullptr;
    // Set only after a successful add. The owning batch outlives this context.
    CURLM* multi = nullptr;
    curl_slist* headers = nullptr;
    std::string error_body;
    IovecUploadContext upload_context;
    DownloadContext download_context;
};

RestObjectStorageAdapter::RestObjectStorageAdapter(std::string key_prefix)
    : key_prefix_(std::move(key_prefix)) {
    const size_t first = key_prefix_.find_first_not_of('/');
    if (first == std::string::npos) {
        key_prefix_.clear();
        return;
    }
    key_prefix_.erase(0, first);
    while (!key_prefix_.empty() && key_prefix_.back() == '/')
        key_prefix_.pop_back();
}

void RestObjectStorageAdapter::FinishInit() {
    std::call_once(curl_init_once,
                   [] { curl_global_init(CURL_GLOBAL_DEFAULT); });
    initialized_ = true;
    LOG(INFO) << LogName() << " adapter initialized: endpoint=" << endpoint_
              << ", bucket=" << bucket_ << ", region=" << region_
              << ", key_prefix=" << key_prefix_
              << ", max_connections=" << max_connections_
              << ", receive_buffer_size=" << receive_buffer_size_
              << ", upload_buffer_size=" << upload_buffer_size_;
}

tl::expected<void, ErrorCode> RestObjectStorageAdapter::CheckHealth() {
    const std::string probe_key =
        ".mooncake_health_probe_" + UuidToString(generate_uuid());
    const std::string probe_data = "health_check";

    auto write_result = Put(
        probe_key, std::span<const char>(probe_data.data(), probe_data.size()));
    if (!write_result) {
        LOG(ERROR) << LogName() << " health check failed to write probe: "
                   << static_cast<int>(write_result.error());
        return write_result;
    }

    auto cleanup_probe = [&] {
        auto delete_result = Delete(probe_key);
        if (!delete_result) {
            LOG(WARNING) << "Failed to delete " << LogName()
                         << " health-check probe: "
                         << static_cast<int>(delete_result.error());
        }
    };

    std::string read_buffer(probe_data.size(), '\0');
    auto read_result = Get(probe_key, read_buffer.data(), read_buffer.size());
    if (!read_result || *read_result != probe_data.size() ||
        read_buffer != probe_data) {
        LOG(ERROR) << LogName() << " health check failed to read back probe";
        cleanup_probe();
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    cleanup_probe();
    LOG(INFO) << LogName() << " health check passed";
    return {};
}

std::string RestObjectStorageAdapter::PhysicalPrefix() const {
    return key_prefix_.empty() ? std::string() : key_prefix_ + "/";
}

std::string RestObjectStorageAdapter::LogicalToPhysicalKey(
    const std::string& logical_key) const {
    return PhysicalPrefix() + UriEncode(logical_key);
}

tl::expected<std::string, ErrorCode>
RestObjectStorageAdapter::PhysicalToLogicalKey(
    const std::string& physical_key) const {
    const std::string prefix = PhysicalPrefix();
    if (physical_key.rfind(prefix, 0) != 0) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return UriDecode(physical_key.substr(prefix.size()));
}

std::string RestObjectStorageAdapter::BuildUrl(
    const std::string& physical_key,
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
    base += "/" + UriEncode(physical_key, true);
    const auto canonical_query =
        CanonicalQuery(query, EqualsForEmptyQueryValue());
    if (!canonical_query.empty()) base += "?" + canonical_query;
    return base;
}

tl::expected<void, ErrorCode> RestObjectStorageAdapter::PrepareRequest(
    RequestContext& context, const std::string& method,
    const std::string& physical_key,
    const std::map<std::string, std::string>& query, const char* body,
    size_t body_size, const std::string& range, const iovec* upload_iov,
    int upload_iovcnt, void* download_buffer, size_t download_capacity) const {
    CURL* curl = context.curl;
    if (!curl) return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    context.upload = method == "PUT";
    context.download_context = {&context.error_body,
                                static_cast<char*>(download_buffer),
                                download_capacity};
    bool headers_ok = true;
    auto add_header = [&](const std::string& header) {
        auto* appended = curl_slist_append(context.headers, header.c_str());
        if (appended)
            context.headers = appended;
        else
            headers_ok = false;
    };

    for (const auto& header :
         BuildSignedHeaders(method, physical_key, query, range)) {
        add_header(header);
    }
    add_header("Expect:");
    add_header("Content-Type:");
    if (!range.empty()) add_header("Range: " + range);
    if (!headers_ok)
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);

    const std::string url = BuildUrl(physical_key, query);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    // Send the path exactly as it was signed. By default libcurl collapses
    // "." and ".." segments, so a key prefix such as "a/../b" (or a key named
    // "..") would reach the service under a different path than the canonical
    // URI in the signature, and be rejected.
    curl_easy_setopt(curl, CURLOPT_PATH_AS_IS, 1L);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, context.headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, DownloadCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &context.download_context);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 10000L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 120000L);
    if (method == "HEAD") curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    if (method == "PUT") {
        if (upload_iov) {
            context.upload_context = {upload_iov, upload_iovcnt};
            curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
            curl_easy_setopt(curl, CURLOPT_READFUNCTION, IovecUploadCallback);
            curl_easy_setopt(curl, CURLOPT_READDATA, &context.upload_context);
            curl_easy_setopt(curl, CURLOPT_SEEKFUNCTION, IovecSeekCallback);
            curl_easy_setopt(curl, CURLOPT_SEEKDATA, &context.upload_context);
            curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE,
                             static_cast<curl_off_t>(body_size));
#if LIBCURL_VERSION_NUM >= 0x073e00  // Added in libcurl 7.62.0.
            curl_easy_setopt(curl, CURLOPT_UPLOAD_BUFFERSIZE,
                             upload_buffer_size_);
#endif
        } else {
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body ? body : "");
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
                             static_cast<curl_off_t>(body_size));
        }
    }
    return {};
}

tl::expected<RestObjectStorageAdapter::Response, ErrorCode>
RestObjectStorageAdapter::Request(
    const std::string& method, const std::string& physical_key,
    const std::map<std::string, std::string>& query, const char* body,
    size_t body_size, const std::string& range, const iovec* upload_iov,
    int upload_iovcnt, void* download_buffer, size_t download_capacity) const {
    if (!initialized_) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    for (int attempt = 1;; ++attempt) {
        if (attempt > 1) BackoffBeforeAttempt(attempt);
        // Each attempt is signed and sent afresh; uploads restart from the
        // first iovec and downloads from the start of the caller's buffer.
        RequestContext context;
        context.curl = curl_easy_init();
        auto prepared = PrepareRequest(
            context, method, physical_key, query, body, body_size, range,
            upload_iov, upload_iovcnt, download_buffer, download_capacity);
        if (!prepared) return tl::make_unexpected(prepared.error());
        Response response;
        curl_easy_setopt(context.curl, CURLOPT_HEADERFUNCTION, HeaderCallback);
        curl_easy_setopt(context.curl, CURLOPT_HEADERDATA, &response.headers);

        const CURLcode result = curl_easy_perform(context.curl);
        curl_easy_getinfo(context.curl, CURLINFO_RESPONSE_CODE,
                          &response.status);
        response.transferred = context.download_context.transferred;
        response.body = std::move(context.error_body);
        // A 404 error body may exceed the caller's object buffer. Preserve the
        // HTTP error only for this local abort, not for other transfer
        // failures.
        const bool missing_object = method == "GET" && response.status == 404 &&
                                    result == CURLE_WRITE_ERROR &&
                                    context.download_context.overflow;
        const bool transient =
            IsTransientFailure(result, response.status,
                               context.download_context.overflow, context.curl);
        if (transient && attempt < kMaxAttempts) {
            LOG(WARNING) << LogName() << " " << method << " attempt " << attempt
                         << " failed ("
                         << (result != CURLE_OK
                                 ? std::string(curl_easy_strerror(result))
                                 : "HTTP " + std::to_string(response.status))
                         << "); retrying";
            continue;
        }
        if (result != CURLE_OK && !missing_object) {
            LOG(ERROR) << LogName() << " " << method
                       << " request failed: " << curl_easy_strerror(result);
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        if (!IsSuccess(response.status) && response.status != 404) {
            LOG(ERROR) << LogName() << " " << method << " returned HTTP "
                       << response.status << ": "
                       << ErrorSummary(response.body);
        }
        return response;
    }
}

tl::expected<void, ErrorCode> RestObjectStorageAdapter::Put(
    const std::string& logical_key, std::span<const char> data) {
    auto response = Request("PUT", LogicalToPhysicalKey(logical_key), {},
                            data.data(), data.size());
    if (!response) return tl::make_unexpected(response.error());
    if (!IsSuccess(response->status))
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

tl::expected<void, ErrorCode> RestObjectStorageAdapter::PutV(
    const std::string& logical_key, const iovec* iov, int iovcnt) {
    auto total = GetIovecSize(iov, iovcnt);
    if (!total) return tl::make_unexpected(total.error());
    if (*total == 0) return Put(logical_key, {});
    auto response = Request("PUT", LogicalToPhysicalKey(logical_key), {},
                            nullptr, *total, "", iov, iovcnt);
    if (!response) return tl::make_unexpected(response.error());
    if (!IsSuccess(response->status))
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

std::vector<tl::expected<size_t, ErrorCode>>
RestObjectStorageAdapter::RequestBatch(
    const std::vector<BatchRequest>& requests) {
    std::vector<std::string> transient_error;
    auto results = RequestBatchOnce(requests, transient_error);
    for (int attempt = 2; attempt <= kMaxAttempts; ++attempt) {
        std::vector<size_t> retry;
        for (size_t i = 0; i < results.size(); ++i) {
            if (!results[i] && !transient_error[i].empty()) retry.push_back(i);
        }
        if (retry.empty()) break;
        LOG(WARNING) << LogName() << " batch: retrying " << retry.size()
                     << " of " << requests.size()
                     << " requests after transient failures (attempt "
                     << attempt << ", first: " << transient_error[retry[0]]
                     << ")";
        BackoffBeforeAttempt(attempt);
        std::vector<BatchRequest> subset;
        subset.reserve(retry.size());
        for (size_t i : retry) subset.push_back(requests[i]);
        std::vector<std::string> subset_error;
        auto retried = RequestBatchOnce(subset, subset_error);
        for (size_t j = 0; j < retry.size(); ++j) {
            results[retry[j]] = std::move(retried[j]);
            transient_error[retry[j]] = std::move(subset_error[j]);
        }
    }
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i] && !transient_error[i].empty()) {
            LOG(ERROR) << LogName() << " batch request failed after "
                       << kMaxAttempts << " attempts: " << transient_error[i];
        }
    }
    return results;
}

std::vector<tl::expected<size_t, ErrorCode>>
RestObjectStorageAdapter::RequestBatchOnce(
    const std::vector<BatchRequest>& requests,
    std::vector<std::string>& transient_error) {
    transient_error.assign(requests.size(), std::string());
    std::vector<tl::expected<size_t, ErrorCode>> results;
    results.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        results.emplace_back(
            tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
    }
    if (requests.empty()) return results;
    if (!initialized_) {
        for (auto& result : results)
            result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        return results;
    }

    // Each batch owns its pool; contexts must be destroyed before the pool.
    std::unique_ptr<CURLM, decltype(&curl_multi_cleanup)> multi(
        curl_multi_init(), &curl_multi_cleanup);
    if (!multi) {
        LOG(ERROR) << LogName()
                   << " batch: failed to initialize curl multi handle";
        return results;
    }
    if (curl_multi_setopt(multi.get(), CURLMOPT_MAX_TOTAL_CONNECTIONS,
                          max_connections_) != CURLM_OK ||
        curl_multi_setopt(multi.get(), CURLMOPT_MAX_HOST_CONNECTIONS,
                          max_connections_) != CURLM_OK ||
        curl_multi_setopt(multi.get(), CURLMOPT_MAXCONNECTS,
                          max_connections_) != CURLM_OK) {
        LOG(ERROR) << LogName()
                   << " batch: failed to configure curl multi handle";
        return results;
    }
    std::vector<RequestContext> contexts(requests.size());

    // Requests are signed when admitted rather than when the batch is built,
    // so a long queue cannot age a signature past the service's clock-skew
    // window before it is sent.
    auto prepare = [&](size_t i) {
        const auto& request = requests[i];
        auto& context = contexts[i];
        context.curl = curl_easy_init();
        context.index = i;
        context.expected_size = request.size;
        const std::string range =
            request.upload || request.size == 0
                ? ""
                : "bytes=0-" + std::to_string(request.size - 1);
        auto prepared = PrepareRequest(
            context, request.upload ? "PUT" : "GET",
            LogicalToPhysicalKey(request.logical_key), {}, nullptr,
            request.upload ? request.size : 0, range, request.upload_iov,
            request.upload_iovcnt, request.download_buffer,
            request.upload ? 0 : request.size);
        if (!prepared) return false;
        curl_easy_setopt(context.curl, CURLOPT_BUFFERSIZE,
                         receive_buffer_size_);
        curl_easy_setopt(context.curl, CURLOPT_PRIVATE, &context);
        return true;
    };

    size_t next = 0;
    size_t active = 0;
    // Wait outside CURLM so queued requests retain their full timeout.
    auto admit = [&]() {
        while (next < contexts.size() &&
               active < static_cast<size_t>(max_connections_)) {
            auto& context = contexts[next];
            // A request that cannot be prepared keeps its
            // DFS_SERVICE_UNAVAILABLE result; the rest of the batch proceeds.
            if (!prepare(next)) {
                ++next;
                continue;
            }
            auto result = curl_multi_add_handle(multi.get(), context.curl);
            if (result != CURLM_OK) return result;
            context.multi = multi.get();
            ++next;
            ++active;
        }
        return CURLM_OK;
    };
    CURLMcode multi_result = admit();
    while (multi_result == CURLM_OK && active > 0) {
        int running = 0;
        multi_result = curl_multi_perform(multi.get(), &running);
        if (multi_result != CURLM_OK) break;
        bool completed = false;
        int remaining = 0;
        while (CURLMsg* message =
                   curl_multi_info_read(multi.get(), &remaining)) {
            if (message->msg != CURLMSG_DONE) continue;
            RequestContext* context = nullptr;
            curl_easy_getinfo(message->easy_handle, CURLINFO_PRIVATE, &context);
            if (!context) continue;
            long status = 0;
            curl_easy_getinfo(message->easy_handle, CURLINFO_RESPONSE_CODE,
                              &status);
            if (!context->upload && status == 404 &&
                (message->data.result == CURLE_OK ||
                 (message->data.result == CURLE_WRITE_ERROR &&
                  context->download_context.overflow))) {
                results[context->index] =
                    tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
            } else if (message->data.result != CURLE_OK) {
                const std::string error =
                    std::string(context->upload ? "PUT" : "GET") +
                    " failed: " + curl_easy_strerror(message->data.result);
                if (IsTransientFailure(message->data.result, status,
                                       context->download_context.overflow,
                                       context->curl)) {
                    transient_error[context->index] =
                        error + " (HTTP " + std::to_string(status) + ")";
                } else {
                    LOG(ERROR) << LogName() << " batch " << error;
                }
                results[context->index] =
                    tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
            } else if (context->upload && IsSuccess(status)) {
                results[context->index] = context->expected_size;
            } else if (!context->upload && status == 206 &&
                       context->download_context.transferred ==
                           context->expected_size) {
                results[context->index] = context->download_context.transferred;
            } else {
                const std::string error =
                    std::string(context->upload ? "PUT" : "GET") +
                    " returned HTTP " + std::to_string(status);
                if (IsTransientStatus(status)) {
                    transient_error[context->index] = error;
                } else {
                    LOG(ERROR) << LogName() << " batch " << error;
                }
                results[context->index] = tl::make_unexpected(
                    context->upload ? ErrorCode::FILE_WRITE_FAIL
                                    : ErrorCode::FILE_READ_FAIL);
            }
            curl_multi_remove_handle(multi.get(), context->curl);
            context->multi = nullptr;
            --active;
            completed = true;
        }
        multi_result = admit();
        if (multi_result != CURLM_OK || active == 0) break;
        if (completed) continue;
        int ready = 0;
        const auto wait_start = std::chrono::steady_clock::now();
        multi_result = curl_multi_wait(multi.get(), nullptr, 0, 1000, &ready);
        if (multi_result == CURLM_OK && ready == 0) {
            // With no sockets, curl_multi_wait can return immediately. Avoid
            // spinning, but do not delay a timer that needs immediate service.
            long timeout_ms = -1;
            curl_multi_timeout(multi.get(), &timeout_ms);
            if (timeout_ms != 0) {
                std::this_thread::sleep_until(wait_start +
                                              std::chrono::milliseconds(1));
            }
        }
    }
    if (multi_result != CURLM_OK) {
        LOG(ERROR) << LogName() << " batch request failed: "
                   << curl_multi_strerror(multi_result);
    }

    return results;
}

std::vector<tl::expected<void, ErrorCode>> RestObjectStorageAdapter::PutBatch(
    const std::vector<ObjectPutRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i)
        results.emplace_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));

    std::vector<BatchRequest> batch;
    std::vector<size_t> mapping;
    batch.reserve(requests.size());
    mapping.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto total = GetIovecSize(request.iov, request.iovcnt);
        if (!total) continue;
        batch.push_back({true, request.logical_key, request.iov, request.iovcnt,
                         nullptr, *total});
        mapping.push_back(i);
    }
    auto batch_results = RequestBatch(batch);
    for (size_t i = 0; i < batch_results.size(); ++i) {
        if (batch_results[i])
            results[mapping[i]] = {};
        else
            results[mapping[i]] = tl::make_unexpected(batch_results[i].error());
    }
    return results;
}

std::vector<tl::expected<size_t, ErrorCode>> RestObjectStorageAdapter::GetBatch(
    const std::vector<ObjectGetRequest>& requests) {
    std::vector<tl::expected<size_t, ErrorCode>> results;
    results.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i)
        results.emplace_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));

    std::vector<BatchRequest> batch;
    std::vector<size_t> mapping;
    batch.reserve(requests.size());
    mapping.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (request.size == 0) {
            results[i] = size_t{0};
            continue;
        }
        if (!request.buffer) continue;
        batch.push_back({false, request.logical_key, nullptr, 0, request.buffer,
                         request.size});
        mapping.push_back(i);
    }
    auto batch_results = RequestBatch(batch);
    for (size_t i = 0; i < batch_results.size(); ++i)
        results[mapping[i]] = std::move(batch_results[i]);
    return results;
}

tl::expected<size_t, ErrorCode> RestObjectStorageAdapter::Get(
    const std::string& logical_key, void* buf, size_t len) {
    return GetRange(logical_key, buf, len, 0);
}

tl::expected<size_t, ErrorCode> RestObjectStorageAdapter::GetRange(
    const std::string& logical_key, void* buf, size_t len, off_t offset) {
    if (offset < 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    if (len == 0) return 0;
    if (!buf) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const uint64_t first = static_cast<uint64_t>(offset);
    if (len - 1 > std::numeric_limits<uint64_t>::max() - first)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const std::string range = "bytes=" + std::to_string(first) + "-" +
                              std::to_string(first + len - 1);
    auto response = Request("GET", LogicalToPhysicalKey(logical_key), {},
                            nullptr, 0, range, nullptr, 0, buf, len);
    if (!response) return tl::make_unexpected(response.error());
    if (response->status == 404)
        return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
    if (response->status != 206 || response->transferred != len)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    return len;
}

tl::expected<size_t, ErrorCode> RestObjectStorageAdapter::GetV(
    const std::string& logical_key, const iovec* iov, int iovcnt,
    off_t offset) {
    if (offset < 0 || iovcnt < 0 || (!iov && iovcnt > 0))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    size_t total = 0;
    for (int i = 0; i < iovcnt; ++i) {
        if ((!iov[i].iov_base && iov[i].iov_len > 0) ||
            iov[i].iov_len > std::numeric_limits<size_t>::max() - total)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        total += iov[i].iov_len;
    }
    if (total == 0) return 0;
    std::string data(total, '\0');
    auto result = GetRange(logical_key, data.data(), data.size(), offset);
    if (!result) return tl::make_unexpected(result.error());
    size_t copied = 0;
    for (int i = 0; i < iovcnt; ++i) {
        if (iov[i].iov_len > 0) {
            std::memcpy(iov[i].iov_base, data.data() + copied, iov[i].iov_len);
        }
        copied += iov[i].iov_len;
    }
    return total;
}

tl::expected<void, ErrorCode> RestObjectStorageAdapter::Delete(
    const std::string& logical_key) {
    auto response = Request("DELETE", LogicalToPhysicalKey(logical_key));
    if (!response) return tl::make_unexpected(response.error());
    if (!IsSuccess(response->status) && response->status != 404)
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
}

tl::expected<bool, ErrorCode> RestObjectStorageAdapter::Exists(
    const std::string& logical_key) {
    auto response = Request("HEAD", LogicalToPhysicalKey(logical_key));
    if (!response) return tl::make_unexpected(response.error());
    if (response->status == 404) return false;
    if (!IsSuccess(response->status))
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    return true;
}

tl::expected<size_t, ErrorCode> RestObjectStorageAdapter::GetSize(
    const std::string& logical_key) {
    auto response = Request("HEAD", LogicalToPhysicalKey(logical_key));
    if (!response) return tl::make_unexpected(response.error());
    if (response->status == 404)
        return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
    if (!IsSuccess(response->status))
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    auto it = response->headers.find("content-length");
    if (it == response->headers.end())
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    try {
        const auto value = std::stoull(it->second);
        if (value > std::numeric_limits<size_t>::max())
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        return static_cast<size_t>(value);
    } catch (...) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
RestObjectStorageAdapter::ListKeys() {
    const std::string prefix = PhysicalPrefix();
    std::vector<KeyInfo> keys;
    std::string token;
    do {
        std::map<std::string, std::string> query{{"encoding-type", "url"},
                                                 {"list-type", "2"},
                                                 {"max-keys", "1000"},
                                                 {"prefix", prefix}};
        if (!token.empty()) query["continuation-token"] = token;
        auto response = Request("GET", "", query);
        if (!response) return tl::make_unexpected(response.error());
        if (!IsSuccess(response->status))
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);

        // A LIST can come back with HTTP 200 and an incomplete document (S3
        // documents this for ListObjectsV2). Such a response is rejected: a
        // partial listing returned as success would let callers such as
        // ScanMeta silently drop existing objects.
        auto page = object_storage_xml::ParseListObjectsV2(response->body);
        if (!page) {
            LOG(ERROR) << LogName()
                       << " LIST response rejected: " << page.error();
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        for (auto& object : page->objects) {
            auto logical_key = PhysicalToLogicalKey(UriDecode(object.key));
            if (!logical_key) return tl::make_unexpected(logical_key.error());
            keys.push_back({std::move(*logical_key), object.size});
        }
        token = std::move(page->next_continuation_token);
        if (ContinuationTokenIsUrlEncoded()) token = UriDecode(token);
        if (!page->is_truncated) token.clear();
        if (page->is_truncated && token.empty())
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    } while (!token.empty());
    return keys;
}

}  // namespace mooncake
