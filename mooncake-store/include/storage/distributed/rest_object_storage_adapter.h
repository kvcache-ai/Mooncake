#pragma once

#include <map>
#include <span>
#include <string>
#include <vector>

#include "storage/distributed/object_storage_adapter.h"

namespace mooncake {

/**
 * Shared request engine for object stores reached over an S3-style REST API
 * with HMAC-SHA256 request signing (Alibaba Cloud OSS V4, AWS Signature V4).
 *
 * It owns everything that does not depend on the signing protocol: logical to
 * physical key mapping, URL construction, libcurl easy and multi transfers,
 * iovec uploads, range reads, batching and LIST parsing. Each service adapter
 * supplies the protocol hooks and its own Init().
 *
 * Currently only S3ObjectStorageAdapter extends it; OssObjectStorageAdapter
 * keeps its own request engine unchanged. The hook defaults follow OSS V4, so
 * the OSS adapter could move onto this base in a separate change.
 *
 * Logical keys are encoded beneath the configured physical key prefix.
 */
class RestObjectStorageAdapter : public ObjectStorageAdapter {
   public:
    explicit RestObjectStorageAdapter(std::string key_prefix);
    ~RestObjectStorageAdapter() override = default;

    tl::expected<void, ErrorCode> Put(const std::string& logical_key,
                                      std::span<const char> data) override;
    tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                       const iovec* iov, int iovcnt) override;
    tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                        void* buf, size_t len) override;
    std::vector<tl::expected<void, ErrorCode>> PutBatch(
        const std::vector<ObjectPutRequest>& requests) override;
    std::vector<tl::expected<size_t, ErrorCode>> GetBatch(
        const std::vector<ObjectGetRequest>& requests) override;
    tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) override;
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override;
    tl::expected<void, ErrorCode> CheckHealth() override;

    // Operations that are intentionally not part of the common
    // ObjectStorageAdapter contract.
    tl::expected<size_t, ErrorCode> GetRange(const std::string& logical_key,
                                             void* buf, size_t len,
                                             off_t offset);
    tl::expected<size_t, ErrorCode> GetV(const std::string& logical_key,
                                         const iovec* iov, int iovcnt,
                                         off_t offset);
    tl::expected<void, ErrorCode> Delete(const std::string& logical_key);
    tl::expected<size_t, ErrorCode> GetSize(const std::string& logical_key);

   protected:
    // Returns complete "Name: value" header lines for one request, including
    // Authorization unless the adapter is anonymous. Called when the request
    // is about to be sent, so the signing time is current.
    virtual std::vector<std::string> BuildSignedHeaders(
        const std::string& method, const std::string& physical_key,
        const std::map<std::string, std::string>& query,
        const std::string& range) const = 0;
    // Whether an empty query value is written as "name=" (AWS Signature V4)
    // rather than "name" (OSS V4). Applies to the sent and the signed query.
    virtual bool EqualsForEmptyQueryValue() const = 0;
    // Whether NextContinuationToken is URL-encoded when encoding-type=url is
    // set (OSS encodes it; S3 returns it unencoded).
    virtual bool ContinuationTokenIsUrlEncoded() const = 0;
    // Service name used in log messages.
    virtual const char* LogName() const = 0;

    std::string BuildUrl(const std::string& physical_key,
                         const std::map<std::string, std::string>& query) const;

    // Called by Init() after the configuration fields are set.
    void FinishInit();

    std::string endpoint_;
    std::string bucket_;
    std::string region_;
    std::string access_key_id_;
    std::string access_key_secret_;
    std::string security_token_;
    std::string key_prefix_;
    bool path_style_ = false;
    bool anonymous_ = false;
    bool initialized_ = false;
    // Each synchronous batch owns its connection pool and applies this limit
    // independently. Concurrent batches do not share a CURLM or an I/O lock.
    long max_connections_ = 64;
    long receive_buffer_size_ = 1024 * 1024;
    long upload_buffer_size_ = 1024 * 1024;

   private:
    struct Response {
        long status = 0;
        size_t transferred = 0;
        std::string body;
        std::map<std::string, std::string> headers;
    };

    struct BatchRequest;
    struct RequestContext;
    tl::expected<void, ErrorCode> PrepareRequest(
        RequestContext& context, const std::string& method,
        const std::string& physical_key,
        const std::map<std::string, std::string>& query, const char* body,
        size_t body_size, const std::string& range, const iovec* upload_iov,
        int upload_iovcnt, void* download_buffer,
        size_t download_capacity) const;

    tl::expected<Response, ErrorCode> Request(
        const std::string& method, const std::string& physical_key,
        const std::map<std::string, std::string>& query = {},
        const char* body = nullptr, size_t body_size = 0,
        const std::string& range = "", const iovec* upload_iov = nullptr,
        int upload_iovcnt = 0, void* download_buffer = nullptr,
        size_t download_capacity = 0) const;

    // Runs the batch, then retries requests that failed transiently.
    std::vector<tl::expected<size_t, ErrorCode>> RequestBatch(
        const std::vector<BatchRequest>& requests);
    // One pass over the batch. For each request, transient_error receives a
    // description when it failed transiently and is empty otherwise.
    std::vector<tl::expected<size_t, ErrorCode>> RequestBatchOnce(
        const std::vector<BatchRequest>& requests,
        std::vector<std::string>& transient_error);

    std::string LogicalToPhysicalKey(const std::string& logical_key) const;
    tl::expected<std::string, ErrorCode> PhysicalToLogicalKey(
        const std::string& physical_key) const;
    std::string PhysicalPrefix() const;
};

}  // namespace mooncake
