#pragma once

#include <map>
#include <span>
#include <string>
#include <vector>

#include "storage/distributed/object_storage_adapter.h"

namespace mooncake {

/**
 * Alibaba Cloud OSS implementation of ObjectStorageAdapter.
 *
 * Configuration is read by Init() from MOONCAKE_OSS_* environment variables.
 * Logical keys are encoded beneath the configured physical key prefix.
 */
class OssObjectStorageAdapter : public ObjectStorageAdapter {
   public:
    explicit OssObjectStorageAdapter(std::string key_prefix);
    ~OssObjectStorageAdapter() override = default;

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

    tl::expected<void, ErrorCode> Init() override;
    tl::expected<void, ErrorCode> CheckHealth() override;
    const char* GetName() const override { return "oss"; }

    // OSS-specific operations that are intentionally not part of the common
    // ObjectStorageAdapter contract.
    tl::expected<size_t, ErrorCode> GetRange(const std::string& logical_key,
                                             void* buf, size_t len,
                                             off_t offset);
    tl::expected<size_t, ErrorCode> GetV(const std::string& logical_key,
                                         const iovec* iov, int iovcnt,
                                         off_t offset);
    tl::expected<void, ErrorCode> Delete(const std::string& logical_key);
    tl::expected<size_t, ErrorCode> GetSize(const std::string& logical_key);

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

    std::vector<tl::expected<size_t, ErrorCode>> RequestBatch(
        const std::vector<BatchRequest>& requests);

    std::string LogicalToPhysicalKey(const std::string& logical_key) const;
    tl::expected<std::string, ErrorCode> PhysicalToLogicalKey(
        const std::string& physical_key) const;
    std::string PhysicalPrefix() const;
    std::string BuildUrl(const std::string& physical_key,
                         const std::map<std::string, std::string>& query) const;
    std::string BuildAuthorization(
        const std::string& method, const std::string& physical_key,
        const std::map<std::string, std::string>& query,
        const std::string& timestamp,
        const std::map<std::string, std::string>& oss_headers) const;

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
};

}  // namespace mooncake
