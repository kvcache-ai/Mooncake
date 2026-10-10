#pragma once

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "storage/distributed/rest_object_storage_adapter.h"

namespace mooncake {

/**
 * S3-compatible implementation of ObjectStorageAdapter (AWS S3, SeaweedFS,
 * MinIO, Ceph RGW and other services that accept AWS Signature V4).
 *
 * Configuration is read by Init() from MOONCAKE_S3_* environment variables,
 * falling back to the standard AWS_* variables. Requests are signed with AWS
 * Signature V4; the request engine is RestObjectStorageAdapter. The OSS
 * adapter is independent of this class.
 */
class S3ObjectStorageAdapter : public RestObjectStorageAdapter {
   public:
    explicit S3ObjectStorageAdapter(std::string key_prefix)
        : RestObjectStorageAdapter(std::move(key_prefix)) {}
    ~S3ObjectStorageAdapter() override = default;

    tl::expected<void, ErrorCode> Init() override;
    const char* GetName() const override { return "s3"; }

   protected:
    std::vector<std::string> BuildSignedHeaders(
        const std::string& method, const std::string& physical_key,
        const std::map<std::string, std::string>& query,
        const std::string& range) const override;
    bool EqualsForEmptyQueryValue() const override { return true; }
    bool ContinuationTokenIsUrlEncoded() const override { return false; }
    const char* LogName() const override { return "S3"; }

    // Signs at a fixed timestamp; BuildSignedHeaders uses the current time.
    // Protected so known-answer tests can reach it through a subclass.
    std::vector<std::string> BuildSignedHeadersAt(
        const std::string& method, const std::string& physical_key,
        const std::map<std::string, std::string>& query,
        const std::string& range, const std::string& timestamp) const;

   private:
    // Host header value, including the bucket for virtual-hosted addressing
    // and the port when the endpoint names one other than the default.
    std::string HostHeader() const;
    std::string CanonicalUri(const std::string& physical_key) const;
};

}  // namespace mooncake
