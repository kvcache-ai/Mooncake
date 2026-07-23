#pragma once

#include <map>
#include <string>
#include <vector>

#include "storage/distributed/fs_adapter.h"

namespace mooncake {

/**
 * Alibaba Cloud OSS adapter backed by the OSS REST API.
 *
 * Configuration is read by Init() from MOONCAKE_OSS_* environment variables.
 * Object names are derived from the paths supplied by
 * DistributedStorageBackend; no local filesystem mount is required.
 */
class OssFileSystemAdapter : public FileSystemAdapter {
   public:
    OssFileSystemAdapter() = default;
    ~OssFileSystemAdapter() override;

    tl::expected<size_t, ErrorCode> WriteFile(
        const std::string& path, std::span<const char> data) override;
    tl::expected<size_t, ErrorCode> ReadFile(const std::string& path, void* buf,
                                             size_t len) override;
    tl::expected<size_t, ErrorCode> VectorWriteFile(const std::string& path,
                                                    const iovec* iov,
                                                    int iovcnt,
                                                    off_t offset) override;
    tl::expected<size_t, ErrorCode> VectorReadFile(const std::string& path,
                                                   const iovec* iov, int iovcnt,
                                                   off_t offset) override;

    tl::expected<void, ErrorCode> DeleteFile(const std::string& path) override;
    tl::expected<bool, ErrorCode> FileExists(const std::string& path) override;
    tl::expected<std::vector<std::string>, ErrorCode> ListFiles(
        const std::string& dir) override;
    tl::expected<size_t, ErrorCode> GetFileSize(
        const std::string& path) override;
    tl::expected<std::vector<FileInfo>, ErrorCode> ListFilesWithInfo(
        const std::string& dir) override;

    tl::expected<void, ErrorCode> Init(const std::string& mount_path) override;
    tl::expected<void, ErrorCode> Shutdown() override;
    const char* GetName() const override { return "oss"; }
    bool RequiresLocalDirectories() const override { return false; }

   private:
    struct Response {
        long status = 0;
        std::string body;
        std::map<std::string, std::string> headers;
    };

    tl::expected<Response, ErrorCode> Request(
        const std::string& method, const std::string& key,
        const std::map<std::string, std::string>& query = {},
        const char* body = nullptr, size_t body_size = 0,
        const std::string& range = "", const iovec* upload_iov = nullptr,
        int upload_iovcnt = 0) const;
    tl::expected<std::vector<FileInfo>, ErrorCode> ListFilesInternal(
        const std::string& dir) const;

    std::string PathToKey(const std::string& path) const;
    std::string BuildUrl(const std::string& key,
                         const std::map<std::string, std::string>& query) const;
    std::string BuildAuthorization(
        const std::string& method, const std::string& key,
        const std::map<std::string, std::string>& query,
        const std::string& timestamp) const;

    std::string endpoint_;
    std::string bucket_;
    std::string region_;
    std::string access_key_id_;
    std::string access_key_secret_;
    std::string security_token_;
    std::string mount_path_;
    bool path_style_ = false;
    bool anonymous_ = false;
    bool initialized_ = false;
};

}  // namespace mooncake
