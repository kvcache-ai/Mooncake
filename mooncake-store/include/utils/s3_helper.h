#pragma once

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>

namespace mooncake {
class S3Helper {
   public:
    static void InitAPI();

    static void ShutdownAPI();

   private:
    static bool aws_initialized;
    static Aws::SDKOptions options_;

   public:
    explicit S3Helper(const std::string &endpoint = "", const std::string &bucket = "",
                      const std::string &region = "us-east-1");

    ~S3Helper();

    // 获取连接信息的函数
    [[nodiscard]] const std::string &GetConnectionInfo() const { return connection_info_; }

    // 内存上传
    tl::expected<void, std::string> UploadBuffer(const std::string &key,
                                                 const std::vector<uint8_t> &buffer);

    tl::expected<void, std::string> UploadBufferMultipart(const std::string &key,
                                                         const std::vector<uint8_t> &buffer);

    tl::expected<void, std::string> UploadString(const std::string &key, const std::string &data);

    // 内存下载
    tl::expected<void, std::string> DownloadBuffer(const std::string &key,
                                                   std::vector<uint8_t> &buffer);

    tl::expected<void, std::string> DownloadBufferMultipart(const std::string &key,
                                                            std::vector<uint8_t> &buffer);

    tl::expected<void, std::string> DownloadString(const std::string &key, std::string &data);

    // 删除对象
    tl::expected<void, std::string> DeleteObject(const std::string &key);

    // 批量删除对象
    tl::expected<void, std::string> DeleteObjects(const std::vector<std::string> &keys);

    tl::expected<void, std::string> UploadFile(const Aws::String &file_path,
                                               const Aws::String &key);

    tl::expected<void, std::string> DownloadFile(const Aws::String &file_path,
                                                 const Aws::String &key);

    // 新增接口：列出指定前缀的对象
    tl::expected<void, std::string> ListObjectsWithPrefix(const std::string &prefix,
                                                          std::vector<std::string> &object_keys);

    tl::expected<void, std::string> DeleteObjectsWithPrefix(const std::string &prefix);

   private:
    Aws::S3::S3Client s3_client_;
    std::string bucket_;
    std::string connection_info_;
};
}  // namespace mooncake
