#include "ha/snapshot/object/backends/s3/s3_snapshot_object_store.h"

#ifdef HAVE_AWS_SDK

#include <algorithm>
#include <cctype>
#include <string_view>

#include <glog/logging.h>

#include "utils/s3_helper.h"

namespace mooncake {

namespace {

bool ContainsAsciiInsensitive(std::string_view haystack,
                              std::string_view needle) {
    return std::search(
               haystack.begin(), haystack.end(), needle.begin(), needle.end(),
               [](char lhs, char rhs) {
                   return std::tolower(static_cast<unsigned char>(lhs)) ==
                          std::tolower(static_cast<unsigned char>(rhs));
               }) != haystack.end();
}

}  // namespace

class S3SnapshotObjectStore::Impl {
   public:
    Impl() : initialized_(InitializeOnce()), s3_helper_("", "", "") {}

    ~Impl() { S3Helper::ShutdownAPI(); }

   private:
    static bool InitializeOnce() {
        S3Helper::InitAPI();
        return true;
    }

    bool initialized_;

   public:
    S3Helper s3_helper_;
};

S3SnapshotObjectStore::S3SnapshotObjectStore()
    : impl_(std::make_unique<Impl>()) {
    LOG(INFO) << "S3SnapshotObjectStore initialized";
}

tl::expected<void, std::string> S3SnapshotObjectStore::UploadBuffer(
    const std::string& key, const std::vector<uint8_t>& buffer) {
    return impl_->s3_helper_.UploadBufferMultipart(key, buffer);
}

tl::expected<void, std::string> S3SnapshotObjectStore::DownloadBuffer(
    const std::string& key, std::vector<uint8_t>& buffer) {
    return impl_->s3_helper_.DownloadBufferMultipart(key, buffer);
}

tl::expected<void, std::string> S3SnapshotObjectStore::UploadString(
    const std::string& key, const std::string& data) {
    return impl_->s3_helper_.UploadString(key, data);
}

tl::expected<void, std::string> S3SnapshotObjectStore::DownloadString(
    const std::string& key, std::string& data) {
    return impl_->s3_helper_.DownloadString(key, data);
}

tl::expected<void, std::string> S3SnapshotObjectStore::DeleteObjectsWithPrefix(
    const std::string& prefix) {
    return impl_->s3_helper_.DeleteObjectsWithPrefix(prefix);
}

tl::expected<void, std::string> S3SnapshotObjectStore::ListObjectsWithPrefix(
    const std::string& prefix, std::vector<std::string>& object_keys) {
    return impl_->s3_helper_.ListObjectsWithPrefix(prefix, object_keys);
}

bool S3SnapshotObjectStore::IsNotFoundError(const std::string& error) const {
    return ContainsAsciiInsensitive(error, "nosuchkey") ||
           ContainsAsciiInsensitive(error, "not found") ||
           ContainsAsciiInsensitive(error, "does not exist") ||
           ContainsAsciiInsensitive(error, "404");
}

std::string S3SnapshotObjectStore::GetConnectionInfo() const {
    return impl_->s3_helper_.GetConnectionInfo();
}

}  // namespace mooncake

#endif  // HAVE_AWS_SDK
