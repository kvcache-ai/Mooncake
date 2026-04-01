#pragma once

#include <memory>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/snapshot/object/snapshot_object_store.h"

namespace mooncake {

#ifdef HAVE_AWS_SDK

/**
 * @brief S3-backed snapshot object store
 *
 * Wraps S3Helper to provide snapshot object storage in S3.
 */
class S3SnapshotObjectStore final : public SnapshotObjectStore {
   public:
    S3SnapshotObjectStore();
    ~S3SnapshotObjectStore() override = default;

    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) override;

    tl::expected<void, std::string> DownloadString(const std::string& key,
                                                   std::string& data) override;

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override;

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix,
        std::vector<std::string>& object_keys) override;

    bool IsNotFoundError(const std::string& error) const override;
    std::string GetConnectionInfo() const override;

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

#endif  // HAVE_AWS_SDK

}  // namespace mooncake
