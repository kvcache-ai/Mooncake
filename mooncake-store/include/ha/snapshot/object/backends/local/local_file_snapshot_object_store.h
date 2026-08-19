#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/snapshot/object/snapshot_object_store.h"

namespace mooncake {

namespace fs = std::filesystem;

/**
 * @brief Local-file snapshot object store
 *
 * Stores snapshot data to local file system.
 * Storage path MUST be configured via MOONCAKE_SNAPSHOT_LOCAL_PATH
 * environment variable. No default path is provided.
 */
class LocalFileSnapshotObjectStore final : public SnapshotObjectStore {
   public:
    LocalFileSnapshotObjectStore();
    explicit LocalFileSnapshotObjectStore(const std::string& base_path);

    ~LocalFileSnapshotObjectStore() override = default;

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
    fs::path KeyToPath(const std::string& key) const;
    tl::expected<void, std::string> EnsureDirectoryExists(
        const fs::path& dir_path) const;
    bool IsPathWithinBase(const fs::path& path) const;

    fs::path base_path_;
};

}  // namespace mooncake
