#pragma once

#include "ha/snapshot/object/snapshot_object_store.h"

#ifdef STORE_USE_ETCD

#include <string>
#include <vector>

namespace mooncake {

/**
 * @brief ETCD-based snapshot object store implementation.
 *
 * Stores snapshot data as key-value pairs in an etcd cluster via the
 * cgo wrapper (libetcd_wrapper).  A dedicated snapshot etcd client is
 * used with enlarged message size limits (2 GB) and extended timeouts
 * to accommodate large snapshot payloads.
 *
 * IMPORTANT: All methods in this class call into cgo and therefore
 * must only be invoked from a process that has a live Go runtime.
 * In particular, they must NOT be called from a forked child process.
 */
class EtcdSnapshotObjectStore : public SnapshotObjectStore {
   public:
    explicit EtcdSnapshotObjectStore(const std::string& endpoints);
    ~EtcdSnapshotObjectStore() override = default;

    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override;

    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) override;

    tl::expected<void, std::string> DownloadString(
        const std::string& key, std::string& data) override;

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override;

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix,
        std::vector<std::string>& object_keys) override;

    bool IsNotFoundError(const std::string& error) const override;

    std::string GetConnectionInfo() const override;

   private:
    std::string endpoints_;
};

}  // namespace mooncake

#endif  // STORE_USE_ETCD
