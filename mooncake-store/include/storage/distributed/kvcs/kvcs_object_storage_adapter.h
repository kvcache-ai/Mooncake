#pragma once

#include <memory>
#include <string>
#include <vector>

#include "storage/distributed/object_storage_adapter.h"
#include "storage/distributed/kvcs/kvcs_driver.h"

namespace mooncake {

inline constexpr char kKvcsLowLevelAdapterName[] = "kvcs-lowlevel";

// Bridges the distributed object-storage contract to KVCS Low Level.
// The adapter owns key encoding and one explicit target; the driver owns I/O.
class KvcsObjectStorageAdapter final : public ObjectStorageAdapter {
   public:
    explicit KvcsObjectStorageAdapter(std::string efc_config_path = {});
    explicit KvcsObjectStorageAdapter(std::unique_ptr<KvcsDriver> driver);
    ~KvcsObjectStorageAdapter() override;

    tl::expected<void, ErrorCode> Put(const std::string& logical_key,
                                      std::span<const char> data) override;
    tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                       const iovec* iov, int iovcnt) override;
    std::vector<tl::expected<void, ErrorCode>> PutBatch(
        const std::vector<ObjectPutRequest>& requests) override;
    tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                        void* buf, size_t len) override;
    std::vector<tl::expected<size_t, ErrorCode>> GetBatch(
        const std::vector<ObjectGetRequest>& requests) override;
    tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) override;
    tl::expected<void, ErrorCode> Delete(
        const std::string& logical_key) override;

    ObjectStorageIoResults DeleteBatch(
        std::span<const std::string> logical_keys) override;
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override;

    tl::expected<void, ErrorCode> Init() override;
    const char* GetName() const override { return kKvcsLowLevelAdapterName; }

   private:
    tl::expected<ObjectKey, ErrorCode> EncodeKey(
        std::string_view logical_key) const;

    std::string efc_config_path_;
    std::unique_ptr<KvcsDriver> driver_;
    bool initialized_ = false;
};

}  // namespace mooncake
