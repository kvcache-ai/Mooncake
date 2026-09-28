#pragma once

#include <memory>

#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/object_storage_adapter.h"

namespace mooncake::testing {

class MissingQueryProvider final : public ObjectStorageAdapter {
   public:
    tl::expected<void, ErrorCode> Put(const std::string &,
                                      std::span<const char>) override {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    tl::expected<void, ErrorCode> PutV(const std::string &, const iovec *,
                                       int) override {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    tl::expected<size_t, ErrorCode> Get(const std::string &, void *,
                                        size_t) override {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    tl::expected<bool, ErrorCode> Exists(const std::string &) override {
        return false;
    }
    tl::expected<void, ErrorCode> Delete(const std::string &) override {
        return {};
    }
    bool SupportsProviderQuery() const override { return true; }
    ObjectStorageQueryResults BatchQueryProvider(
        std::span<const std::string> logical_keys) override {
        return ObjectStorageQueryResults(
            logical_keys.size(),
            tl::unexpected(ErrorCode::OBJECT_NOT_FOUND));
    }
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override {
        return std::vector<KeyInfo>{};
    }
    tl::expected<void, ErrorCode> Init() override { return {}; }
    const char *GetName() const override { return "missing-query-provider"; }
};

inline std::shared_ptr<DistributedStorageBackend>
MakeMissingQueryProviderBackend() {
    auto backend = std::make_shared<DistributedStorageBackend>(
        FileStorageConfig{}, DistributedStorageConfig{}, nullptr,
        std::make_unique<MissingQueryProvider>());
    if (!backend->Init()) return nullptr;
    return backend;
}

}  // namespace mooncake::testing
