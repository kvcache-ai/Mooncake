// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "types.h"
#include <optional>
#include <chrono>
#include <ylt/util/tl/expected.hpp>
namespace mooncake {
class Client;
class FileStorage;
namespace ssd_offload {
struct SsdRuntimeObject {
    std::string key;
    uint64_t expected_size;
    uint64_t source_offset;
    std::vector<Slice> destinations;
};
class SsdDirectRuntime {
   public:
    static bool Enabled(const std::shared_ptr<Client>&);
    SsdDirectRuntime(std::shared_ptr<Client>, std::shared_ptr<FileStorage>,
                     std::string legacy_endpoint, std::string host);
    ~SsdDirectRuntime();
    std::string PeerInfo() const;
    int ConfigurePeers(const std::vector<std::string>&);
    int ImportFixedPool(void*, size_t, std::shared_ptr<void> lifetime = {});
    int RevokeFixedPool(void*);
    bool HasPending(void*, size_t) const;
    // nullopt: unsupported/unconfigured before submission, so legacy is safe.
    std::optional<tl::expected<void, ErrorCode>> Read(
        const std::string&, const std::vector<SsdRuntimeObject>&,
        std::chrono::milliseconds);
    int Recover();
    // Busy preserves recovery and requires the caller to retain external L2.
    int Close();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace ssd_offload
}  // namespace mooncake
