// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {
class Client;
class FileStorage;

namespace ssd_offload {

struct SsdRuntimeObject {
    std::string key;
    uint64_t expected_size{0};
    uint64_t source_offset{0};
    std::vector<Slice> destinations;
};

class SsdDirectRuntime {
   public:
    static bool Enabled(const std::shared_ptr<Client>& client);

    SsdDirectRuntime(std::shared_ptr<Client> client,
                     std::shared_ptr<FileStorage> storage,
                     std::string legacy_endpoint, std::string host);
    ~SsdDirectRuntime();

    // Canonical version-1 peer bootstrap document (contains shared HMAC
    // credentials; do not log or export as public metrics).
    std::string PeerInfo() const;
    // Non-secret operational metrics JSON (no credentials).
    std::string StatsJson() const;
    int ConfigurePeers(const std::vector<std::string>& documents);
    int ImportFixedPool(void* base, size_t bytes,
                        std::shared_ptr<void> lifetime = {});
    int RevokeFixedPool(void* base);
    bool HasPending(void* base, size_t bytes) const;
    // Non-destructive check for active or un-drained push attempts.
    bool HasPendingAttempts() const;
    // True when endpoint resolves to a readable push owner and [ptr, ptr+bytes)
    // is covered by an imported fixed CPU pool.
    bool CanPush(const std::string& endpoint, const void* ptr,
                 size_t bytes) const;

    // nullopt: unsupported/unconfigured/preflight-rejected before any write,
    // so legacy fallback is safe.
    std::optional<tl::expected<void, ErrorCode>> Read(
        const std::string& endpoint,
        const std::vector<SsdRuntimeObject>& objects,
        std::chrono::milliseconds timeout);

    // Bounded recovery probe over currently pending attempts; returns the
    // number of attempts still un-drained.
    int Recover();

    // When force_quarantine is false, returns -1 without closing the requester
    // if any read is in flight or any pending attempt remains, allowing callers
    // to call Recover() and retry Close(). When force_quarantine is true
    // (final destructor path), stops the owner RPC server/directory and
    // quarantines any un-drained destination registrations in
    // DestinationRegistry without unregistering user memory prematurely.
    int Close(bool force_quarantine = false);

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ssd_offload
}  // namespace mooncake
