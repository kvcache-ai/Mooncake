#pragma once

#include <atomic>
#include <map>
#include <set>
#include <unordered_set>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Applications supply unique IDs and do not reuse them for another session.
// IDs use the client's existing tenant namespace; they are not credentials.
struct KvSessionInfo {
    std::string session_id;
    bool pinned{false};
    uint64_t member_count{0};
};
YLT_REFL(KvSessionInfo, session_id, pinned, member_count);

struct KvSessionPage {
    std::vector<std::string> keys;
    // Exclusive lexical cursor. Empty means the current enumeration is done.
    std::string next_cursor;
};
YLT_REFL(KvSessionPage, keys, next_cursor);

using KvSessionTags = std::vector<std::vector<std::string>>;

struct KvSessionLimits {
    size_t sessions{100000};
    size_t members_per_session{1000000};
    size_t sessions_per_object{100000};
    size_t batch_size{4096};
    static KvSessionLimits FromEnvironment();
};

class KvSessionRegistry;

struct KvSessionRecord {
    std::string session_id;
    std::atomic<bool> pinned{false};
    // Keys only; membership never keeps a pointer to another object.
    // Guarded by the registry mutex.
    std::set<std::string> members;
};

// Owned exclusively by ObjectMetadata. The metadata shard lock protects this
// object's session references. Destruction unregisters its keys, including on
// cleanup paths that bypass EraseMetadata. A shared registry keeps that cleanup
// safe even when the master's registry reference has already been destroyed.
struct KvSessionMembership {
    explicit KvSessionMembership(std::shared_ptr<KvSessionRegistry> registry,
                                 std::string key, TenantId tenant)
        : registry(std::move(registry)),
          key(std::move(key)),
          tenant(std::move(tenant)) {}
    ~KvSessionMembership();
    KvSessionMembership(const KvSessionMembership&) = delete;
    KvSessionMembership& operator=(const KvSessionMembership&) = delete;
    // Caller holds the object's metadata shard lock (exclusive for removal).
    bool IsPinned() const;
    void RemoveSession(const std::string& session_id);
    std::shared_ptr<KvSessionRegistry> registry;
    std::string key;
    TenantId tenant;
    std::unordered_set<std::shared_ptr<KvSessionRecord>> sessions;
};

// Construct with shared ownership. The mutex protects the index, not a whole
// session update: MasterService lists bounded pages, releases the index lock,
// then visits each key under its existing metadata shard lock. Lock order is
// metadata shard -> registry; the registry never acquires metadata locks.
// Nothing here is serialized into snapshots.
class KvSessionRegistry
    : public std::enable_shared_from_this<KvSessionRegistry> {
   public:
    explicit KvSessionRegistry(
        KvSessionLimits limits = KvSessionLimits::FromEnvironment())
        : limits_(limits) {}

    tl::expected<void, ErrorCode> SetPin(const TenantId& tenant,
                                         const std::string& session_id,
                                         bool pinned);
    tl::expected<KvSessionInfo, ErrorCode> Get(const TenantId& tenant,
                                               const std::string& session_id);
    tl::expected<KvSessionPage, ErrorCode> List(const TenantId& tenant,
                                                const std::string& session_id,
                                                const std::string& cursor,
                                                size_t limit);
    tl::expected<void, ErrorCode> Validate(
        const std::vector<std::string>& session_ids) const;
    tl::expected<void, ErrorCode> Attach(
        std::unique_ptr<KvSessionMembership>& membership,
        const TenantId& tenant, const std::string& key,
        const std::vector<std::string>& session_ids);
    tl::expected<void, ErrorCode> ValidateUpdate(
        const std::string& session_id,
        const std::vector<std::string>& keep_keys) const;
    size_t batch_limit() const { return limits_.batch_size; }

   private:
    tl::expected<std::shared_ptr<KvSessionRecord>, ErrorCode> FindLocked(
        const TenantId& tenant, const std::string& session_id);
    friend struct KvSessionMembership;
    void EraseMemberLocked(const TenantId& tenant, const std::string& key,
                           const std::shared_ptr<KvSessionRecord>& record);
    KvSessionLimits limits_;
    std::mutex mutex_;
    std::map<std::string, std::shared_ptr<KvSessionRecord>> sessions_;
};

}  // namespace mooncake
