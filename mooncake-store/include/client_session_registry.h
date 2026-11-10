#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "client_liveness.h"
#include "types.h"

namespace mooncake {
class MasterService;
namespace test {
class MasterServiceTestPeer;
class ClientSessionRegistryTestPeer;
}  // namespace test

// Owns membership, readiness, host hints and registration rollback. This first
// extraction deliberately retains the global registration lock and liveness
// guards. Per-client admission and heartbeat isolation are a separate change.
class ClientSessionRegistry {
    using Record = std::shared_ptr<ClientLivenessRecord>;
    using Records = std::unordered_map<UUID, Record, boost::hash<UUID>>;
    enum class RegistrationKind { Register, Remount };

   public:
    using Clock = ClientLivenessRecord::Clock;
    ~ClientSessionRegistry() { Reset(); }
    class SessionGuard;
    class Registration;
    class RestoreBatch;
    std::optional<SessionGuard> TryAcquireServingSession(const UUID& id) const;
    std::optional<SessionGuard> TryAcquireRetainingSession(
        const UUID& id) const;
    std::optional<Registration> BeginRegistration(
        const UUID& id,
        std::optional<Clock::time_point> observed_at = std::nullopt);
    std::optional<Registration> BeginRemount(const UUID& id);
    RestoreBatch BeginRestore();
    bool Remove(const UUID& id, const ClientSessionSharedPtr& expected);
    void UpdateHostId(const UUID& id, const std::string& host_id);
    std::string GetHostId(const UUID& id) const;
    ClientStatus Ping(const UUID& id);
    void Reset();

   private:
    // Only the legacy expiry driver still needs the old critical sections.
    // It is migrated together with its retirement side effects later.
    friend class MasterService;
    friend class test::MasterServiceTestPeer;
    friend class test::ClientSessionRegistryTestPeer;
    Record Find(const UUID& id) const;
    void EraseLocked(const UUID& id, const Record& record);
    mutable std::shared_mutex mutex_;
    Records records_;
    std::unordered_set<UUID, boost::hash<UUID>> ready_;
    std::unordered_map<UUID, std::string, boost::hash<UUID>> host_ids_;
};

class ClientSessionRegistry::SessionGuard {
   public:
    SessionGuard(SessionGuard&&) noexcept = default;
    SessionGuard& operator=(SessionGuard&&) = delete;
    ClientSessionSharedPtr Session() const { return record_; }

   private:
    friend class ClientSessionRegistry;
    SessionGuard(Record record, ClientLivenessRecord::RetainingGuard guard)
        : record_(std::move(record)), guard_(std::move(guard)) {}
    Record record_;
    ClientLivenessRecord::RetainingGuard guard_;
};

class ClientSessionRegistry::Registration {
   public:
    ~Registration();
    Registration(Registration&&) noexcept = default;
    Registration& operator=(Registration&&) = delete;
    ClientSessionSharedPtr Session() const { return record_; }
    bool NeedsRemount() const;
    void UpdateHostId(const std::string& host_id);
    void Commit();

   private:
    friend class ClientSessionRegistry;
    Registration(ClientSessionRegistry& registry, const UUID& id,
                 RegistrationKind kind, Clock::time_point observed_at);
    ClientSessionRegistry& registry_;
    std::unique_lock<std::shared_mutex> lock_;
    UUID id_;
    RegistrationKind kind_;
    Clock::time_point observed_at_;
    Record record_;
    std::optional<ClientLivenessRecord::RetainingGuard> guard_;
    bool provisional_{false};
    bool committed_{false};
};

class ClientSessionRegistry::RestoreBatch {
   public:
    RestoreBatch(RestoreBatch&&) noexcept = default;
    RestoreBatch& operator=(RestoreBatch&&) = delete;
    ClientSessionSharedPtr FindOrCreate(const UUID& id);
    void Commit();

   private:
    friend class ClientSessionRegistry;
    explicit RestoreBatch(ClientSessionRegistry& registry)
        : registry_(registry), lock_(registry.mutex_) {}
    ClientSessionRegistry& registry_;
    std::unique_lock<std::shared_mutex> lock_;
    Records pending_;
    bool committed_{false};
};
}  // namespace mooncake
