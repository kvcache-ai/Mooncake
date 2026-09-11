#pragma once

#include <string>
#include <string_view>

#include "types.h"
#ifdef STORE_USE_REDIS
#include "p2p/ha/redis_election_helper.h"
#endif

namespace mooncake {

std::string BuildP2PEtcdMasterViewKey(std::string_view cluster_id);

class P2PMasterView {
   public:
    virtual ~P2PMasterView() = default;

    virtual void ElectLeader(const std::string& master_address,
                             ViewVersionId& version, EtcdLeaseId& lease_id) = 0;
    virtual void KeepLeader(EtcdLeaseId lease_id) = 0;
    virtual void CancelKeepAlive(EtcdLeaseId lease_id) = 0;
    virtual int GetLeaderLeaseTTLSeconds() const = 0;
    virtual ErrorCode GetMasterView(std::string& master_address,
                                    ViewVersionId& version) = 0;
};

class P2PEtcdMasterView final : public P2PMasterView {
   public:
    explicit P2PEtcdMasterView(std::string_view cluster_id);

    ErrorCode Connect(const std::string& etcd_endpoints);
    void ElectLeader(const std::string& master_address, ViewVersionId& version,
                     EtcdLeaseId& lease_id) override;
    void KeepLeader(EtcdLeaseId lease_id) override;
    void CancelKeepAlive(EtcdLeaseId lease_id) override;
    int GetLeaderLeaseTTLSeconds() const override;
    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version) override;

   private:
    std::string master_view_key_;
};

#ifdef STORE_USE_REDIS
class P2PRedisMasterView final : public P2PMasterView {
   public:
    P2PRedisMasterView(const std::string& cluster_id,
                       const std::string& redis_endpoint,
                       const std::string& password, int db_index,
                       int ttl_seconds, int heartbeat_interval_seconds,
                       const std::string& username = "");

    ErrorCode Connect();
    void ElectLeader(const std::string& master_address, ViewVersionId& version,
                     EtcdLeaseId& lease_id) override;
    void KeepLeader(EtcdLeaseId lease_id) override;
    void CancelKeepAlive(EtcdLeaseId lease_id) override;
    int GetLeaderLeaseTTLSeconds() const override;
    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version) override;

   private:
    RedisElectionHelper redis_election_helper_;
    int ttl_seconds_;
};
#endif

}  // namespace mooncake
