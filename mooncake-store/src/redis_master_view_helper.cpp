#ifdef STORE_USE_REDIS

#include "redis_master_view_helper.h"

namespace mooncake {

RedisMasterViewHelper::RedisMasterViewHelper(const std::string& cluster_id,
                                             const std::string& redis_endpoint,
                                             const std::string& password,
                                             int db_index, int ttl_sec,
                                             int heartbeat_interval_sec,
                                             const std::string& username)
    : redis_election_helper_(cluster_id, redis_endpoint, password, db_index,
                             ttl_sec, heartbeat_interval_sec, username),
      ttl_sec_(ttl_sec) {}

ErrorCode RedisMasterViewHelper::Connect() {
    return redis_election_helper_.Connect();
}

void RedisMasterViewHelper::ElectLeader(const std::string& master_address,
                                        ViewVersionId& version,
                                        EtcdLeaseId& lease_id) {
    int redis_lease_id = 0;
    redis_election_helper_.ElectLeader(master_address, version, redis_lease_id);
    lease_id = static_cast<EtcdLeaseId>(redis_lease_id);
}

void RedisMasterViewHelper::KeepLeader(EtcdLeaseId lease_id) {
    redis_election_helper_.KeepLeader(static_cast<int>(lease_id));
}

void RedisMasterViewHelper::CancelKeepAlive(EtcdLeaseId lease_id) {
    (void)lease_id;
    redis_election_helper_.CancelKeepAlive();
}

int RedisMasterViewHelper::GetLeaderLeaseTTLSeconds() const { return ttl_sec_; }

void RedisMasterViewHelper::CancelElection() {
    redis_election_helper_.CancelElection();
}

ErrorCode RedisMasterViewHelper::GetMasterView(std::string& master_address,
                                               ViewVersionId& version) {
    return redis_election_helper_.GetMasterView(master_address, version);
}

}  // namespace mooncake

#endif  // STORE_USE_REDIS
