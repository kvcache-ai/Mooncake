#ifndef MOONCAKE_REDIS_MASTER_VIEW_HELPER_H_
#define MOONCAKE_REDIS_MASTER_VIEW_HELPER_H_

#include "ha_helper.h"

#ifdef STORE_USE_REDIS
#include "redis_helper.h"
#endif

namespace mooncake {

/*
 * @brief Redis-based leader election backend for MasterViewHelper.
 *        Overrides the etcd-based virtual methods to use Redis
 *        for leader election, leader keep-alive, and master view queries.
 *
 * NOTE: This class is introduced temporarily to minimize divergence
 * from the community mainline. It may be refactored or removed once
 * a unified election abstraction is upstreamed.
 */
#ifdef STORE_USE_REDIS
class RedisMasterViewHelper : public MasterViewHelper {
   public:
    RedisMasterViewHelper(const std::string& cluster_id,
                          const std::string& redis_endpoint,
                          const std::string& password, int db_index,
                          int ttl_sec, int heartbeat_interval_sec);

    ErrorCode Connect();

    void ElectLeader(const std::string& master_address, ViewVersionId& version,
                     EtcdLeaseId& lease_id) override;

    void KeepLeader(EtcdLeaseId lease_id) override;

    void CancelKeepAlive(EtcdLeaseId lease_id) override;

    void CancelElection();

    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version) override;

   private:
    RedisHelper redis_helper_;
};
#endif  // STORE_USE_REDIS

}  // namespace mooncake

#endif  // MOONCAKE_REDIS_MASTER_VIEW_HELPER_H_
