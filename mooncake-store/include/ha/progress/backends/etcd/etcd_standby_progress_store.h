#pragma once

#include <string>

#include "ha/progress/standby_progress_store.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace etcd {

class EtcdStandbyProgressStore final : public StandbyProgressStore {
   public:
    explicit EtcdStandbyProgressStore(ClusterNamespace cluster_namespace);

    ErrorCode Publish(const StandbyProgressRecord& record) override;

    tl::expected<std::vector<StandbyProgressRecord>, ErrorCode> List() override;

    ErrorCode Delete(std::string_view standby_id) override;

   private:
    static ClusterNamespace ResolveClusterNamespace(
        const ClusterNamespace& cluster_namespace);
    static std::string BuildProgressPrefix(
        const ClusterNamespace& cluster_namespace);
    static std::string BuildRecordKey(const ClusterNamespace& cluster_namespace,
                                      std::string_view standby_id);
    static std::string BuildRangeEnd(std::string_view prefix);

    ClusterNamespace cluster_namespace_;
    std::string prefix_;
};

}  // namespace etcd
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
