#pragma once

#include <string>
#include <vector>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_types.h"
#include "types.h"

namespace mooncake {

class OpLogBatchStorage {
   public:
    OpLogBatchStorage(std::string cluster_id, HaKvBackend& backend);

    ErrorCode InitDurablePrefix(DurablePrefix& prefix);
    ErrorCode ClaimProducerView(ViewVersionId producer_view_version);
    ErrorCode ValidateProducerView(ViewVersionId producer_view_version) const;
    ErrorCode ReadProducerView(ViewVersionId& producer_view_version) const;
    ErrorCode ReadDurablePrefix(DurablePrefix& prefix);
    ErrorCode WriteBatchAndAdvancePrefix(const OpLogBatchRecord& batch,
                                         const DurablePrefix& expected_prefix);
    ErrorCode WriteBatchAndAdvancePrefix(const OpLogBatchRecord& batch,
                                         const DurablePrefix& expected_prefix,
                                         ViewVersionId producer_view_version);
    ErrorCode ReadBatch(uint64_t batch_id, OpLogBatchRecord& batch);
    ErrorCode ReadBatchesAfter(uint64_t after_batch_id, size_t limit,
                               std::vector<OpLogBatchRecord>& batches);

   private:
    bool IsValidClusterId() const;
    ErrorCode WriteBatchAndAdvancePrefixImpl(
        const OpLogBatchRecord& batch, const DurablePrefix& expected_prefix,
        const ViewVersionId* producer_view_version);
    ErrorCode RejectLegacyLayout() const;
    ErrorCode ValidateDurablePrefixAtStartup(const DurablePrefix& prefix);

    std::string cluster_id_;
    HaKvBackend& backend_;
    bool cluster_id_valid_{false};
};

}  // namespace mooncake
