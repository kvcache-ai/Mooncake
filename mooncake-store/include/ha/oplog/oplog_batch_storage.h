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
    ErrorCode ReadDurablePrefix(DurablePrefix& prefix);
    ErrorCode WriteBatchAndAdvancePrefix(const OpLogBatchRecord& batch,
                                         const DurablePrefix& expected_prefix);
    ErrorCode ReadBatch(uint64_t batch_id, OpLogBatchRecord& batch);
    ErrorCode ReadBatchesAfter(uint64_t after_batch_id, size_t limit,
                               std::vector<OpLogBatchRecord>& batches);

   private:
    bool IsValidClusterId() const;
    ErrorCode RejectLegacyLayout() const;

    std::string cluster_id_;
    HaKvBackend& backend_;
    bool cluster_id_valid_{false};
};

}  // namespace mooncake
