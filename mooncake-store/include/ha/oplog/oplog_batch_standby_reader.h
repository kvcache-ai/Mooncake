#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "ha/oplog/oplog_batch_storage.h"
#include "types.h"

namespace mooncake {

class HaKvBackend;
class OpLogApplier;

struct OpLogBatchStandbyPollResult {
    ErrorCode error{ErrorCode::OK};
    bool used_legacy_path{false};
    bool waiting_for_legacy_catch_up{false};
    size_t applied_entries{0};
    DurablePrefix durable_prefix{};
};

class OpLogBatchStandbyReader {
   public:
    OpLogBatchStandbyReader(std::string cluster_id, HaKvBackend& backend,
                            OpLogApplier& applier);

    OpLogBatchStandbyPollResult PollOnce(size_t max_batches = 1024);

   private:
    OpLogBatchStorage storage_;
    OpLogApplier& applier_;
    uint64_t last_applied_batch_id_{0};
};

}  // namespace mooncake
