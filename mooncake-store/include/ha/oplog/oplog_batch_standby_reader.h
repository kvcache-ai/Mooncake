#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "ha/oplog/oplog_batch_storage.h"
#include "types.h"

namespace mooncake {

class HaKvBackend;
class OpLogApplier;

enum class OpLogBatchStandbyPollDisposition {
    OK,
    RETRYABLE,
    FATAL,
};

struct OpLogBatchStandbyPollResult {
    OpLogBatchStandbyPollDisposition disposition{
        OpLogBatchStandbyPollDisposition::OK};
    ErrorCode error{ErrorCode::OK};
    bool durable_prefix_present{false};
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
    bool batch_format_seen_{false};
    std::optional<DurablePrefix> last_observed_prefix_;
    std::optional<uint64_t> last_scanned_batch_last_seq_;
    uint64_t last_applied_batch_id_{0};
};

}  // namespace mooncake
