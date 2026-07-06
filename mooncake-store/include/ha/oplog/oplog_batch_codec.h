#pragma once

#include <string>

#include "ha/oplog/oplog_batch_types.h"

namespace mooncake {

std::string EncodeDurablePrefix(const DurablePrefix& prefix);
bool DecodeDurablePrefix(const std::string& value, DurablePrefix* prefix,
                         std::string* reason = nullptr);

std::string EncodeOpLogBatchRecord(const OpLogBatchRecord& batch);
bool DecodeOpLogBatchRecord(const std::string& value, OpLogBatchRecord* batch,
                            std::string* reason = nullptr);

}  // namespace mooncake
