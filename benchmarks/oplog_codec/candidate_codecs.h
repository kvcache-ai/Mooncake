#pragma once

#include <string>
#include <vector>
#include <array>

#include "ha/oplog/oplog_batch_types.h"

// P01 experiments only. These formats are not supported production history.
namespace mooncake::codec_bench {
enum class Format { Json, JsonControl, Cbor, MessagePack };
inline constexpr std::array<Format, 4> kFormats = {
    Format::Json, Format::JsonControl, Format::Cbor, Format::MessagePack};
const char* Name(Format format);
Format ParseFormat(const std::string& name);
std::string Encode(Format format, const OpLogBatchRecord& batch);
bool Decode(Format format, const std::string& wire, OpLogBatchRecord* batch);
bool Equivalent(const OpLogBatchRecord& lhs, const OpLogBatchRecord& rhs);
struct Workload {
    std::string name;
    OpLogBatchRecord batch;
};
std::vector<Workload> Workloads();
std::vector<Workload> TypedWorkloads();
Workload ReplayWorkload();
void VerifyReplay(const OpLogBatchRecord& batch);
}  // namespace mooncake::codec_bench
