#pragma once

#include <string>

#include "ha/oplog/oplog_store.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace redis {

class RedisOpLogStore final : public OpLogStore {
   public:
    RedisOpLogStore(std::string connstring, ClusterNamespace cluster_namespace);

    tl::expected<OpLogSequenceId, ErrorCode> Append(
        const OpLogAppendRequest& request) override;

    tl::expected<OpLogPollResult, ErrorCode> PollFrom(
        OpLogSequenceId start_seq, size_t max_records,
        std::chrono::milliseconds timeout) override;

    tl::expected<OpLogSequenceId, ErrorCode> GetLatestSequence() override;

    ErrorCode CleanupBefore(OpLogSequenceId before_sequence_id) override;

   private:
    static ClusterNamespace ResolveClusterNamespace(
        const ClusterNamespace& cluster_namespace);
    static std::string FormatSequence(OpLogSequenceId seq);
    static std::string BuildLatestKey(
        const ClusterNamespace& cluster_namespace);
    static std::string BuildIndexKey(const ClusterNamespace& cluster_namespace);
    static std::string BuildEntryKey(const ClusterNamespace& cluster_namespace,
                                     OpLogSequenceId seq);
    static tl::expected<OpLogSequenceId, ErrorCode> ParseSequenceMember(
        std::string_view member);

    std::string connstring_;
    ClusterNamespace cluster_namespace_;
    std::string latest_key_;
    std::string index_key_;
};

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
