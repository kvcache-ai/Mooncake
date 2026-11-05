#pragma once

#include <mutex>
#include <string>

#include "ha/common/redis/redis_connection.h"
#include "ha/kv/ha_kv_backend.h"

namespace mooncake {

// Redis implementation of the OpLog KV contract. Logical `/oplog/{cluster}/...`
// keys are stored under one hash tag so a transaction stays on one slot.
// kCreateRevisionEquals is rejected until snapshot fencing is implemented.
class RedisHaKvBackend : public HaKvBackend {
   public:
    // Task 2 resolves/checks these requirements against the slot topology.
    explicit RedisHaKvBackend(
        KvWriteDurabilityPolicy policy = {.replicas_required = 1})
        : write_policy_(policy) {}

    ErrorCode Connect(std::string connstring);
    KvWriteDurabilityPolicy GetWriteDurabilityPolicy() const override {
        return write_policy_;
    }

    ErrorCode Get(std::string_view key, std::string& value) override;
    ErrorCode Put(std::string_view key, std::string_view value) override;
    KvWriteResult PutWithResult(std::string_view key,
                                std::string_view value) override;
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override;
    ErrorCode DeleteRange(std::string_view begin_key,
                          std::string_view end_key) override;
    KvWriteResult DeleteRangeWithResult(std::string_view begin_key,
                                        std::string_view end_key) override;
    bool SupportsTxn() const override;
    ErrorCode Txn(const KvTxn& txn) override;
    KvWriteResult TxnWithResult(const KvTxn& txn) override;

   private:
    ErrorCode EnsureConnectedLocked();
    KvWriteResult ExecuteWriteLocked(const std::vector<std::string>& command,
                                     const char* operation,
                                     const std::string& cluster_id);

    const KvWriteDurabilityPolicy write_policy_;
    std::mutex mu_;
    std::string connstring_;
    ha::common::redis::RedisContextPtr context_;
};

}  // namespace mooncake
