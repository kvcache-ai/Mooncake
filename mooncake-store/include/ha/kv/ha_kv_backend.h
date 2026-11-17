#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "types.h"

namespace mooncake {

struct KvPair {
    std::string key;
    std::string value;
};

enum class KvCompareKind {
    kValueEquals,
    kKeyNotExists,
    kCreateRevisionEquals,
};

struct KvCompare {
    std::string key;
    KvCompareKind kind{KvCompareKind::kValueEquals};
    std::string expected_value;
    // Used only by kCreateRevisionEquals.
    EtcdRevisionId expected_revision{0};
};

struct KvTxn {
    std::vector<KvCompare> compares;
    std::vector<KvPair> puts;
};

enum class KvWriteStatus {
    kCommitted,
    kCompareFailed,
    kRejected,
    kFailedBeforeCommit,
    kOutcomeUnknown,
};

// Fixed for the lifetime of a backend; callers cannot weaken it per write.
// Zero counts describe a backend's native durability (e.g. etcd). Redis
// requires a positive replica count and must never interpret zero as HA.
struct KvWriteDurabilityPolicy {
    size_t replicas_required{0};
    std::chrono::milliseconds replica_ack_timeout{1000};
    bool require_local_aof{false};
    size_t replica_aof_required{0};
    std::chrono::milliseconds replica_aof_ack_timeout{1000};
};

struct KvWriteResult {
    KvWriteStatus status{KvWriteStatus::kOutcomeUnknown};
    ErrorCode error{ErrorCode::ETCD_OPERATION_ERROR};
    size_t replicas_acked{0};
    size_t replicas_required{0};
    bool local_aof_acked{false};
    size_t replica_aof_acked{0};

    static KvWriteResult Committed() {
        return {.status = KvWriteStatus::kCommitted, .error = ErrorCode::OK};
    }

    // A legacy operation error says nothing about whether the server committed.
    // Only errors whose contract proves no write occurred are classified as
    // rejected/compare-failed. In particular, etcd timeouts remain unknown.
    static KvWriteResult FromError(ErrorCode error) {
        if (error == ErrorCode::OK) {
            return Committed();
        }
        KvWriteStatus status = KvWriteStatus::kOutcomeUnknown;
        if (error == ErrorCode::ETCD_TRANSACTION_FAIL) {
            status = KvWriteStatus::kCompareFailed;
        } else if (error == ErrorCode::INVALID_PARAMS ||
                   error == ErrorCode::NOT_SUPPORTED ||
                   error == ErrorCode::UNAVAILABLE_IN_CURRENT_MODE) {
            status = KvWriteStatus::kRejected;
        }
        return {.status = status, .error = error};
    }

    ErrorCode ToLegacyError() const {
        if (status == KvWriteStatus::kCommitted) {
            return ErrorCode::OK;
        }
        if (status == KvWriteStatus::kCompareFailed) {
            return ErrorCode::ETCD_TRANSACTION_FAIL;
        }
        if (status == KvWriteStatus::kOutcomeUnknown ||
            error == ErrorCode::OK) {
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        return error;
    }
};

class HaKvBackend {
   public:
    virtual ~HaKvBackend() = default;

    virtual ErrorCode Get(std::string_view key, std::string& value) = 0;
    virtual ErrorCode Put(std::string_view key, std::string_view value) = 0;
    // Defaults preserve existing backends/fakes implementing only ErrorCode.
    // Backends with detailed write results override these methods and keep
    // their legacy methods as adapters. Never automatically replay unknowns.
    virtual KvWriteResult PutWithResult(std::string_view key,
                                        std::string_view value) {
        return KvWriteResult::FromError(Put(key, value));
    }
    virtual ErrorCode Range(std::string_view begin_key,
                            std::string_view end_key, size_t limit,
                            std::vector<KvPair>& kvs) = 0;
    // Delete [begin_key, end_key). Both bounds must be nonempty and finite.
    // Equal bounds are a no-op for backends supporting deletion.
    // Unsupported backends must return INVALID_PARAMS.
    virtual ErrorCode DeleteRange(std::string_view begin_key,
                                  std::string_view end_key) = 0;
    virtual KvWriteResult DeleteRangeWithResult(std::string_view begin_key,
                                                std::string_view end_key) {
        return KvWriteResult::FromError(DeleteRange(begin_key, end_key));
    }
    virtual bool SupportsTxn() const = 0;
    virtual ErrorCode Txn(const KvTxn& txn) = 0;
    virtual KvWriteResult TxnWithResult(const KvTxn& txn) {
        return KvWriteResult::FromError(Txn(txn));
    }
    virtual KvWriteDurabilityPolicy GetWriteDurabilityPolicy() const {
        return {};
    }
};

}  // namespace mooncake
