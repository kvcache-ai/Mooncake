#include "master/ha/oplog/oplog_batch_storage.h"

#include <gtest/gtest.h>
#include <xxhash.h>

#include <unistd.h>

#include <cstdlib>
#include <functional>
#include <limits>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "common/etcd_helper.h"
#include "master/ha/kv/etcd_ha_kv_backend.h"
#include "master/ha/kv/ha_kv_backend.h"
#include "master/ha/oplog/oplog_batch_codec.h"
#include "master/ha/oplog/oplog_batch_types.h"
#include "master/ha/snapshot/batch_oplog/metadata.h"

namespace mooncake::test {
namespace {

class FakeHaKvBackend : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        if (next_get_key_error_ != ErrorCode::OK &&
            key == next_get_error_key_) {
            ErrorCode err = next_get_key_error_;
            next_get_key_error_ = ErrorCode::OK;
            next_get_error_key_.clear();
            return err;
        }
        if (next_get_error_ != ErrorCode::OK) {
            ErrorCode err = next_get_error_;
            next_get_error_ = ErrorCode::OK;
            return err;
        }
        auto it = kvs_.find(std::string(key));
        if (it == kvs_.end()) {
            return ErrorCode::ETCD_KEY_NOT_EXIST;
        }
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        kvs_[std::string(key)] = std::string(value);
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        range_limits_.push_back(limit);
        if (next_range_error_ != ErrorCode::OK) {
            ErrorCode err = next_range_error_;
            next_range_error_ = ErrorCode::OK;
            return err;
        }
        kvs.clear();
        for (auto it = kvs_.lower_bound(std::string(begin_key));
             it != kvs_.end() && it->first < end_key; ++it) {
            kvs.push_back({.key = it->first, .value = it->second});
            if (limit != 0 && kvs.size() >= limit) {
                break;
            }
        }
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return supports_txn_; }

    ErrorCode DeleteRange(std::string_view begin_key,
                          std::string_view end_key) override {
        delete_ranges.push_back({std::string(begin_key), std::string(end_key)});
        if (before_delete) {
            auto callback = std::move(before_delete);
            before_delete = nullptr;
            callback();
        }
        size_t deleted = 0;
        auto it = kvs_.lower_bound(std::string(begin_key));
        while (it != kvs_.end() && it->first < end_key) {
            if (deleted == delete_fail_after) {
                return ErrorCode::ETCD_OPERATION_ERROR;
            }
            it = kvs_.erase(it);
            ++deleted;
        }
        return delete_result;
    }

    std::vector<BatchRecordRange> delete_ranges;
    std::function<void()> before_delete;
    size_t delete_fail_after{std::numeric_limits<size_t>::max()};
    ErrorCode delete_result{ErrorCode::OK};

    ErrorCode Txn(const KvTxn& txn) override {
        if (!supports_txn_) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (next_txn_error_ != ErrorCode::OK) {
            ErrorCode err = next_txn_error_;
            next_txn_error_ = ErrorCode::OK;
            return err;
        }
        if (race_before_next_txn_) {
            kvs_[race_key_] = race_value_;
            race_before_next_txn_ = false;
        }
        for (const auto& compare : txn.compares) {
            auto it = kvs_.find(compare.key);
            if (compare.kind == KvCompareKind::kKeyNotExists) {
                if (it != kvs_.end()) {
                    return ErrorCode::ETCD_TRANSACTION_FAIL;
                }
            } else if (it == kvs_.end() ||
                       it->second != compare.expected_value) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) {
            kvs_[put.key] = put.value;
        }
        if (next_committed_txn_error_ != ErrorCode::OK) {
            ErrorCode err = next_committed_txn_error_;
            next_committed_txn_error_ = ErrorCode::OK;
            return err;
        }
        return ErrorCode::OK;
    }

    void SetSupportsTxn(bool supports_txn) { supports_txn_ = supports_txn; }
    void CreateBeforeNextTxn(std::string key, std::string value) {
        race_before_next_txn_ = true;
        race_key_ = std::move(key);
        race_value_ = std::move(value);
    }
    void FailNextGet(ErrorCode err) { next_get_error_ = err; }
    void FailNextGetForKey(std::string key, ErrorCode err) {
        next_get_error_key_ = std::move(key);
        next_get_key_error_ = err;
    }
    void FailNextRange(ErrorCode err) { next_range_error_ = err; }
    void FailNextTxn(ErrorCode err) { next_txn_error_ = err; }
    void FailNextCommittedTxn(ErrorCode err) {
        next_committed_txn_error_ = err;
    }
    const std::vector<size_t>& range_limits() const { return range_limits_; }

   private:
    std::map<std::string, std::string> kvs_;
    bool supports_txn_{true};
    bool race_before_next_txn_{false};
    std::string race_key_;
    std::string race_value_;
    ErrorCode next_get_error_{ErrorCode::OK};
    std::string next_get_error_key_;
    ErrorCode next_get_key_error_{ErrorCode::OK};
    ErrorCode next_range_error_{ErrorCode::OK};
    ErrorCode next_txn_error_{ErrorCode::OK};
    ErrorCode next_committed_txn_error_{ErrorCode::OK};
    std::vector<size_t> range_limits_;
};

OpLogEntry MakeEntry(uint64_t seq) {
    OpLogEntry entry;
    entry.sequence_id = seq;
    entry.timestamp_ms = 1234567890;
    entry.op_type = OpType::PUT_END;
    entry.tenant_id = "tenant";
    entry.object_key = "key" + std::to_string(seq);
    entry.payload = "value" + std::to_string(seq);
    entry.checksum = static_cast<uint32_t>(
        XXH32(entry.payload.data(), entry.payload.size(), 0));
    entry.prefix_hash = static_cast<uint32_t>(
        XXH32(entry.object_key.data(), entry.object_key.size(), 0));
    return entry;
}

OpLogBatchRecord MakeBatch(uint64_t batch_id, uint64_t first_seq,
                           size_t count) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.first_seq = first_seq;
    batch.last_seq = first_seq + count - 1;
    for (size_t i = 0; i < count; ++i) {
        batch.entries.push_back(MakeEntry(first_seq + i));
    }
    return batch;
}

}  // namespace

TEST(OpLogBatchStorageTest, InitializesEmptyNamespaceAtZero) {
    FakeHaKvBackend backend;
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::OK, storage.InitDurablePrefix(prefix));

    EXPECT_EQ(0u, prefix.batch_id);
    EXPECT_EQ(0u, prefix.last_seq);
    std::string encoded;
    ASSERT_EQ(ErrorCode::OK,
              backend.Get("/oplog/clusterA/durable_prefix", encoded));
    DurablePrefix stored;
    ASSERT_TRUE(DecodeDurablePrefix(encoded, &stored));
    EXPECT_EQ(prefix.batch_id, stored.batch_id);
    EXPECT_EQ(prefix.last_seq, stored.last_seq);
}

TEST(OpLogBatchStorageTest, ResolvesCommittedInitTxnError) {
    FakeHaKvBackend backend;
    backend.FailNextCommittedTxn(ErrorCode::ETCD_OPERATION_ERROR);
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::OK, storage.InitDurablePrefix(prefix));
    EXPECT_EQ((DurablePrefix{.batch_id = 0, .last_seq = 0}), prefix);
}

TEST(OpLogBatchStorageTest, ClaimProducerViewCreatesIndependentKey) {
    FakeHaKvBackend backend;
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, storage.ValidateProducerView(7));
    ASSERT_EQ(ErrorCode::OK, storage.ClaimProducerView(7));
    EXPECT_EQ(ErrorCode::OK, storage.ValidateProducerView(7));

    std::string stored;
    ASSERT_EQ(ErrorCode::OK,
              backend.Get("/oplog/clusterA/producer_view", stored));
    EXPECT_EQ("7", stored);
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
              backend.Get("/oplog/clusterA/durable_prefix", stored));
}

TEST(OpLogBatchStorageTest, ClaimProducerViewIsIdempotent) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/producer_view", "7"));
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::OK, storage.ClaimProducerView(7));
    EXPECT_EQ(ErrorCode::OK, storage.ValidateProducerView(7));
}

TEST(OpLogBatchStorageTest, ClaimProducerViewAcceptsMaxView) {
    FakeHaKvBackend backend;
    OpLogBatchStorage storage("clusterA", backend);
    const auto max_view = std::numeric_limits<ViewVersionId>::max();

    EXPECT_EQ(ErrorCode::OK, storage.ClaimProducerView(max_view));
    EXPECT_EQ(ErrorCode::OK, storage.ValidateProducerView(max_view));
}

TEST(OpLogBatchStorageTest, ClaimProducerViewRejectsStaleView) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/producer_view", "8"));
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, storage.ClaimProducerView(7));
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              storage.ValidateProducerView(7));
    EXPECT_EQ(ErrorCode::OK, storage.ValidateProducerView(8));
}

TEST(OpLogBatchStorageTest, ClaimProducerViewIgnoresDurablePrefixRace) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/producer_view", "7"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 3})));
    backend.CreateBeforeNextTxn(
        "/oplog/clusterA/durable_prefix",
        EncodeDurablePrefix({.batch_id = 2, .last_seq = 6}));
    OpLogBatchStorage storage("clusterA", backend);

    ASSERT_EQ(ErrorCode::OK, storage.ClaimProducerView(8));
    EXPECT_EQ(ErrorCode::OK, storage.ValidateProducerView(8));
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    EXPECT_EQ((DurablePrefix{.batch_id = 2, .last_seq = 6}), prefix);
}

TEST(OpLogBatchStorageTest, ClaimProducerViewLosesRaceToNewerView) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/producer_view", "7"));
    backend.CreateBeforeNextTxn("/oplog/clusterA/producer_view", "9");
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, storage.ClaimProducerView(8));
    EXPECT_EQ(ErrorCode::OK, storage.ValidateProducerView(9));
}

TEST(OpLogBatchStorageTest, ClaimProducerViewPropagatesBackendTxnError) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/producer_view", "7"));
    backend.FailNextTxn(ErrorCode::ETCD_OPERATION_ERROR);
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, storage.ClaimProducerView(8));
    EXPECT_EQ(ErrorCode::OK, storage.ValidateProducerView(7));
}

TEST(OpLogBatchStorageTest, ClaimProducerViewResolvesCommittedTxnError) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/producer_view", "7"));
    backend.FailNextCommittedTxn(ErrorCode::ETCD_OPERATION_ERROR);
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::OK, storage.ClaimProducerView(8));
    EXPECT_EQ(ErrorCode::OK, storage.ValidateProducerView(8));
}

TEST(OpLogBatchStorageTest, ClaimProducerViewRejectsZeroAndNonTxnBackend) {
    FakeHaKvBackend backend;
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::INVALID_PARAMS, storage.ClaimProducerView(0));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, storage.ValidateProducerView(0));
    backend.SetSupportsTxn(false);
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, storage.ClaimProducerView(1));
}

TEST(OpLogBatchStorageTest, RejectsMalformedProducerView) {
    for (const std::string value :
         {"", "0", "-1", "07", "7x", " 7", "8.0", "9223372036854775808"}) {
        SCOPED_TRACE(value);
        FakeHaKvBackend backend;
        ASSERT_EQ(ErrorCode::OK,
                  backend.Put("/oplog/clusterA/producer_view", value));
        OpLogBatchStorage storage("clusterA", backend);

        EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.ClaimProducerView(8));
        EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.ValidateProducerView(8));
    }
}

TEST(OpLogBatchStorageTest, RejectsLegacyLatest) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/latest", "42"));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_NE(ErrorCode::OK, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, RejectsLegacyEntry) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/00000000000000000042", "entry"));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_NE(ErrorCode::OK, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, RejectsLegacySnapshotSidecar) {
    FakeHaKvBackend backend;
    for (const std::string_view key :
         {"compaction_floor", "fallback", "latest", "maintenance"}) {
        ASSERT_EQ(ErrorCode::OK,
                  backend.Put("/oplog/clusterA/snapshot/" + std::string(key),
                              "control"));
    }
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/snapshot/old/sequence_id", "42"));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_NE(ErrorCode::OK, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, AllowsBatchSnapshotControlKeys) {
    FakeHaKvBackend backend;
    for (const std::string_view key :
         {"compaction_floor", "fallback", "latest", "maintenance"}) {
        ASSERT_EQ(ErrorCode::OK,
                  backend.Put("/oplog/clusterA/snapshot/" + std::string(key),
                              "control"));
    }
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::OK, storage.InitDurablePrefix(prefix));
    EXPECT_EQ((DurablePrefix{.batch_id = 0, .last_seq = 0}), prefix);
}

TEST(OpLogBatchStorageTest, InitDurablePrefixFailsClosedWhenBatchesExist) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000001",
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, RejectsInvalidClusterId) {
    FakeHaKvBackend backend;
    OpLogBatchStorage storage("bad/cluster", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, storage.InitDurablePrefix(prefix));
    ViewVersionId producer_view = 0;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              storage.ReadProducerView(producer_view));
}

TEST(OpLogBatchStorageTest, RereadsDurablePrefixWhenCreateIfAbsentLosesRace) {
    FakeHaKvBackend backend;
    backend.CreateBeforeNextTxn(
        "/oplog/clusterA/durable_prefix",
        EncodeDurablePrefix({.batch_id = 0, .last_seq = 0}));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::OK, storage.InitDurablePrefix(prefix));

    EXPECT_EQ(0u, prefix.batch_id);
    EXPECT_EQ(0u, prefix.last_seq);
}

TEST(OpLogBatchStorageTest, ValidatesExistingDurablePrefix) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000003",
                          EncodeOpLogBatchRecord(MakeBatch(3, 7, 3))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 3, .last_seq = 9})));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::OK, storage.InitDurablePrefix(prefix));

    EXPECT_EQ(3u, prefix.batch_id);
    EXPECT_EQ(9u, prefix.last_seq);
}

TEST(OpLogBatchStorageTest, RejectsZeroPrefixWithExistingBatch) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 0, .last_seq = 0})));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000001",
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, RejectsInconsistentDurablePrefixZeroFields) {
    for (const DurablePrefix stored :
         {DurablePrefix{.batch_id = 0, .last_seq = 9},
          DurablePrefix{.batch_id = 3, .last_seq = 0}}) {
        SCOPED_TRACE("batch_id=" + std::to_string(stored.batch_id) +
                     " last_seq=" + std::to_string(stored.last_seq));
        FakeHaKvBackend backend;
        ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/durable_prefix",
                                             EncodeDurablePrefix(stored)));
        OpLogBatchStorage storage("clusterA", backend);

        DurablePrefix prefix;
        EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.InitDurablePrefix(prefix));
    }
}

TEST(OpLogBatchStorageTest, RejectsPrefixWithoutTerminalBatch) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 3, .last_seq = 9})));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, RejectsTerminalBatchIdMismatch) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000003",
                          EncodeOpLogBatchRecord(MakeBatch(2, 7, 3))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 3, .last_seq = 9})));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, RejectsTerminalBatchLastSequenceMismatch) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000003",
                          EncodeOpLogBatchRecord(MakeBatch(3, 7, 3))));
    ASSERT_EQ(
        ErrorCode::OK,
        backend.Put("/oplog/clusterA/durable_prefix",
                    EncodeDurablePrefix({.batch_id = 3, .last_seq = 10})));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, RejectsCorruptedTerminalBatch) {
    FakeHaKvBackend backend;
    std::string encoded = EncodeOpLogBatchRecord(MakeBatch(1, 1, 1));
    const auto pos =
        encoded.find_first_of("0123456789", encoded.rfind(',') + 1);
    ASSERT_NE(std::string::npos, pos);
    encoded[pos] = encoded[pos] == '0' ? '1' : '0';
    ASSERT_EQ(
        ErrorCode::OK,
        backend.Put("/oplog/clusterA/batches/00000000000000000001", encoded));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 1})));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, RejectsInvalidTerminalBatchSequenceRange) {
    FakeHaKvBackend backend;
    auto batch = MakeBatch(1, 1, 1);
    batch.last_seq = 2;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000001",
                          EncodeOpLogBatchRecord(batch)));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 2})));
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageTest, WriteBatchAndAdvancePrefixCommitsAtomically) {
    FakeHaKvBackend backend;
    const auto old_prefix = EncodeDurablePrefix({.batch_id = 1, .last_seq = 3});
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix", old_prefix));
    OpLogBatchStorage storage("clusterA", backend);

    auto batch = MakeBatch(/*batch_id=*/2, /*first_seq=*/4, /*count=*/2);
    EXPECT_EQ(ErrorCode::OK, storage.WriteBatchAndAdvancePrefix(
                                 batch, {.batch_id = 1, .last_seq = 3}));

    OpLogBatchRecord stored_batch;
    ASSERT_EQ(ErrorCode::OK, storage.ReadBatch(2, stored_batch));
    EXPECT_EQ(2u, stored_batch.batch_id);
    EXPECT_EQ(4u, stored_batch.first_seq);
    EXPECT_EQ(5u, stored_batch.last_seq);

    std::string encoded_prefix;
    ASSERT_EQ(ErrorCode::OK,
              backend.Get("/oplog/clusterA/durable_prefix", encoded_prefix));
    DurablePrefix prefix;
    ASSERT_TRUE(DecodeDurablePrefix(encoded_prefix, &prefix));
    EXPECT_EQ(2u, prefix.batch_id);
    EXPECT_EQ(5u, prefix.last_seq);
}

TEST(OpLogBatchStorageTest, FencedAdvanceDoesNotModifyProducerView) {
    FakeHaKvBackend backend;
    const DurablePrefix expected_prefix{.batch_id = 1, .last_seq = 3};
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/durable_prefix",
                                         EncodeDurablePrefix(expected_prefix)));
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/producer_view", "7"));
    OpLogBatchStorage storage("clusterA", backend);

    auto batch = MakeBatch(/*batch_id=*/2, /*first_seq=*/4, /*count=*/2);
    ASSERT_EQ(ErrorCode::OK,
              storage.WriteBatchAndAdvancePrefix(batch, expected_prefix, 7));

    std::string encoded_prefix;
    ASSERT_EQ(ErrorCode::OK,
              backend.Get("/oplog/clusterA/durable_prefix", encoded_prefix));
    DurablePrefix prefix;
    ASSERT_TRUE(DecodeDurablePrefix(encoded_prefix, &prefix));
    EXPECT_EQ(2u, prefix.batch_id);
    EXPECT_EQ(5u, prefix.last_seq);
    std::string producer_view;
    ASSERT_EQ(ErrorCode::OK,
              backend.Get("/oplog/clusterA/producer_view", producer_view));
    EXPECT_EQ("7", producer_view);
}

TEST(OpLogBatchStorageTest, FencedAdvanceResolvesCommittedTxnError) {
    FakeHaKvBackend backend;
    const DurablePrefix expected_prefix{.batch_id = 1, .last_seq = 3};
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/durable_prefix",
                                         EncodeDurablePrefix(expected_prefix)));
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/producer_view", "7"));
    backend.FailNextCommittedTxn(ErrorCode::ETCD_OPERATION_ERROR);
    OpLogBatchStorage storage("clusterA", backend);

    auto batch = MakeBatch(/*batch_id=*/2, /*first_seq=*/4, /*count=*/2);
    EXPECT_EQ(ErrorCode::OK,
              storage.WriteBatchAndAdvancePrefix(batch, expected_prefix, 7));

    OpLogBatchRecord stored_batch;
    EXPECT_EQ(ErrorCode::OK, storage.ReadBatch(2, stored_batch));
    EXPECT_EQ(EncodeOpLogBatchRecord(batch),
              EncodeOpLogBatchRecord(stored_batch));
}

TEST(OpLogBatchStorageTest, HigherProducerViewFencesStaleWriter) {
    FakeHaKvBackend backend;
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK, storage.ClaimProducerView(7));
    ASSERT_EQ(ErrorCode::OK, storage.ClaimProducerView(8));

    auto batch = MakeBatch(/*batch_id=*/1, /*first_seq=*/1, /*count=*/1);
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              storage.WriteBatchAndAdvancePrefix(batch, prefix, 7));
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, storage.ReadBatch(1, batch));
    EXPECT_EQ(ErrorCode::OK,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/1, /*first_seq=*/1, /*count=*/1),
                  prefix, 8));
}

TEST(OpLogBatchStorageTest, FencedAdvanceRequiresClaimedViewKey) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 0, .last_seq = 0})));
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/1, /*first_seq=*/1, /*count=*/1),
                  {.batch_id = 0, .last_seq = 0}, 7));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/1, /*first_seq=*/1, /*count=*/1),
                  {.batch_id = 0, .last_seq = 0}, 0));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/producer_view", "invalid"));
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/1, /*first_seq=*/1, /*count=*/1),
                  {.batch_id = 0, .last_seq = 0}, 7));
}

TEST(OpLogBatchStorageTest, CompareFailureDoesNotWriteBatchOrAdvancePrefix) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 6})));
    OpLogBatchStorage storage("clusterA", backend);

    auto batch = MakeBatch(/*batch_id=*/2, /*first_seq=*/4, /*count=*/1);
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              storage.WriteBatchAndAdvancePrefix(
                  batch, {.batch_id = 1, .last_seq = 3}));

    OpLogBatchRecord missing;
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, storage.ReadBatch(2, missing));
    std::string encoded_prefix;
    ASSERT_EQ(ErrorCode::OK,
              backend.Get("/oplog/clusterA/durable_prefix", encoded_prefix));
    DurablePrefix prefix;
    ASSERT_TRUE(DecodeDurablePrefix(encoded_prefix, &prefix));
    EXPECT_EQ(2u, prefix.batch_id);
    EXPECT_EQ(6u, prefix.last_seq);
}

TEST(OpLogBatchStorageTest, CompareFailureIsOkWhenTargetBatchAlreadyDurable) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 5})));
    auto batch = MakeBatch(/*batch_id=*/2, /*first_seq=*/4, /*count=*/2);
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000002",
                          EncodeOpLogBatchRecord(batch)));
    backend.FailNextTxn(ErrorCode::ETCD_TRANSACTION_FAIL);
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::OK, storage.WriteBatchAndAdvancePrefix(
                                 batch, {.batch_id = 1, .last_seq = 3}));
}

TEST(OpLogBatchStorageTest, FencedBatchIsNotIdempotentSuccess) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 5})));
    ASSERT_EQ(ErrorCode::OK, backend.Put("/oplog/clusterA/producer_view", "8"));
    auto batch = MakeBatch(/*batch_id=*/2, /*first_seq=*/4, /*count=*/2);
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000002",
                          EncodeOpLogBatchRecord(batch)));
    backend.FailNextTxn(ErrorCode::ETCD_TRANSACTION_FAIL);
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              storage.WriteBatchAndAdvancePrefix(
                  batch, {.batch_id = 1, .last_seq = 3}, 7));
}

TEST(OpLogBatchStorageTest, RejectsSkippedBatchId) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 3})));
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/3, /*first_seq=*/4, /*count=*/1),
                  {.batch_id = 1, .last_seq = 3}));
}

TEST(OpLogBatchStorageTest, RejectsSkippedSequenceRange) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 3})));
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/2, /*first_seq=*/5, /*count=*/1),
                  {.batch_id = 1, .last_seq = 3}));
}

TEST(OpLogBatchStorageTest, RejectsRegressedSequenceRange) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 3})));
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/2, /*first_seq=*/3, /*count=*/1),
                  {.batch_id = 1, .last_seq = 3}));
}

TEST(OpLogBatchStorageTest, RejectsMaxDurablePrefixAdvance) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = UINT64_MAX,
                                               .last_seq = UINT64_MAX})));
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/0, /*first_seq=*/1, /*count=*/1),
                  {.batch_id = UINT64_MAX, .last_seq = UINT64_MAX}));
}

TEST(OpLogBatchStorageTest, NonTxnBackendRejectsWriteBatchAndAdvancePrefix) {
    FakeHaKvBackend backend;
    backend.SetSupportsTxn(false);
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/1, /*first_seq=*/1, /*count=*/1),
                  {.batch_id = 0, .last_seq = 0}));
}

TEST(OpLogBatchStorageTest, ReadBatchRejectsCorruptedRecord) {
    FakeHaKvBackend backend;
    auto batch = MakeBatch(/*batch_id=*/1, /*first_seq=*/1, /*count=*/1);
    std::string encoded = EncodeOpLogBatchRecord(batch);
    auto pos = encoded.find_first_of("0123456789", encoded.rfind(',') + 1);
    ASSERT_NE(std::string::npos, pos);
    encoded[pos] = encoded[pos] == '0' ? '1' : '0';
    ASSERT_EQ(
        ErrorCode::OK,
        backend.Put("/oplog/clusterA/batches/00000000000000000001", encoded));
    OpLogBatchStorage storage("clusterA", backend);

    OpLogBatchRecord out;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.ReadBatch(1, out));
}

TEST(OpLogBatchStorageTest, ReadBatchRejectsMismatchedPayloadBatchId) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000001",
                          EncodeOpLogBatchRecord(MakeBatch(2, 1, 1))));
    OpLogBatchStorage storage("clusterA", backend);

    OpLogBatchRecord out;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, storage.ReadBatch(1, out));
}

TEST(OpLogBatchStorageTest, ReadBatchesAfterReturnsOrderedBatches) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000002",
                          EncodeOpLogBatchRecord(MakeBatch(2, 4, 2))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000001",
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 3))));
    OpLogBatchStorage storage("clusterA", backend);

    std::vector<OpLogBatchRecord> batches;
    EXPECT_EQ(ErrorCode::OK, storage.ReadBatchesAfter(/*after_batch_id=*/0,
                                                      /*limit=*/10, batches));

    ASSERT_EQ(2u, batches.size());
    EXPECT_EQ(1u, batches[0].batch_id);
    EXPECT_EQ(2u, batches[1].batch_id);
}

TEST(OpLogBatchStorageTest, ReadBatchesAfterSkipsNonBatchKeysInRange) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000001",
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/sidecar", "not a batch"));
    OpLogBatchStorage storage("clusterA", backend);

    std::vector<OpLogBatchRecord> batches;
    EXPECT_EQ(ErrorCode::OK, storage.ReadBatchesAfter(/*after_batch_id=*/0,
                                                      /*limit=*/10, batches));

    ASSERT_EQ(1u, batches.size());
    EXPECT_EQ(1u, batches[0].batch_id);
}

TEST(OpLogBatchStorageTest, ReadBatchesAfterLimitCountsOnlyBatchKeys) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000001",
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000002",
                          EncodeOpLogBatchRecord(MakeBatch(2, 2, 1))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000001.meta",
                          "sidecar"));
    OpLogBatchStorage storage("clusterA", backend);

    std::vector<OpLogBatchRecord> batches;
    EXPECT_EQ(ErrorCode::OK, storage.ReadBatchesAfter(/*after_batch_id=*/0,
                                                      /*limit=*/2, batches));

    ASSERT_EQ(2u, batches.size());
    EXPECT_EQ(1u, batches[0].batch_id);
    EXPECT_EQ(2u, batches[1].batch_id);
    ASSERT_EQ(2u, backend.range_limits().size());
    EXPECT_EQ(2u, backend.range_limits()[0]);
    EXPECT_EQ(1u, backend.range_limits()[1]);
}

TEST(OpLogBatchStorageTest, ReadBatchesAfterHonorsLimit) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000001",
                          EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/batches/00000000000000000002",
                          EncodeOpLogBatchRecord(MakeBatch(2, 2, 1))));
    OpLogBatchStorage storage("clusterA", backend);

    std::vector<OpLogBatchRecord> batches;
    EXPECT_EQ(ErrorCode::OK, storage.ReadBatchesAfter(/*after_batch_id=*/0,
                                                      /*limit=*/1, batches));

    ASSERT_EQ(1u, batches.size());
    EXPECT_EQ(1u, batches[0].batch_id);
}

TEST(OpLogBatchStorageBackendErrorTest, PropagatesReadDurablePrefixError) {
    FakeHaKvBackend backend;
    backend.FailNextGet(ErrorCode::ETCD_OPERATION_ERROR);
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR,
              storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageBackendErrorTest,
     PropagatesZeroPrefixValidationRangeError) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 0, .last_seq = 0})));
    backend.FailNextRange(ErrorCode::ETCD_OPERATION_ERROR);
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR,
              storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageBackendErrorTest,
     PropagatesTerminalBatchReadErrorAtStartup) {
    FakeHaKvBackend backend;
    const std::string batch_key =
        "/oplog/clusterA/batches/00000000000000000001";
    ASSERT_EQ(
        ErrorCode::OK,
        backend.Put(batch_key, EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 1})));
    backend.FailNextGetForKey(batch_key, ErrorCode::ETCD_OPERATION_ERROR);
    OpLogBatchStorage storage("clusterA", backend);

    DurablePrefix prefix;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR,
              storage.InitDurablePrefix(prefix));
}

TEST(OpLogBatchStorageBackendErrorTest, PropagatesRangeError) {
    FakeHaKvBackend backend;
    backend.FailNextRange(ErrorCode::ETCD_OPERATION_ERROR);
    OpLogBatchStorage storage("clusterA", backend);

    std::vector<OpLogBatchRecord> batches;
    EXPECT_EQ(
        ErrorCode::ETCD_OPERATION_ERROR,
        storage.ReadBatchesAfter(/*after_batch_id=*/0, /*limit=*/1, batches));
    EXPECT_TRUE(batches.empty());
}

TEST(OpLogBatchStorageBackendErrorTest, TxnErrorDoesNotAdvanceDurablePrefix) {
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/durable_prefix",
                          EncodeDurablePrefix({.batch_id = 1, .last_seq = 3})));
    backend.FailNextTxn(ErrorCode::ETCD_OPERATION_ERROR);
    OpLogBatchStorage storage("clusterA", backend);

    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR,
              storage.WriteBatchAndAdvancePrefix(
                  MakeBatch(/*batch_id=*/2, /*first_seq=*/4, /*count=*/1),
                  {.batch_id = 1, .last_seq = 3}));

    std::string encoded_prefix;
    ASSERT_EQ(ErrorCode::OK,
              backend.Get("/oplog/clusterA/durable_prefix", encoded_prefix));
    DurablePrefix prefix;
    ASSERT_TRUE(DecodeDurablePrefix(encoded_prefix, &prefix));
    EXPECT_EQ(1u, prefix.batch_id);
    EXPECT_EQ(3u, prefix.last_seq);
}

TEST(OpLogBatchStorageDeleteTest, InclusiveBoundariesAndProtectedKeys) {
    FakeHaKvBackend backend;
    OpLogBatchStorage storage("clusterA", backend);
    const std::vector<uint64_t> ids{0, 1, 2, 9, 10, UINT64_MAX - 1, UINT64_MAX};
    const std::vector<std::string> protected_keys{
        BuildDurablePrefixKey("clusterA"),
        BuildProducerViewKey("clusterA"),
        ha::BuildBatchOpLogSnapshotLatestKey("clusterA"),
        ha::BuildBatchOpLogSnapshotFallbackKey("clusterA"),
        ha::BuildBatchOpLogSnapshotCompactionFloorKey("clusterA"),
        ha::BuildBatchOpLogSnapshotMaintenanceKey("clusterA"),
        BuildBatchRecordKey("clusterAB", 1),
        BuildBatchRecordKey("clusterB", 0)};
    for (const auto& key : protected_keys) {
        ASSERT_EQ(ErrorCode::OK, backend.Put(key, "protected"));
    }
    for (auto id : ids) {
        ASSERT_EQ(ErrorCode::OK,
                  backend.Put(BuildBatchRecordKey("clusterA", id), "record"));
    }
    for (auto cutoff : ids) {
        ASSERT_EQ(ErrorCode::OK, storage.DeleteBatchesThrough(cutoff));
        ASSERT_EQ(ErrorCode::OK, storage.DeleteBatchesThrough(cutoff));
        std::string value;
        for (auto id : ids) {
            EXPECT_EQ(
                id <= cutoff ? ErrorCode::ETCD_KEY_NOT_EXIST : ErrorCode::OK,
                backend.Get(BuildBatchRecordKey("clusterA", id), value));
        }
        for (const auto& key : protected_keys) {
            ASSERT_EQ(ErrorCode::OK, backend.Get(key, value)) << key;
            EXPECT_EQ("protected", value);
        }
    }
}

TEST(OpLogBatchStorageDeleteTest, EmptyNamespaceAndInvalidCluster) {
    FakeHaKvBackend backend;
    OpLogBatchStorage storage("clusterA", backend);
    EXPECT_EQ(ErrorCode::OK, storage.DeleteBatchesThrough(0));
    EXPECT_EQ(ErrorCode::OK, storage.DeleteBatchesThrough(UINT64_MAX));
    backend.delete_ranges.clear();
    for (const auto* cluster : {"", "/", "a/b"}) {
        OpLogBatchStorage invalid(cluster, backend);
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, invalid.DeleteBatchesThrough(1));
    }
    EXPECT_TRUE(backend.delete_ranges.empty());
}

TEST(OpLogBatchStorageDeleteTest, AppendInterleavedBeforeDeleteSurvives) {
    FakeHaKvBackend backend;
    OpLogBatchStorage storage("clusterA", backend);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK,
              storage.WriteBatchAndAdvancePrefix(MakeBatch(1, 1, 1), prefix));
    backend.before_delete = [&] {
        EXPECT_EQ(ErrorCode::OK,
                  storage.WriteBatchAndAdvancePrefix(
                      MakeBatch(2, 2, 1), {.batch_id = 1, .last_seq = 1}));
    };
    ASSERT_EQ(ErrorCode::OK, storage.DeleteBatchesThrough(1));
    OpLogBatchRecord record;
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, storage.ReadBatch(1, record));
    EXPECT_EQ(ErrorCode::OK, storage.ReadBatch(2, record));
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    EXPECT_EQ(2u, prefix.batch_id);
}

TEST(OpLogBatchStorageDeleteTest, FailureDoesNotRetryOrExpandRange) {
    // Before mutation, partial deletion, and committed-but-response-lost.
    for (size_t fail_after : {0u, 1u, 3u}) {
        FakeHaKvBackend backend;
        OpLogBatchStorage storage("clusterA", backend);
        for (uint64_t id = 1; id <= 4; ++id) {
            ASSERT_EQ(
                ErrorCode::OK,
                backend.Put(BuildBatchRecordKey("clusterA", id), "record"));
        }
        backend.delete_fail_after = fail_after;
        backend.delete_result = ErrorCode::ETCD_OPERATION_ERROR;
        EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR,
                  storage.DeleteBatchesThrough(3));
        ASSERT_EQ(1u, backend.delete_ranges.size());
        const auto attempted = backend.delete_ranges.front();
        EXPECT_EQ(BuildBatchRecordKey("clusterA", 0), attempted.begin_key);
        EXPECT_EQ(BuildBatchRecordKey("clusterA", 4), attempted.end_key);
        std::string value;
        for (uint64_t id = 1; id <= 4; ++id) {
            EXPECT_EQ(id <= fail_after ? ErrorCode::ETCD_KEY_NOT_EXIST
                                       : ErrorCode::OK,
                      backend.Get(BuildBatchRecordKey("clusterA", id), value));
        }
        backend.delete_fail_after = std::numeric_limits<size_t>::max();
        backend.delete_result = ErrorCode::OK;
        EXPECT_EQ(ErrorCode::OK, storage.DeleteBatchesThrough(3));
        ASSERT_EQ(2u, backend.delete_ranges.size());
        EXPECT_EQ(attempted.begin_key, backend.delete_ranges.back().begin_key);
        EXPECT_EQ(attempted.end_key, backend.delete_ranges.back().end_key);
        EXPECT_EQ(ErrorCode::OK,
                  backend.Get(BuildBatchRecordKey("clusterA", 4), value));
    }
}

TEST(EtcdHaKvBackendDeleteTest, RejectsUnboundedAndReversedRanges) {
    EtcdHaKvBackend backend;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, backend.DeleteRange("", "z"));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, backend.DeleteRange("a", ""));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              backend.DeleteRange("a", std::string_view("\0", 1)));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, backend.DeleteRange("z", "a"));
    EXPECT_EQ(ErrorCode::OK, backend.DeleteRange("a", "a"));
}

#ifdef STORE_USE_ETCD
TEST(EtcdHaKvBackendDeleteTest, RealEtcdDeleteWhileAppending) {
    const char* endpoints = std::getenv("MOONCAKE_N10_TEST_ETCD_ENDPOINTS");
    if (endpoints == nullptr) {
        GTEST_SKIP()
            << "Set MOONCAKE_N10_TEST_ETCD_ENDPOINTS for isolated etcd";
    }
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::ConnectToEtcdStoreClient(endpoints));
    EtcdHaKvBackend backend;
    const std::string cluster = "n10-delete-" + std::to_string(getpid());
    OpLogBatchStorage storage(cluster, backend);
    DurablePrefix prefix;
    ASSERT_EQ(ErrorCode::OK, storage.InitDurablePrefix(prefix));
    ASSERT_EQ(ErrorCode::OK, storage.ClaimProducerView(1));
    ASSERT_EQ(ErrorCode::OK, storage.WriteBatchAndAdvancePrefix(
                                 MakeBatch(1, 1, 1), prefix, 1));
    const std::vector<std::string> protected_keys{
        ha::BuildBatchOpLogSnapshotLatestKey(cluster),
        ha::BuildBatchOpLogSnapshotFallbackKey(cluster),
        ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster),
        ha::BuildBatchOpLogSnapshotCompactionFloorKey(cluster),
        BuildBatchRecordKey(cluster + "-other", 1)};
    for (const auto& key : protected_keys) {
        ASSERT_EQ(ErrorCode::OK, backend.Put(key, "protected"));
    }
    std::thread append([&] {
        for (uint64_t id = 2; id <= 16; ++id) {
            EXPECT_EQ(ErrorCode::OK,
                      storage.WriteBatchAndAdvancePrefix(
                          MakeBatch(id, id, 1),
                          {.batch_id = id - 1, .last_seq = id - 1}, 1));
        }
    });
    for (int i = 0; i < 16; ++i) {
        EXPECT_EQ(ErrorCode::OK, storage.DeleteBatchesThrough(1));
    }
    append.join();
    OpLogBatchRecord record;
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, storage.ReadBatch(1, record));
    for (uint64_t id = 2; id <= 16; ++id) {
        EXPECT_EQ(ErrorCode::OK, storage.ReadBatch(id, record));
    }
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    EXPECT_EQ(16u, prefix.batch_id);
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey(cluster, UINT64_MAX), "record"));
    EXPECT_EQ(ErrorCode::OK, storage.DeleteBatchesThrough(UINT64_MAX));
    EXPECT_EQ(ErrorCode::OK, storage.DeleteBatchesThrough(UINT64_MAX));
    std::string value;
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
              backend.Get(BuildBatchRecordKey(cluster, UINT64_MAX), value));
    EXPECT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(prefix));
    ViewVersionId view = 0;
    EXPECT_EQ(ErrorCode::OK, storage.ReadProducerView(view));
    EXPECT_EQ(1, view);
    for (const auto& key : protected_keys) {
        EXPECT_EQ(ErrorCode::OK, backend.Get(key, value));
        EXPECT_EQ("protected", value);
    }
}
#endif

}  // namespace mooncake::test
