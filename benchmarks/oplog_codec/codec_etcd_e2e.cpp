#include "candidate_codecs.h"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>

#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/ordered_oplog_writer.h"

using namespace mooncake;
using namespace mooncake::codec_bench;
using namespace std::chrono_literals;

namespace {
void Require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

// Test-only bridge: keep production storage transactions/fencing unchanged,
// but persist candidate bytes inside this fixture's isolated batch namespace.
// Its JSON<->candidate transcoding overhead is NOT a performance measurement.
class CodecBackend : public HaKvBackend {
   public:
    CodecBackend(EtcdHaKvBackend& raw, Format format,
                 const std::string& cluster)
        : raw_(raw),
          format_(format),
          prefix_("/oplog/" + cluster + "/batches/") {}
    ErrorCode Get(std::string_view key, std::string& value) override {
        auto result = raw_.Get(key, value);
        return result == ErrorCode::OK ? FromWire(key, value) : result;
    }
    ErrorCode Put(std::string_view key, std::string_view value) override {
        std::string encoded(value);
        auto result = ToWire(key, encoded);
        return result == ErrorCode::OK ? raw_.Put(key, encoded) : result;
    }
    ErrorCode Range(std::string_view begin, std::string_view end, size_t limit,
                    std::vector<KvPair>& kvs) override {
        auto result = raw_.Range(begin, end, limit, kvs);
        if (result != ErrorCode::OK) return result;
        for (auto& pair : kvs) {
            result = FromWire(pair.key, pair.value);
            if (result != ErrorCode::OK) return result;
        }
        return ErrorCode::OK;
    }
    ErrorCode DeleteRange(std::string_view begin,
                          std::string_view end) override {
        return raw_.DeleteRange(begin, end);
    }
    bool SupportsTxn() const override { return raw_.SupportsTxn(); }
    ErrorCode Txn(const KvTxn& txn) override {
        auto converted = txn;
        for (auto& compare : converted.compares) {
            if (compare.kind == KvCompareKind::kValueEquals) {
                auto result = ToWire(compare.key, compare.expected_value);
                if (result != ErrorCode::OK) return result;
            }
        }
        for (auto& put : converted.puts) {
            auto result = ToWire(put.key, put.value);
            if (result != ErrorCode::OK) return result;
        }
        return raw_.Txn(converted);
    }

   private:
    ErrorCode ToWire(std::string_view key, std::string& value) {
        if (!key.starts_with(prefix_) || format_ == Format::Json)
            return ErrorCode::OK;
        OpLogBatchRecord batch;
        if (!DecodeOpLogBatchRecord(value, &batch))
            return ErrorCode::INVALID_PARAMS;
        value = Encode(format_, batch);
        return ErrorCode::OK;
    }
    ErrorCode FromWire(std::string_view key, std::string& value) {
        if (!key.starts_with(prefix_) || format_ == Format::Json)
            return ErrorCode::OK;
        OpLogBatchRecord batch;
        if (!Decode(format_, value, &batch)) return ErrorCode::INVALID_PARAMS;
        value = EncodeOpLogBatchRecord(batch);
        return ErrorCode::OK;
    }
    EtcdHaKvBackend& raw_;
    Format format_;
    std::string prefix_;
};

ErrorCode Result(async_simple::Future<ErrorCode> future) {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!future.hasResult() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    Require(future.hasResult(), "asynchronous durable future timed out");
    return std::move(future).get();
}

uint64_t Commit(OrderedOpLogWriter& writer, OpLogEntry entry) {
    auto reservation = writer.Reserve();
    Require(reservation.has_value(), "reserve failed");
    auto pending = writer.Commit(std::move(*reservation), std::move(entry), {});
    Require(pending.has_value(), "commit failed");
    return pending->sequence_id();
}

struct Gate {
    std::mutex mutex;
    std::condition_variable cv;
    bool released = false;
    bool entered = false;
    void Block() {
        std::unique_lock lock(mutex);
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return released; });
    }
    bool Entered() {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, 5s, [&] { return entered; });
    }
    void Release() {
        {
            std::lock_guard lock(mutex);
            released = true;
        }
        cv.notify_all();
    }
};

void Run(Format format, const std::string& phase) {
    const char* endpoint = std::getenv("P01_ETCD_ENDPOINT");
    Require(endpoint != nullptr, "missing P01_ETCD_ENDPOINT");
    Require(EtcdHelper::ConnectToEtcdStoreClient(endpoint) == ErrorCode::OK,
            "etcd connect");
    const std::string cluster = "p01-w02-" + std::string(Name(format));
    EtcdHaKvBackend raw;
    CodecBackend bridge(raw, format, cluster);
    OpLogBatchStorage storage(cluster, bridge);
    DurablePrefix prefix;
    Require(storage.InitDurablePrefix(prefix) == ErrorCode::OK,
            "prefix bootstrap");
    const auto expected = ReplayWorkload().batch;
    if (phase == "persist") {
        Require(prefix.last_seq == 0, "fixture was not empty");
        Require(storage.ClaimProducerView(1) == ErrorCode::OK,
                "claim producer");
        Gate gate;
        OrderedOpLogWriter writer(
            {.max_entries_per_batch = expected.entries.size()},
            [&](const auto& batch, const auto& prior) {
                gate.Block();
                return storage.WriteBatchAndAdvancePrefix(batch, prior, 1);
            });
        // Unblock before the writer destructor even when a check throws.
        struct Release {
            Gate& gate;
            ~Release() { gate.Release(); }
        } release{gate};
        uint64_t last = 0;
        for (const auto& entry : expected.entries) last = Commit(writer, entry);
        // Latest W02 API registers on the calling thread and returns pending;
        // no std::async/thread-pool wrapper around AwaitDurable is used.
        auto pending = writer.AwaitDurable(last);
        Require(!pending.hasResult(), "future completed before storage");
        auto reentrant = std::move(pending).thenValue([&](ErrorCode error) {
            writer.IsAccepting();
            writer.LastError();
            Require(writer.AwaitDurable(last).hasResult(),
                    "reentrant covered wait");
            return error;
        });
        writer.Start();
        Require(gate.Entered(), "storage gate not entered");
        Require(!reentrant.hasResult(), "future completed before transaction");
        Require(storage.ReadDurablePrefix(prefix) == ErrorCode::OK &&
                    prefix.last_seq == 0,
                "prefix became visible before transaction");
        gate.Release();
        Require(Result(std::move(reentrant)) == ErrorCode::OK, "durable await");
        writer.Stop();
    } else {
        Require(prefix.last_seq == expected.last_seq,
                "restart lost durable prefix");
        Require(storage.ClaimProducerView(2) == ErrorCode::OK,
                "restore producer claim");
        OrderedOpLogWriter stale({.initial_durable_prefix = prefix},
                                 [&](const auto& batch, const auto& prior) {
                                     return storage.WriteBatchAndAdvancePrefix(
                                         batch, prior, 2);
                                 });
        auto restored = stale.AwaitDurable(prefix.last_seq);
        Require(restored.hasResult(), "restored-prefix future was not ready");
        Require(Result(std::move(restored)) == ErrorCode::OK,
                "restored prefix did not return ready success");
        Require(storage.ClaimProducerView(3) == ErrorCode::OK,
                "new producer claim");
        const auto seq = Commit(stale, expected.entries.front());
        auto pending = stale.AwaitDurable(seq);
        stale.Start();
        Require(Result(std::move(pending)) == ErrorCode::ETCD_TRANSACTION_FAIL,
                "stale writer was not fenced");
        stale.Stop();
        auto terminal = stale.GetTerminalState();
        Require(terminal && terminal->reason ==
                                OrderedOpLogWriterTerminalReason::kFenced,
                "wrong terminal reason");
        DurablePrefix after;
        Require(storage.ReadDurablePrefix(after) == ErrorCode::OK &&
                    after == prefix,
                "fenced transaction advanced prefix");
        std::string missing;
        Require(raw.Get(BuildBatchRecordKey(cluster, prefix.batch_id + 1),
                        missing) == ErrorCode::ETCD_KEY_NOT_EXIST,
                "fenced batch was persisted");
    }
    Require(storage.ReadDurablePrefix(prefix) == ErrorCode::OK,
            "prefix readback");
    // Commit seals the first idle batch immediately. Audit all actual batch
    // boundaries instead of assuming commits before Start form one batch.
    OpLogBatchRecord reconstructed = expected;
    reconstructed.entries.clear();
    std::string wire;
    size_t wire_bytes = 0;
    for (uint64_t id = 1; id <= prefix.batch_id; ++id) {
        OpLogBatchRecord decoded;
        Require(storage.ReadBatch(id, decoded) == ErrorCode::OK,
                "storage batch readback");
        Require(decoded.batch_id == id, "stored batch identity changed");
        Require(
            raw.Get(BuildBatchRecordKey(cluster, id), wire) == ErrorCode::OK,
            "raw batch readback");
        Require(wire == Encode(format, decoded),
                "etcd did not contain candidate bytes");
        OpLogBatchRecord direct;
        Require(Decode(format, wire, &direct) && Equivalent(decoded, direct),
                "raw candidate decode/equality");
        wire_bytes += wire.size();
        reconstructed.entries.insert(reconstructed.entries.end(),
                                     decoded.entries.begin(),
                                     decoded.entries.end());
    }
    Require(ValidateOpLogBatchRecordShape(reconstructed) &&
                Equivalent(expected, reconstructed),
            "complete stored history changed or lost acknowledged data");
    const auto key = BuildBatchRecordKey(cluster, prefix.batch_id);
    VerifyReplay(reconstructed);
    if (phase == "restore-fence") {
        auto corrupt = wire;
        corrupt.back() ^= 1;
        Require(raw.Put(key, corrupt) == ErrorCode::OK,
                "inject fixture corruption");
        OpLogBatchRecord decoded;
        const auto rejected = storage.ReadBatch(prefix.batch_id, decoded);
        Require(raw.Put(key, wire) == ErrorCode::OK, "restore fixture bytes");
        Require(rejected != ErrorCode::OK, "corrupt batch accepted");
    }
    std::cout << "PASS: " << Name(format) << ' ' << phase
              << " entries=" << expected.entries.size()
              << " semantic-replay=passed batches=" << prefix.batch_id
              << " bytes=" << wire_bytes << '\n';
}
}  // namespace

int main(int argc, char** argv) {
    try {
        Require(argc == 3, "usage: binary FORMAT persist|restore-fence");
        const std::string phase(argv[2]);
        Require(phase == "persist" || phase == "restore-fence",
                "invalid phase");
        Run(ParseFormat(argv[1]), phase);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
