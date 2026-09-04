// V1/V2 differential replay (section 9.2).
//
// One deterministic trace is replayed against both implementations and their
// observations compared. What is compared is normalized: error codes, bytes
// read, key visibility, object sizes, which tier by placement rank, and the
// metadata callbacks each step produced. What is deliberately NOT compared:
// UUIDs, BlockIds, addresses, timestamps and the ordering of background work,
// none of which any caller can depend on.
//
// The design's exemption list is encoded here rather than described: a step
// marked Compare::kExempt still runs and still records both answers, and the
// reason travels with it. Deleting such a step instead would lose the evidence
// that the difference is the one we intended.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "p2p/client/data_manager.h"
#include "p2p/client/data_manager_factory.h"
#include "p2p/client/data_manager_test_hook.h"
#include "transfer_engine.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace {

constexpr const char* kTiers = R"({
    "tiers": [
        {
            "type": "DRAM",
            "capacity": 134217728,
            "priority": 100,
            "tags": ["dram"],
            "allocator_type": "OFFSET"
        },
        {
            "type": "STORAGE",
            "capacity": 268435456,
            "priority": 10,
            "tags": ["ssd"]
        }
    ]
})";

/** Deterministic payload: same bytes for the same (key, size) in both runs. */
std::string Payload(const std::string& key, size_t size) {
    std::string out(size, '\0');
    uint64_t state = StringHash{}(key) | 1u;
    for (size_t i = 0; i < size; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        out[i] = static_cast<char>((state >> 33) & 0xff);
    }
    return out;
}

/** Short, stable digest so a mismatch names the content without dumping it. */
std::string Digest(const std::string& bytes) {
    uint64_t hash = 1469598103934665603ULL;
    for (char c : bytes) {
        hash ^= static_cast<unsigned char>(c);
        hash *= 1099511628211ULL;
    }
    return std::to_string(bytes.size()) + ":" + std::to_string(hash);
}

/**
 * @enum Compare
 * @brief Whether a step's answers must match across versions.
 */
enum class Compare {
    kStrict,
    /** A difference the design intends; the reason is recorded with it. */
    kExempt,
};

/**
 * @struct CallbackLog
 * @brief The metadata callbacks a step produced, in a form both versions can
 *        be held to.
 */
struct CallbackLog {
    mutable std::mutex mu;
    std::vector<std::string> entries;

    void Add(std::string entry) {
        std::lock_guard<std::mutex> lock(mu);
        entries.push_back(std::move(entry));
    }
    std::vector<std::string> TakeSorted() {
        std::lock_guard<std::mutex> lock(mu);
        std::vector<std::string> out;
        out.swap(entries);
        // Sorted, not sequenced: two replicas of one step may be reported by
        // different threads, and no caller can depend on which lands first.
        std::sort(out.begin(), out.end());
        return out;
    }
};

/**
 * @class Replay
 * @brief One implementation under one trace, plus the normalization that makes
 *        its answers comparable to the other implementation's.
 */
class Replay {
   public:
    Replay(DataManagerVersion version, const std::string& storage_dir)
        : version_(version) {
        std::filesystem::remove_all(storage_dir);
        std::filesystem::create_directories(storage_dir);
        setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
               "bucket_storage_backend", 1);
        setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", storage_dir.c_str(), 1);

        transfer_engine_ = std::make_shared<TransferEngine>(false);

        Json::Value tier_config;
        Json::CharReaderBuilder builder;
        std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        std::string errs;
        const std::string text(kTiers);
        CHECK(reader->parse(text.data(), text.data() + text.size(),
                            &tier_config, &errs))
            << errs;

        DataManagerConfig config;
        config.version = version;
        config.tier_config = tier_config;
        config.v1_lock_shard_count = 64;
        config.local_transfer.mode = LocalTransferMode::MEMCPY;
        config.local_transfer.local_memcpy_async_worker_num = 0;
        config.local_transfer.te_async_poll_worker_num = 0;
        config.register_tiers_with_transfer_engine = false;
        config.stop_drain_timeout = std::chrono::milliseconds(300);

        MetadataCallbacks callbacks;
        callbacks.add_replica =
            [this](std::string_view key, const UUID& tier_id,
                   size_t size) -> tl::expected<void, ErrorCode> {
            callbacks_.Add("add " + std::string(key) +
                           " tier=" + std::to_string(RankOf(tier_id)) +
                           " size=" + std::to_string(size));
            return {};
        };
        callbacks.remove_replica =
            [this](std::string_view key,
                   const UUID& tier_id) -> tl::expected<void, ErrorCode> {
            callbacks_.Add("remove " + std::string(key) +
                           " tier=" + std::to_string(RankOf(tier_id)));
            return {};
        };
        callbacks.segment_sync = [](const Segment&,
                                    bool) -> tl::expected<void, ErrorCode> {
            return {};
        };
        // Deliberately not logged into the comparable stream: section 7.3
        // makes rectify best-effort and unordered against a concurrent write,
        // so its callbacks are exemption 4.
        callbacks.rectify_route = [this](std::string_view key,
                                         std::optional<UUID>) {
            ++rectify_calls_;
            (void)key;
        };

        auto created = CreateDataManager(config, transfer_engine_,
                                         std::move(callbacks), {});
        CHECK(created.has_value())
            << "CreateDataManager failed: " << toString(created.error());
        manager_ = std::move(created.value());

        // Placement rank, not UUID: both versions build tiers from the same
        // JSON, so "the highest-priority tier" is the comparable identity.
        auto views = manager_->GetTierViews();
        std::sort(views.begin(), views.end(),
                  [](const TierView& lhs, const TierView& rhs) {
                      return lhs.priority > rhs.priority;
                  });
        for (size_t i = 0; i < views.size(); ++i) {
            rank_.emplace(views[i].id, static_cast<int>(i));
        }
    }

    ~Replay() {
        if (manager_) {
            manager_->Stop();
            manager_->Destroy();
        }
    }

    DataManager& manager() { return *manager_; }
    DataManagerVersion version() const { return version_; }

    int RankOf(const UUID& tier_id) const {
        auto it = rank_.find(tier_id);
        return it == rank_.end() ? -1 : it->second;
    }

    /** Tier id at a placement rank, so a step can name a tier portably. */
    UUID TierAtRank(int rank) const {
        for (const auto& [id, r] : rank_) {
            if (r == rank) return id;
        }
        return UUID{0, 0};
    }

    void Drain() {
        if (auto* hook = dynamic_cast<DataManagerTestHook*>(manager_.get())) {
            hook->DrainForTest();
        }
    }

    std::vector<std::string> TakeCallbacks() { return callbacks_.TakeSorted(); }
    uint64_t RectifyCalls() const { return rectify_calls_; }

    /** Tokens minted during the trace, addressed by name rather than value. */
    std::map<std::string, UUID> tokens;
    /** Buffers handed out by PreWrite, so a later step can fill them. */
    std::map<std::string, RemoteBufferDesc> buffers;

   private:
    DataManagerVersion version_;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::unique_ptr<DataManager> manager_;
    std::map<UUID, int> rank_;
    CallbackLog callbacks_;
    std::atomic<uint64_t> rectify_calls_{0};
};

std::string Err(ErrorCode code) { return std::string("err:") + toString(code); }

// --- step helpers ----------------------------------------------------------

std::string DoPut(Replay& replay, const std::string& key, size_t size) {
    const std::string payload = Payload(key, size);
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), payload.size()}};
    auto handle = replay.manager().Put(key, slices);
    if (!handle) return Err(handle.error());
    auto waited = handle.value()->Wait();
    if (!waited) return Err(waited.error());
    return "ok";
}

std::string DoGet(Replay& replay, const std::string& key, size_t size) {
    std::string out(size, '\0');
    std::vector<Slice> slices = {{out.data(), out.size()}};
    auto handle = replay.manager().Get(key, slices);
    if (!handle) return Err(handle.error());
    auto waited = handle.value().task_handle->Wait();
    if (!waited) return Err(waited.error());
    return "ok " + Digest(out);
}

std::string DoQuery(Replay& replay, const std::string& key) {
    auto result = replay.manager().Query(key);
    if (!result) return Err(result.error());
    return "ok tier=" + std::to_string(replay.RankOf(result->first)) +
           " size=" + std::to_string(result->second);
}

std::string DoReplicaTiers(Replay& replay, const std::string& key) {
    auto ids = replay.manager().GetReplicaTierIds(key);
    std::vector<int> ranks;
    ranks.reserve(ids.size());
    for (const auto& id : ids) ranks.push_back(replay.RankOf(id));
    std::sort(ranks.begin(), ranks.end());
    std::string out = "tiers=[";
    for (size_t i = 0; i < ranks.size(); ++i) {
        if (i != 0) out += ",";
        out += std::to_string(ranks[i]);
    }
    return out + "]";
}

std::string DoWalk(Replay& replay) {
    replay.Drain();
    std::vector<std::string> rows;
    replay.manager().ForEachKeyBatch([&](std::vector<ReplicaLocation>&& batch) {
        for (const auto& entry : batch) {
            rows.push_back(entry.key + "@" +
                           std::to_string(replay.RankOf(entry.tier_id)) + ":" +
                           std::to_string(entry.size));
        }
        return true;
    });
    std::sort(rows.begin(), rows.end());
    std::string out = "walk=";
    for (const auto& row : rows) out += row + ";";
    return out;
}

/**
 * @struct Step
 * @brief One operation in the trace.
 */
struct Step {
    std::string name;
    Compare compare = Compare::kStrict;
    std::string exempt_reason;
    std::function<std::string(Replay&)> run;
};

Step Strict(std::string name, std::function<std::string(Replay&)> run) {
    return Step{std::move(name), Compare::kStrict, {}, std::move(run)};
}

Step Exempt(std::string name, std::string reason,
            std::function<std::string(Replay&)> run) {
    return Step{std::move(name), Compare::kExempt, std::move(reason),
                std::move(run)};
}

/**
 * @brief The trace.
 *
 * Sized to stay far below the fast tier's capacity on purpose: what a full
 * tier does is a policy decision the two versions make differently, and it is
 * pinned by its own contract test rather than smuggled in here.
 */
std::vector<Step> BuildTrace() {
    std::vector<Step> steps;
    const size_t kSmall = 4096;
    const size_t kLarge = 256 * 1024;

    // --- writes and reads ---
    for (int i = 0; i < 6; ++i) {
        const std::string key = "diff/obj/" + std::to_string(i);
        const size_t size = (i % 2 == 0) ? kSmall : kLarge;
        steps.push_back(Strict("put " + key, [key, size](Replay& r) {
            return DoPut(r, key, size);
        }));
        steps.push_back(Strict("get " + key, [key, size](Replay& r) {
            return DoGet(r, key, size);
        }));
        steps.push_back(Strict("query " + key,
                               [key](Replay& r) { return DoQuery(r, key); }));
        steps.push_back(Strict("replicas " + key, [key](Replay& r) {
            return DoReplicaTiers(r, key);
        }));
    }

    // --- argument validation ---
    steps.push_back(Strict("put empty key", [](Replay& r) {
        const std::string payload = Payload("x", 16);
        std::vector<Slice> slices = {
            {const_cast<char*>(payload.data()), payload.size()}};
        auto handle = r.manager().Put("", slices);
        if (!handle) return Err(handle.error());
        auto waited = handle.value()->Wait();
        return waited ? std::string("ok") : Err(waited.error());
    }));
    steps.push_back(Strict("put duplicate", [kSmall](Replay& r) {
        return DoPut(r, "diff/obj/0", kSmall);
    }));
    steps.push_back(Strict("get missing", [kSmall](Replay& r) {
        return DoGet(r, "diff/absent", kSmall);
    }));
    steps.push_back(Strict(
        "query missing", [](Replay& r) { return DoQuery(r, "diff/absent"); }));
    steps.push_back(Strict("size missing", [](Replay& r) {
        auto result = r.manager().QueryObjectSize("diff/absent");
        return result ? "ok size=" + std::to_string(*result)
                      : Err(result.error());
    }));
    steps.push_back(Strict("exist missing", [](Replay& r) {
        return r.manager().Exist("diff/absent") ? "true" : "false";
    }));
    steps.push_back(Strict("exist present", [](Replay& r) {
        return r.manager().Exist("diff/obj/1") ? "true" : "false";
    }));
    steps.push_back(Strict("exist on fast tier", [](Replay& r) {
        return r.manager().Exist("diff/obj/1", r.TierAtRank(0)) ? "true"
                                                                : "false";
    }));
    steps.push_back(Strict("exist on slow tier", [](Replay& r) {
        return r.manager().Exist("diff/obj/1", r.TierAtRank(1)) ? "true"
                                                                : "false";
    }));

    // --- the forward write protocol ---
    steps.push_back(Strict("prewrite", [kSmall](Replay& r) {
        auto result = r.manager().PreWrite("diff/forward", kSmall);
        if (!result) return Err(result.error());
        r.tokens["forward"] = result->write_operation_id;
        r.buffers["forward"] = result->remote_buffer;
        // The address and the token are per-run values; only the size is
        // something both versions must agree on.
        return "ok size=" + std::to_string(result->remote_buffer.size);
    }));
    steps.push_back(Strict("prewrite same key again", [kSmall](Replay& r) {
        auto result = r.manager().PreWrite("diff/forward", kSmall);
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("commit wrong token", [](Replay& r) {
        auto result = r.manager().WriteCommit("diff/forward", generate_uuid());
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("commit", [kSmall](Replay& r) {
        auto pre = r.tokens.find("forward");
        auto buffer = r.buffers.find("forward");
        if (pre == r.tokens.end() || buffer == r.buffers.end()) {
            return std::string("err:no-token");
        }
        // Filled through the address PreWrite published, exactly as a peer
        // would over RDMA. Committing an untouched buffer would compare only
        // error codes and never notice a forward-write path that corrupts
        // what it stores.
        const std::string payload = Payload("diff/forward", kSmall);
        if (buffer->second.addr == 0 || buffer->second.size != payload.size()) {
            return std::string("err:unusable-buffer");
        }
        std::memcpy(reinterpret_cast<void*>(buffer->second.addr),
                    payload.data(), payload.size());
        auto result = r.manager().WriteCommit("diff/forward", pre->second);
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict(
        "read what the forward write stored",
        [kSmall](Replay& r) { return DoGet(r, "diff/forward", kSmall); }));
    steps.push_back(Strict("query forward-written key", [](Replay& r) {
        return DoQuery(r, "diff/forward");
    }));
    steps.push_back(Strict("commit again", [](Replay& r) {
        auto result =
            r.manager().WriteCommit("diff/forward", r.tokens["forward"]);
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("revoke unknown", [](Replay& r) {
        auto result = r.manager().WriteRevoke("diff/never", generate_uuid());
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("prewrite then revoke", [kSmall](Replay& r) {
        auto pre = r.manager().PreWrite("diff/revoked", kSmall);
        if (!pre) return Err(pre.error());
        auto revoked =
            r.manager().WriteRevoke("diff/revoked", pre->write_operation_id);
        if (!revoked) return Err(revoked.error());
        return r.manager().Exist("diff/revoked") ? std::string("visible")
                                                 : std::string("gone");
    }));

    // --- pin protocol ---
    steps.push_back(Strict("pin", [](Replay& r) {
        auto result = r.manager().PinKey("diff/obj/1");
        if (!result) return Err(result.error());
        r.tokens["pin1"] = result->read_operation_id;
        return "ok size=" + std::to_string(result->remote_buffer.size);
    }));
    steps.push_back(Strict("pin again", [](Replay& r) {
        auto result = r.manager().PinKey("diff/obj/1");
        if (!result) return Err(result.error());
        return std::string(result->read_operation_id == r.tokens["pin1"]
                               ? "ok same-token"
                               : "ok new-token");
    }));
    steps.push_back(Strict("unpin wrong token", [](Replay& r) {
        auto result = r.manager().UnPinKey("diff/obj/1", generate_uuid());
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("unpin missing key", [](Replay& r) {
        auto result = r.manager().UnPinKey("diff/absent", generate_uuid());
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("unpin twice", [](Replay& r) {
        auto first = r.manager().UnPinKey("diff/obj/1", r.tokens["pin1"]);
        auto second = r.manager().UnPinKey("diff/obj/1", r.tokens["pin1"]);
        return (first ? std::string("ok") : Err(first.error())) + "/" +
               (second ? std::string("ok") : Err(second.error()));
    }));
    steps.push_back(Strict("pin missing key", [](Replay& r) {
        auto result = r.manager().PinKey("diff/absent");
        return result ? std::string("ok") : Err(result.error());
    }));

    // --- deletes ---
    steps.push_back(Strict("delete", [](Replay& r) {
        auto result = r.manager().Delete("diff/obj/2");
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("delete again", [](Replay& r) {
        auto result = r.manager().Delete("diff/obj/2");
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("exist after delete", [](Replay& r) {
        return r.manager().Exist("diff/obj/2") ? "true" : "false";
    }));
    steps.push_back(Strict("replicas after delete", [](Replay& r) {
        return DoReplicaTiers(r, "diff/obj/2");
    }));
    steps.push_back(Strict("delete on wrong tier", [](Replay& r) {
        auto result = r.manager().Delete("diff/obj/3", r.TierAtRank(1));
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("survives wrong-tier delete", [](Replay& r) {
        return r.manager().Exist("diff/obj/3") ? "true" : "false";
    }));
    steps.push_back(Strict("delete on right tier", [](Replay& r) {
        auto result = r.manager().Delete("diff/obj/3", r.TierAtRank(0));
        return result ? std::string("ok") : Err(result.error());
    }));
    steps.push_back(Strict("recreate deleted key", [kSmall](Replay& r) {
        return DoPut(r, "diff/obj/2", kSmall);
    }));
    steps.push_back(Strict("read recreated key", [kSmall](Replay& r) {
        return DoGet(r, "diff/obj/2", kSmall);
    }));

    // --- traversal and topology ---
    steps.push_back(Strict("walk", [](Replay& r) { return DoWalk(r); }));
    steps.push_back(Strict("tier views", [](Replay& r) {
        auto views = r.manager().GetTierViews();
        std::sort(views.begin(), views.end(),
                  [](const TierView& lhs, const TierView& rhs) {
                      return lhs.priority > rhs.priority;
                  });
        std::string out = "views=";
        for (const auto& view : views) {
            // Not the id: it is a fresh UUID in every run.
            out += std::to_string(static_cast<int>(view.type)) + "/" +
                   std::to_string(view.capacity) + "/" +
                   std::to_string(view.priority) + ";";
        }
        return out;
    }));

    // --- route rectification: fires only on a local miss ---
    steps.push_back(Strict("rectify present key", [](Replay& r) {
        const uint64_t before = r.RectifyCalls();
        r.manager().RectifyReadRoute("diff/obj/1");
        return "delta=" + std::to_string(r.RectifyCalls() - before);
    }));
    steps.push_back(Strict("rectify missing key", [](Replay& r) {
        const uint64_t before = r.RectifyCalls();
        r.manager().RectifyReadRoute("diff/absent");
        return "delta=" + std::to_string(r.RectifyCalls() - before);
    }));

    // --- the intended divergences ---
    steps.push_back(Exempt(
        "prewrite onto the slow tier",
        "exemption 3: V1 silently redirects to DRAM and hands back an address "
        "for storage the caller did not ask for; V2 refuses",
        [kSmall](Replay& r) {
            auto result =
                r.manager().PreWrite("diff/slow", kSmall, r.TierAtRank(1));
            if (!result) return Err(result.error());
            (void)r.manager().WriteRevoke("diff/slow",
                                          result->write_operation_id);
            return std::string("ok");
        }));
    steps.push_back(Exempt(
        "pin a recreated key",
        "exemption 9: V1 reuses the pin token of the deleted object, V2 mints "
        "a new identity for the new one",
        [](Replay& r) {
            auto first = r.manager().PinKey("diff/obj/4");
            if (!first) return Err(first.error());
            const UUID token = first->read_operation_id;
            // Deliberately not unpinned: the lease has to still be there when
            // the key is recreated, or neither version has an old token to
            // reuse and the divergence never shows.
            (void)r.manager().Delete("diff/obj/4");
            const std::string payload = Payload("diff/obj/4", 4096);
            std::vector<Slice> slices = {
                {const_cast<char*>(payload.data()), payload.size()}};
            auto handle = r.manager().Put("diff/obj/4", slices);
            if (!handle) return Err(handle.error());
            if (!handle.value()->Wait()) return std::string("err:recreate");
            auto again = r.manager().PinKey("diff/obj/4");
            if (!again) return Err(again.error());
            return std::string(again->read_operation_id == token
                                   ? "reused-token"
                                   : "new-token");
        }));

    // --- teardown ---
    steps.push_back(Strict("remove all", [](Replay& r) {
        auto result = r.manager().RemoveAll();
        return result ? "ok removed=" + std::to_string(*result)
                      : Err(result.error());
    }));
    steps.push_back(
        Strict("walk after remove all", [](Replay& r) { return DoWalk(r); }));
    steps.push_back(Strict("remove all again", [](Replay& r) {
        auto result = r.manager().RemoveAll();
        return result ? "ok removed=" + std::to_string(*result)
                      : Err(result.error());
    }));

    return steps;
}

/**
 * @struct StepResult
 */
struct StepResult {
    std::string answer;
    std::vector<std::string> callbacks;
};

std::vector<StepResult> RunTrace(DataManagerVersion version,
                                 const std::vector<Step>& steps,
                                 const std::string& storage_dir) {
    Replay replay(version, storage_dir);
    std::vector<StepResult> results;
    results.reserve(steps.size());
    for (const auto& step : steps) {
        StepResult result;
        result.answer = step.run(replay);
        // Drained before reading the callbacks: V2 publishes some of them from
        // a background worker, and a comparison taken too early would be
        // measuring thread timing rather than behaviour.
        replay.Drain();
        result.callbacks = replay.TakeCallbacks();
        results.push_back(std::move(result));
    }
    return results;
}

std::string Join(const std::vector<std::string>& items) {
    std::string out;
    for (const auto& item : items) out += item + "|";
    return out;
}

}  // namespace

class DataManagerDifferentialTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("DataManagerDifferentialTest");
            FLAGS_logtostderr = 1;
        });
    }

    void TearDown() override {
        std::filesystem::remove_all("/tmp/mooncake_diff_v1");
        std::filesystem::remove_all("/tmp/mooncake_diff_v2");
    }
};

// The whole point of the harness: replay one trace against both and hold them
// to the same answers, except where the design says otherwise.
TEST_F(DataManagerDifferentialTest, V1AndV2AnswerTheSameTraceIdentically) {
    const auto steps = BuildTrace();
    ASSERT_FALSE(steps.empty());

    const auto v1 =
        RunTrace(DataManagerVersion::kV1, steps, "/tmp/mooncake_diff_v1");
    const auto v2 =
        RunTrace(DataManagerVersion::kV2, steps, "/tmp/mooncake_diff_v2");
    ASSERT_EQ(v1.size(), steps.size());
    ASSERT_EQ(v2.size(), steps.size());

    size_t exempt_differences = 0;
    for (size_t i = 0; i < steps.size(); ++i) {
        const auto& step = steps[i];
        if (step.compare == Compare::kStrict) {
            EXPECT_EQ(v1[i].answer, v2[i].answer)
                << "step " << i << " '" << step.name << "' diverged";
            EXPECT_EQ(Join(v1[i].callbacks), Join(v2[i].callbacks))
                << "step " << i << " '" << step.name
                << "' produced different metadata callbacks";
            continue;
        }
        // Exempt steps still run, and both answers are recorded: the evidence
        // that a difference is the intended one is worth more than silence.
        if (v1[i].answer != v2[i].answer) {
            ++exempt_differences;
            LOG(INFO) << "exempt divergence at step " << i << " '" << step.name
                      << "': v1=" << v1[i].answer << " v2=" << v2[i].answer
                      << " (" << step.exempt_reason << ")";
        }
    }
    // If none of them differ any more, the exemption list has outlived its
    // reason and should be revisited rather than left in place.
    EXPECT_GT(exempt_differences, 0U)
        << "every exempt step now agrees; the exemption list is stale";
}

// A trace is only evidence if replaying it twice on one version gives the same
// answers. Without this, an unstable trace could mask a real divergence.
TEST_F(DataManagerDifferentialTest, TheTraceIsDeterministicWithinOneVersion) {
    const auto steps = BuildTrace();
    const auto first =
        RunTrace(DataManagerVersion::kV2, steps, "/tmp/mooncake_diff_v2");
    const auto second =
        RunTrace(DataManagerVersion::kV2, steps, "/tmp/mooncake_diff_v2");

    ASSERT_EQ(first.size(), second.size());
    for (size_t i = 0; i < steps.size(); ++i) {
        EXPECT_EQ(first[i].answer, second[i].answer)
            << "step " << i << " '" << steps[i].name << "' is not stable";
        EXPECT_EQ(Join(first[i].callbacks), Join(second[i].callbacks))
            << "step " << i << " '" << steps[i].name
            << "' produced unstable callbacks";
    }
}

}  // namespace mooncake
