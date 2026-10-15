#include "handoff/handoff_runtime.h"

#include <algorithm>
#include <sstream>
#include <thread>

#include <glog/logging.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

namespace mooncake::handoff {

namespace {

using SteadyClock = std::chrono::steady_clock;

uint64_t NsSince(SteadyClock::time_point start) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            SteadyClock::now() - start)
            .count());
}

int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Escapes a free-form string for a JSON string literal.
std::string JsonString(const std::string& s) {
    std::string out = "\"";
    for (const char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    out += ' ';
                } else {
                    out += c;
                }
        }
    }
    return out + "\"";
}

std::string RawOrNull(const std::string& json) {
    return json.empty() ? "null" : json;
}

}  // namespace

// ---------------------------------------------------------------- watchdog

Watchdog::Watchdog(std::chrono::milliseconds deadline, Action on_fire) {
    thread_ = std::thread([this, deadline, on_fire = std::move(on_fire)]() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (cv_.wait_for(lock, deadline, [this] { return cancelled_; })) {
            return;
        }
        fired_ = true;
        lock.unlock();
        on_fire();
    });
}

Watchdog::~Watchdog() {
    Cancel();
    if (thread_.joinable()) thread_.join();
}

void Watchdog::Cancel() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cancelled_ = true;
    }
    cv_.notify_all();
}

// ---------------------------------------------------------------- shutdown

std::string ShutdownReport::ToJson() const {
    std::ostringstream o;
    o << "{\"result\":" << JsonString(result)
      << ",\"detail\":" << JsonString(detail)
      << ",\"snapshot\":" << JsonString(snapshot) << ",\"objects\":" << objects
      << ",\"restore_needed\":" << restore_needed << ",\"bytes\":" << bytes
      << ",\"workers\":" << workers << ",\"stop_ns\":" << stop_ns
      << ",\"quiesce_ns\":" << quiesce_ns << ",\"trim_ns\":" << trim_ns
      << ",\"write_ns\":" << write_ns << ",\"publish_ns\":" << publish_ns
      << ",\"total_ns\":" << total_ns
      << ",\"finished_at_ms\":" << finished_at_ms
      << ",\"admission\":" << RawOrNull(admission)
      << ",\"memory\":" << RawOrNull(memory)
      << ",\"writer\":" << RawOrNull(writer) << "}";
    return o.str();
}

ShutdownReport RunHandoffShutdown(const ShutdownSteps& steps,
                                  const ShutdownOptions& options,
                                  std::atomic<bool>* published) {
    const auto start = SteadyClock::now();
    ShutdownReport report;
    auto finish = [&](std::string result, std::string detail) {
        report.result = std::move(result);
        report.detail = std::move(detail);
        report.total_ns = NsSince(start);
        report.finished_at_ms = NowMs();
        if (options.owner) WriteLastShutdown(options.root, report.ToJson());
        LOG(INFO) << "handoff_shutdown=" << report.result
                  << ", report=" << report.ToJson();
        return report;
    };

    // 1. No new or running request handlers.
    auto t = SteadyClock::now();
    steps.stop_serving();
    report.stop_ns = NsSince(t);
    LOG(INFO) << "handoff_shutdown=serving_stopped, stop_ms="
              << report.stop_ns / 1000000;
    // 2. No background thread mutates metadata or allocators from here on.
    t = SteadyClock::now();
    steps.quiesce();
    report.quiesce_ns = NsSince(t);
    LOG(INFO) << "handoff_shutdown=quiesced, quiesce_ms="
              << report.quiesce_ns / 1000000;

    if (!options.owner) {
        return finish("skipped_not_owner", "handoff dir lock not held");
    }
    if (PoisonExists(options.root)) {
        return finish("skipped_poisoned",
                      "another master served next to this one");
    }
    // 3. Nothing older may survive next to the new snapshot.
    if (auto reset = ResetHandoffDir(options.root); !reset) {
        return finish("error", reset.error());
    }
    // 4. Admission. malloc_trim can take seconds on a large heap, so it only
    // runs when the first check falls short of the wanted worker count.
    ShutdownAdmissionInput in;
    in.objects = steps.object_count();
    in.restore_cost = options.restore_cost;
    in.disk_free_bytes = steps.disk_free(options.root);
    in.max_workers = options.max_workers;
    in.cpus = options.cpus;
    report.objects = in.objects;
    ShutdownAdmission admission;
    auto decide = [&]() -> bool {
        auto memory = steps.read_memory();
        if (!memory) {
            report.detail = "memory: " + memory.error();
            return false;
        }
        report.memory = memory->ToJson();
        in.memory = *memory;
        admission = DecideShutdownAdmission(in);
        report.admission = admission.ToJson();
        return true;
    };
    if (!decide()) return finish("error", report.detail);
    const size_t wanted =
        std::min(options.max_workers, std::max<size_t>(1, options.cpus));
    if (steps.trim && admission.reason != "disk_space" &&
        admission.reason != "restore_memory" && admission.workers < wanted) {
        t = SteadyClock::now();
        steps.trim();
        report.trim_ns = NsSince(t);
        if (!decide()) return finish("error", report.detail);
    }
    if (!admission.admit) {
        return finish("skipped_" + admission.reason, "");
    }
    report.workers = admission.workers;

    // 5. Write into snap-<id>.tmp.
    const int64_t created_at_ms = NowMs();
    report.snapshot = NewSnapshotName(created_at_ms);
    const std::string tmp_dir =
        options.root + "/" + report.snapshot + kTmpSuffix;
    t = SteadyClock::now();
    auto written = steps.write(tmp_dir, admission.workers);
    report.write_ns = NsSince(t);
    if (!written) {
        return finish("error", "write: " + written.error());
    }
    report.objects = written->objects;
    report.bytes = written->bytes;
    report.writer = written->json;

    // 6. The admission above counted every object; now the written count is
    // known. A snapshot the next master could not restore is not published
    // (cold start), and its files are removed.
    report.restore_needed =
        RestoreNeedBytes(report.objects, options.restore_cost);
    if (report.restore_needed > RestoreBudgetBytes(in.memory.limit)) {
        if (auto reset = ResetHandoffDir(options.root); !reset) {
            LOG(ERROR) << "handoff: cannot remove the unpublished snapshot: "
                       << reset.error();
        }
        return finish("skipped_restore_memory",
                      "exact restore need " +
                          std::to_string(report.restore_needed) +
                          " > restore budget " +
                          std::to_string(RestoreBudgetBytes(in.memory.limit)));
    }

    // 7. Publish unless another master started serving meanwhile.
    if (steps.before_publish) steps.before_publish();
    if (PoisonExists(options.root)) {
        return finish("skipped_poisoned",
                      "POISON appeared during the snapshot");
    }
    t = SteadyClock::now();
    ReadyMarker marker{report.snapshot, created_at_ms, report.objects,
                       report.bytes};
    if (auto pub = PublishSnapshot(options.root, report.snapshot, marker);
        !pub) {
        return finish("error", "publish: " + pub.error());
    }
    if (published) published->store(true);
    report.publish_ns = NsSince(t);
    return finish("ok", "");
}

// ----------------------------------------------------------------- startup

std::string StartupReport::ToJson() const {
    std::ostringstream o;
    o << "{\"result\":" << JsonString(result)
      << ",\"detail\":" << JsonString(detail)
      << ",\"snapshot\":" << JsonString(snapshot) << ",\"objects\":" << objects
      << ",\"segments\":" << segments << ",\"age_ms\":" << age_ms
      << ",\"claim_ns\":" << claim_ns << ",\"attempts\":" << attempts
      << ",\"serve\":" << (serve ? "true" : "false")
      << ",\"restore_ns\":" << restore_ns << ",\"total_ns\":" << total_ns
      << ",\"restore\":" << RawOrNull(restore) << "}";
    return o.str();
}

std::chrono::milliseconds RetryBackoff(int attempt) {
    int64_t ms = 1000;
    for (int i = 1; i < attempt && ms < 10000; ++i) ms *= 2;
    return std::chrono::milliseconds(std::min<int64_t>(ms, 10000));
}

StartupReport RunHandoffStartup(const StartupSteps& steps,
                                const StartupOptions& options) {
    const auto start = SteadyClock::now();
    StartupReport report;
    auto finish = [&](std::string result, std::string detail) {
        report.result = std::move(result);
        report.detail = std::move(detail);
        report.total_ns = NsSince(start);
        LOG(INFO) << "handoff_startup=" << report.result
                  << ", report=" << report.ToJson();
        return report;
    };
    const auto wait_retry = [&steps](int attempt) {
        if (steps.wait_retry) return steps.wait_retry(attempt);
        std::this_thread::sleep_for(RetryBackoff(attempt));
        return true;
    };
    if (!options.owner) {
        // Make sure a snapshot written by the lock owner after we started
        // serving is never restored: do not serve before POISON is durable.
        for (int attempt = 1;; ++attempt) {
            report.attempts = attempt;
            auto poison = WritePoison(options.root);
            if (poison) break;
            LOG(ERROR) << "handoff_startup=poison_failed, attempt=" << attempt
                       << ", error=" << poison.error()
                       << ", action=retry_before_serving";
            if (!wait_retry(attempt)) {
                report.serve = false;
                return finish("error",
                              "stopped before POISON could be written: " +
                                  poison.error());
            }
        }
        return finish("not_owner", "handoff dir lock not held");
    }

    auto t = SteadyClock::now();
    ClaimResult claim = ClaimHandoffSnapshot(options.root);
    report.attempts = 1;
    // A READY that could be neither claimed nor removed must never be
    // restored by a later master after this one served. Retry until the
    // claim works or the READY is too old to be restored by anyone.
    const int64_t wait_start_ms = NowMs();
    for (int attempt = 1;
         claim.state == ClaimState::kError && claim.ready_may_remain;
         ++attempt) {
        const auto ready = PeekReadyMarker(options.root);
        // An unreadable READY was created before this master started
        // waiting. After stale_at_ms every master's age check refuses the
        // READY (with the same --handoff_max_age_sec).
        const int64_t base_ms = ready ? ready->created_at_ms : wait_start_ms;
        const int64_t stale_at_ms = base_ms + options.max_age_ms + 1000;
        const int64_t now_ms = NowMs();
        LOG(ERROR) << "handoff_startup=claim_failed, attempt=" << attempt
                   << ", detail=" << claim.detail
                   << ", stale_in_ms=" << (stale_at_ms - now_ms)
                   << ", action=retry_before_serving";
        if (now_ms >= stale_at_ms) {
            report.claim_ns = NsSince(t);
            return finish("error",
                          "READY could not be removed but is older "
                          "than max_age now; serving cold: " +
                              claim.detail);
        }
        if (!wait_retry(attempt)) {
            report.claim_ns = NsSince(t);
            report.serve = false;
            return finish("error",
                          "stopped while a READY that could not be removed "
                          "remains: " +
                              claim.detail);
        }
        claim = ClaimHandoffSnapshot(options.root);
        report.attempts = attempt + 1;
    }
    report.claim_ns = NsSince(t);
    report.snapshot = claim.marker.snapshot;
    switch (claim.state) {
        case ClaimState::kNone:
            return finish("none", "");
        case ClaimState::kClaimed:
            break;
        default:
            return finish(ToString(claim.state), claim.detail);
    }
    report.objects = claim.marker.objects;
    // From here on every path releases the claim (one attempt only).
    struct Release {
        const std::string& root;
        const ClaimResult& claim;
        ~Release() { ReleaseClaim(root, claim); }
    } release{options.root, claim};

    if (!options.restore_enabled) {
        return finish("disabled", "restore disabled; snapshot discarded");
    }
    const int64_t now_ms = options.now_ms != 0 ? options.now_ms : NowMs();
    const int64_t age = now_ms - claim.marker.created_at_ms;
    report.age_ms = age > 0 ? static_cast<uint64_t>(age) : 0;
    if (age < 0 || age > options.max_age_ms) {
        return finish("stale", "snapshot age " + std::to_string(age) +
                                   " ms outside [0, " +
                                   std::to_string(options.max_age_ms) + "]");
    }
    auto memory = steps.read_memory();
    if (!memory) {
        return finish("error", "memory: " + memory.error());
    }
    const RestoreAdmission admission = DecideRestoreAdmission(
        *memory, claim.marker.objects, options.restore_cost);
    if (!admission.admit) {
        return finish("memory_headroom", admission.ToJson());
    }
    t = SteadyClock::now();
    auto restored = steps.restore(claim.snapshot_dir, options.workers);
    report.restore_ns = NsSince(t);
    if (!restored) {
        return finish("error", restored.error());
    }
    report.objects = restored->objects;
    report.segments = restored->segments;
    report.restore = restored->json;
    return finish("ok", "");
}

// ----------------------------------------------------------------- metrics

HandoffMetrics& HandoffMetrics::Instance() {
    static HandoffMetrics instance;
    return instance;
}

void HandoffMetrics::SetStartup(const StartupReport& report) {
    std::lock_guard<std::mutex> lock(mutex_);
    has_startup_ = true;
    startup_ = report;
}

void HandoffMetrics::SetLastShutdownJson(const std::string& json) {
    std::lock_guard<std::mutex> lock(mutex_);
    last_shutdown_json_ = json;
}

void HandoffMetrics::SetSegmentCounters(
    std::function<void(uint64_t*, uint64_t*, uint64_t*)> provider) {
    std::lock_guard<std::mutex> lock(mutex_);
    segment_counters_ = std::move(provider);
}

std::string HandoffMetrics::Serialize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!has_startup_) return "";
    std::ostringstream o;
    auto gauge = [&o](const char* name, const char* help, double value,
                      const std::string& labels = "") {
        o << "# HELP " << name << " " << help << "\n# TYPE " << name
          << " gauge\n"
          << name << labels << " " << value << "\n";
    };
    gauge("master_handoff_restore_result",
          "Outcome of the handoff restore at startup (label result)", 1,
          "{result=\"" + startup_.result + "\"}");
    gauge("master_handoff_restore_objects", "Objects restored at startup",
          static_cast<double>(startup_.result == "ok" ? startup_.objects : 0));
    gauge("master_handoff_restore_duration_seconds",
          "Wall time of the handoff restore",
          static_cast<double>(startup_.restore_ns) / 1e9);
    gauge("master_handoff_snapshot_age_seconds",
          "Age of the claimed snapshot at startup",
          static_cast<double>(startup_.age_ms) / 1e3);
    if (segment_counters_) {
        uint64_t pending = 0, confirmed = 0, dropped = 0;
        segment_counters_(&pending, &confirmed, &dropped);
        gauge("master_handoff_segments_pending",
              "Restored segments not yet confirmed by their owner",
              static_cast<double>(pending));
        gauge("master_handoff_segments_confirmed",
              "Restored segments confirmed by an identical remount",
              static_cast<double>(confirmed));
        gauge("master_handoff_segments_dropped",
              "Restored segments dropped on an identity mismatch or "
              "unmounted before their owner confirmed them",
              static_cast<double>(dropped));
    }
    Json::Value last;
    Json::CharReaderBuilder builder;
    std::string errors;
    std::istringstream in(last_shutdown_json_);
    if (!last_shutdown_json_.empty() &&
        Json::parseFromStream(builder, in, &last, &errors) && last.isObject()) {
        gauge("master_handoff_last_snapshot_result",
              "Outcome of the previous master's shutdown snapshot (label "
              "result)",
              1,
              "{result=\"" + last.get("result", "unknown").asString() + "\"}");
        gauge("master_handoff_last_snapshot_objects",
              "Objects in the previous master's shutdown snapshot",
              last.get("objects", 0).asDouble());
        gauge("master_handoff_last_snapshot_bytes",
              "Bytes of the previous master's shutdown snapshot",
              last.get("bytes", 0).asDouble());
        gauge("master_handoff_last_snapshot_workers",
              "Writer workers of the previous master's shutdown snapshot",
              last.get("workers", 0).asDouble());
        gauge("master_handoff_last_shutdown_duration_seconds",
              "SIGTERM handling time of the previous master",
              last.get("total_ns", 0).asDouble() / 1e9);
    }
    return o.str();
}

// ------------------------------------------------------------------ config

std::optional<std::string> ValidateHandoffConfig(
    bool enable_ha, bool enable_dfs, const std::string& memory_allocator) {
    if (enable_ha) {
        return "the shutdown handoff cannot be used with enable_ha: the HA "
               "OpLog and standby already own the index";
    }
    if (enable_dfs) {
        return "the shutdown handoff cannot be used with DFS replicas";
    }
    if (memory_allocator != "offset") {
        return "the shutdown handoff requires memory_allocator=offset";
    }
    return std::nullopt;
}

}  // namespace mooncake::handoff
