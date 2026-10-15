#pragma once

// Shutdown and startup sequences of the handoff, the deadline watchdog and a
// few metrics. The steps that touch the RPC server
// and MasterService are injected, so tests can check ordering and outcomes
// without a real server.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <ylt/util/tl/expected.hpp>

#include "handoff/handoff_admission.h"
#include "handoff/handoff_lifecycle.h"

namespace mooncake::handoff {

// Runs `on_fire` on its own thread once `deadline` has passed, unless
// Cancel() was called first. In the master, on_fire records the outcome and
// calls _exit(), so a slow or stuck snapshot never outlives the grace period.
class Watchdog {
   public:
    using Action = std::function<void()>;
    Watchdog(std::chrono::milliseconds deadline, Action on_fire);
    ~Watchdog();
    Watchdog(const Watchdog&) = delete;
    Watchdog& operator=(const Watchdog&) = delete;

    void Cancel();
    bool fired() const { return fired_.load(); }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool cancelled_{false};
    std::atomic<bool> fired_{false};
    std::thread thread_;
};

// ---------------------------------------------------------------- shutdown

struct ShutdownWriteResult {
    uint64_t objects{0};
    uint64_t bytes{0};
    std::string json;  // writer stats
};

struct ShutdownSteps {
    // Stops the RPC and admin servers and returns only after every request
    // handler has returned.
    std::function<void()> stop_serving;
    // Stops and joins every background thread that mutates metadata.
    std::function<void()> quiesce;
    // malloc_trim(0); may be empty. Only called when the first admission
    // check gives fewer workers than wanted (or none).
    std::function<void()> trim;
    std::function<tl::expected<MemorySnapshot, std::string>()> read_memory;
    std::function<uint64_t()> object_count;
    std::function<uint64_t(const std::string&)> disk_free;
    // Writes a complete, fsynced snapshot into `dir` (created by the step).
    std::function<tl::expected<ShutdownWriteResult, std::string>(
        const std::string& dir, size_t workers)>
        write;
    // Called right before the READY marker is published; tests use it to
    // inject races (e.g. a POISON written by another master).
    std::function<void()> before_publish;
};

struct ShutdownOptions {
    std::string root;
    bool owner{false};  // holds the HandoffDirLock
    size_t max_workers{8};
    size_t cpus{1};
    RestoreCostModel restore_cost;
};

struct ShutdownReport {
    // ok | skipped_not_owner | skipped_poisoned | skipped_memory_headroom |
    // skipped_disk_space | skipped_restore_memory | error | deadline
    std::string result;
    std::string detail;
    std::string snapshot;
    uint64_t objects{0};
    // Predicted restore need of the written snapshot (0 before).
    uint64_t restore_needed{0};
    uint64_t bytes{0};
    size_t workers{0};
    uint64_t stop_ns{0};
    uint64_t quiesce_ns{0};
    uint64_t trim_ns{0};
    uint64_t write_ns{0};
    uint64_t publish_ns{0};
    uint64_t total_ns{0};
    int64_t finished_at_ms{0};
    std::string admission;  // ShutdownAdmission JSON
    std::string memory;     // MemorySnapshot JSON
    std::string writer;     // writer JSON
    std::string ToJson() const;
};

// Order: stop_serving -> quiesce -> ownership / poison checks -> reset the
// directory -> admission (trim and re-check if short) -> write into
// snap-<id>.tmp -> restore-need check of what was written -> poison re-check
// -> publish (rename + READY). Writes last_shutdown at the end.
// Never throws; the caller exits the process afterwards.
ShutdownReport RunHandoffShutdown(const ShutdownSteps& steps,
                                  const ShutdownOptions& options,
                                  std::atomic<bool>* published = nullptr);

// ----------------------------------------------------------------- startup

struct RestoreSummary {
    uint64_t objects{0};
    uint64_t segments{0};
    std::string json;  // restore stats
};

struct StartupSteps {
    std::function<tl::expected<MemorySnapshot, std::string>()> read_memory;
    // Restores the snapshot in `dir` into the (empty) master. On error the
    // caller must start from an empty master again (cold start).
    std::function<tl::expected<RestoreSummary, std::string>(
        const std::string& dir, size_t workers)>
        restore;
    // Waits before retry number `attempt` (1, 2, ...) of a step whose
    // failure would otherwise let this master serve next to a restorable
    // READY (or without POISON). Returns false if the master is shutting
    // down; the master then never serves. Empty: sleep with backoff.
    std::function<bool(int attempt)> wait_retry;
};

// Backoff between such retries: 1 s doubling up to 10 s.
std::chrono::milliseconds RetryBackoff(int attempt);

struct StartupOptions {
    std::string root;
    bool owner{false};
    bool restore_enabled{true};
    // Real gaps are 15-30 s plus pod start; an older snapshot means the gap
    // was unusual (or another master served meanwhile): start cold.
    int64_t max_age_ms{120000};
    RestoreCostModel restore_cost;
    size_t workers{8};
    int64_t now_ms{0};  // 0: system clock
};

struct StartupReport {
    // ok | none | disabled | not_owner | aborted_previous | poisoned |
    // invalid | stale | memory_headroom | error
    std::string result;
    std::string detail;
    std::string snapshot;
    uint64_t objects{0};
    uint64_t segments{0};
    uint64_t age_ms{0};
    uint64_t claim_ns{0};
    uint64_t restore_ns{0};
    uint64_t total_ns{0};
    // Claim or POISON attempts (more than 1 only after I/O errors).
    uint64_t attempts{0};
    // False: the master must not serve (it stopped while a READY it could
    // not remove, or a missing POISON, made serving unsafe).
    bool serve{true};
    std::string restore;  // restore stats JSON
    std::string ToJson() const;
};

// Claims READY (before anything is allocated), checks age and memory, runs
// the restore once and always releases the claim afterwards. Never lets the
// master serve while a READY it could not claim or remove is on disk: it
// retries (wait_retry) until the claim succeeds, the READY is removed, or the
// READY is older than max_age (no master restores it anymore). A non-owner
// retries writing POISON until it succeeds.
StartupReport RunHandoffStartup(const StartupSteps& steps,
                                const StartupOptions& options);

// ----------------------------------------------------------------- metrics

// Small process-wide holder exported on the admin /metrics endpoint.
class HandoffMetrics {
   public:
    static HandoffMetrics& Instance();
    void SetStartup(const StartupReport& report);
    void SetLastShutdownJson(const std::string& json);
    // Live counters of restored segments (pending / confirmed / dropped).
    void SetSegmentCounters(
        std::function<void(uint64_t*, uint64_t*, uint64_t*)> provider);
    std::string Serialize() const;

   private:
    mutable std::mutex mutex_;
    bool has_startup_{false};
    StartupReport startup_;
    std::string last_shutdown_json_;
    std::function<void(uint64_t*, uint64_t*, uint64_t*)> segment_counters_;
};

// ------------------------------------------------------------------ config

// Rejects settings the handoff cannot run with. The handoff carries the
// index of a standalone master only: with HA the OpLog and the standby own
// that state (one authority per state), DFS replicas are not carried, and
// the restore rebuilds offset allocators only. Returns the reason, or
// nullopt.
std::optional<std::string> ValidateHandoffConfig(
    bool enable_ha, bool enable_dfs, const std::string& memory_allocator);

}  // namespace mooncake::handoff
