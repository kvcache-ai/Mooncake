#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace mooncake {
namespace hkvm {

// Description of one upstream Mooncake master whose KV events are consumed.
struct DiffMasterConfig {
  // Logical name used in reports, e.g. "master1".
  std::string id;
  // ZMQ endpoint the master's KvEventPublisher binds to,
  // e.g. "tcp://10.0.0.1:5557" or "ipc:///tmp/kv_events".
  std::string endpoint;
  // If non-empty, only events whose "backend_id" field matches are counted;
  // empty accepts every event arriving on this socket.
  std::string backend_id_filter;

  // --- Snapshot bootstrap (optional) ---
  // HTTP host of the master's metrics/admin server. If empty, no snapshot is
  // fetched and the server runs in event-only mode (only keys whose
  // stored/removed events arrive after Start() are tracked).
  std::string http_host;
  // metrics_port the admin HTTP server listens on (default 9003 on the master).
  int metrics_port{0};
};

struct DiffServerConfig {
  // Two masters are expected for the two-list diff. masters_[0] is "master1"
  // and masters_[1] is "master2" in DiffResult.
  std::vector<DiffMasterConfig> masters;
  // ZMQ SUB receive high-water mark per socket.
  int recv_hwm{100000};
  // Receiver poll timeout in milliseconds (also caps stop latency).
  int poll_timeout_ms{200};
  // Time to let each ZMQ SUB connection establish before fetching the
  // bootstrap snapshot. The SUB must be receiving *before* the snapshot is
  // taken so the buffered-event replay covers the snapshot boundary; see
  // BootstrapSnapshot. Set to 0 to skip the settle.
  int snapshot_settle_ms{300};
  // Per-socket timeout (ms) for the snapshot HTTP request.
  int snapshot_timeout_ms{5000};
};

// Consumes KV events (Mooncake KvEventPublisher wire format, RFC #1527) from
// two Mooncake masters over ZMQ and maintains the live object-key set of each.
//
// The symmetric difference is exposed as two lists:
//   only_in_master1 : keys present in master1 but absent from master2
//   only_in_master2 : keys present in master2 but absent from master1
//
// Snapshot + replay: to avoid PUB/SUB slow-joiner loss of pre-existing keys,
// Start() optionally fetches each master's /get_all_keys snapshot and seeds the
// live set from it. To reconcile the snapshot with the concurrent event stream
// without a race, receiver threads buffer events until the snapshot is taken,
// then the snapshot is seeded and the buffered events are replayed in order.
// This converges to the true state because the master publishes a "removed"
// event only after deleting the key from its own set, so a key present in the
// snapshot cannot have a pre-snapshot removal in the buffer.
class DiffServer {
 public:
  explicit DiffServer(DiffServerConfig config);
  ~DiffServer();

  DiffServer(const DiffServer&) = delete;
  DiffServer& operator=(const DiffServer&) = delete;

  // Connects SUB sockets, starts receiver threads (buffering), fetches the
  // bootstrap snapshot from each master, replays buffered events, then enters
  // live mode. Returns false if fewer than two masters are configured or ZMQ
  // setup fails. Snapshot fetch failures are logged and fall back to
  // event-only mode for that master (non-fatal).
  bool Start();
  // Signals receiver threads to stop, joins them, and tears down ZMQ state.
  void Stop();

  bool running() const { return running_.load(); }

  // Re-fetches each master's /get_all_keys snapshot and re-seeds the live key
  // sets (buffering events during the fetch and replaying them). This clears
  // the derived diff lists by recomputing them from ground truth — use it
  // after an external reconciler (e.g. CopyEngine) has mutated the masters so
  // the diff reflects the new reality. No-op if not running.
  void Rebootstrap();

  struct DiffResult {
    std::vector<std::string> only_in_master1;
    std::vector<std::string> only_in_master2;
    size_t master1_key_count{0};
    size_t master2_key_count{0};
  };
  // Returns the current symmetric difference, sorted for stable output.
  DiffResult GetDiff() const;

  struct MasterStats {
    uint64_t events_received{0};  // events parsed and applied (post-filter)
    uint64_t stored_events{0};
    uint64_t removed_events{0};
    uint64_t malformed_events{0};
    uint64_t seq_gaps{0};  // publisher-side drops detected via ZMQ seq gaps
    uint64_t last_seq{0};  // last ZMQ sequence seen; 0 == none seen
    bool has_last_seq{false};
    uint64_t snapshot_key_count{0};  // keys seeded from /get_all_keys
    bool bootstrapped{false};        // snapshot+replay completed
  };
  std::vector<MasterStats> GetStats() const;

 private:
  struct MasterState {
    DiffMasterConfig config;

    mutable std::mutex mutex;
    std::unordered_set<std::string> live_keys;

    // Bootstrap buffering: while true, the receiver appends decoded events to
    // `pending` instead of mutating `live_keys`. DrainAndGoLive() seeds
    // `live_keys` from the snapshot, replays `pending`, clears it, and flips
    // this to false — all under `mutex`.
    bool buffering{true};
    struct PendingEvent {
      bool stored;
      std::string key;
    };
    std::vector<PendingEvent> pending;

    // Per-socket monotonic ZMQ sequence tracking. Updated only by the receiver
    // thread, but read by GetStats(), so they are atomic.
    std::atomic<bool> has_last_seq{false};
    std::atomic<uint64_t> last_seq{0};

    std::atomic<uint64_t> events_received{0};
    std::atomic<uint64_t> stored_events{0};
    std::atomic<uint64_t> removed_events{0};
    std::atomic<uint64_t> malformed_events{0};
    std::atomic<uint64_t> seq_gaps{0};

    std::atomic<uint64_t> snapshot_key_count{0};
    std::atomic<bool> bootstrapped{false};

    // Owned exclusively by the receiver thread.
    void* socket{nullptr};
    std::thread thread;
    std::atomic<bool> stop_flag{false};
  };

  void ReceiverLoop(size_t index);
  // Decodes the msgpack payload and records each event (buffered or live).
  bool ApplyEvent(MasterState& state, const void* payload, size_t size);
  // Records one event under `mutex`, honoring the buffering flag.
  void RecordEvent(MasterState& state, bool stored, const std::string& key);
  // Fetches /get_all_keys for master `index`, seeds live_keys, replays pending.
  void BootstrapSnapshot(size_t index);
  void DrainAndGoLive(MasterState& s, std::vector<std::string> snapshot_keys);
  void CloseSockets();

  DiffServerConfig config_;
  void* zmq_context_{nullptr};
  std::vector<std::unique_ptr<MasterState>> masters_;
  std::atomic<bool> running_{false};
};

}  // namespace hkvm
}  // namespace mooncake
