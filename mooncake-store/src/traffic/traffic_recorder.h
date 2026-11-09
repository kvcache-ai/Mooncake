#pragma once

#include <sys/uio.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "route_names.h"
#include "traffic_config.h"
#include "traffic_format.h"
#include "traffic_redaction.h"

namespace cinatra {
class coro_http_server;
}

namespace mooncake::traffic {

class TrafficSink;

// One request as the RPC layer saw it, before deserialization. The method
// name is not carried here: coro_rpc stores the handler it was given, which
// for a pooled handler is the pool's coroutine, so the recorder resolves the
// name from the routes the service registered.
struct RequestView {
    std::uint32_t route_key = 0;
    std::string_view payload;
    std::uint64_t conn_id = 0;
    std::uint32_t seq_num = 0;
    std::uint32_t attach_length = 0;
};

struct RecorderStatus {
    std::string state = "disabled";
    std::uint64_t observed_requests = 0;
    std::uint64_t recorded_requests = 0;
    std::uint64_t filtered_requests = 0;
    std::uint64_t dropped_requests = 0;
    std::uint64_t redaction_failed_requests = 0;
    std::uint64_t bytes_written = 0;
    std::uint64_t segments = 0;
    std::uint64_t ring_used_bytes = 0;
    std::uint64_t ring_capacity_bytes = 0;
    bool stop_timed_out = false;
    bool format_complete = false;
    bool capture_complete = false;
    std::string session_dir;
};

class TrafficRecorder {
   public:
    static TrafficRecorder& Instance();

    // Reads the configuration and starts the control server; called by
    // RegisterRpcService once the RPC handlers are registered. The registered
    // routes are what makes a method name in the configuration checkable, so a
    // name that matches no route fails startup; without them a configured name
    // cannot be resolved and startup fails as well.
    void Install(std::size_t rpc_threads = 0,
                 RedactionRegistry redaction_registry = {},
                 RouteNameRegistry route_names = {});
    bool Start();
    void Stop();
    // Returns true when a stop is in progress or was just started, and false
    // when the recorder was already idle.
    bool StopAsync();
    void OnRequest(const RequestView& view) noexcept;
    bool Recording() const noexcept;
    RecorderStatus GetStatus() const;
    void Shutdown();

   private:
    struct Shard;

    TrafficRecorder();
    ~TrafficRecorder();
    TrafficRecorder(const TrafficRecorder&) = delete;
    TrafficRecorder& operator=(const TrafficRecorder&) = delete;

    void WriterLoop();
    void StopForWriterFailure();
    bool DrainShardDirect(Shard& shard, std::vector<struct iovec>& iov);
    bool DrainShardRedacted(Shard& shard);
    bool FlushRecords(Shard& shard, std::vector<struct iovec>& iov,
                      std::uint64_t bytes, std::uint32_t records,
                      std::uint64_t commit_tail);
    void StopInternal();
    void ResetConfig();
    Shard* BindShard() noexcept;
    bool RouteAllowed(const RequestView& view) const noexcept;
    void AppendRecord(Shard& shard, const RequestView& view,
                      std::uint64_t total) noexcept;
    void RememberRoute(const RequestView& view) noexcept;
    bool Fail(const std::string& message);

    // The recorder moves through these states; the HTTP status reports them by
    // name. Kept as an enum so a mistyped literal cannot leave the recorder in
    // a state that no comparison recognizes.
    enum class State { kDisabled, kIdle, kRecording, kStopping, kError };
    static const char* StateName(State state);
    void SetState(State state);
    void StartControlServer();

    mutable std::mutex lifecycle_mu_;
    mutable std::mutex status_mu_;
    mutable std::mutex shards_mu_;
    mutable std::mutex writer_mu_;
    std::condition_variable writer_cv_;

    std::atomic<bool> active_{false};
    std::atomic<bool> shutdown_{false};
    // Set while the stop thread is draining; a joinable thread alone does not
    // mean a stop is in progress, because the thread outlives its work until
    // someone joins it.
    std::atomic<bool> stop_in_progress_{false};
    std::atomic<bool> writer_stop_{false};
    std::atomic<std::uint32_t> in_request_{0};
    std::atomic<std::uint64_t> generation_{0};
    std::atomic<std::uint64_t> next_shard_{0};
    std::atomic<std::uint64_t> dropped_no_shard_{0};

    std::vector<std::unique_ptr<Shard>> shards_;
    std::thread writer_thread_;
    std::thread stop_thread_;

    std::unique_ptr<TrafficSink> sink_;
    // Scratch buffer for redacted writes; only the writer thread touches it.
    std::vector<char> redacted_block_;
    std::atomic<std::uint64_t> stop_deadline_ms_{0};
    std::atomic<bool> stop_timed_out_{false};
    std::atomic<bool> writer_failed_{false};

    RecorderConfig config_;
    bool config_loaded_ = false;
    RedactionRegistry redaction_registry_;
    std::mutex route_names_mu_;
    std::unordered_map<std::uint32_t, std::string> route_names_;
    // Routes the RPC service registered, keyed by route key. Empty when the
    // caller did not provide them (tests, direct use of the recorder).
    RouteNameRegistry registered_routes_;
    std::unique_ptr<cinatra::coro_http_server> control_server_;

    State state_ = State::kDisabled;
    std::string session_dir_;
};

}  // namespace mooncake::traffic
