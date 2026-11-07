#pragma once

#include <sys/uio.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <ylt/coro_http/coro_http_server.hpp>

#include "request_view.h"

namespace mooncake::traffic {

class TrafficSink;

struct RecorderStatus {
    std::string state = "disabled";
    std::uint64_t observed_requests = 0;
    std::uint64_t recorded_requests = 0;
    std::uint64_t dropped_requests = 0;
    std::uint64_t dropped_ring = 0;
    std::uint64_t dropped_filtered = 0;
    std::uint64_t dropped_oversize = 0;
    std::uint64_t bytes_written = 0;
    std::uint64_t segments = 0;
    std::uint64_t ring_used_bytes = 0;
    std::uint64_t ring_capacity_bytes = 0;
    bool stop_timed_out = false;
    bool format_complete = true;
    bool capture_complete = true;
    std::string session_dir;
};

class TrafficRecorder {
   public:
    static TrafficRecorder& Instance();

    // 读取配置并启动控制面；由 RegisterRpcService 在注册完成后调用。
    void Install();
    bool Start();
    void Stop();
    void StopAsync();
    void OnRequest(const RequestView& view) noexcept;
    RecorderStatus GetStatus() const;
    void Shutdown();

   private:
    struct alignas(64) Shard {
        std::unique_ptr<char[]> ring;
        std::uint64_t capacity = 0;
        std::uint64_t mask = 0;
        char cache_pad_[40] = {};
        alignas(64) std::atomic<std::uint64_t> head{0};
        std::atomic<std::uint64_t> state{1};
        std::atomic<std::uint64_t> observed{0};
        std::atomic<std::uint64_t> recorded{0};
        alignas(64) std::atomic<std::uint64_t> tail{0};
        alignas(64) std::atomic<std::uint64_t> dropped_ring{0};
        std::atomic<std::uint64_t> dropped_filtered{0};
        std::atomic<std::uint64_t> dropped_oversize{0};
    };

    TrafficRecorder() = default;
    ~TrafficRecorder();
    TrafficRecorder(const TrafficRecorder&) = delete;
    TrafficRecorder& operator=(const TrafficRecorder&) = delete;

    static void OnRequestThunk(void* context, const RequestView& view) noexcept;
    Shard* BindShard();
    bool Allowed(std::uint32_t route_key);
    void WriterLoop();
    void DrainShard(Shard& shard, std::vector<char>& block,
                    std::uint32_t& block_records);
    void DrainShardDirect(Shard& shard);
    bool FlushRecords(Shard& shard, std::vector<struct iovec>& iov,
                      std::uint64_t bytes, std::uint32_t records,
                      std::uint64_t commit_tail);
    void FlushBlock(std::vector<char>& block, std::uint32_t& block_records);
    void StopInternal();
    void StartControlServer();
    std::string StatusJson() const;
    void RememberRoute(const RequestView& view);
    std::vector<std::pair<std::uint32_t, std::string>> RouteTable() const;

    mutable std::mutex lifecycle_mu_;
    mutable std::mutex status_mu_;
    mutable std::mutex shards_mu_;
    mutable std::mutex writer_mu_;
    mutable std::mutex routes_mu_;
    std::map<std::uint32_t, std::string> routes_;
    std::condition_variable writer_cv_;

    std::atomic<bool> active_{false};
    std::atomic<bool> shutdown_{false};
    std::atomic<bool> writer_stop_{false};
    std::atomic<std::uint32_t> starting_{0};
    std::atomic<std::uint64_t> generation_{0};
    std::atomic<std::uint64_t> next_shard_{0};

    std::vector<std::unique_ptr<Shard>> shards_;
    std::thread writer_thread_;
    std::thread stop_thread_;

    std::unique_ptr<TrafficSink> sink_;
    std::vector<char> block_;
    std::uint64_t block_bytes_ = 4ull * 1024 * 1024;
    std::uint64_t segment_bytes_ = 256ull * 1024 * 1024;
    std::uint64_t segment_seconds_ = 300;
    std::uint64_t ring_bytes_ = 1024ull * 1024 * 1024;
    std::uint64_t max_record_bytes_ = 4ull * 1024 * 1024;
    std::uint32_t ring_shards_ = 8;
    std::uint64_t stop_timeout_ms_ = 30000;
    std::atomic<std::uint64_t> stop_deadline_ms_{0};
    std::atomic<bool> stop_timed_out_{false};

    std::string output_dir_;
    std::string config_path_;
    bool config_loaded_ = false;
    bool allow_all_routes_ = false;
    std::unordered_set<std::uint32_t> allowed_routes_;
    std::string control_host_ = "127.0.0.1";
    std::uint16_t control_port_ = 37901;
    std::string control_token_;
    std::unique_ptr<coro_http::coro_http_server> control_server_;
    std::atomic<bool> control_started_{false};

    std::string state_ = "disabled";
    std::string session_dir_;
    std::uint64_t observed_total_ = 0;
    std::uint64_t recorded_total_ = 0;
};

}  // namespace mooncake::traffic
