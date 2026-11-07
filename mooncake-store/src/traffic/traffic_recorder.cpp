#include "traffic/traffic_recorder.h"

#include <json/json.h>
#include <ylt/coro_http/coro_http_server.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <new>
#include <thread>

#include <glog/logging.h>

#include "traffic/file_sink.h"
#include "traffic/traffic_format.h"

namespace mooncake::traffic {
namespace {

constexpr std::uint64_t kAccepting = 1ull << 63;
constexpr std::uint64_t kProducerMask = ~kAccepting;
constexpr std::uint64_t kMinShardBytes = 64ull * 1024;
constexpr std::uint64_t kDefaultStopTimeoutMs = 30000;

std::uint64_t FloorPowerOfTwo(std::uint64_t value) {
    if (value == 0) {
        return 0;
    }
    std::uint64_t result = 1;
    while (result <= value / 2) {
        result <<= 1;
    }
    return result;
}

bool ParseSize(const Json::Value& node, const char* name,
               std::uint64_t* out) {
    if (node.isNull()) {
        return true;
    }
    if (!node.isUInt64()) {
        LOG(ERROR) << "traffic: config " << name << " must be an integer";
        return false;
    }
    *out = node.asUInt64();
    return true;
}

}  // namespace

TrafficRecorder& TrafficRecorder::Instance() {
    static TrafficRecorder instance;
    return instance;
}

TrafficRecorder::~TrafficRecorder() { Shutdown(); }

void TrafficRecorder::OnRequestThunk(void* context,
                                     const RequestView& view) noexcept {
    static_cast<TrafficRecorder*>(context)->OnRequest(view);
}

void TrafficRecorder::Install() {
    if (config_loaded_ || shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    const char* path = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    if (path == nullptr || *path == '\0') {
        return;
    }
    config_path_ = path;
    std::ifstream input(config_path_);
    if (!input) {
        LOG(ERROR) << "traffic: cannot open config " << config_path_;
        state_ = "error";
        return;
    }
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    if (!Json::parseFromStream(builder, input, &root, &errors)) {
        LOG(ERROR) << "traffic: invalid config: " << errors;
        state_ = "error";
        return;
    }
    if (!root.isObject()) {
        LOG(ERROR) << "traffic: config root must be an object";
        state_ = "error";
        return;
    }
    if (!root["output_dir"].isString() || root["output_dir"].asString().empty()) {
        LOG(ERROR) << "traffic: output_dir is required";
        state_ = "error";
        return;
    }
    output_dir_ = root["output_dir"].asString();
    if (!ParseSize(root["ring_bytes"], "ring_bytes", &ring_bytes_) ||
        !ParseSize(root["max_record_bytes"], "max_record_bytes",
                   &max_record_bytes_) ||
        !ParseSize(root["segment_bytes"], "segment_bytes", &segment_bytes_) ||
        !ParseSize(root["segment_seconds"], "segment_seconds",
                   &segment_seconds_) ||
        !ParseSize(root["block_bytes"], "block_bytes", &block_bytes_) ||
        !ParseSize(root["max_total_bytes"], "max_total_bytes",
                   &max_total_bytes_) ||
        !ParseSize(root["stop_timeout_ms"], "stop_timeout_ms",
                   &stop_timeout_ms_)) {
        state_ = "error";
        return;
    }
    if (root["ring_shards"].isUInt()) {
        ring_shards_ = root["ring_shards"].asUInt();
    }
    if (const char* port = std::getenv("MOONCAKE_TRAFFIC_CONTROL_PORT");
        port != nullptr && *port != '\0') {
        const int parsed = std::atoi(port);
        if (parsed > 0 && parsed < 65536) {
            control_port_ = static_cast<std::uint16_t>(parsed);
        }
    }
    if (root["control"].isObject()) {
        const Json::Value& control = root["control"];
        if (control["host"].isString()) {
            control_host_ = control["host"].asString();
        }
        if (control["port"].isUInt()) {
            const auto port = control["port"].asUInt();
            if (port > 0 && port < 65536) {
                control_port_ = static_cast<std::uint16_t>(port);
            }
        }
        if (control["token"].isString()) {
            control_token_ = control["token"].asString();
        }
    }
    if (control_host_ != "127.0.0.1" && control_host_ != "localhost" &&
        control_host_ != "::1") {
        LOG(ERROR) << "traffic: control.host must be a loopback address";
        state_ = "error";
        return;
    }
    if (root["routes"].isArray()) {
        for (const auto& item : root["routes"]) {
            if (!item.isString()) {
                LOG(ERROR) << "traffic: routes[] must contain strings";
                state_ = "error";
                return;
            }
            const std::string route = item.asString();
            if (route == "*") {
                allow_all_routes_ = true;
                continue;
            }
            char* end = nullptr;
            const unsigned long key = std::strtoul(route.c_str(), &end, 10);
            if (end == route.c_str() || *end != '\0') {
                LOG(ERROR) << "traffic: invalid route key " << route;
                state_ = "error";
                return;
            }
            allowed_routes_.insert(static_cast<std::uint32_t>(key));
        }
    }
    if (ring_shards_ == 0) {
        ring_shards_ = std::max(1u, std::thread::hardware_concurrency());
    }
    if (block_bytes_ == 0 || block_bytes_ > segment_bytes_) {
        LOG(ERROR) << "traffic: block_bytes must be in (0, segment_bytes]";
        state_ = "error";
        return;
    }
    if (max_record_bytes_ == 0 || max_record_bytes_ > block_bytes_) {
        LOG(ERROR) << "traffic: max_record_bytes must be in (0, block_bytes]";
        state_ = "error";
        return;
    }
    if (max_total_bytes_ > 0 && max_total_bytes_ < segment_bytes_) {
        LOG(ERROR) << "traffic: max_total_bytes must be >= segment_bytes";
        state_ = "error";
        return;
    }
    config_loaded_ = true;
    state_ = "idle";
    static std::once_flag atexit_once;
    std::call_once(atexit_once, [] {
        std::atexit([] { TrafficRecorder::Instance().Shutdown(); });
    });
    StartControlServer();
    const char* auto_start = std::getenv("MOONCAKE_TRAFFIC_AUTOSTART");
    if (auto_start != nullptr && std::strcmp(auto_start, "1") == 0) {
        if (!Start()) {
            LOG(ERROR) << "traffic: autostart failed";
        }
    }
}

bool TrafficRecorder::Start() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mu_);
    if (!config_loaded_) {
        return false;
    }
    if (stop_thread_.joinable()) {
        stop_thread_.join();
    }
    if (active_.load(std::memory_order_acquire)) {
        return false;
    }
    stop_timed_out_.store(false, std::memory_order_relaxed);
    stop_deadline_ms_.store(0, std::memory_order_relaxed);
    const std::uint64_t shard_bytes =
        FloorPowerOfTwo(ring_bytes_ / std::max(1u, ring_shards_));
    if (shard_bytes < kMinShardBytes) {
        LOG(ERROR) << "traffic: ring is too small";
        state_ = "error";
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        shards_.clear();
        shards_.reserve(ring_shards_);
        for (std::uint32_t i = 0; i < ring_shards_; ++i) {
            auto shard = std::make_unique<Shard>();
            shard->ring = std::make_unique<char[]>(shard_bytes);
            shard->capacity = shard_bytes;
            shard->mask = shard_bytes - 1;
            shard->state.store(kAccepting, std::memory_order_relaxed);
            shards_.push_back(std::move(shard));
        }
        next_shard_.store(0, std::memory_order_relaxed);
        dropped_no_shard_.store(0, std::memory_order_relaxed);
    }
    block_.clear();
    block_.reserve(block_bytes_);
    sink_.reset();
    sink_ = std::make_unique<FileSink>(segment_bytes_, segment_seconds_,
                                       max_total_bytes_);
    const std::uint64_t start_ms = TrafficNowMs();
    if (!sink_->OpenSession(output_dir_, start_ms)) {
        state_ = "error";
        return false;
    }
    session_dir_ = sink_->session_dir();
    writer_stop_.store(false, std::memory_order_release);
    generation_.fetch_add(1, std::memory_order_acq_rel);
    writer_thread_ = std::thread(&TrafficRecorder::WriterLoop, this);
    state_ = "recording";
    active_.store(true, std::memory_order_release);
    return true;
}

void TrafficRecorder::Stop() {
    StopAsync();
    std::lock_guard<std::mutex> lifecycle(lifecycle_mu_);
    if (stop_thread_.joinable()) {
        stop_thread_.join();
    }
}

void TrafficRecorder::StopAsync() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mu_);
    if (!active_.load(std::memory_order_acquire) ||
        stop_thread_.joinable()) {
        return;
    }
    state_ = "stopping";
    stop_timeout_ms_ =
        stop_timeout_ms_ == 0 ? kDefaultStopTimeoutMs : stop_timeout_ms_;
    stop_deadline_ms_.store(TrafficNowMs() + stop_timeout_ms_,
                            std::memory_order_relaxed);
    stop_thread_ = std::thread(&TrafficRecorder::StopInternal, this);
}

void TrafficRecorder::StopInternal() {
    const std::uint64_t stop_begin_ms = TrafficNowMs();
    active_.store(false, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        for (auto& shard : shards_) {
            shard->state.fetch_and(~kAccepting, std::memory_order_acq_rel);
        }
    }
    while (starting_.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        for (auto& shard : shards_) {
            while ((shard->state.load(std::memory_order_acquire) &
                    kProducerMask) != 0) {
                std::this_thread::yield();
            }
        }
    }
    writer_stop_.store(true, std::memory_order_release);
    writer_cv_.notify_all();
    if (writer_thread_.joinable()) {
        writer_thread_.join();
    }
    if (sink_ != nullptr) {
        {
            std::lock_guard<std::mutex> lock(shards_mu_);
            for (auto& shard : shards_) {
                DrainShardDirect(*shard);
            }
        }
        sink_->Flush();
        sink_->Close();
    }
    const std::uint64_t elapsed_ms = TrafficNowMs() - stop_begin_ms;
    if (elapsed_ms > stop_timeout_ms_) {
        stop_timed_out_.store(true, std::memory_order_relaxed);
    }
    stop_deadline_ms_.store(0, std::memory_order_relaxed);
    state_ = "idle";
}

void TrafficRecorder::OnRequest(const RequestView& view) noexcept {
    if (!active_.load(std::memory_order_acquire)) {
        return;
    }
    static thread_local Shard* tls_shard = nullptr;
    static thread_local std::uint64_t tls_generation = 0;
    const std::uint64_t generation =
        generation_.load(std::memory_order_relaxed);
    if (tls_shard == nullptr || tls_generation != generation) {
        starting_.fetch_add(1, std::memory_order_relaxed);
        if (!active_.load(std::memory_order_acquire)) {
            starting_.fetch_sub(1, std::memory_order_release);
            return;
        }
        std::lock_guard<std::mutex> lock(shards_mu_);
        const std::uint64_t index =
            next_shard_.fetch_add(1, std::memory_order_relaxed);
        if (index >= shards_.size()) {
            dropped_no_shard_.fetch_add(1, std::memory_order_relaxed);
            starting_.fetch_sub(1, std::memory_order_release);
            return;
        }
        tls_shard = shards_[index].get();
        tls_generation = generation;
        starting_.fetch_sub(1, std::memory_order_release);
    }
    Shard& shard = *tls_shard;
    shard.observed.fetch_add(1, std::memory_order_relaxed);
    if (!allow_all_routes_ &&
        allowed_routes_.find(view.route_key) == allowed_routes_.end()) {
        shard.dropped_filtered.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    const std::uint64_t total =
        kTrafficRecordHeaderSize + static_cast<std::uint64_t>(view.payload.size());
    if (total > max_record_bytes_ || total > shard.capacity) {
        shard.dropped_oversize.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    std::uint64_t state = shard.state.load(std::memory_order_acquire);
    for (;;) {
        if ((state & kAccepting) == 0) {
            return;
        }
        if (shard.state.compare_exchange_weak(state, state + 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
            break;
        }
    }

    std::uint64_t head = shard.head.load(std::memory_order_relaxed);
    const std::uint64_t tail = shard.tail.load(std::memory_order_acquire);
    std::uint64_t used = head - tail;
    std::uint64_t index = head & shard.mask;
    if (index + total > shard.capacity) {
        const std::uint64_t remaining = shard.capacity - index;
        if (used + remaining > shard.capacity) {
            shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
            shard.state.fetch_sub(1, std::memory_order_release);
            return;
        }
        if (remaining >= kTrafficPadSize) {
            const std::uint32_t pad_len = static_cast<std::uint32_t>(remaining);
            const std::uint32_t pad_magic = kTrafficPadMagic;
            std::memcpy(shard.ring.get() + index, &pad_len, sizeof(pad_len));
            std::memcpy(shard.ring.get() + index + sizeof(pad_len), &pad_magic,
                        sizeof(pad_magic));
        }
        head += remaining;
        used += remaining;
        index = 0;
    }
    if (total > shard.capacity - used) {
        shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
        shard.state.fetch_sub(1, std::memory_order_release);
        return;
    }

    TrafficRecordHeader header;
    header.total_len = static_cast<std::uint32_t>(total);
    header.route_key = view.route_key;
    header.conn_id = view.conn_id;
    header.seq_num = view.seq_num;
    header.payload_len = static_cast<std::uint32_t>(view.payload.size());
    header.timestamp_ms = static_cast<std::uint32_t>(TrafficNowMs());
    char* base = shard.ring.get() + index;
    std::memcpy(base, &header, sizeof(header));
    if (!view.payload.empty()) {
        std::memcpy(base + sizeof(header), view.payload.data(),
                    view.payload.size());
    }
    shard.head.store(head + total, std::memory_order_release);
    shard.recorded.fetch_add(1, std::memory_order_relaxed);
    shard.state.fetch_sub(1, std::memory_order_release);
}

void TrafficRecorder::WriterLoop() {
    if (const char* cpu_env = std::getenv("MOONCAKE_TRAFFIC_WRITER_CPU")) {
        const long cpu = std::strtol(cpu_env, nullptr, 10);
        if (cpu >= 0 && cpu < CPU_SETSIZE) {
            cpu_set_t set;
            CPU_ZERO(&set);
            CPU_SET(static_cast<int>(cpu), &set);
            pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
        }
    }
    while (true) {
        {
            std::unique_lock<std::mutex> lock(writer_mu_);
            writer_cv_.wait_for(lock, std::chrono::milliseconds(1), [this] {
                return writer_stop_.load(std::memory_order_acquire);
            });
        }
        {
            std::lock_guard<std::mutex> lock(shards_mu_);
            for (auto& shard : shards_) {
                DrainShardDirect(*shard);
            }
        }
        if (sink_ != nullptr && sink_->failed()) {
            active_.store(false, std::memory_order_release);
            state_ = "error";
            writer_stop_.store(true, std::memory_order_release);
            break;
        }
        if (writer_stop_.load(std::memory_order_acquire)) {
            break;
        }
    }
}

void TrafficRecorder::DrainShardDirect(Shard& shard) {
    std::vector<struct iovec> iov;
    iov.reserve(256);
    std::uint64_t bytes = 0;
    std::uint32_t records = 0;
    std::uint64_t tail = shard.tail.load(std::memory_order_acquire);
    const std::uint64_t head = shard.head.load(std::memory_order_acquire);
    std::uint64_t commit_tail = tail;
    while (tail != head) {
        const std::uint64_t index = tail & shard.mask;
        if (index + kTrafficPadSize > shard.capacity) {
            if (!FlushRecords(shard, iov, bytes, records, commit_tail)) {
                return;
            }
            bytes = 0;
            records = 0;
            tail += shard.capacity - index;
            commit_tail = tail;
            shard.tail.store(tail, std::memory_order_release);
            continue;
        }
        std::uint32_t pad_len = 0;
        std::uint32_t pad_magic = 0;
        std::memcpy(&pad_len, shard.ring.get() + index, sizeof(pad_len));
        std::memcpy(&pad_magic, shard.ring.get() + index + sizeof(pad_len),
                    sizeof(pad_magic));
        if (pad_magic == kTrafficPadMagic) {
            if (pad_len < kTrafficPadSize || pad_len > shard.capacity - index) {
                shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
                shard.tail.store(head, std::memory_order_release);
                return;
            }
            if (!FlushRecords(shard, iov, bytes, records, commit_tail)) {
                return;
            }
            bytes = 0;
            records = 0;
            tail += pad_len;
            commit_tail = tail;
            shard.tail.store(tail, std::memory_order_release);
            continue;
        }
        TrafficRecordHeader header;
        std::memcpy(&header, shard.ring.get() + index, sizeof(header));
        if (header.total_len < kTrafficRecordHeaderSize ||
            header.total_len > shard.capacity ||
            index + header.total_len > shard.capacity) {
            shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
            shard.tail.store(head, std::memory_order_release);
            return;
        }
        if (records > 0 &&
            (bytes + header.total_len > block_bytes_ ||
             iov.size() >= static_cast<std::size_t>(IOV_MAX))) {
            if (!FlushRecords(shard, iov, bytes, records, commit_tail)) {
                return;
            }
            bytes = 0;
            records = 0;
        }
        iov.push_back({shard.ring.get() + index, header.total_len});
        bytes += header.total_len;
        ++records;
        tail += header.total_len;
        commit_tail = tail;
    }
    FlushRecords(shard, iov, bytes, records, commit_tail);
}

bool TrafficRecorder::FlushRecords(Shard& shard,
                                   std::vector<struct iovec>& iov,
                                   std::uint64_t bytes,
                                   std::uint32_t records,
                                   std::uint64_t commit_tail) {
    if (iov.empty()) {
        shard.tail.store(commit_tail, std::memory_order_release);
        return true;
    }
    if (sink_ == nullptr ||
        !sink_->WriteRecords(iov.data(), static_cast<int>(iov.size()), bytes,
                             records)) {
        LOG(ERROR) << "traffic: write records failed";
        return false;
    }
    shard.tail.store(commit_tail, std::memory_order_release);
    iov.clear();
    return true;
}

void TrafficRecorder::StartControlServer() {
    using namespace coro_http;
    try {
        control_server_ = std::make_unique<coro_http_server>(
            2, control_port_, control_host_);
        control_server_->set_http_handler<GET>(
            "/v1/recording/status",
            [this](coro_http_request& req, coro_http_response& resp) {
                if (!control_token_.empty() &&
                    req.get_header_value("Authorization") !=
                        "Bearer " + control_token_) {
                    resp.set_status_and_content(status_type::unauthorized,
                                                "{\"error\":\"unauthorized\"}");
                    return;
                }
                resp.add_header("Content-Type", "application/json");
                resp.set_status_and_content(status_type::ok, StatusJson());
            });
        control_server_->set_http_handler<POST>(
            "/v1/recording/start",
            [this](coro_http_request& req, coro_http_response& resp) {
                if (!control_token_.empty() &&
                    req.get_header_value("Authorization") !=
                        "Bearer " + control_token_) {
                    resp.set_status_and_content(status_type::unauthorized,
                                                "{\"error\":\"unauthorized\"}");
                    return;
                }
                if (!Start()) {
                    resp.set_status_and_content(status_type::conflict,
                                                "{\"error\":\"busy\"}");
                    return;
                }
                resp.set_status_and_content(status_type::ok, StatusJson());
            });
        control_server_->set_http_handler<POST>(
            "/v1/recording/stop",
            [this](coro_http_request& req, coro_http_response& resp) {
                if (!control_token_.empty() &&
                    req.get_header_value("Authorization") !=
                        "Bearer " + control_token_) {
                    resp.set_status_and_content(status_type::unauthorized,
                                                "{\"error\":\"unauthorized\"}");
                    return;
                }
                StopAsync();
                resp.set_status_and_content(status_type::accepted,
                                            "{\"state\":\"stopping\"}");
            });
        auto ec = control_server_->async_start();
        if (ec.hasResult()) {
            LOG(ERROR) << "traffic: control server failed on "
                       << control_host_ << ":" << control_port_ << ": "
                       << ec.value().message();
            state_ = "error";
            control_server_.reset();
            return;
        }
        control_started_.store(true, std::memory_order_release);
        LOG(INFO) << "traffic: control server on " << control_host_ << ":"
                  << control_port_;
    } catch (const std::exception& e) {
        LOG(ERROR) << "traffic: control server exception: " << e.what();
        state_ = "error";
        control_server_.reset();
    }
}

std::string TrafficRecorder::StatusJson() const {
    const RecorderStatus status = GetStatus();
    Json::Value root;
    root["state"] = status.state;
    root["observed_requests"] =
        static_cast<Json::UInt64>(status.observed_requests);
    root["recorded_requests"] =
        static_cast<Json::UInt64>(status.recorded_requests);
    root["dropped_requests"] =
        static_cast<Json::UInt64>(status.dropped_requests);
    root["dropped_ring"] = static_cast<Json::UInt64>(status.dropped_ring);
    root["dropped_filtered"] =
        static_cast<Json::UInt64>(status.dropped_filtered);
    root["dropped_oversize"] =
        static_cast<Json::UInt64>(status.dropped_oversize);
    root["bytes_written"] = static_cast<Json::UInt64>(status.bytes_written);
    root["segments"] = static_cast<Json::UInt64>(status.segments);
    root["ring_used_bytes"] =
        static_cast<Json::UInt64>(status.ring_used_bytes);
    root["ring_capacity_bytes"] =
        static_cast<Json::UInt64>(status.ring_capacity_bytes);
    root["stop_timed_out"] = status.stop_timed_out;
    root["format_complete"] = status.format_complete;
    root["capture_complete"] = status.capture_complete;
    root["session_dir"] = status.session_dir;
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

RecorderStatus TrafficRecorder::GetStatus() const {
    RecorderStatus status;
    {
        std::lock_guard<std::mutex> lock(status_mu_);
        status.state = state_;
        status.session_dir = session_dir_;
    }
    std::uint64_t observed = 0;
    std::uint64_t recorded = 0;
    std::uint64_t dropped_ring = 0;
    std::uint64_t dropped_filtered = 0;
    std::uint64_t dropped_oversize = 0;
    std::uint64_t used = 0;
    std::uint64_t capacity = 0;
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        for (const auto& shard : shards_) {
            observed += shard->observed.load(std::memory_order_relaxed);
            recorded += shard->recorded.load(std::memory_order_relaxed);
            dropped_ring +=
                shard->dropped_ring.load(std::memory_order_relaxed);
            dropped_filtered +=
                shard->dropped_filtered.load(std::memory_order_relaxed);
            dropped_oversize +=
                shard->dropped_oversize.load(std::memory_order_relaxed);
            used += shard->head.load(std::memory_order_relaxed) -
                    shard->tail.load(std::memory_order_relaxed);
            capacity += shard->capacity;
        }
    }
    status.observed_requests = observed;
    status.recorded_requests = recorded;
    status.dropped_ring =
        dropped_ring + dropped_no_shard_.load(std::memory_order_relaxed);
    status.dropped_filtered = dropped_filtered;
    status.dropped_oversize = dropped_oversize;
    status.dropped_requests =
        status.dropped_ring + dropped_filtered + dropped_oversize;
    status.ring_used_bytes = used;
    status.ring_capacity_bytes = capacity;
    if (sink_ != nullptr) {
        status.bytes_written = sink_->bytes_written();
        status.segments = sink_->segments();
        status.format_complete = !sink_->failed();
    }
    status.stop_timed_out = stop_timed_out_.load(std::memory_order_relaxed);
    const std::uint64_t deadline =
        stop_deadline_ms_.load(std::memory_order_relaxed);
    if (status.state == "stopping" && deadline != 0 &&
        TrafficNowMs() >= deadline) {
        status.stop_timed_out = true;
    }
    status.capture_complete =
        status.dropped_requests == 0 && status.format_complete &&
        !status.stop_timed_out;
    return status;
}

void TrafficRecorder::Shutdown() {
    if (shutdown_.exchange(true)) {
        return;
    }
    Stop();
    if (control_server_ != nullptr) {
        control_server_->stop();
        control_server_.reset();
    }
}

}  // namespace mooncake::traffic
