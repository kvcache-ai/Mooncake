#include "traffic/traffic_recorder.h"

#include <ylt/coro_http/coro_http_server.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif
#include <sys/uio.h>
#include <limits.h>
#include <new>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "traffic/file_sink.h"
#include "traffic/traffic_admin.h"
#include "traffic/traffic_format.h"

namespace mooncake::traffic {

namespace {

constexpr std::uint64_t kAccepting = 1ull << 63;
constexpr std::uint64_t kProducerMask = ~kAccepting;
constexpr std::uint64_t kMinShardBytes = 64ull * 1024;
constexpr std::uint64_t kDefaultStopTimeoutMs = 30000;
constexpr std::uint32_t kRingPadMagic = 0x50414431;  // PAD1
constexpr std::size_t kRingPadSize = 8;

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

}  // namespace

struct alignas(64) TrafficRecorder::Shard {
    std::unique_ptr<char[]> ring;
    std::uint64_t capacity = 0;
    std::uint64_t mask = 0;
    alignas(64) std::atomic<std::uint64_t> head{0};
    std::atomic<std::uint64_t> state{1};
    std::atomic<std::uint64_t> observed{0};
    std::atomic<std::uint64_t> recorded{0};
    std::atomic<std::uint64_t> filtered{0};
    alignas(64) std::atomic<std::uint64_t> tail{0};
    alignas(64) std::atomic<std::uint64_t> dropped{0};
    alignas(64) std::atomic<std::uint64_t> redaction_failed{0};
};

namespace {

struct RingRecord {
    TrafficRecordHeader header;
    const char* data = nullptr;
};

enum class RingReadResult { kEnd, kSkip, kRecord, kCorrupt };

template <typename ShardT>
RingReadResult NextRingRecord(ShardT& shard, std::uint64_t head,
                              std::uint64_t* tail, RingRecord* record) {
    if (*tail == head) {
        return RingReadResult::kEnd;
    }
    const std::uint64_t index = *tail & shard.mask;
    if (index + kRingPadSize > shard.capacity) {
        *tail += shard.capacity - index;
        return RingReadResult::kSkip;
    }
    std::uint32_t pad_len = 0;
    std::uint32_t pad_magic = 0;
    std::memcpy(&pad_len, shard.ring.get() + index, sizeof(pad_len));
    std::memcpy(&pad_magic, shard.ring.get() + index + sizeof(pad_len),
                sizeof(pad_magic));
    if (pad_magic == kRingPadMagic) {
        if (pad_len < kRingPadSize || pad_len > shard.capacity - index) {
            return RingReadResult::kCorrupt;
        }
        *tail += pad_len;
        return RingReadResult::kSkip;
    }
    std::memcpy(&record->header, shard.ring.get() + index,
                sizeof(record->header));
    const auto& header = record->header;
    if (header.total_len < kTrafficRecordHeaderSize ||
        header.total_len > shard.capacity ||
        index + header.total_len > shard.capacity ||
        header.payload_len != header.total_len - kTrafficRecordHeaderSize) {
        return RingReadResult::kCorrupt;
    }
    record->data = shard.ring.get() + index;
    *tail += header.total_len;
    return RingReadResult::kRecord;
}

}  // namespace

TrafficRecorder& TrafficRecorder::Instance() {
    static TrafficRecorder instance;
    return instance;
}

TrafficRecorder::TrafficRecorder() { ResetConfig(); }

TrafficRecorder::~TrafficRecorder() { Shutdown(); }

void TrafficRecorder::ResetConfig() {
    config_ = RecorderConfig{};
    registered_routes_ = RouteNameRegistry{};
}

void TrafficRecorder::Install(std::size_t rpc_threads,
                              RedactionRegistry redaction_registry,
                              RouteNameRegistry route_names) {
    if (config_loaded_ || shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    const char* path = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    if (path == nullptr || *path == '\0') {
        return;
    }
    redaction_registry_ = std::move(redaction_registry);
    ResetConfig();
    registered_routes_ = std::move(route_names);
    const std::string config_path(path);
    RecorderConfig config;
    std::string error;
    if (!LoadRecordingConfig(config_path, rpc_threads, redaction_registry_,
                             registered_routes_, &config, &error)) {
        Fail(error);
        return;
    }
    config_ = std::move(config);
    LOG(INFO) << "traffic: recording to " << config_.output_dir << " (config "
              << config_path << ")";
    config_loaded_ = true;
    SetState(State::kIdle);
    static std::once_flag atexit_once;
    std::call_once(atexit_once, [] {
        std::atexit([] { TrafficRecorder::Instance().Shutdown(); });
    });
    StartControlServer();
    {
        std::lock_guard<std::mutex> status_lock(status_mu_);
        if (state_ == State::kError) {
            return;
        }
    }
    const char* auto_start = std::getenv("MOONCAKE_TRAFFIC_AUTOSTART");
    if (auto_start == nullptr || std::strcmp(auto_start, "0") != 0) {
        if (!Start()) {
            LOG(ERROR) << "traffic: autostart failed";
        }
    }
}

bool TrafficRecorder::Start() {
    std::unique_lock<std::mutex> lifecycle(lifecycle_mu_);
    if (!config_loaded_ || shutdown_.load(std::memory_order_acquire)) {
        return false;
    }
    if (stop_thread_.joinable()) {
        stop_thread_.join();
    }
    if (active_.load(std::memory_order_acquire)) {
        return false;
    }
    if (writer_thread_.joinable()) {
        lifecycle.unlock();
        Stop();
        lifecycle.lock();
        // Another start may have run while the lock was released; leave the
        // session it opened alone instead of replacing it.
        if (active_.load(std::memory_order_acquire)) {
            return false;
        }
    }
    writer_failed_.store(false, std::memory_order_relaxed);
    stop_timed_out_.store(false, std::memory_order_relaxed);
    stop_deadline_ms_.store(0, std::memory_order_relaxed);
    const std::uint64_t shard_bytes =
        FloorPowerOfTwo(config_.ring_bytes / std::max(1u, config_.ring_shards));
    if (shard_bytes < kMinShardBytes) {
        return Fail("ring_bytes is too small for the number of RPC threads");
    }
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        shards_.clear();
        shards_.reserve(config_.ring_shards);
        for (std::uint32_t i = 0; i < config_.ring_shards; ++i) {
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
    {
        std::lock_guard<std::mutex> status_lock(status_mu_);
        sink_.reset();
        sink_ = std::make_unique<FileSink>(
            config_.segment_bytes, config_.segment_seconds,
            config_.max_total_bytes, config_.redaction_mode);
        const std::uint64_t start_ms = TrafficNowMs();
        if (!sink_->OpenSession(config_.output_dir, start_ms)) {
            state_ = State::kError;
            return false;
        }
        session_dir_ = sink_->session_dir();
    }
    writer_stop_.store(false, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(route_names_mu_);
        route_names_.clear();
    }
    generation_.fetch_add(1, std::memory_order_acq_rel);
    writer_thread_ = std::thread(&TrafficRecorder::WriterLoop, this);
    SetState(State::kRecording);
    active_.store(true, std::memory_order_release);
    return true;
}

void TrafficRecorder::Stop() {
    static_cast<void>(StopAsync());
    std::lock_guard<std::mutex> lifecycle(lifecycle_mu_);
    if (stop_thread_.joinable()) {
        stop_thread_.join();
    }
}

bool TrafficRecorder::StopAsync() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mu_);
    if (stop_thread_.joinable()) {
        if (stop_in_progress_.load(std::memory_order_acquire)) {
            return true;
        }
        // The previous stop has finished; reap its thread so that this call
        // reports the state the recorder is actually in.
        stop_thread_.join();
    }
    if (!active_.load(std::memory_order_acquire) &&
        !writer_thread_.joinable()) {
        return false;
    }
    SetState(State::kStopping);
    config_.stop_timeout_ms = config_.stop_timeout_ms == 0
                                  ? kDefaultStopTimeoutMs
                                  : config_.stop_timeout_ms;
    stop_deadline_ms_.store(TrafficNowMs() + config_.stop_timeout_ms,
                            std::memory_order_relaxed);
    stop_in_progress_.store(true, std::memory_order_release);
    stop_thread_ = std::thread(&TrafficRecorder::StopInternal, this);
    return true;
}

void TrafficRecorder::StopInternal() {
    const std::uint64_t stop_begin_ms = TrafficNowMs();
    active_.store(false, std::memory_order_release);
    while (in_request_.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        for (auto& shard : shards_) {
            shard->state.fetch_and(~kAccepting, std::memory_order_acq_rel);
        }
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
        std::vector<struct iovec> iov;
        iov.reserve(static_cast<std::size_t>(IOV_MAX));
        for (auto& shard : shards_) {
            const bool ok = config_.redaction_mode == RedactionMode::kNone
                                ? DrainShardDirect(*shard, iov)
                                : DrainShardRedacted(*shard);
            // A session that stopped at its size limit is finalized normally.
            if (!ok && !sink_->quota_reached()) {
                writer_failed_.store(true, std::memory_order_relaxed);
            }
        }
        sink_->Close();
    }
    const std::uint64_t elapsed_ms = TrafficNowMs() - stop_begin_ms;
    if (elapsed_ms > config_.stop_timeout_ms) {
        stop_timed_out_.store(true, std::memory_order_relaxed);
    }
    stop_deadline_ms_.store(0, std::memory_order_relaxed);
    const bool failed = writer_failed_.load(std::memory_order_relaxed) ||
                        (sink_ != nullptr && sink_->failed());
    SetState(failed ? State::kError : State::kIdle);
    const RecorderStatus status = GetStatus();
    if (status.recorded_requests == 0 && status.filtered_requests > 0) {
        LOG(WARNING) << "traffic: session " << session_dir_
                     << " recorded no requests; " << status.filtered_requests
                     << " requests did not match the routes filter";
    }
    if (status.redaction_failed_requests > 0) {
        LOG(WARNING) << "traffic: " << status.redaction_failed_requests
                     << " requests could not be redacted and were not written";
    }
    const std::uint64_t unbound =
        dropped_no_shard_.load(std::memory_order_relaxed);
    if (unbound > 0) {
        LOG(WARNING) << "traffic: " << unbound
                     << " requests found no ring shard; ring_shards is sized "
                        "from rpc_thread_num";
    }
    if (!session_dir_.empty()) {
        WriteSummaryJson(session_dir_, status);
        std::lock_guard<std::mutex> lock(route_names_mu_);
        WriteRouteNamesJson(session_dir_, route_names_);
    }
    stop_in_progress_.store(false, std::memory_order_release);
}

void TrafficRecorder::SetState(State state) {
    std::lock_guard<std::mutex> lock(status_mu_);
    state_ = state;
}

const char* TrafficRecorder::StateName(State state) {
    switch (state) {
        case State::kIdle:
            return "idle";
        case State::kRecording:
            return "recording";
        case State::kStopping:
            return "stopping";
        case State::kError:
            return "error";
        case State::kDisabled:
            break;
    }
    return "disabled";
}

bool TrafficRecorder::Fail(const std::string& message) {
    LOG(ERROR) << "traffic: " << message;
    SetState(State::kError);
    return false;
}

bool TrafficRecorder::Recording() const noexcept {
    return active_.load(std::memory_order_acquire);
}

void TrafficRecorder::RememberRoute(const RequestView& view) noexcept {
    const std::string_view name = registered_routes_.NameOf(view.route_key);
    if (name.empty()) {
        return;
    }
    static thread_local std::uint32_t last_key = 0;
    static thread_local const char* last_name = nullptr;
    if (last_key == view.route_key && last_name == name.data()) {
        return;
    }
    std::lock_guard<std::mutex> lock(route_names_mu_);
    route_names_.try_emplace(view.route_key, name);
    last_key = view.route_key;
    last_name = name.data();
}

TrafficRecorder::Shard* TrafficRecorder::BindShard() noexcept {
    static thread_local Shard* tls_shard = nullptr;
    static thread_local std::uint64_t tls_generation = 0;
    const std::uint64_t generation =
        generation_.load(std::memory_order_relaxed);
    if (tls_shard != nullptr && tls_generation == generation) {
        return tls_shard;
    }
    // shards_ is immutable while active_; binding must not wait on the writer
    // lock on the request path.
    const std::uint64_t index =
        next_shard_.fetch_add(1, std::memory_order_relaxed);
    if (index >= shards_.size()) {
        dropped_no_shard_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    tls_shard = shards_[index].get();
    tls_generation = generation;
    return tls_shard;
}

bool TrafficRecorder::RouteAllowed(const RequestView& view) const noexcept {
    if (config_.allowed_routes.find(view.route_key) !=
        config_.allowed_routes.end()) {
        return true;
    }
    if (config_.allowed_route_names.empty()) {
        return false;
    }
    const std::string_view name = registered_routes_.NameOf(view.route_key);
    for (const auto& allowed : config_.allowed_route_names) {
        if (allowed == name) {
            return true;
        }
    }
    return false;
}

void TrafficRecorder::AppendRecord(Shard& shard, const RequestView& view,
                                   std::uint64_t total) noexcept {
    // state carries the number of in-flight producers in its low bits and a
    // stop flag in the top bit: taking a slot and observing a stop are then a
    // single atomic operation.
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
    struct ProducerGuard {
        std::atomic<std::uint64_t>& state;
        ~ProducerGuard() { state.fetch_sub(1, std::memory_order_release); }
    } producer_guard{shard.state};

    std::uint64_t head = shard.head.load(std::memory_order_relaxed);
    const std::uint64_t tail = shard.tail.load(std::memory_order_acquire);
    std::uint64_t used = head - tail;
    std::uint64_t index = head & shard.mask;
    if (index + total > shard.capacity) {
        const std::uint64_t remaining = shard.capacity - index;
        if (used + remaining > shard.capacity) {
            shard.dropped.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        if (remaining >= kRingPadSize) {
            const std::uint32_t pad_len = static_cast<std::uint32_t>(remaining);
            const std::uint32_t pad_magic = kRingPadMagic;
            std::memcpy(shard.ring.get() + index, &pad_len, sizeof(pad_len));
            std::memcpy(shard.ring.get() + index + sizeof(pad_len), &pad_magic,
                        sizeof(pad_magic));
        }
        head += remaining;
        used += remaining;
        index = 0;
    }
    if (total > shard.capacity - used) {
        shard.dropped.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    TrafficRecordHeader header;
    header.total_len = static_cast<std::uint32_t>(total);
    header.route_key = view.route_key;
    header.conn_id = view.conn_id;
    header.seq_num = view.seq_num;
    header.payload_len = static_cast<std::uint32_t>(view.payload.size());
    header.timestamp_ms = TrafficNowMs();
    char* base = shard.ring.get() + index;
    std::memcpy(base, &header, sizeof(header));
    if (!view.payload.empty()) {
        std::memcpy(base + sizeof(header), view.payload.data(),
                    view.payload.size());
    }
    shard.head.store(head + total, std::memory_order_release);
    shard.recorded.fetch_add(1, std::memory_order_relaxed);
}

void TrafficRecorder::OnRequest(const RequestView& view) noexcept {
    in_request_.fetch_add(1, std::memory_order_acq_rel);
    struct InRequestGuard {
        std::atomic<std::uint32_t>& count;
        ~InRequestGuard() { count.fetch_sub(1, std::memory_order_acq_rel); }
    } in_request_guard{in_request_};
    if (!active_.load(std::memory_order_acquire)) {
        return;
    }
    Shard* shard = BindShard();
    if (shard == nullptr) {
        return;
    }
    shard->observed.fetch_add(1, std::memory_order_relaxed);
    const bool has_route_filter =
        !config_.allowed_routes.empty() || !config_.allowed_route_names.empty();
    if (has_route_filter && !RouteAllowed(view)) {
        shard->filtered.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (view.attach_length != 0) {
        // The replay client cannot resend attachments, so keep the record
        // consistent with what replay can reproduce.
        shard->dropped.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    const std::uint64_t total = kTrafficRecordHeaderSize +
                                static_cast<std::uint64_t>(view.payload.size());
    if (total > config_.max_record_bytes || total > shard->capacity) {
        shard->dropped.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    RememberRoute(view);
    AppendRecord(*shard, view, total);
}

void TrafficRecorder::StopForWriterFailure() {
    writer_failed_.store(true, std::memory_order_relaxed);
    active_.store(false, std::memory_order_release);
    SetState(State::kError);
    writer_stop_.store(true, std::memory_order_release);
}

void TrafficRecorder::WriterLoop() {
#if defined(__linux__)
    if (const char* cpu_env = std::getenv("MOONCAKE_TRAFFIC_WRITER_CPU")) {
        const long cpu = std::strtol(cpu_env, nullptr, 10);
        if (cpu >= 0 && cpu < CPU_SETSIZE) {
            cpu_set_t set;
            CPU_ZERO(&set);
            CPU_SET(static_cast<int>(cpu), &set);
            pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
        }
    }
#endif
    std::vector<struct iovec> iov;
    iov.reserve(static_cast<std::size_t>(IOV_MAX));
    while (true) {
        {
            std::unique_lock<std::mutex> lock(writer_mu_);
            writer_cv_.wait_for(lock, std::chrono::milliseconds(1), [this] {
                return writer_stop_.load(std::memory_order_acquire);
            });
        }
        // Start() joins the previous writer before replacing shards_, so the
        // shard list is stable for the lifetime of this thread.
        for (auto& shard : shards_) {
            const bool ok = config_.redaction_mode == RedactionMode::kNone
                                ? DrainShardDirect(*shard, iov)
                                : DrainShardRedacted(*shard);
            if (!ok) {
                if (sink_ != nullptr && sink_->quota_reached()) {
                    // The session hit its size limit. Stop accepting records;
                    // the stop path finalizes the segment, so the file stays
                    // complete.
                    active_.store(false, std::memory_order_release);
                    writer_stop_.store(true, std::memory_order_release);
                    SetState(State::kIdle);
                    return;
                }
                StopForWriterFailure();
                return;
            }
        }
        if (sink_ != nullptr && sink_->failed()) {
            StopForWriterFailure();
            break;
        }
        if (writer_stop_.load(std::memory_order_acquire)) {
            break;
        }
    }
}

bool TrafficRecorder::DrainShardDirect(Shard& shard,
                                       std::vector<struct iovec>& iov) {
    std::uint64_t tail = shard.tail.load(std::memory_order_acquire);
    const std::uint64_t head = shard.head.load(std::memory_order_acquire);
    if (tail == head) {
        return true;
    }
    iov.clear();
    std::uint64_t bytes = 0;
    std::uint32_t records = 0;
    std::uint64_t commit_tail = tail;
    RingRecord record;
    for (;;) {
        const RingReadResult result =
            NextRingRecord(shard, head, &tail, &record);
        if (result == RingReadResult::kEnd) {
            break;
        }
        if (result == RingReadResult::kCorrupt) {
            shard.dropped.fetch_add(1, std::memory_order_relaxed);
            if (!FlushRecords(shard, iov, bytes, records, commit_tail)) {
                return false;
            }
            shard.tail.store(head, std::memory_order_release);
            return true;
        }
        if (result == RingReadResult::kSkip) {
            if (!FlushRecords(shard, iov, bytes, records, commit_tail)) {
                return false;
            }
            bytes = 0;
            records = 0;
            commit_tail = tail;
            continue;
        }
        if (records > 0 &&
            (bytes + record.header.total_len > config_.block_bytes ||
             iov.size() >= static_cast<std::size_t>(IOV_MAX))) {
            if (!FlushRecords(shard, iov, bytes, records, commit_tail)) {
                return false;
            }
            bytes = 0;
            records = 0;
        }
        iov.push_back(
            {const_cast<char*>(record.data), record.header.total_len});
        bytes += record.header.total_len;
        ++records;
        commit_tail = tail;
    }
    return FlushRecords(shard, iov, bytes, records, commit_tail);
}

bool TrafficRecorder::DrainShardRedacted(Shard& shard) {
    std::uint64_t tail = shard.tail.load(std::memory_order_acquire);
    const std::uint64_t head = shard.head.load(std::memory_order_acquire);
    std::string redacted;
    RingRecord record;
    // Redaction rewrites each payload, so records are copied into a buffer and
    // written in blocks, matching the batching of the direct path.
    redacted_block_.clear();
    std::uint32_t block_records = 0;
    std::uint64_t commit_tail = tail;
    auto flush = [&]() {
        if (redacted_block_.empty()) {
            return true;
        }
        struct iovec iov{redacted_block_.data(), redacted_block_.size()};
        if (sink_ == nullptr ||
            !sink_->WriteRecords(&iov, 1, redacted_block_.size(),
                                 block_records)) {
            return false;
        }
        redacted_block_.clear();
        block_records = 0;
        shard.tail.store(commit_tail, std::memory_order_release);
        return true;
    };
    while (true) {
        const RingReadResult result =
            NextRingRecord(shard, head, &tail, &record);
        if (result == RingReadResult::kEnd) {
            if (!flush()) {
                return false;
            }
            shard.tail.store(commit_tail, std::memory_order_release);
            return true;
        }
        if (result == RingReadResult::kCorrupt) {
            shard.dropped.fetch_add(1, std::memory_order_relaxed);
            if (!flush()) {
                return false;
            }
            shard.tail.store(head, std::memory_order_release);
            return true;
        }
        if (result == RingReadResult::kSkip) {
            // Padding carries no record, so the committed tail can move past
            // it without waiting for a write.
            commit_tail = tail;
            continue;
        }
        const char* body = record.data + sizeof(TrafficRecordHeader);
        redacted.clear();
        bool ok = true;
        if (config_.redaction_mode == RedactionMode::kRedacted) {
            ok = redaction_registry_.Redact(
                record.header.route_key,
                std::string_view(body, record.header.payload_len), redacted);
        }
        if (!ok) {
            shard.redaction_failed.fetch_add(1, std::memory_order_relaxed);
            commit_tail = tail;
            continue;
        }
        const std::uint64_t record_bytes =
            kTrafficRecordHeaderSize + redacted.size();
        if (record_bytes > config_.block_bytes) {
            // A redactor must not expand a payload past the block size.
            shard.redaction_failed.fetch_add(1, std::memory_order_relaxed);
            commit_tail = tail;
            continue;
        }
        if (redacted_block_.size() + record_bytes > config_.block_bytes &&
            !flush()) {
            return false;
        }
        TrafficRecordHeader output = record.header;
        output.payload_len = static_cast<std::uint32_t>(redacted.size());
        output.total_len = static_cast<std::uint32_t>(record_bytes);
        const char* header_bytes = reinterpret_cast<const char*>(&output);
        redacted_block_.insert(redacted_block_.end(), header_bytes,
                               header_bytes + sizeof(output));
        redacted_block_.insert(redacted_block_.end(), redacted.begin(),
                               redacted.end());
        ++block_records;
        commit_tail = tail;
    }
}

bool TrafficRecorder::FlushRecords(Shard& shard, std::vector<struct iovec>& iov,
                                   std::uint64_t bytes, std::uint32_t records,
                                   std::uint64_t commit_tail) {
    if (iov.empty()) {
        shard.tail.store(commit_tail, std::memory_order_release);
        return true;
    }
    if (sink_ == nullptr ||
        !sink_->WriteRecords(iov.data(), static_cast<int>(iov.size()), bytes,
                             records)) {
        if (sink_ != nullptr && sink_->quota_reached()) {
            // The sink already reported the size limit.
            return false;
        }
        LOG(ERROR) << "traffic: write records failed";
        return false;
    }
    shard.tail.store(commit_tail, std::memory_order_release);
    iov.clear();
    return true;
}

void TrafficRecorder::StartControlServer() {
    control_server_ = traffic::StartControlServer(*this, config_.control_host,
                                                  config_.control_port,
                                                  config_.control_token);
    if (control_server_ == nullptr) {
        // StartControlServer already logged the address it tried.
        SetState(State::kError);
    }
}

RecorderStatus TrafficRecorder::GetStatus() const {
    RecorderStatus status;
    State state = State::kDisabled;
    {
        std::lock_guard<std::mutex> lock(status_mu_);
        state = state_;
        status.state = StateName(state);
        status.session_dir = session_dir_;
    }
    std::uint64_t used = 0;
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        for (const auto& shard : shards_) {
            status.observed_requests +=
                shard->observed.load(std::memory_order_relaxed);
            status.recorded_requests +=
                shard->recorded.load(std::memory_order_relaxed);
            status.filtered_requests +=
                shard->filtered.load(std::memory_order_relaxed);
            status.dropped_requests +=
                shard->dropped.load(std::memory_order_relaxed);
            status.redaction_failed_requests +=
                shard->redaction_failed.load(std::memory_order_relaxed);
            used += shard->head.load(std::memory_order_relaxed) -
                    shard->tail.load(std::memory_order_relaxed);
            status.ring_capacity_bytes += shard->capacity;
        }
    }
    status.ring_used_bytes = used;
    status.dropped_requests +=
        dropped_no_shard_.load(std::memory_order_relaxed);
    bool quota_reached = false;
    {
        std::lock_guard<std::mutex> status_lock(status_mu_);
        if (sink_ != nullptr) {
            status.bytes_written = sink_->bytes_written();
            status.segments = sink_->segments();
            status.format_complete =
                state == State::kIdle && !sink_->failed() &&
                !writer_failed_.load(std::memory_order_relaxed);
            quota_reached = sink_->quota_reached();
        }
    }
    status.stop_timed_out = stop_timed_out_.load(std::memory_order_relaxed);
    const std::uint64_t deadline =
        stop_deadline_ms_.load(std::memory_order_relaxed);
    if (state == State::kStopping && deadline != 0 &&
        TrafficNowMs() >= deadline) {
        status.stop_timed_out = true;
    }
    status.capture_complete =
        status.dropped_requests == 0 && status.redaction_failed_requests == 0 &&
        status.format_complete && !status.stop_timed_out && !quota_reached;
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
    config_loaded_ = false;
    ResetConfig();
    shutdown_.store(false, std::memory_order_release);
}

}  // namespace mooncake::traffic
