#include "mooncake_logging.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstdlib>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace mooncake::logging {
namespace {

thread_local uint64_t current_trace_id = 0;

uint64_t GetPidForTrace() {
#ifdef _WIN32
    return static_cast<uint64_t>(_getpid());
#else
    return static_cast<uint64_t>(getpid());
#endif
}

uint64_t SteadyClockNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

double ParseHiFreqLogSampleRate() {
    const char* value = std::getenv("MC_HIFREQ_LOG_SAMPLE_RATE");
    if (value == nullptr || *value == '\0') return 1.0;  // default: full sampling
    errno = 0;
    char* end = nullptr;
    double rate = std::strtod(value, &end);
    if (end == value || errno != 0) return 1.0;  // non-numeric / overflow
    if (rate < 0.0) return 0.0;
    if (rate > 1.0) return 1.0;
    return rate;
}

// Interval for the background periodic flush performed by the async worker
// thread.  Default 1s; tunable via MC_LOG_FLUSH_SECS (accepts fractional
// seconds, e.g. "0.5").  Clamped to a 50ms floor to avoid pathological busy
// flushing.
std::chrono::milliseconds ParseFlushIntervalMs() {
    double secs = 1.0;
    const char* value = std::getenv("MC_LOG_FLUSH_SECS");
    if (value != nullptr && *value != '\0') {
        errno = 0;
        char* end = nullptr;
        double parsed = std::strtod(value, &end);
        if (end != value && errno == 0 && parsed > 0.0) secs = parsed;
    }
    if (secs < 0.05) secs = 0.05;
    return std::chrono::milliseconds(static_cast<long long>(secs * 1000.0));
}

// Ring buffer capacity (messages).  Default 65536; tunable via
// MC_LOG_QUEUE_SIZE.  Floored at 64 so the buffer stays usable.
size_t ParseQueueSize() {
    const char* value = std::getenv("MC_LOG_QUEUE_SIZE");
    if (value == nullptr || *value == '\0') return 65536;
    long parsed = std::strtol(value, nullptr, 10);
    if (parsed < 64) return 64;
    return static_cast<size_t>(parsed);
}

// Number of background writer threads.  Default 2; tunable via MC_LOG_WORKERS,
// clamped to [1, 16].  Note glog serializes the actual file write internally,
// so extra workers mostly overlap message formatting with IO and keep the ring
// drained rather than parallelizing disk writes.
size_t ParseWorkerCount() {
    const char* value = std::getenv("MC_LOG_WORKERS");
    if (value == nullptr || *value == '\0') return 2;
    long parsed = std::strtol(value, nullptr, 10);
    if (parsed < 1) return 1;
    if (parsed > 16) return 16;
    return static_cast<size_t>(parsed);
}

struct LogEntry {
    const char* file;
    int line;
    google::LogSeverity severity;
    uint64_t trace_id;
    std::string message;
};

// Async log pipeline backing MC_LOG().
//
// Hot path (Enqueue): take the mutex, move the entry into a PRE-ALLOCATED ring
// buffer, release.  No per-message heap allocation for queue nodes and no
// blocking: when the ring is full the OLDEST entry is overwritten (overrun) and
// a counter is bumped.  This bounds producer latency -- a hot business thread is
// never stalled waiting on slow log IO -- at the cost of dropping the oldest
// pending lines under sustained overload.  The dropped count is reported
// periodically as a WARNING so loss is never silent.
//
// Background: ParseWorkerCount() writer threads drain the ring via glog.  One of
// them also performs the periodic FlushLogFiles (coordinated by an atomic so it
// runs once per interval regardless of worker count) and the overrun report --
// both off the hot path.
class AsyncLogQueue {
   public:
    static AsyncLogQueue& Instance() {
        static AsyncLogQueue* queue = new AsyncLogQueue();
        return *queue;
    }

    void Enqueue(LogEntry entry) {
        EnsureStarted();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_) {
                WriteSync(entry);
                return;
            }
            RingPush(std::move(entry));  // overruns oldest if full
        }
        queue_not_empty_.notify_one();
    }

    void Flush() {
        EnsureStarted();
        {
            std::unique_lock<std::mutex> lock(mutex_);
            queue_empty_.wait(
                lock, [this] { return RingEmpty() && active_writes_ == 0; });
        }
        google::FlushLogFiles(google::INFO);
    }

   private:
    AsyncLogQueue()
        : capacity_(ParseQueueSize() + 1),  // +1 slot reserved as full marker
          worker_count_(ParseWorkerCount()),
          flush_interval_(ParseFlushIntervalMs()),
          ring_(capacity_) {}

    void EnsureStarted() {
        std::call_once(start_once_, [this] {
            for (size_t i = 0; i < worker_count_; ++i) {
                workers_.emplace_back([this] { WorkerLoop(); });
            }
            std::atexit([] { AsyncLogQueue::Instance().Stop(); });
        });
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped_ = true;
        }
        queue_not_empty_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
        ReportOverruns();
        google::FlushLogFiles(google::INFO);
    }

    void WorkerLoop() {
        // The periodic flush runs here, off the hot path: log producers never
        // pay a flush cost.  google::FlushLogFiles drains glog's file buffer,
        // covering BOTH the async MC_LOG output written below AND synchronous
        // LOG()/MC_LOG emitted on business threads (they share the same glog
        // file).  This keeps the tail of the log (e.g. get_into_breakdown /
        // put_result) from being lost when the host process is torn down without
        // running atexit/static destructors (Go's os.Exit, SIGKILL under k8s).
        while (true) {
            LogEntry entry;
            bool have_entry = false;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                queue_not_empty_.wait_for(lock, flush_interval_, [this] {
                    return stopped_ || !RingEmpty();
                });
                if (!RingEmpty()) {
                    entry = std::move(ring_[head_]);
                    head_ = (head_ + 1) % capacity_;
                    ++active_writes_;
                    have_entry = true;
                } else {
                    queue_empty_.notify_all();
                    if (stopped_) return;
                }
            }
            if (have_entry) {
                WriteSync(entry);
                std::lock_guard<std::mutex> lock(mutex_);
                --active_writes_;
                if (RingEmpty() && active_writes_ == 0) {
                    queue_empty_.notify_all();
                }
            }
            MaybePeriodicFlush();
        }
    }

    // Flush + overrun report, gated by an atomic so it runs at most once per
    // interval across all workers.  Bounds the worst-case loss window on a hard
    // kill to one interval, with no per-message flush overhead.
    void MaybePeriodicFlush() {
        const int64_t now_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
        const int64_t interval_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(flush_interval_)
                .count();
        int64_t last = last_flush_ns_.load(std::memory_order_relaxed);
        if (now_ns - last < interval_ns) return;
        if (!last_flush_ns_.compare_exchange_strong(
                last, now_ns, std::memory_order_relaxed)) {
            return;  // another worker won this interval
        }
        google::FlushLogFiles(google::INFO);
        ReportOverruns();
    }

    // Emit one synchronous WARNING if entries were overrun since the last
    // report, so dropped logs are never silent.  The counter is read+reset under
    // the queue mutex; the WriteSync itself does not touch the ring.
    void ReportOverruns() {
        uint64_t dropped = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            dropped = overrun_count_;
            overrun_count_ = 0;
        }
        if (dropped > 0) {
            google::LogMessage(__FILE__, __LINE__, google::WARNING).stream()
                << "trace_id[none] async log ring overran, dropped " << dropped
                << " message(s); raise MC_LOG_QUEUE_SIZE or MC_LOG_WORKERS";
        }
    }

    // --- ring buffer, all access guarded by mutex_ ---
    bool RingEmpty() const { return head_ == tail_; }

    void RingPush(LogEntry&& entry) {
        ring_[tail_] = std::move(entry);
        tail_ = (tail_ + 1) % capacity_;
        if (tail_ == head_) {  // full: overwrite oldest
            head_ = (head_ + 1) % capacity_;
            ++overrun_count_;
        }
    }

    static void WriteSync(const LogEntry& entry) {
        google::LogMessage log_message(entry.file, entry.line, entry.severity);
        auto& stream = log_message.stream();
        if (entry.trace_id != 0) {
            stream << "trace_id[" << entry.trace_id << "] ";
        } else {
            stream << "trace_id[none] ";
        }
        stream << entry.message;
        if (entry.severity == google::FATAL)
            google::FlushLogFiles(google::INFO);
    }

    const size_t capacity_;
    const size_t worker_count_;
    const std::chrono::milliseconds flush_interval_;

    std::once_flag start_once_;
    std::vector<std::thread> workers_;

    std::mutex mutex_;
    std::condition_variable queue_not_empty_;
    std::condition_variable queue_empty_;

    std::vector<LogEntry> ring_;
    size_t head_ = 0;
    size_t tail_ = 0;
    uint64_t overrun_count_ = 0;
    size_t active_writes_ = 0;
    bool stopped_ = false;

    std::atomic<int64_t> last_flush_ns_{0};
};

}  // namespace

uint64_t NewTraceId() {
    static const uint64_t process_seed =
        (GetPidForTrace() << 48) ^ (SteadyClockNs() & 0x0000FFFFFFFF0000ULL);
    static std::atomic<uint64_t> counter{1};
    return process_seed ^ counter.fetch_add(1, std::memory_order_relaxed);
}

uint64_t CurrentTraceId() { return current_trace_id; }

double HiFreqLogSampleRate() {
    static const double rate = ParseHiFreqLogSampleRate();
    return rate;
}

bool ShouldSampleHiFreqLog() {
    const double rate = HiFreqLogSampleRate();
    if (rate >= 1.0) return true;
    if (rate <= 0.0) return false;
    thread_local std::mt19937 rng(static_cast<uint32_t>(
        SteadyClockNs() ^ reinterpret_cast<uintptr_t>(&rng)));
    thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng) < rate;
}

bool ShouldLog(google::LogSeverity severity) {
    // MC_LOG_ENABLE was removed: MC_LOG now behaves like plain glog LOG and is
    // gated only by glog's own severity threshold (still async + trace_id).
    if (severity == google::FATAL) return true;
    return severity >= FLAGS_minloglevel;
}

bool ShouldVLog(int level) { return VLOG_IS_ON(level); }

void ApplyMooncakeLogEnableToGlog() {
    // No-op retained for call-site compatibility (master/real_client main).
    // MC_LOG_ENABLE was removed; nothing to apply.
}

ScopedTraceId::ScopedTraceId(uint64_t trace_id)
    : previous_trace_id_(current_trace_id) {
    current_trace_id = trace_id;
}

ScopedTraceId::~ScopedTraceId() { current_trace_id = previous_trace_id_; }

AsyncLogMessage::AsyncLogMessage(const char* file, int line,
                                 google::LogSeverity severity, bool enabled)
    : file_(file),
      line_(line),
      severity_(severity),
      enabled_(enabled),
      trace_id_(CurrentTraceId()) {}

AsyncLogMessage::~AsyncLogMessage() {
    if (!enabled_) return;
    if (severity_ == google::FATAL) {
        google::LogMessage log_message(file_, line_, severity_);
        auto& output = log_message.stream();
        if (trace_id_ != 0) {
            output << "trace_id[" << trace_id_ << "] ";
        } else {
            output << "trace_id[none] ";
        }
        output << stream_.str();
        return;
    }
    AsyncLogQueue::Instance().Enqueue(
        LogEntry{file_, line_, severity_, trace_id_, stream_.str()});
}

std::ostream& AsyncLogMessage::stream() { return stream_; }

void FlushAsyncLogs() { AsyncLogQueue::Instance().Flush(); }

}  // namespace mooncake::logging
