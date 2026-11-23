#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "session_reader.h"
#include "traffic_wire.h"

namespace {

using Clock = std::chrono::steady_clock;
using mooncake::traffic::Record;
using mooncake::traffic::SessionReader;
using mooncake::traffic::wire::kMaxResponseBytes;
using mooncake::traffic::wire::kProtocolVersion;
using mooncake::traffic::wire::kRequestMagic;
using mooncake::traffic::wire::RequestHeader;
using mooncake::traffic::wire::ResponseHeader;

constexpr std::uint64_t kStallThresholdUs = 100000;
constexpr std::size_t kReportedRoutes = 5;

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

bool WriteAll(int fd, const void* data, std::size_t len, int* io_error) {
    const auto* p = static_cast<const char*>(data);
    while (len > 0) {
        const ssize_t n = ::send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0) {
            *io_error = errno;
            return false;
        }
        if (n == 0) {
            *io_error = 0;
            return false;
        }
        p += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

bool ReadAll(int fd, void* data, std::size_t len, int* io_error) {
    auto* p = static_cast<char*>(data);
    while (len > 0) {
        const ssize_t n = ::recv(fd, p, len, 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0) {
            *io_error = errno;
            return false;
        }
        if (n == 0) {
            *io_error = 0;
            return false;
        }
        p += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

std::uint64_t Micros(Clock::duration duration) {
    if (duration.count() <= 0) {
        return 0;
    }
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(duration)
            .count());
}

// One finished request. queue_us is how long the request sat between the moment
// the schedule wanted it sent and the moment it actually left, service_us is
// send to complete response. Their sum is what a client feels end to end.
struct Sample {
    std::uint32_t route = 0;
    std::uint64_t queue_us = 0;
    std::uint64_t service_us = 0;
};

class Connection {
   public:
    ~Connection() { Close(); }

    bool EnsureConnected(const std::string& target, int timeout_ms,
                         std::string* error) {
        if (fd_ >= 0) {
            return true;
        }
        const std::size_t colon = target.rfind(':');
        if (colon == std::string::npos || colon == 0 ||
            colon + 1 == target.size()) {
            *error = "target must be host:port";
            return false;
        }
        std::string host = target.substr(0, colon);
        const std::string port = target.substr(colon + 1);

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* result = nullptr;
        const int rc =
            ::getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
        if (rc != 0) {
            *error = std::string("getaddrinfo: ") + ::gai_strerror(rc);
            return false;
        }
        timeval tv{};
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        int one = 1;
        for (addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
            const int fd =
                ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (fd < 0) {
                continue;
            }
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef SO_NOSIGPIPE
            ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
            if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
                fd_ = fd;
                break;
            }
            ::close(fd);
        }
        ::freeaddrinfo(result);
        if (fd_ < 0) {
            *error = "connect " + target + ": " + std::strerror(errno);
            return false;
        }
        return true;
    }

    // Sends one recorded request and consumes its reply. The request goes out
    // as a single write: header and payload in one buffer, which is what a real
    // coro_rpc client does and what keeps the server's read path comparable.
    // Returns false when the connection failed; in_window reports whether the
    // request left after the warmup ended.
    bool Exchange(const Record& record, std::uint32_t seq_num,
                  Clock::time_point planned, Clock::time_point measure_from,
                  Sample* sample, bool* in_window, bool* rpc_ok,
                  std::string* error) {
        RequestHeader request{};
        request.magic = static_cast<std::uint8_t>(kRequestMagic);
        request.version = static_cast<std::uint8_t>(kProtocolVersion);
        request.serialize_type = 0;
        request.msg_type = 0;
        request.seq_num = seq_num;
        request.function_id = record.header.route_key;
        request.length = static_cast<std::uint32_t>(record.payload.size());
        request.attach_length = 0;

        scratch_.resize(sizeof(RequestHeader) + record.payload.size());
        std::memcpy(scratch_.data(), &request, sizeof(request));
        if (!record.payload.empty()) {
            std::memcpy(scratch_.data() + sizeof(request),
                        record.payload.data(), record.payload.size());
        }

        const auto send_at = Clock::now();
        *in_window = send_at >= measure_from;
        int io_error = 0;
        if (!WriteAll(fd_, scratch_.data(), scratch_.size(), &io_error)) {
            *error = "send: " + std::to_string(io_error);
            Close();
            return false;
        }
        ResponseHeader response{};
        if (!ReadAll(fd_, &response, sizeof(response), &io_error)) {
            *error = "read header: " + std::to_string(io_error);
            Close();
            return false;
        }
        if (response.magic != kRequestMagic ||
            response.version != kProtocolVersion ||
            response.length > kMaxResponseBytes ||
            response.attach_length != 0 ||
            response.seq_num != request.seq_num) {
            *error = "bad response header";
            Close();
            return false;
        }
        if (response.length > 0) {
            std::vector<char> body(response.length);
            if (!ReadAll(fd_, body.data(), body.size(), &io_error)) {
                *error = "read body: " + std::to_string(io_error);
                Close();
                return false;
            }
        }
        const auto done = Clock::now();
        sample->route = record.header.route_key;
        sample->queue_us =
            send_at > planned ? Micros(send_at - planned) : std::uint64_t{0};
        sample->service_us = Micros(done - send_at);
        *rpc_ok = response.err_code == 0;
        return true;
    }

    void Close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

   private:
    int fd_ = -1;
    std::vector<char> scratch_;
};

std::uint64_t Percentile(std::vector<std::uint64_t>* values, double q) {
    if (values->empty()) {
        return 0;
    }
    const std::size_t index =
        static_cast<std::size_t>(q * static_cast<double>(values->size() - 1));
    std::nth_element(values->begin(), values->begin() + index, values->end());
    return (*values)[index];
}

std::uint64_t Max(const std::vector<std::uint64_t>& values) {
    return values.empty() ? 0 : *std::max_element(values.begin(), values.end());
}

void PrintPercentiles(const char* name, std::vector<std::uint64_t>* values) {
    std::printf(
        "  \"%s\": {\"p50\": %llu, \"p90\": %llu, \"p99\": %llu, \"p999\": "
        "%llu, \"max\": %llu},\n",
        name, static_cast<unsigned long long>(Percentile(values, 0.50)),
        static_cast<unsigned long long>(Percentile(values, 0.90)),
        static_cast<unsigned long long>(Percentile(values, 0.99)),
        static_cast<unsigned long long>(Percentile(values, 0.999)),
        static_cast<unsigned long long>(Max(*values)));
}

struct Options {
    std::string dir;
    std::string target;
    double rate = 0;
    int connections = 1;
    double duration_s = 10;
    double warmup_s = 1;
    int timeout_ms = 1000;
    bool saturate = false;
    bool allow_incomplete = false;
    bool allow_all = false;
    std::vector<std::uint32_t> routes;
};

void PrintUsage() {
    std::printf(
        "usage: traffic_load --dir <session-dir> --target host:port\n"
        "                    (--rate N | --saturate)\n"
        "                    [--connections N] [--duration S] [--warmup S]\n"
        "                    [--timeout-ms N]\n"
        "                    (--allow-route key ... | --allow-all)\n"
        "                    [--allow-incomplete]\n"
        "\n"
        "Replays the recorded requests at a fixed aggregate rate. Every\n"
        "connection keeps one request in flight: it sends, waits for the "
        "reply,\n"
        "then sleeps until the next slot. When the server falls behind the\n"
        "schedule the client sends immediately and counts a missed deadline, "
        "so\n"
        "the gap shows up as queue time instead of a silent rate drop.\n"
        "\n"
        "--saturate drops the pacing entirely and lets every connection send "
        "as\n"
        "fast as the server answers, which is how the capacity of a given\n"
        "concurrency is measured. Without it a rate above the server's "
        "ceiling\n"
        "is a lie: the client cannot hold it and only reports what it "
        "managed.\n"
        "\n"
        "service_us is send to complete response; queue_us is the delay "
        "between\n"
        "the scheduled slot and the actual send, so it is only reported when "
        "the\n"
        "run is paced. The report is a JSON object on stdout and the exit code "
        "is\n"
        "nonzero when the connection failed or the server returned an error.\n"
        "\n"
        "Recorded requests include methods that modify the Master. Pass\n"
        "--allow-route for each route to replay, or --allow-all to replay every\n"
        "route; point the tool only at a disposable Master.\n"
        "\n"
        "A session without a footer is rejected unless --allow-incomplete is\n"
        "given: its last records may be missing, which makes it a poor\n"
        "baseline.\n");
}

bool ParseDouble(const std::string& text, double* out) {
    if (text.empty()) {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const double value = std::strtod(text.c_str(), &end);
    if (errno == ERANGE || end == text.c_str() || *end != '\0') {
        return false;
    }
    *out = value;
    return true;
}

bool ParseInt(const std::string& text, long long* out) {
    if (text.empty()) {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const long long value = std::strtoll(text.c_str(), &end, 10);
    if (errno == ERANGE || end == text.c_str() || *end != '\0') {
        return false;
    }
    *out = value;
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string value;
        const auto take = [&](std::string* out) {
            if (i + 1 >= argc) {
                return false;
            }
            *out = argv[++i];
            return true;
        };
        if (arg == "--dir") {
            if (!take(&options->dir)) {
                std::fprintf(stderr, "--dir needs a value\n");
                return false;
            }
        } else if (arg == "--target") {
            if (!take(&options->target)) {
                std::fprintf(stderr, "--target needs a value\n");
                return false;
            }
        } else if (arg == "--rate") {
            if (!take(&value) || !ParseDouble(value, &options->rate) ||
                options->rate <= 0) {
                std::fprintf(stderr, "bad --rate\n");
                return false;
            }
        } else if (arg == "--connections") {
            long long count = 0;
            if (!take(&value) || !ParseInt(value, &count) || count <= 0 ||
                count > 4096) {
                std::fprintf(stderr, "bad --connections\n");
                return false;
            }
            options->connections = static_cast<int>(count);
        } else if (arg == "--duration") {
            if (!take(&value) || !ParseDouble(value, &options->duration_s) ||
                options->duration_s <= 0) {
                std::fprintf(stderr, "bad --duration\n");
                return false;
            }
        } else if (arg == "--warmup") {
            if (!take(&value) || !ParseDouble(value, &options->warmup_s) ||
                options->warmup_s < 0) {
                std::fprintf(stderr, "bad --warmup\n");
                return false;
            }
        } else if (arg == "--timeout-ms") {
            long long timeout = 0;
            if (!take(&value) || !ParseInt(value, &timeout) || timeout <= 0 ||
                timeout > 600000) {
                std::fprintf(stderr, "bad --timeout-ms\n");
                return false;
            }
            options->timeout_ms = static_cast<int>(timeout);
        } else if (arg == "--allow-route") {
            long long key = 0;
            if (!take(&value) || !ParseInt(value, &key) || key < 0 ||
                key > 0xffffffffLL) {
                std::fprintf(stderr, "bad --allow-route\n");
                return false;
            }
            options->routes.push_back(static_cast<std::uint32_t>(key));
        } else if (arg == "--allow-all") {
            options->allow_all = true;
        } else if (arg == "--saturate") {
            options->saturate = true;
        } else if (arg == "--allow-incomplete") {
            options->allow_incomplete = true;
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage();
            std::exit(0);
        } else {
            std::fprintf(stderr, "unknown argument: %s\n", arg.c_str());
            return false;
        }
    }
    if (options->dir.empty() || options->target.empty()) {
        std::fprintf(stderr, "--dir and --target are required\n");
        return false;
    }
    if (options->allow_all && !options->routes.empty()) {
        std::fprintf(stderr,
                     "--allow-all and --allow-route are mutually exclusive\n");
        return false;
    }
    if (!options->allow_all && options->routes.empty()) {
        std::fprintf(stderr,
                     "--allow-route or --allow-all is required; recorded "
                     "requests may modify the target\n");
        return false;
    }
    if (!options->saturate && options->rate <= 0) {
        std::fprintf(stderr, "--rate or --saturate is required\n");
        return false;
    }
    if (options->warmup_s >= options->duration_s) {
        std::fprintf(stderr, "--warmup must be shorter than --duration\n");
        return false;
    }
    return true;
}

struct WorkerResult {
    std::uint64_t attempted = 0;
    std::uint64_t completed = 0;
    std::uint64_t rpc_errors = 0;
    std::uint64_t transport_errors = 0;
    std::uint64_t missed_deadlines = 0;
    std::uint64_t sent_bytes = 0;
    std::vector<Sample> samples;
    std::unordered_map<std::uint32_t, std::uint64_t> rpc_errors_by_route;
};

void AddSample(
    std::unordered_map<std::uint32_t, std::vector<std::uint64_t>>* by_route,
    const Sample& sample) {
    (*by_route)[sample.route].push_back(sample.service_us);
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseOptions(argc, argv, &options)) {
        PrintUsage();
        return 2;
    }

    SessionReader reader;
    std::string error;
    if (!reader.Open(options.dir, &error)) {
        std::fprintf(stderr, "%s\n", error.c_str());
        return 1;
    }
    if (reader.redaction() == 1) {
        std::fprintf(stderr,
                     "metadata sessions carry no request payload and cannot be "
                     "replayed\n");
        return 1;
    }

    std::vector<Record> records;
    {
        Record record;
        while (true) {
            const auto next = reader.Next(&record, &error);
            if (next == SessionReader::NextResult::kError) {
                std::fprintf(stderr, "%s\n", error.c_str());
                return 1;
            }
            if (next == SessionReader::NextResult::kDone) {
                break;
            }
            if (!options.allow_all &&
                std::find(options.routes.begin(), options.routes.end(),
                          record.header.route_key) == options.routes.end()) {
                record = Record{};
                continue;
            }
            records.push_back(std::move(record));
            record = Record{};
        }
    }
    if (records.empty()) {
        std::fprintf(stderr, "no records in %s\n", options.dir.c_str());
        return 1;
    }
    // The flag is only set once the reader reaches the end of the segment.
    if (reader.incomplete() && !options.allow_incomplete) {
        std::fprintf(stderr,
                     "%s has no footer, so the tail of the session is "
                     "missing; pass --allow-incomplete to replay it anyway\n",
                     options.dir.c_str());
        return 2;
    }
    if (reader.incomplete()) {
        std::fprintf(stderr,
                     "%s has no footer; replaying the records that survived\n",
                     options.dir.c_str());
    }

    std::atomic<std::size_t> next_record{0};
    std::atomic<bool> stop{false};
    std::vector<WorkerResult> results(
        static_cast<std::size_t>(options.connections));
    std::vector<std::thread> threads;
    const auto start = Clock::now();
    const auto measure_from =
        start + std::chrono::duration_cast<Clock::duration>(
                    std::chrono::duration<double>(options.warmup_s));
    const auto finish =
        start + std::chrono::duration_cast<Clock::duration>(
                    std::chrono::duration<double>(options.duration_s));
    const double interval_ns =
        options.saturate
            ? 0.0
            : 1e9 * static_cast<double>(options.connections) / options.rate;

    for (int index = 0; index < options.connections; ++index) {
        threads.emplace_back([&, index]() {
            WorkerResult& result = results[static_cast<std::size_t>(index)];
            result.samples.reserve(1 << 20);
            Connection connection;
            std::string worker_error;
            std::uint32_t seq = 1;
            auto planned = start;
            while (!stop.load(std::memory_order_relaxed)) {
                const auto now = Clock::now();
                if (now >= finish) {
                    break;
                }
                const std::size_t offset =
                    next_record.fetch_add(1, std::memory_order_relaxed) %
                    records.size();
                const Record& record = records[offset];
                if (!connection.EnsureConnected(
                        options.target, options.timeout_ms, &worker_error)) {
                    ++result.transport_errors;
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    planned = Clock::now();
                    continue;
                }
                Sample sample{};
                bool in_window = false;
                bool rpc_ok = false;
                if (!connection.Exchange(record, seq++, planned, measure_from,
                                         &sample, &in_window, &rpc_ok,
                                         &worker_error)) {
                    if (Clock::now() >= measure_from) {
                        ++result.transport_errors;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    planned = Clock::now();
                    continue;
                }
                result.sent_bytes += record.payload.size();
                if (in_window) {
                    ++result.attempted;
                    if (rpc_ok) {
                        ++result.completed;
                        result.samples.push_back(sample);
                    } else {
                        ++result.rpc_errors;
                        ++result.rpc_errors_by_route[record.header.route_key];
                    }
                }
                if (!options.saturate) {
                    const auto done = Clock::now();
                    planned += std::chrono::nanoseconds(
                        static_cast<long long>(interval_ns));
                    if (done > planned) {
                        ++result.missed_deadlines;
                        planned = done;
                    } else {
                        std::this_thread::sleep_until(planned);
                    }
                }
            }
            connection.Close();
        });
    }

    std::this_thread::sleep_until(finish);
    stop.store(true);
    for (auto& thread : threads) {
        thread.join();
    }
    const auto end = Clock::now();

    std::uint64_t attempted = 0;
    std::uint64_t completed = 0;
    std::uint64_t rpc_errors = 0;
    std::uint64_t transport_errors = 0;
    std::uint64_t missed_deadlines = 0;
    std::uint64_t sent_bytes = 0;
    std::vector<std::uint64_t> service_us;
    std::vector<std::uint64_t> queue_us;
    std::unordered_map<std::uint32_t, std::vector<std::uint64_t>>
        service_by_route;
    std::unordered_map<std::uint32_t, std::uint64_t> rpc_errors_by_route;
    for (auto& result : results) {
        attempted += result.attempted;
        completed += result.completed;
        rpc_errors += result.rpc_errors;
        transport_errors += result.transport_errors;
        missed_deadlines += result.missed_deadlines;
        sent_bytes += result.sent_bytes;
        service_us.reserve(service_us.size() + result.samples.size());
        if (!options.saturate) {
            queue_us.reserve(queue_us.size() + result.samples.size());
        }
        for (const Sample& sample : result.samples) {
            service_us.push_back(sample.service_us);
            if (!options.saturate) {
                queue_us.push_back(sample.queue_us);
            }
            AddSample(&service_by_route, sample);
        }
        for (const auto& entry : result.rpc_errors_by_route) {
            rpc_errors_by_route[entry.first] += entry.second;
        }
    }

    // The window is fixed by the schedule, not by how long the last responses
    // took: a slow drain would otherwise shrink every candidate's throughput.
    const double window_s =
        std::max(1e-9, options.duration_s - options.warmup_s);
    const double drain_s =
        std::max(0.0, std::chrono::duration<double>(end - finish).count());
    std::uint64_t stalls = 0;
    for (const std::uint64_t value : service_us) {
        if (value > kStallThresholdUs) {
            ++stalls;
        }
    }

    std::vector<std::pair<std::uint32_t, std::size_t>> route_order;
    route_order.reserve(service_by_route.size());
    for (const auto& entry : service_by_route) {
        route_order.emplace_back(entry.first, entry.second.size());
    }
    std::sort(route_order.begin(), route_order.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    std::printf("{\n");
    std::printf("  \"session\": \"%s\",\n", options.dir.c_str());
    std::printf("  \"target\": \"%s\",\n", options.target.c_str());
    std::printf("  \"rate_target\": %.1f,\n", options.rate);
    std::printf("  \"saturate\": %s,\n", options.saturate ? "true" : "false");
    std::printf("  \"connections\": %d,\n", options.connections);
    std::printf("  \"duration_s\": %.2f,\n", options.duration_s);
    std::printf("  \"warmup_s\": %.2f,\n", options.warmup_s);
    std::printf("  \"attempted\": %llu,\n",
                static_cast<unsigned long long>(attempted));
    std::printf("  \"completed\": %llu,\n",
                static_cast<unsigned long long>(completed));
    std::printf("  \"rpc_errors\": %llu,\n",
                static_cast<unsigned long long>(rpc_errors));
    std::printf("  \"transport_errors\": %llu,\n",
                static_cast<unsigned long long>(transport_errors));
    std::printf("  \"missed_deadlines\": %llu,\n",
                static_cast<unsigned long long>(missed_deadlines));
    std::printf("  \"achieved_rps\": %.1f,\n",
                static_cast<double>(completed) / window_s);
    std::printf("  \"offered_rps\": %.1f,\n",
                static_cast<double>(attempted) / window_s);
    std::printf("  \"sent_bytes\": %llu,\n",
                static_cast<unsigned long long>(sent_bytes));
    std::printf("  \"drain_s\": %.2f,\n", drain_s);
    PrintPercentiles("service_us", &service_us);
    if (!options.saturate) {
        PrintPercentiles("queue_us", &queue_us);
    }
    std::printf("  \"stalls_over_100ms\": %llu,\n",
                static_cast<unsigned long long>(stalls));
    std::vector<std::pair<std::uint32_t, std::uint64_t>> error_order(
        rpc_errors_by_route.begin(), rpc_errors_by_route.end());
    std::sort(error_order.begin(), error_order.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    std::printf("  \"rpc_errors_by_route\": [");
    for (std::size_t i = 0; i < error_order.size() && i < kReportedRoutes;
         ++i) {
        std::printf("%s\n    {\"key\": %u, \"errors\": %llu}",
                    i == 0 ? "" : ",",
                    static_cast<unsigned>(error_order[i].first),
                    static_cast<unsigned long long>(error_order[i].second));
    }
    std::printf("%s],\n", error_order.empty() ? "" : "\n  ");
    std::printf("  \"routes\": [");
    for (std::size_t i = 0; i < route_order.size() && i < kReportedRoutes;
         ++i) {
        std::vector<std::uint64_t>& values =
            service_by_route[route_order[i].first];
        std::printf(
            "%s\n    {\"key\": %u, \"count\": %llu, \"p50\": %llu, "
            "\"p99\": %llu}",
            i == 0 ? "" : ",", static_cast<unsigned>(route_order[i].first),
            static_cast<unsigned long long>(values.size()),
            static_cast<unsigned long long>(Percentile(&values, 0.50)),
            static_cast<unsigned long long>(Percentile(&values, 0.99)));
    }
    std::printf("%s]\n", route_order.empty() ? "" : "\n  ");
    std::printf("}\n");
    return (transport_errors == 0 && rpc_errors == 0) ? 0 : 1;
}
