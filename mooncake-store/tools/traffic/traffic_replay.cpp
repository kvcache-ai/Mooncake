#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <json/json.h>

#include "session_reader.h"

namespace {

constexpr std::uint32_t kRequestMagic = 21;
constexpr std::uint32_t kMaxResponseBytes = 512U * 1024U * 1024U;

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#pragma pack(push, 1)
struct RequestHeader {
    std::uint8_t magic;
    std::uint8_t version;
    std::uint8_t serialize_type;
    std::uint8_t msg_type;
    std::uint32_t seq_num;
    std::uint32_t function_id;
    std::uint32_t length;
    std::uint32_t attach_length;
};
static_assert(sizeof(RequestHeader) == 20);

struct ResponseHeader {
    std::uint8_t magic;
    std::uint8_t version;
    std::uint8_t err_code;
    std::uint8_t msg_type;
    std::uint32_t seq_num;
    std::uint32_t length;
    std::uint32_t attach_length;
};
static_assert(sizeof(ResponseHeader) == 16);
#pragma pack(pop)

using mooncake::traffic::Record;
using mooncake::traffic::RedactionName;
using mooncake::traffic::SessionReader;

// Moves the whole buffer. On failure *io_error holds errno, or 0 when the peer
// closed the connection; a socket timeout also arrives as errno, so the caller
// can tell an expired --timeout-ms from a malformed reply.
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

// Names why a send or receive stopped, so a stalled peer is not reported as a
// malformed reply.
std::string DescribeIoFailure(int io_error, int timeout_ms) {
    if (io_error == EAGAIN || io_error == EWOULDBLOCK) {
        return "timed out after " + std::to_string(timeout_ms) + " ms";
    }
    if (io_error == 0) {
        return "connection closed by peer";
    }
    return std::strerror(io_error);
}

class SocketClient {
   public:
    ~SocketClient() { Close(); }

    bool EnsureConnected(const std::string& target, int timeout_ms,
                         std::string* error) {
        if (fd_ >= 0) {
            return true;
        }
        timeout_ms_ = timeout_ms;
        const std::size_t colon = target.rfind(':');
        if (colon == std::string::npos || colon == 0 ||
            colon + 1 == target.size()) {
            *error = "target must be host:port";
            return false;
        }
        std::string host = target.substr(0, colon);
        const std::string port = target.substr(colon + 1);
        if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
            host = host.substr(1, host.size() - 2);
        }

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
        int connect_error = 0;
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
            connect_error = errno;
            ::close(fd);
        }
        ::freeaddrinfo(result);
        if (fd_ < 0) {
            // Linux honours SO_SNDTIMEO for a blocking connect and reports the
            // expired timeout as EINPROGRESS, which reads like a success.
            if (connect_error == EINPROGRESS || connect_error == EAGAIN ||
                connect_error == EWOULDBLOCK) {
                *error = "connect " + target + ": timed out after " +
                         std::to_string(timeout_ms) + " ms";
            } else {
                *error =
                    "connect " + target + ": " + std::strerror(connect_error);
            }
            return false;
        }
        return true;
    }

    bool Replay(const Record& record, bool* rpc_ok, std::string* error) {
        RequestHeader request{};
        request.magic = static_cast<std::uint8_t>(kRequestMagic);
        request.version = 0;
        request.serialize_type = 0;
        request.msg_type = 0;
        request.seq_num = record.header.seq_num;
        request.function_id = record.header.route_key;
        request.length = static_cast<std::uint32_t>(record.payload.size());
        request.attach_length = 0;
        int io_error = 0;
        if (!WriteAll(fd_, &request, sizeof(request), &io_error) ||
            (!record.payload.empty() &&
             !WriteAll(fd_, record.payload.data(), record.payload.size(),
                       &io_error))) {
            Close();
            *error = "send: " + DescribeIoFailure(io_error, timeout_ms_);
            return false;
        }

        ResponseHeader response{};
        if (!ReadAll(fd_, &response, sizeof(response), &io_error)) {
            Close();
            *error = "read response header: " +
                     DescribeIoFailure(io_error, timeout_ms_);
            return false;
        }
        if (response.magic != kRequestMagic || response.version != 0 ||
            response.length > kMaxResponseBytes) {
            Close();
            *error = "bad response header";
            return false;
        }
        if (response.attach_length != 0) {
            Close();
            *error = "response carries an attachment of " +
                     std::to_string(response.attach_length) +
                     " bytes; this tool cannot consume response attachments";
            return false;
        }
        if (response.seq_num != request.seq_num) {
            // Replaying over a fresh connection would surface this as a silent
            // response mix-up, so fail instead of attributing the reply to the
            // wrong record.
            Close();
            *error = "response seq_num " + std::to_string(response.seq_num) +
                     " does not match request seq_num " +
                     std::to_string(request.seq_num);
            return false;
        }
        if (response.length > 0) {
            std::vector<char> body(response.length);
            if (!ReadAll(fd_, body.data(), body.size(), &io_error)) {
                Close();
                *error = "read response body: " +
                         DescribeIoFailure(io_error, timeout_ms_);
                return false;
            }
        }
        *rpc_ok = response.err_code == 0;
        return true;
    }

   private:
    void Close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    int fd_ = -1;
    int timeout_ms_ = 5000;
};

struct Options {
    std::string command;
    std::string dir;
    std::string target;
    std::uint64_t limit = 0;
    double rate = 0.0;
    double speed = 0.0;
    int timeout_ms = 5000;
    bool allow_all = false;
    bool dry_run = false;
    bool verbose = false;
    std::vector<std::string> allow_route_args;
};

bool ParseU64(const std::string& text, std::uint64_t* value) {
    if (text.empty() || text[0] == '-' || text[0] == '+') {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long v = std::strtoull(text.c_str(), &end, 10);
    if (errno != 0 || end == text.c_str() || *end != '\0') {
        return false;
    }
    *value = v;
    return true;
}

bool ParseU32(const std::string& text, std::uint32_t* value) {
    std::uint64_t v = 0;
    if (!ParseU64(text, &v) || v > UINT32_MAX) {
        return false;
    }
    *value = static_cast<std::uint32_t>(v);
    return true;
}

bool LoadRouteNames(const std::filesystem::path& session_dir,
                    std::map<std::string, std::uint32_t>* names,
                    std::string* error) {
    names->clear();
    const std::filesystem::path path = session_dir / "routes.json";
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        return true;
    }
    std::ifstream in(path);
    if (!in) {
        *error = "cannot read " + path.string();
        return false;
    }
    Json::CharReaderBuilder builder;
    Json::Value root;
    std::string parse_error;
    if (!Json::parseFromStream(builder, in, &root, &parse_error)) {
        *error = "cannot parse " + path.string() + ": " + parse_error;
        return false;
    }
    if (!root.isObject()) {
        *error = path.string() + " must be a JSON object";
        return false;
    }
    for (const auto& key : root.getMemberNames()) {
        std::uint32_t route = 0;
        if (!ParseU32(key, &route) || !root[key].isString()) {
            *error = "invalid route entry in " + path.string();
            return false;
        }
        (*names)[root[key].asString()] = route;
    }
    return true;
}

bool ParseDouble(const std::string& text, double* value) {
    char* end = nullptr;
    errno = 0;
    const double v = std::strtod(text.c_str(), &end);
    if (errno != 0 || end == text.c_str() || *end != '\0' || v < 0) {
        return false;
    }
    *value = v;
    return true;
}

void PrintUsage() {
    std::cout
        << "usage:\n"
        << "  traffic_replay inspect --dir SESSION\n"
        << "  traffic_replay run --dir SESSION --target HOST:PORT "
           "[options]\n"
        << "\noptions:\n"
        << "  --limit N             replay at most N records (at least 1)\n"
        << "  --allow-route ROUTE   allow a route key or name (repeatable)\n"
        << "  --allow-all           allow every route\n"
        << "  --dry-run             parse and count without sending\n"
        << "  --rate QPS            fixed send rate\n"
        << "  --speed N             replay at N times the recorded speed\n"
        << "  --timeout-ms N        socket timeout (default 5000)\n"
        << "  --verbose             print progress\n";
}

bool NextValue(int argc, char** argv, int* index, std::string* value) {
    if (*index + 1 >= argc) {
        return false;
    }
    *value = argv[++(*index)];
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options, std::string* error) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string inline_value;
        bool inline_present = false;
        const auto equals = arg.find('=');
        if (arg.rfind("--", 0) == 0 && equals != std::string::npos) {
            inline_present = true;
            inline_value = arg.substr(equals + 1);
            arg.resize(equals);
        }
        const auto take = [&](std::string* out) {
            if (inline_present) {
                if (inline_value.empty()) {
                    return false;
                }
                *out = inline_value;
                return true;
            }
            return NextValue(argc, argv, &i, out);
        };
        std::string value;
        if (arg == "--dir") {
            if (!take(&options->dir)) {
                *error = "--dir needs a value";
                return false;
            }
        } else if (arg == "--target") {
            if (!take(&options->target)) {
                *error = "--target needs a value";
                return false;
            }
        } else if (arg == "--limit") {
            if (!take(&value) || !ParseU64(value, &options->limit) ||
                options->limit == 0) {
                *error = "bad --limit";
                return false;
            }
        } else if (arg == "--rate") {
            if (!take(&value) || !ParseDouble(value, &options->rate) ||
                options->rate == 0) {
                *error = "bad --rate";
                return false;
            }
        } else if (arg == "--speed") {
            if (!take(&value) || !ParseDouble(value, &options->speed) ||
                options->speed == 0) {
                *error = "bad --speed";
                return false;
            }
        } else if (arg == "--timeout-ms") {
            std::uint64_t timeout = 0;
            if (!take(&value) || !ParseU64(value, &timeout) || timeout == 0 ||
                timeout > 600000) {
                *error = "bad --timeout-ms";
                return false;
            }
            options->timeout_ms = static_cast<int>(timeout);
        } else if (arg == "--allow-route") {
            if (!take(&value) || value.empty()) {
                *error = "--allow-route needs a route key or name";
                return false;
            }
            options->allow_route_args.push_back(value);
        } else if (arg == "--allow-all") {
            options->allow_all = true;
        } else if (arg == "--dry-run") {
            options->dry_run = true;
        } else if (arg == "--verbose") {
            options->verbose = true;
        } else if (arg == "--help") {
            PrintUsage();
            std::exit(0);
        } else if (arg.rfind("--", 0) == 0) {
            *error = "unknown option: " + arg;
            return false;
        } else if (options->command.empty()) {
            options->command = arg;
        } else {
            *error = "unexpected argument: " + arg;
            return false;
        }
    }
    if (options->rate > 0 && options->speed > 0) {
        *error = "--rate and --speed are mutually exclusive";
        return false;
    }
    return true;
}

// Routes a run may send. A filter that is neither "all" nor a non-empty key
// list is a configuration the caller rejects before replay starts.
struct RouteFilter {
    bool all = false;
    std::vector<std::uint32_t> keys;

    bool empty() const { return !all && keys.empty(); }
    bool Matches(std::uint32_t route) const {
        return all || std::find(keys.begin(), keys.end(), route) != keys.end();
    }
};

// Reports why a command stopped and returns the exit code it maps to.
int ReportError(const std::string& message, int code = 1) {
    std::cerr << message << "\n";
    return code;
}

class Pacer {
   public:
    explicit Pacer(const Options& options)
        : rate_(options.rate), speed_(options.speed) {}

    void Wait(std::uint64_t timestamp_ms) {
        if (start_ == std::chrono::steady_clock::time_point{}) {
            start_ = std::chrono::steady_clock::now();
            first_timestamp_ms_ = timestamp_ms;
        }
        std::chrono::steady_clock::time_point due = start_;
        if (speed_ > 0) {
            const std::uint64_t delta = timestamp_ms > first_timestamp_ms_
                                            ? timestamp_ms - first_timestamp_ms_
                                            : 0;
            const auto ns = static_cast<std::int64_t>(
                static_cast<double>(delta) * 1000000.0 / speed_);
            due += std::chrono::nanoseconds(ns);
        } else if (rate_ > 0) {
            const auto ns = static_cast<std::int64_t>(
                static_cast<double>(count_) * 1000000000.0 / rate_);
            due += std::chrono::nanoseconds(ns);
        } else {
            return;
        }
        ++count_;
        std::this_thread::sleep_until(due);
    }

   private:
    double rate_ = 0;
    double speed_ = 0;
    std::chrono::steady_clock::time_point start_{};
    std::uint64_t first_timestamp_ms_ = 0;
    std::uint64_t count_ = 0;
};

bool ResolveAllowRoutes(const Options& options,
                        const std::map<std::string, std::uint32_t>& names,
                        RouteFilter* filter, std::string* error) {
    filter->all = options.allow_all;
    filter->keys.clear();
    for (const std::string& arg : options.allow_route_args) {
        std::uint32_t key = 0;
        if (ParseU32(arg, &key)) {
            filter->keys.push_back(key);
            continue;
        }
        const auto it = names.find(arg);
        if (it != names.end()) {
            filter->keys.push_back(it->second);
            continue;
        }
        std::vector<std::uint32_t> matches;
        const std::string suffix = "::" + arg;
        for (const auto& [name, key] : names) {
            if (name.size() >= suffix.size() &&
                name.compare(name.size() - suffix.size(), suffix.size(),
                             suffix) == 0) {
                matches.push_back(key);
            }
        }
        if (matches.size() != 1) {
            *error = matches.empty() ? "unknown route name: " + arg +
                                           "; use `inspect` to list names"
                                     : "ambiguous route name: " + arg;
            return false;
        }
        filter->keys.push_back(matches.front());
    }
    return true;
}

int Inspect(const Options& options) {
    SessionReader reader;
    std::string error;
    if (!reader.Open(options.dir, &error)) {
        return ReportError(error);
    }
    if (!reader.Finish(&error)) {
        return ReportError(error);
    }
    std::map<std::string, std::uint32_t> names;
    if (!LoadRouteNames(reader.session_dir(), &names, &error)) {
        return ReportError(error);
    }
    std::map<std::uint32_t, std::string> by_key;
    for (const auto& [name, key] : names) {
        by_key[key] = name;
    }
    std::cout << "dir=" << options.dir << "\n"
              << "segments=" << reader.segment_count() << "\n"
              << "blocks=" << reader.block_count() << "\n"
              << "records=" << reader.records() << "\n"
              << "payload_bytes=" << reader.payload_bytes() << "\n"
              << "duration_ms=" << reader.duration_ms() << "\n";
    if (reader.duration_ms() > 0) {
        const double rate = static_cast<double>(reader.records()) * 1000.0 /
                            static_cast<double>(reader.duration_ms());
        std::cout << "requests_per_second=" << std::fixed
                  << std::setprecision(1) << rate << std::defaultfloat << "\n";
    }
    std::cout << "redaction=" << RedactionName(reader.redaction()) << "\n";
    if (reader.incomplete()) {
        std::cout << "incomplete=yes\n";
    }
    std::cout << "routes:\n";
    for (const auto& [route, count] : reader.routes()) {
        std::cout << "  " << route;
        const auto it = by_key.find(route);
        if (it != by_key.end()) {
            std::cout << " " << it->second;
        }
        std::cout << " " << count << "\n";
    }
    return 0;
}

int Run(const Options& options) {
    SessionReader reader;
    std::string error;
    if (!reader.Open(options.dir, &error)) {
        return ReportError(error);
    }
    if (reader.redaction() == 1) {
        return ReportError(
            "metadata sessions contain no request payload and cannot be "
            "replayed");
    }
    std::map<std::string, std::uint32_t> names;
    if (!LoadRouteNames(reader.session_dir(), &names, &error)) {
        return ReportError(error);
    }
    RouteFilter filter;
    if (!ResolveAllowRoutes(options, names, &filter, &error)) {
        return ReportError(error);
    }
    if (!options.dry_run && filter.empty()) {
        return ReportError(
            "refusing to replay every route without --allow-all or "
            "--allow-route");
    }

    SocketClient client;
    Pacer pacer(options);
    std::uint64_t selected = 0;
    std::uint64_t sent = 0;
    std::uint64_t ok = 0;
    std::uint64_t rpc_error = 0;
    std::uint64_t transport_error = 0;
    std::uint64_t bytes = 0;
    std::uint64_t records_read = 0;
    Record record;
    const auto start = std::chrono::steady_clock::now();

    while (true) {
        const auto result = reader.Next(&record, &error);
        if (result == SessionReader::NextResult::kDone) {
            break;
        }
        if (result == SessionReader::NextResult::kError) {
            return ReportError(error);
        }
        ++records_read;
        if (!filter.Matches(record.header.route_key)) {
            continue;
        }
        if (options.limit > 0 && selected >= options.limit) {
            break;
        }
        ++selected;
        if (options.dry_run) {
            continue;
        }
        if (!client.EnsureConnected(options.target, options.timeout_ms,
                                    &error)) {
            return ReportError(error, 2);
        }
        pacer.Wait(record.header.timestamp_ms);
        bool rpc_ok = false;
        if (!client.Replay(record, &rpc_ok, &error)) {
            ++transport_error;
            return ReportError(
                "record " + std::to_string(records_read) + ": " + error, 2);
        }
        ++sent;
        bytes += record.payload.size();
        if (rpc_ok) {
            ++ok;
        } else {
            ++rpc_error;
        }
        if (options.verbose && sent % 10000 == 0) {
            std::cerr << "sent=" << sent << "\n";
        }
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    const double seconds = std::max(0.001, elapsed.count() / 1000.0);
    std::cout << "records_read=" << records_read << " selected=" << selected
              << " sent=" << sent << " ok=" << ok << " rpc_error=" << rpc_error
              << " transport_error=" << transport_error << " bytes=" << bytes
              << " elapsed_ms=" << elapsed.count()
              << " qps=" << static_cast<double>(sent) / seconds << "\n";
    if (reader.incomplete()) {
        std::cerr << "warning: the session has no footer; it was cut short and "
                     "the records after the cut are missing\n";
    }
    return transport_error == 0 ? 0 : 2;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        std::cerr << error << "\n";
        PrintUsage();
        return 1;
    }
    if (options.command.empty() || options.command == "help") {
        PrintUsage();
        return options.command.empty() ? 1 : 0;
    }
    if (options.dir.empty()) {
        std::cerr << "--dir is required\n";
        return 1;
    }
    if (options.command == "inspect") {
        return Inspect(options);
    }
    if (options.command == "run") {
        if (!options.dry_run && options.target.empty()) {
            std::cerr << "--target is required for run\n";
            return 1;
        }
        return Run(options);
    }
    std::cerr << "unknown command: " << options.command << "\n";
    PrintUsage();
    return 1;
}
