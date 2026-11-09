#include "traffic_admin.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <map>
#include <vector>

#include <json/json.h>
#include <openssl/rand.h>
#include <ylt/coro_http/coro_http_server.hpp>

#include <exception>

#include <glog/logging.h>

#include "traffic_recorder.h"

namespace mooncake::traffic {
namespace {

Json::Value BuildStatusJson(const RecorderStatus& status) {
    Json::Value root;
    root["state"] = status.state;
    root["session_dir"] = status.session_dir;
    root["observed_requests"] =
        static_cast<Json::UInt64>(status.observed_requests);
    root["recorded_requests"] =
        static_cast<Json::UInt64>(status.recorded_requests);
    root["filtered_requests"] =
        static_cast<Json::UInt64>(status.filtered_requests);
    root["dropped_requests"] =
        static_cast<Json::UInt64>(status.dropped_requests);
    root["redaction_failed_requests"] =
        static_cast<Json::UInt64>(status.redaction_failed_requests);
    root["bytes_written"] = static_cast<Json::UInt64>(status.bytes_written);
    root["segments"] = static_cast<Json::UInt64>(status.segments);
    root["ring_used_bytes"] = static_cast<Json::UInt64>(status.ring_used_bytes);
    root["ring_capacity_bytes"] =
        static_cast<Json::UInt64>(status.ring_capacity_bytes);
    root["stop_timed_out"] = status.stop_timed_out;
    root["format_complete"] = status.format_complete;
    root["capture_complete"] = status.capture_complete;
    return root;
}

std::string WriteJson(const Json::Value& root) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

bool Authorized(coro_http::coro_http_request& request,
                const std::string& token) {
    if (token.empty()) {
        return true;
    }
    return request.get_header_value("Authorization") == "Bearer " + token;
}

// Writes one file of a session directory. Both summary.json and routes.json
// go through this so the 0600 permission and the failure messages stay in one
// place: a session file left at the default mode would be readable by every
// local user.
bool WriteSessionFile(const std::filesystem::path& session_dir,
                      const char* name, const std::string& body) {
    if (session_dir.empty()) {
        return false;
    }
    const std::filesystem::path path = session_dir / name;
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        LOG(ERROR) << "traffic: cannot write " << name << " in " << session_dir;
        return false;
    }
    out << body;
    out.close();
    if (::chmod(path.c_str(), 0600) != 0) {
        LOG(WARNING) << "traffic: cannot chmod " << name;
    }
    return true;
}

}  // namespace

bool GenerateControlToken(std::string* token) {
    std::array<unsigned char, 16> random_bytes{};
    if (RAND_bytes(random_bytes.data(),
                   static_cast<int>(random_bytes.size())) != 1) {
        return false;
    }
    static constexpr char kHex[] = "0123456789abcdef";
    token->clear();
    token->reserve(random_bytes.size() * 2);
    for (unsigned char byte : random_bytes) {
        token->push_back(kHex[byte >> 4]);
        token->push_back(kHex[byte & 0x0f]);
    }
    return true;
}

std::optional<std::string> ReadControlToken(const std::filesystem::path& path,
                                            TokenFormat format) {
    int flags = O_RDONLY | O_CLOEXEC;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    const int fd = ::open(path.c_str(), flags);
    if (fd < 0) {
        return std::nullopt;
    }
    struct stat metadata{};
    if (::fstat(fd, &metadata) != 0 || !S_ISREG(metadata.st_mode) ||
        metadata.st_uid != ::geteuid() || (metadata.st_mode & 07777) != 0600) {
        ::close(fd);
        return std::nullopt;
    }
    std::string token;
    char chunk[128];
    bool done = false;
    while (!done && token.size() <= 4096) {
        const ssize_t bytes = ::read(fd, chunk, sizeof(chunk));
        if (bytes < 0 && errno == EINTR) {
            continue;
        }
        if (bytes < 0) {
            ::close(fd);
            return std::nullopt;
        }
        if (bytes == 0) {
            break;
        }
        const char* newline =
            static_cast<const char*>(std::memchr(chunk, '\n', bytes));
        const std::size_t length =
            newline == nullptr ? static_cast<std::size_t>(bytes)
                               : static_cast<std::size_t>(newline - chunk);
        token.append(chunk, length);
        done = newline != nullptr;
    }
    const bool closed = ::close(fd) == 0;
    if (!closed || token.size() > 4096) {
        return std::nullopt;
    }
    while (!token.empty() && (token.back() == '\n' || token.back() == '\r' ||
                              token.back() == ' ' || token.back() == '\t')) {
        token.pop_back();
    }
    if (token.empty()) {
        return std::nullopt;
    }
    if (format == TokenFormat::kHex &&
        (token.size() != 32 ||
         !std::all_of(token.begin(), token.end(), [](unsigned char ch) {
             return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
         }))) {
        return std::nullopt;
    }
    return token;
}

bool WriteControlToken(const std::filesystem::path& path, std::string* token) {
    std::string temp_path = path.string() + ".tmp.XXXXXX";
    std::vector<char> temp_path_buffer(temp_path.begin(), temp_path.end());
    temp_path_buffer.push_back('\0');
    const int fd = ::mkstemp(temp_path_buffer.data());
    if (fd < 0) {
        return false;
    }

    const std::string published_temp_path(temp_path_buffer.data());
    bool success =
        ::fcntl(fd, F_SETFD, FD_CLOEXEC) == 0 && ::fchmod(fd, 0600) == 0;
    const std::string contents = *token + '\n';
    std::size_t written_total = 0;
    while (success && written_total < contents.size()) {
        const ssize_t written = ::write(fd, contents.data() + written_total,
                                        contents.size() - written_total);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            success = false;
            break;
        }
        written_total += static_cast<std::size_t>(written);
    }
    if (success && ::fsync(fd) != 0) {
        success = false;
    }
    if (::close(fd) != 0) {
        success = false;
    }
    if (success && ::link(published_temp_path.c_str(), path.c_str()) == 0) {
        ::unlink(published_temp_path.c_str());
        return true;
    }
    const int link_error = errno;
    ::unlink(published_temp_path.c_str());
    if (success && link_error == EEXIST) {
        if (auto published = ReadControlToken(path, TokenFormat::kHex);
            published.has_value()) {
            *token = std::move(*published);
            return true;
        }
    }
    return false;
}

bool WriteSummaryJson(const std::filesystem::path& session_dir,
                      const RecorderStatus& status) {
    return WriteSessionFile(session_dir, "summary.json",
                            WriteJson(BuildStatusJson(status)));
}

bool WriteRouteNamesJson(
    const std::filesystem::path& session_dir,
    const std::unordered_map<std::uint32_t, std::string>& route_names) {
    Json::Value root(Json::objectValue);
    std::map<std::uint32_t, std::string> sorted(route_names.begin(),
                                                route_names.end());
    for (const auto& [key, name] : sorted) {
        root[std::to_string(key)] = name;
    }
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "  ";
    return WriteSessionFile(session_dir, "routes.json",
                            Json::writeString(builder, root));
}

namespace {

// Every control response except health carries a JSON body, so the content type
// is set in one place instead of at each handler.
void RespondJson(coro_http::coro_http_response& resp,
                 coro_http::status_type status, const std::string& body) {
    resp.add_header("Content-Type", "application/json");
    resp.set_status_and_content(status, body);
}

void RespondUnauthorized(coro_http::coro_http_response& resp) {
    RespondJson(resp, coro_http::status_type::unauthorized,
                "{\"error\":\"unauthorized\"}");
}

// Answers 401 itself when the caller is not authorized, so a handler only needs
// `if (!CheckAuthorized(req, resp, token)) return;`.
bool CheckAuthorized(coro_http::coro_http_request& req,
                     coro_http::coro_http_response& resp,
                     const std::string& token) {
    if (Authorized(req, token)) {
        return true;
    }
    RespondUnauthorized(resp);
    return false;
}

}  // namespace

std::unique_ptr<cinatra::coro_http_server> StartControlServer(
    TrafficRecorder& recorder, const std::string& host, std::uint16_t port,
    const std::string& token) {
    using namespace coro_http;
    try {
        auto server = std::make_unique<coro_http_server>(2, port, host);
        server->set_http_handler<GET>(
            "/v1/recording/health",
            [](coro_http_request&, coro_http_response& resp) {
                resp.set_status_and_content(status_type::ok, "ok");
            });
        server->set_http_handler<GET>(
            "/v1/recording/status",
            [&recorder, token](coro_http_request& req,
                               coro_http_response& resp) {
                if (!CheckAuthorized(req, resp, token)) {
                    return;
                }
                RespondJson(resp, status_type::ok,
                            WriteJson(BuildStatusJson(recorder.GetStatus())));
            });
        server->set_http_handler<
            POST>("/v1/recording/start", [&recorder, token](
                                             coro_http_request& req,
                                             coro_http_response& resp) {
            if (!CheckAuthorized(req, resp, token)) {
                return;
            }
            if (!recorder.Start()) {
                // A failed start is either another session already running
                // or a recorder that could not open its session at all.
                if (recorder.GetStatus().state == "error") {
                    RespondJson(
                        resp, status_type::internal_server_error,
                        "{\"error\":\"cannot start recording; see the log\"}");
                    return;
                }
                RespondJson(resp, status_type::conflict,
                            "{\"error\":\"busy\"}");
                return;
            }
            RespondJson(resp, status_type::ok,
                        WriteJson(BuildStatusJson(recorder.GetStatus())));
        });
        server->set_http_handler<POST>(
            "/v1/recording/stop", [&recorder, token](coro_http_request& req,
                                                     coro_http_response& resp) {
                if (!CheckAuthorized(req, resp, token)) {
                    return;
                }
                if (!recorder.StopAsync()) {
                    RespondJson(resp, status_type::conflict,
                                "{\"error\":\"not recording\"}");
                    return;
                }
                RespondJson(resp, status_type::accepted,
                            "{\"state\":\"stopping\"}");
            });
        auto ec = server->async_start();
        if (ec.hasResult()) {
            LOG(ERROR) << "traffic: control server failed on " << host << ":"
                       << port << ": " << ec.value().message();
            return nullptr;
        }
        LOG(INFO) << "traffic: control server on " << host << ":" << port;
        return server;
    } catch (const std::exception& e) {
        LOG(ERROR) << "traffic: control server exception: " << e.what();
        return nullptr;
    }
}

}  // namespace mooncake::traffic
