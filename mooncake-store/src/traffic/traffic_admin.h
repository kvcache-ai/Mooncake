#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace cinatra {
class coro_http_server;
}

namespace mooncake::traffic {

class TrafficRecorder;
struct RecorderStatus;

// A token file read from disk may be an operator-provided string, or a file
// this code generated earlier and therefore expects to see in its own format.
enum class TokenFormat { kAny, kHex };

bool GenerateControlToken(std::string* token);
std::optional<std::string> ReadControlToken(const std::filesystem::path& path,
                                            TokenFormat format);
bool WriteControlToken(const std::filesystem::path& path, std::string* token);

bool WriteSummaryJson(const std::filesystem::path& session_dir,
                      const RecorderStatus& status);

bool WriteRouteNamesJson(
    const std::filesystem::path& session_dir,
    const std::unordered_map<std::uint32_t, std::string>& route_names);

std::unique_ptr<cinatra::coro_http_server> StartControlServer(
    TrafficRecorder& recorder, const std::string& host, std::uint16_t port,
    const std::string& token);

}  // namespace mooncake::traffic
