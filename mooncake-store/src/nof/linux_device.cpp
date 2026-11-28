#include "linux_device.h"

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mooncake::detail {
namespace {
std::string Lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return value;
}

std::string Address(const std::string& text) {
    std::array<unsigned char, 16> bytes{};
    std::array<char, INET6_ADDRSTRLEN> result{};
    for (int family : {AF_INET, AF_INET6}) {
        if (inet_pton(family, text.c_str(), bytes.data()) == 1 &&
            inet_ntop(family, bytes.data(), result.data(), result.size())) {
            return result.data();
        }
    }
    return text;
}

bool Number(const std::string& text, uint32_t& value) {
    const auto [end, ec] =
        std::from_chars(text.data(), text.data() + text.size(), value);
    return ec == std::errc{} && end == text.data() + text.size() && value != 0;
}

std::string Read(const std::filesystem::path& path) {
    std::ifstream stream(path);
    std::string value;
    std::getline(stream, value);
    return value;
}

std::string AddressField(const std::string& address, const std::string& key) {
    std::istringstream stream(address);
    std::string field;
    while (std::getline(stream, field, ',')) {
        if (field.compare(0, key.size(), key) == 0 &&
            field.size() > key.size() && field[key.size()] == '=')
            return field.substr(key.size() + 1);
    }
    return {};
}
}  // namespace

bool ParseNofEndpoint(const std::string& text, NofEndpoint& endpoint,
                      std::string& error) {
    std::unordered_map<std::string, std::string> fields;
    std::istringstream stream(text);
    std::string token;
    while (stream >> token) {
        const auto split = token.find(':');
        const auto key = Lower(token.substr(0, split));
        if (split == std::string::npos || split + 1 == token.size() ||
            (key != "trtype" && key != "traddr" && key != "trsvcid" &&
             key != "subnqn" && key != "ns" && key != "adrfam") ||
            !fields.emplace(key, token.substr(split + 1)).second) {
            error = "invalid or duplicate NVMe-oF endpoint field: " + token;
            return false;
        }
    }
    NofEndpoint parsed;
    parsed.transport = Lower(fields["trtype"]);
    parsed.address = Address(fields["traddr"]);
    parsed.nqn = fields["subnqn"];
    uint32_t port = 0;
    if ((parsed.transport != "tcp" && parsed.transport != "rdma") ||
        parsed.address.empty() || parsed.nqn.empty() ||
        !Number(fields["trsvcid"], port) || port > 65535 ||
        (fields.count("ns") && !Number(fields["ns"], parsed.nsid)) ||
        parsed.nsid == UINT32_MAX) {
        error =
            "expected TCP/RDMA endpoint with traddr, trsvcid, subnqn and valid "
            "ns";
        return false;
    }
    if (fields.count("adrfam") && Lower(fields["adrfam"]) != "ipv4" &&
        Lower(fields["adrfam"]) != "ipv6") {
        error = "Linux NoF supports IPv4 and IPv6 endpoints";
        return false;
    }
    parsed.service = std::to_string(port);
    endpoint = std::move(parsed);
    error.clear();
    return true;
}

// Example layouts under /sys/class/nvme-subsystem/:
//   multipath=Y (two controllers in the same subsystem)
//     nvme-subsys11/nvme11/nvme11c11n1   NSID 1: hidden path through nvme11
//     nvme-subsys11/nvme11n1             NSID 1: visible head -> /dev/nvme11n1
//     nvme-subsys11/nvme12/nvme11c12n2   NSID 2: hidden path through nvme12
//     nvme-subsys11/nvme11n2             NSID 2: visible head -> /dev/nvme11n2
//   multipath=N
//     nvme-subsys20/nvme20/nvme20n1      NSID 1: visible path -> /dev/nvme20n1
std::string FindLinuxNvmeDevice(const NofEndpoint& endpoint, std::string& error,
                                const std::string& subsystem_root) {
    namespace fs = std::filesystem;
    std::set<std::string> devices;
    std::vector<fs::path> directories;
    size_t controller_count = 0;
    size_t matches = 0;
    fs::path selected;
    size_t target_paths = 0;
    try {
        for (const auto& subsystem : fs::directory_iterator(subsystem_root)) {
            if (Read(subsystem.path() / "subsysnqn") != endpoint.nqn) continue;
            directories.push_back(subsystem.path());
            for (const auto& entry : fs::directory_iterator(subsystem.path())) {
                const auto name = entry.path().filename().string();
                if (!name.starts_with("nvme") || name.size() == 4 ||
                    name.find_first_not_of("0123456789", 4) !=
                        std::string::npos)
                    continue;
                directories.push_back(entry.path());
                ++controller_count;
                const auto transport = Read(entry.path() / "transport");
                const auto address = Read(entry.path() / "address");
                if (transport.empty() || address.empty()) {
                    error = "cannot read NVMe controller binding: " + name;
                    return {};
                }
                if (transport == endpoint.transport &&
                    Address(AddressField(address, "traddr")) ==
                        endpoint.address &&
                    AddressField(address, "trsvcid") == endpoint.service) {
                    ++matches;
                    selected = entry.path();
                }
            }
        }
        if (matches != 1) {
            error = "expected one controller for " + endpoint.nqn + " at " +
                    endpoint.transport + " " + endpoint.address + ":" +
                    endpoint.service + ", found " + std::to_string(matches);
            return {};
        }

        // With multiple controllers, check all paths, including hidden ones,
        // to ensure only the selected controller exposes the requested NSID.
        const bool single_controller = controller_count == 1;
        for (const auto& directory : directories) {
            for (const auto& entry : fs::directory_iterator(directory)) {
                const auto& path = entry.path();
                const auto name = path.filename().string();
                if (!name.starts_with("nvme")) continue;
                const auto id = Read(path / "nsid");
                if (!single_controller && id.empty() &&
                    (fs::exists(path / "size") ||
                     fs::exists(path / "hidden"))) {
                    error = "cannot determine namespace ID: " + path.string();
                    return {};
                }
                uint32_t nsid = 0;
                if (!Number(id, nsid) || nsid != endpoint.nsid) continue;
                if (!single_controller && directory != selected &&
                    directory != selected.parent_path()) {
                    error = "NSID " + std::to_string(nsid) + " for " +
                            endpoint.nqn + ": requested target " +
                            endpoint.address + ":" + endpoint.service + " (" +
                            selected.filename().string() + "), found at " +
                            path.string();
                    return {};
                }
                const bool hidden = Read(path / "hidden") == "1";
                if (!hidden && Read(path / "dev").empty()) continue;
                if (directory == selected) ++target_paths;
                if (hidden) continue;
                devices.insert("/dev/" + name);
            }
        }
    } catch (const fs::filesystem_error& exception) {
        error = "cannot scan kernel NVMe topology: " +
                std::string(exception.what());
        return {};
    }
    if ((controller_count > 1 && target_paths != 1) || devices.size() != 1) {
        error = "expected one connected controller/namespace for trtype:" +
                endpoint.transport + " traddr:" + endpoint.address +
                " trsvcid:" + endpoint.service + " subnqn:" + endpoint.nqn +
                " ns:" + std::to_string(endpoint.nsid) +
                "; connect it with nvme-cli and remove ambiguous connections";
        return {};
    }
    error.clear();
    return *devices.begin();
}
}  // namespace mooncake::detail
