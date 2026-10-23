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

std::string FindLinuxNvmeDevice(const NofEndpoint& endpoint, std::string& error,
                                const std::string& subsystem_root) {
    namespace fs = std::filesystem;
    std::set<std::string> devices;
    size_t matches = 0;
    try {
        for (const auto& subsystem : fs::directory_iterator(subsystem_root)) {
            if (Read(subsystem.path() / "subsysnqn") != endpoint.nqn) continue;
            size_t controllers = 0;
            fs::path selected;
            for (const auto& entry : fs::directory_iterator(subsystem.path())) {
                const auto transport = Read(entry.path() / "transport");
                if (transport.empty()) continue;
                ++controllers;
                const auto address = Read(entry.path() / "address");
                if (transport == endpoint.transport &&
                    Address(AddressField(address, "traddr")) ==
                        endpoint.address &&
                    AddressField(address, "trsvcid") == endpoint.service) {
                    selected = entry.path();
                    ++matches;
                }
            }
            if (selected.empty()) continue;
            // This backend initially supports one path per subsystem. A
            // multipath head could otherwise route an endpoint's writes over
            // a different controller and alias two Store allocation domains.
            if (controllers != 1) {
                error =
                    "Linux NoF requires one connected controller per subsystem";
                return {};
            }
            // With native multipath enabled, the visible namespace head lives
            // under the subsystem. Otherwise it lives under the controller.
            // Hidden per-controller paths have no dev attribute; never open
            // one or derive a device name from NSID (NSID != device suffix).
            for (const auto& directory : {subsystem.path(), selected}) {
                for (const auto& entry : fs::directory_iterator(directory)) {
                    const auto name = entry.path().filename().string();
                    uint32_t nsid = 0;
                    if (name.rfind("nvme", 0) == 0 &&
                        !Read(entry.path() / "dev").empty() &&
                        Read(entry.path() / "hidden") != "1" &&
                        Number(Read(entry.path() / "nsid"), nsid) &&
                        nsid == endpoint.nsid)
                        devices.insert("/dev/" + name);
                }
            }
        }
    } catch (const fs::filesystem_error& exception) {
        error = "cannot scan kernel NVMe topology: " +
                std::string(exception.what());
        return {};
    }
    if (matches != 1 || devices.size() != 1) {
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
