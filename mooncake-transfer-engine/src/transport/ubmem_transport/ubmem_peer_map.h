// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif

namespace mooncake {

// "peerhost:/dev/obmm_shmdev1=/dev/obmm_shmdev5,..." ->
// {"peerhost\x1F/dev/obmm_shmdev1": "/dev/obmm_shmdev5", ...}
inline std::unordered_map<std::string, std::string> parsePeerDevMap(
    const char* spec) {
    std::unordered_map<std::string, std::string> out;
    if (!spec || !*spec) return out;
    std::string s(spec);
    size_t beg = 0;
    for (;;) {
        size_t end = s.find(',', beg);
        std::string entry = s.substr(
            beg, end == std::string::npos ? std::string::npos : end - beg);
        size_t sep = entry.find('=');
        size_t colon = entry.find(':');
        if (sep != std::string::npos && colon != std::string::npos &&
            colon < sep) {
            std::string peer = entry.substr(0, colon);
            std::string remote = entry.substr(colon + 1, sep - colon - 1);
            std::string local = entry.substr(sep + 1);
            auto trim = [](std::string& v) {
                size_t b = v.find_first_not_of(' ');
                size_t e = v.find_last_not_of(' ');
                v = b == std::string::npos ? "" : v.substr(b, e - b + 1);
            };
            trim(peer);
            trim(remote);
            trim(local);
            if (!peer.empty() && !remote.empty() && !local.empty())
                out[peer + "\x1f" + remote] = local;
        }
        if (end == std::string::npos) break;
        beg = end + 1;
    }
    return out;
}

// host part of a "host" / "host:port" / "scheme://host:port" endpoint
inline std::string hostOf(const std::string& endpoint) {
    auto scheme = endpoint.find("://");
    size_t beg = scheme == std::string::npos ? 0 : scheme + 3;
    auto colon = endpoint.find(':', beg);
    return endpoint.substr(
        beg, colon == std::string::npos ? std::string::npos : colon - beg);
}

// Cluster-wide device map file (MC_UBMEM_PEER_DEV_MAP_FILE), JSON:
//   { "<source-host>": { "<source-device>":
//       { "<reader-host>": "<reader-local-mirror-device>" } } }
// Every node loads the same file and keeps the entries where the
// reader host is itself; a node that exports devices but imports
// none simply ends up with no mappings.
inline std::unordered_map<std::string, std::string> parsePeerDevMapFile(
    const char* path, const std::vector<std::string>& self_hosts) {
    std::unordered_map<std::string, std::string> out;
    std::ifstream in(path);
    if (!in) {
        LOG(ERROR) << "UbmemTransport: cannot open MC_UBMEM_PEER_DEV_MAP_FILE "
                   << path;
        return out;
    }
    Json::Value root;
    std::string errors;
    if (!Json::parseFromStream(Json::CharReaderBuilder(), in, &root, &errors)) {
        LOG(ERROR) << "UbmemTransport: parse error in " << path << ": "
                   << errors;
        return out;
    }
    for (const auto& src_host : root.getMemberNames()) {
        const Json::Value& devs = root[src_host];
        if (!devs.isObject()) {
            LOG(ERROR) << "UbmemTransport: bad entry for " << src_host << " in "
                       << path;
            continue;
        }
        for (const auto& src_dev : devs.getMemberNames()) {
            const Json::Value& readers = devs[src_dev];
            if (!readers.isObject()) {
                LOG(ERROR) << "UbmemTransport: bad entry for " << src_host
                           << " device " << src_dev << " in " << path;
                continue;
            }
            for (const auto& reader : readers.getMemberNames()) {
                if (std::find(self_hosts.begin(), self_hosts.end(), reader) ==
                    self_hosts.end())
                    continue;
                const std::string local = readers[reader].asString();
                if (local.empty()) {
                    LOG(ERROR) << "UbmemTransport: bad entry for reader "
                               << reader << " in " << path;
                    continue;
                }
                out[src_host + "\x1f" + src_dev] = local;
            }
        }
    }
    return out;
}

}  // namespace mooncake
