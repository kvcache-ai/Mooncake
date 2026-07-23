// Copyright 2026 KVCache.AI
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

#ifndef MULTI_TRANSPORT_LOCALITY_H
#define MULTI_TRANSPORT_LOCALITY_H

#include <cctype>
#include <string>

namespace mooncake {

// Extract the host portion of a segment name. Segment names carry an optional
// ":port" suffix, and the host may be an IPv4 address, a hostname, or an IPv6
// literal. IPv6 literals contain multiple colons, so a naive rfind(':') would
// corrupt them; the following forms are handled explicitly:
//   "10.0.0.1:8000"      -> "10.0.0.1"      (IPv4 / hostname with port)
//   "node-a"             -> "node-a"        (no port)
//   "[2001:db8::1]:8000" -> "2001:db8::1"   (bracketed IPv6 with port)
//   "[2001:db8::1]"      -> "2001:db8::1"   (bracketed IPv6, no port)
//   "2001:db8::1"        -> "2001:db8::1"   (bare IPv6, no port)
inline std::string segmentHost(const std::string& segment_name) {
    if (!segment_name.empty() && segment_name.front() == '[') {
        // Bracketed IPv6 literal: strip the brackets and ignore any ":port".
        auto close = segment_name.find(']');
        if (close != std::string::npos) {
            return segment_name.substr(1, close - 1);
        }
        return segment_name;  // Malformed; return as-is.
    }
    auto first = segment_name.find(':');
    if (first == std::string::npos) {
        return segment_name;  // No port and no colon.
    }
    if (first != segment_name.rfind(':')) {
        // More than one colon and not bracketed: a bare IPv6 literal without a
        // port (an IPv6 host with a port must use brackets). Treat it all as
        // the host.
        return segment_name;
    }
    return segment_name.substr(0, first);  // "host:port".
}

// Case-insensitive equality. Hostnames are case-insensitive per DNS, and IPv6
// hex literals may differ only in letter case, so a plain "==" would spuriously
// classify the same host as remote and lose the intra-node hip fast path.
inline bool hostEquals(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

// GPU IPC transports (HIP and MUSA) only work between processes on the same
// physical host. A cross-host target must fall back to RDMA/TCP. Two engines
// co-located on one host share the host portion of their segment name (they
// differ only in port).
inline bool isGpuIpcReachableTarget(const std::string& target_segment_name,
                                    const std::string& local_server_name) {
    return hostEquals(segmentHost(target_segment_name),
                      segmentHost(local_server_name));
}

// Compatibility name retained for existing callers/tests.
inline bool isHipReachableTarget(const std::string& target_segment_name,
                                 const std::string& local_server_name) {
    return isGpuIpcReachableTarget(target_segment_name, local_server_name);
}

}  // namespace mooncake

#endif  // MULTI_TRANSPORT_LOCALITY_H
