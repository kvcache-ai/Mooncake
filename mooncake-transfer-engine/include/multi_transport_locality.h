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

#include <string>

namespace mooncake {

// Segment names are formatted as "host:port". Return the host portion.
// If no ':' is present the whole string is treated as the host.
inline std::string segmentHost(const std::string& segment_name) {
    auto pos = segment_name.rfind(':');
    return pos == std::string::npos ? segment_name
                                    : segment_name.substr(0, pos);
}

// The device KV pool is registered under both rdma and hip in a "rdma,hip"
// multi-protocol segment. hip transport uses GPU IPC, which only works between
// processes on the same physical host, so a hip buffer is a valid transport for
// a target only when that target lives on the same host as the initiator.
// A cross-host target must fall back to rdma. Two engines co-located on one host
// share the host portion of their segment name (they differ only in port).
inline bool isHipReachableTarget(const std::string& target_segment_name,
                                 const std::string& local_server_name) {
    return segmentHost(target_segment_name) == segmentHost(local_server_name);
}

}  // namespace mooncake

#endif  // MULTI_TRANSPORT_LOCALITY_H
