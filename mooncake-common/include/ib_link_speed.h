// Copyright 2025 KVCache.AI
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

#ifndef MOONCAKE_IB_LINK_SPEED_H_
#define MOONCAKE_IB_LINK_SPEED_H_

namespace mooncake {

// Per-lane signalling rate for an ibv_port_attr::active_speed encoding
// (see ibv_query_port(3)). Returns 0 for an encoding that is not known.
inline double ibLaneSpeedGbps(int active_speed) {
    switch (active_speed) {
        case 1:
            return 2.5;  // SDR
        case 2:
            return 5.0;  // DDR
        case 4:
            return 10.0;  // QDR
        case 8:
            return 10.0;  // FDR10
        case 16:
            return 14.0;  // FDR
        case 32:
            return 25.0;  // EDR
        case 64:
            return 50.0;  // HDR
        case 128:
            return 100.0;  // NDR
        case 256:
            return 200.0;  // XDR
        default:
            return 0.0;
    }
}

// Lane count for an ibv_port_attr::active_width encoding. Returns 0 for an
// encoding that is not known.
inline int ibLinkWidthLanes(int active_width) {
    switch (active_width) {
        case 1:
            return 1;
        case 2:
            return 4;
        case 4:
            return 8;
        case 8:
            return 12;
        case 16:
            return 2;
        default:
            return 0;
    }
}

// Link speed in Gbps from the raw ibv_port_attr encodings, or 0 when either
// encoding is unknown so the caller can fall back explicitly rather than
// scheduling against a guessed rate.
inline double ibLinkSpeedGbps(int active_speed, int active_width) {
    return ibLaneSpeedGbps(active_speed) * ibLinkWidthLanes(active_width);
}

}  // namespace mooncake

#endif  // MOONCAKE_IB_LINK_SPEED_H_
