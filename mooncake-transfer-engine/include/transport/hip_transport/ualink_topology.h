// Copyright(C) 2026 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef UALINK_TOPOLOGY_H_
#define UALINK_TOPOLOGY_H_

#include <cstdint>
#include <string>

namespace mooncake {
namespace ualink {

constexpr uint32_t kNoVpod = 0xffffffffu;
constexpr const char *kDefaultPciRoot = "/sys/bus/pci/devices";

// UALink fabric domain of one accelerator. Two accelerators can exchange
// fabric handles only inside the same vPOD of the same pPOD.
struct VpodInfo {
    bool valid = false;
    std::string ppod_id;  // empty when set manually through the environment
    uint32_t vpod_id = kNoVpod;
    uint32_t vpod_size = 0;
    std::string link_type;
    std::string source;  // "sysfs", "env" or "none"
};

// MC_UALOE_DISABLE=1 turns fabric export and cross-host fabric routing off.
bool fabricDisabledByEnv();

// Reads <pci_root>/<bdf>/ualink/{accel_state,ppod_id,vpod_id,...}. The
// accelerator must be in the "active" state. bdf is matched lowercase.
VpodInfo readVpodFromSysfs(const std::string &bdf,
                           const std::string &pci_root = kDefaultPciRoot);

// Applies MC_UALOE_DISABLE and MC_UALOE_VPOD_ID on top of sysfs.
VpodInfo resolveVpod(const std::string &bdf,
                     const std::string &pci_root = kDefaultPciRoot);

// An empty ppod on either side matches any pPOD (manual configuration).
bool sameVpod(const VpodInfo &local, const std::string &remote_ppod,
              uint32_t remote_vpod);

}  // namespace ualink
}  // namespace mooncake

#endif  // UALINK_TOPOLOGY_H_
