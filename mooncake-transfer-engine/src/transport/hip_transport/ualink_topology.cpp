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

#include "transport/hip_transport/ualink_topology.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>

namespace mooncake {
namespace ualink {

namespace {

bool readAttr(const std::string &dir, const char *name, std::string &value) {
    std::ifstream in(dir + "/" + name);
    if (!in) return false;
    std::getline(in, value);
    while (!value.empty() && std::isspace((unsigned char)value.back()))
        value.pop_back();
    return true;
}

bool parseU32(const std::string &text, uint32_t &value) {
    if (text.empty()) return false;
    char *end = nullptr;
    errno = 0;
    unsigned long parsed = strtoul(text.c_str(), &end, 0);
    if (errno || !end || *end != '\0' || parsed >= kNoVpod) return false;
    value = static_cast<uint32_t>(parsed);
    return true;
}

}  // namespace

bool fabricDisabledByEnv() {
    const char *env = getenv("MC_UALOE_DISABLE");
    return env && strcmp(env, "0") != 0 && env[0] != '\0';
}

VpodInfo readVpodFromSysfs(const std::string &bdf,
                           const std::string &pci_root) {
    VpodInfo info;
    info.source = "none";
    std::string lower = bdf;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    const std::string dir = pci_root + "/" + lower + "/ualink";

    std::string state, vpod;
    if (!readAttr(dir, "accel_state", state)) return info;
    if (state != "active") {
        LOG(WARNING) << "UALink accelerator " << lower
                     << " is not active (state=" << state << ")";
        return info;
    }
    if (!readAttr(dir, "vpod_id", vpod) || !parseU32(vpod, info.vpod_id))
        return info;
    readAttr(dir, "ppod_id", info.ppod_id);
    readAttr(dir, "link_type", info.link_type);
    std::string size;
    if (readAttr(dir, "vpod_size", size)) parseU32(size, info.vpod_size);
    info.valid = true;
    info.source = "sysfs";
    return info;
}

VpodInfo resolveVpod(const std::string &bdf, const std::string &pci_root) {
    if (fabricDisabledByEnv()) {
        VpodInfo info;
        info.source = "none";
        return info;
    }
    const char *env = getenv("MC_UALOE_VPOD_ID");
    if (env && env[0] != '\0') {
        VpodInfo info;
        if (parseU32(env, info.vpod_id)) {
            info.valid = true;
            info.source = "env";
            return info;
        }
        LOG(WARNING) << "Ignoring invalid MC_UALOE_VPOD_ID";
    }
    return readVpodFromSysfs(bdf, pci_root);
}

bool sameVpod(const VpodInfo &local, const std::string &remote_ppod,
              uint32_t remote_vpod) {
    if (!local.valid || remote_vpod == kNoVpod) return false;
    if (local.vpod_id != remote_vpod) return false;
    if (local.ppod_id.empty() || remote_ppod.empty()) return true;
    return local.ppod_id == remote_ppod;
}

}  // namespace ualink
}  // namespace mooncake
