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

#pragma once

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <string>

#include <sys/stat.h>

#include "cuda_alike.h"

namespace mooncake::nvlink_test {

inline bool parseStrictMode(const char* name, bool& strict,
                            std::string& error) {
    strict = false;
    const char* value = std::getenv(name);
    if (value == nullptr || std::string(value) == "0") return true;
    if (std::string(value) == "1") {
        strict = true;
        return true;
    }
    error = std::string(name) + " must be 0 or 1";
    return false;
}

inline bool parseNonNegativeInt(const std::string& text, int& value) {
    if (text.empty()) return false;
    size_t parsed = 0;
    try {
        const long result = std::stol(text, &parsed, 10);
        if (parsed != text.size() || result < 0 ||
            result > std::numeric_limits<int>::max()) {
            return false;
        }
        value = static_cast<int>(result);
        return true;
    } catch (...) {
        return false;
    }
}

inline bool isOnlineNumaNode(int node, std::string& error) {
    const std::string node_path =
        "/sys/devices/system/node/node" + std::to_string(node);
    struct stat node_stat{};
    if (stat(node_path.c_str(), &node_stat) != 0 ||
        !S_ISDIR(node_stat.st_mode)) {
        error = "NUMA node " + std::to_string(node) + " is absent from sysfs";
        return false;
    }

    std::ifstream online(node_path + "/online");
    if (!online.is_open()) {
        // Linux commonly omits node0/online. An existing node directory is
        // online in that case.
        return true;
    }
    int flag = 0;
    if (!(online >> flag) || flag != 1) {
        error = "NUMA node " + std::to_string(node) + " is not online";
        return false;
    }
    return true;
}

inline bool deviceNumaNodeFromSysfs(int device, int& node, std::string& error) {
    char pci_bus_id[64] = {};
    const cudaError_t result =
        cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), device);
    if (result != cudaSuccess) {
        error = "cudaDeviceGetPCIBusId failed for visible GPU " +
                std::to_string(device) + ": " + cudaGetErrorString(result);
        return false;
    }

    std::string bdf(pci_bus_id);
    std::transform(bdf.begin(), bdf.end(), bdf.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (std::count(bdf.begin(), bdf.end(), ':') == 1) bdf = "0000:" + bdf;

    std::ifstream numa_file("/sys/bus/pci/devices/" + bdf + "/numa_node");
    if (!numa_file.is_open() || !(numa_file >> node) || node < 0) {
        error = "visible GPU " + std::to_string(device) +
                " has no usable PCI-to-NUMA mapping in sysfs";
        return false;
    }
    return isOnlineNumaNode(node, error);
}

}  // namespace mooncake::nvlink_test
