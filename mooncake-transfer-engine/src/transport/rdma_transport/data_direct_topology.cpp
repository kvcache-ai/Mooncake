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

#include "transport/rdma_transport/data_direct_topology.h"

#include <glog/logging.h>
#include <limits.h>
#include <stdlib.h>

#include <algorithm>
#include <cctype>

#ifdef USE_CUDA
#include <dlfcn.h>
#endif

namespace mooncake {

std::string firstPciRootSegment(const std::string &sysfs_path) {
    size_t start = 0;
    while (start <= sysfs_path.size()) {
        size_t end = sysfs_path.find('/', start);
        if (end == std::string::npos) end = sysfs_path.size();
        if (end > start) {
            const std::string segment = sysfs_path.substr(start, end - start);
            if (segment.rfind("pci", 0) == 0 && segment.size() > 3)
                return segment;
        }
        start = end + 1;
    }
    return "";
}

std::string pciSysfsRoot(const std::string &pci_bus_id) {
    std::string bus_id = pci_bus_id;
    std::transform(bus_id.begin(), bus_id.end(), bus_id.begin(),
                   [](unsigned char ch) { return std::tolower(ch); });
    const std::string path = "/sys/bus/pci/devices/" + bus_id;
    char resolved[PATH_MAX];
    if (realpath(path.c_str(), resolved) == nullptr) return "";
    return firstPciRootSegment(resolved);
}

#ifdef USE_CUDA
namespace {
using Mlx5DataDirectSysfsPath = int (*)(ibv_context *, char *, size_t);

Mlx5DataDirectSysfsPath dataDirectSysfsPathFn() {
    static const Mlx5DataDirectSysfsPath fn = []() -> Mlx5DataDirectSysfsPath {
        dlerror();
        void *handle = dlopen("libmlx5.so.1", RTLD_NOW | RTLD_LOCAL);
        if (!handle) return nullptr;
        void *symbol =
            dlvsym(handle, "mlx5dv_get_data_direct_sysfs_path", "MLX5_1.25");
        if (!symbol) {
            VLOG(1) << "libmlx5 has no mlx5dv_get_data_direct_sysfs_path@"
                       "MLX5_1.25; Data Direct NIC pairing is unknown";
            dlclose(handle);
            return nullptr;
        }
        return reinterpret_cast<Mlx5DataDirectSysfsPath>(symbol);
    }();
    return fn;
}
}  // namespace

std::string dataDirectSysfsRoot(ibv_context *context) {
    auto fn = dataDirectSysfsPathFn();
    if (!fn || !context) return "";
    char path[PATH_MAX];
    path[0] = '\0';
    if (fn(context, path, sizeof(path)) != 0) return "";
    return firstPciRootSegment(path);
}
#else
std::string dataDirectSysfsRoot(ibv_context *) { return ""; }
#endif

std::map<std::string, std::string> dataDirectRootsByDevice(
    const std::vector<std::string> &device_names) {
    std::map<std::string, std::string> roots;
    int num_devices = 0;
    ibv_device **device_list = ibv_get_device_list(&num_devices);
    if (!device_list) return roots;
    for (int i = 0; i < num_devices; ++i) {
        const std::string name = ibv_get_device_name(device_list[i]);
        if (!device_names.empty() &&
            std::find(device_names.begin(), device_names.end(), name) ==
                device_names.end())
            continue;
        ibv_context *context = ibv_open_device(device_list[i]);
        if (!context) continue;
        const std::string root = dataDirectSysfsRoot(context);
        ibv_close_device(context);
        if (!root.empty()) roots[name] = root;
    }
    ibv_free_device_list(device_list);
    return roots;
}

}  // namespace mooncake
