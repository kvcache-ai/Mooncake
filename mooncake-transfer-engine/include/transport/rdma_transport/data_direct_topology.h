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

#ifndef DATA_DIRECT_TOPOLOGY_H
#define DATA_DIRECT_TOPOLOGY_H

#include <infiniband/verbs.h>

#include <map>
#include <string>
#include <vector>

namespace mooncake {

// NVIDIA ConnectX-8 "Data Direct" registers GPU memory through a companion
// PCI function that sits on the GPU's PCIe root, and only memory of GPUs under
// that root can be registered this way (mlx5dv_reg_dmabuf_mr with
// MLX5DV_REG_DMABUF_ACCESS_DATA_DIRECT fails with ENOTSUPP for any other GPU).
// These helpers expose the pairing: a NIC and a GPU are paired when the sysfs
// path of the NIC's Data Direct function and the GPU's own sysfs path share
// the same PCI root ("pci0009:00" for 0009:06:00.0).

// First path component that names a PCI root ("pci0009:00") in a sysfs path
// such as "/sys/devices/pci0009:00/0009:00:00.0/..." or
// "/devices/pci0009:00/...". Empty when the path has none.
std::string firstPciRootSegment(const std::string &sysfs_path);

// PCI root of a PCI device given its bus id ("0009:06:00.0", any case), or
// empty when the device is not visible in sysfs.
std::string pciSysfsRoot(const std::string &pci_bus_id);

// PCI root of the NIC's Data Direct function, resolved through
// mlx5dv_get_data_direct_sysfs_path (loaded at runtime from libmlx5
// MLX5_1.25). Empty when the NIC has no Data Direct function, the running
// libmlx5 predates the API, or this is not a CUDA build.
std::string dataDirectSysfsRoot(ibv_context *context);

// Data Direct PCI root per RDMA device name, for the named devices (all
// devices when `device_names` is empty). Devices without a Data Direct
// function are absent from the map.
std::map<std::string, std::string> dataDirectRootsByDevice(
    const std::vector<std::string> &device_names);

}  // namespace mooncake

#endif  // DATA_DIRECT_TOPOLOGY_H
