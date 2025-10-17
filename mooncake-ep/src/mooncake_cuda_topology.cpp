#include <cuda_runtime.h>
#include <dirent.h>
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <topology.h>

namespace mooncake {

struct InfinibandDevice {
    std::string name;
    std::string pci_bus_id;
};

static std::vector<InfinibandDevice> listInfiniBandDevices(
    const std::vector<std::string> &filter) {
    int num_devices = 0;
    std::vector<InfinibandDevice> devices;

    struct ibv_device **device_list = ibv_get_device_list(&num_devices);
    if (!device_list) {
        LOG(WARNING) << "No RDMA devices found, check your device installation";
        return {};
    }
    if (device_list && num_devices <= 0) {
        LOG(WARNING) << "No RDMA devices found, check your device installation";
        ibv_free_device_list(device_list);
        return {};
    }

    for (int i = 0; i < num_devices; ++i) {
        std::string device_name = ibv_get_device_name(device_list[i]);
        if (!filter.empty() && std::find(filter.begin(), filter.end(),
                                         device_name) == filter.end())
            continue;
        char path[PATH_MAX + 32];
        char resolved_path[PATH_MAX];
        // Get the PCI bus id for the infiniband device. Note that
        // "/sys/class/infiniband/mlx5_X/" is a symlink to
        // "/sys/devices/pciXXXX:XX/XXXX:XX:XX.X/infiniband/mlx5_X/".
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/../..",
                 device_name.c_str());
        if (realpath(path, resolved_path) == NULL) {
            PLOG(ERROR) << "listInfiniBandDevices: realpath " << path
                        << " failed";
            continue;
        }
        std::string pci_bus_id = basename(resolved_path);

        devices.push_back(
            InfinibandDevice{.name = std::move(device_name),
                             .pci_bus_id = std::move(pci_bus_id)});
    }
    ibv_free_device_list(device_list);
    return devices;
}

static int getPciDistance(const char *bus1, const char *bus2) {
    char buf[PATH_MAX];
    char path1[PATH_MAX];
    char path2[PATH_MAX];
    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus1);
    if (realpath(buf, path1) == NULL) {
        return -1;
    }
    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus2);
    if (realpath(buf, path2) == NULL) {
        return -1;
    }

    char *ptr1 = path1;
    char *ptr2 = path2;
    while (*ptr1 && *ptr1 == *ptr2) {
        ptr1++;
        ptr2++;
    }
    int distance = 0;
    for (; *ptr1; ptr1++) {
        distance += (*ptr1 == '/');
    }
    for (; *ptr2; ptr2++) {
        distance += (*ptr2 == '/');
    }

    return distance;
}

static std::vector<TopologyEntry> discoverCudaTopology(
    const std::vector<InfinibandDevice> &all_hca) {
    std::vector<TopologyEntry> topology;
    int device_count;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess) {
        device_count = 0;
    }
    for (int i = 0; i < device_count; i++) {
        char pci_bus_id[20];
        if (cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), i) !=
            cudaSuccess) {
            continue;
        }
        for (char *ch = pci_bus_id; (*ch = tolower(*ch)); ch++);

        std::vector<std::string> preferred_hca;
        std::vector<std::string> avail_hca;

        // Find HCAs with minimum distance in one pass
        int min_distance = INT_MAX;
        std::vector<std::string> min_distance_hcas;

        for (const auto &hca : all_hca) {
            int distance = getPciDistance(hca.pci_bus_id.c_str(), pci_bus_id);
            if (distance >= 0) {
                if (distance < min_distance) {
                    min_distance = distance;
                    min_distance_hcas.clear();
                    min_distance_hcas.push_back(hca.name);
                } else if (distance == min_distance) {
                    min_distance_hcas.push_back(hca.name);
                }
            }
        }

        // Add HCAs with minimum distance to preferred_hca, others to avail_hca
        for (const auto &hca : all_hca) {
            if (std::find(min_distance_hcas.begin(), min_distance_hcas.end(),
                          hca.name) != min_distance_hcas.end()) {
                preferred_hca.push_back(hca.name);
            } else {
                avail_hca.push_back(hca.name);
            }
        }
        topology.push_back(
            TopologyEntry{.name = "cuda:" + std::to_string(i),
                          .preferred_hca = std::move(preferred_hca),
                          .avail_hca = std::move(avail_hca)});
    }
    return topology;
}

std::string getCudaTopologyJson(const std::vector<std::string> &filter) {
    Json::Value topology(Json::objectValue);
    auto all_hca = listInfiniBandDevices(filter);
    for (auto &ent : discoverCudaTopology(all_hca)) {
        topology[ent.name] = ent.toJson();
    }
    return topology.toStyledString();
}
}  // namespace mooncake