// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "transport/rdma_transport/pxn/pxn_topology_manager.h"

#include <cuda_runtime.h>
#include <glog/logging.h>
#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <algorithm>
#include <cctype>

namespace mooncake {
namespace pxn {

// ---- Helpers ---------------------------------------------------------

// PCIe distance between two devices (bus ids like "0000:xx:yy.z"), measured
// as number of "/" segments between their respective /sys realpaths.
// Returns -1 on failure. Same algorithm as topology.cpp::getPciDistance.
int PxnPciDistance(const char* bus1, const char* bus2) {
    char buf[PATH_MAX];
    char path1[PATH_MAX];
    char path2[PATH_MAX];
    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus1);
    if (realpath(buf, path1) == NULL) return -1;
    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus2);
    if (realpath(buf, path2) == NULL) return -1;

    char* path1_iter = path1;
    char* path2_iter = path2;
    while (*path1_iter && *path1_iter == *path2_iter) {
        path1_iter++;
        path2_iter++;
    }
    int distance = 0;
    for (; *path1_iter; ++path1_iter) {
        if (*path1_iter == '/') ++distance;
    }
    for (; *path2_iter; ++path2_iter) {
        if (*path2_iter == '/') ++distance;
    }
    return distance;
}

bool PxnCrossesHostBridge(const char* bus1, const char* bus2) {
    char buf[PATH_MAX];
    char path1[PATH_MAX];
    char path2[PATH_MAX];
    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus1);
    if (realpath(buf, path1) == NULL) return true;
    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus2);
    if (realpath(buf, path2) == NULL) return true;

    const char* dev1 = strstr(path1, "/devices/");
    const char* dev2 = strstr(path2, "/devices/");
    if (!dev1 || !dev2) return true;

    dev1 += strlen("/devices/");
    dev2 += strlen("/devices/");

    size_t root_len1 = strcspn(dev1, "/");
    size_t root_len2 = strcspn(dev2, "/");
    if (root_len1 != root_len2) return true;
    if (strncmp(dev1, dev2, root_len1) != 0) return true;

    return false;
}

std::string PxnLowercase(const std::string& input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char ch) { return std::tolower(ch); });
    return result;
}

// ---- PxnTopologyManager public API -----------------------------------

int PxnTopologyManager::PxnCreateAndBuild(
    std::unique_ptr<PxnTopologyManager>& pxn_mgr,
    const std::vector<std::string>& hca_list) {
    auto mgr = std::unique_ptr<PxnTopologyManager>(new PxnTopologyManager());
    std::vector<PxnNicRecord> records;
    records.reserve(hca_list.size());
    for (const auto& name : hca_list) {
        PxnNicRecord record;
        record.pxn_nic_name = name;
        records.push_back(std::move(record));
    }
    int rc = mgr->PxnBuild(records);
    if (rc != 0) return rc;
    pxn_mgr = std::move(mgr);
    return 0;
}

int PxnTopologyManager::PxnBuild(const std::vector<PxnNicRecord>& hca_records) {
    if (pxn_built_) return 0;  // idempotent

    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        LOG(WARNING) << "PXN: no CUDA devices found; PXN disabled";
        pxn_built_ = true;
        return 0;
    }

    // cuda device info
    pxn_cuda_list_.clear();
    // cuda physics id -> pxn_cuda_id_to_index_ group subscript
    pxn_cuda_id_to_index_.clear();
    for (int cuda_dev = 0; cuda_dev < device_count; ++cuda_dev) {
        char pci[32] = {0};
        if (cudaDeviceGetPCIBusId(pci, sizeof(pci), cuda_dev) != cudaSuccess) {
            LOG(WARNING) << "PXN: cudaDeviceGetPCIBusId failed for dev "
                         << cuda_dev;
            continue;
        }
        PxnGpuRecord gpu_record;
        gpu_record.pxn_cuda_device_id = cuda_dev;
        gpu_record.pxn_pci_bus_id = PxnLowercase(pci);
        // cuda physics id -> pxn_cuda_list_ group subscript
        pxn_cuda_id_to_index_[cuda_dev] = (int)pxn_cuda_list_.size();
        // cuda device info
        pxn_cuda_list_.push_back(std::move(gpu_record));
    }

    pxn_hca_list_ = hca_records;
    for (auto& hca : pxn_hca_list_) {
        if (hca.pxn_pci_bus_id.empty()) {
            char link_path[PATH_MAX];
            snprintf(link_path, sizeof(link_path),
                     "/sys/class/infiniband/%s/device",
                     hca.pxn_nic_name.c_str());
            char resolved[PATH_MAX];
            if (realpath(link_path, resolved) != nullptr) {
                char* pci_bus_basename = basename(resolved);
                if (pci_bus_basename != nullptr) {
                    hca.pxn_pci_bus_id = pci_bus_basename;
                }
            }
        }
        hca.pxn_pci_bus_id = PxnLowercase(hca.pxn_pci_bus_id);
    }
    pxn_hca_name_to_index_.clear();
    // nic name -> pxn_hca_list_ group subscript
    for (int nic_idx = 0; nic_idx < (int)pxn_hca_list_.size(); ++nic_idx) {
        pxn_hca_name_to_index_[pxn_hca_list_[nic_idx].pxn_nic_name] = nic_idx;
    }

    // pxn_hca_list_ group subscript->affinity pxn_cuda_list_ group subscript
    pxn_nic_affinity_gpu_.assign(pxn_hca_list_.size(), -1);
    for (int nic_idx = 0; nic_idx < (int)pxn_hca_list_.size(); ++nic_idx) {
        pxn_nic_affinity_gpu_[nic_idx] = PxnComputeAffinityGpu(nic_idx);
    }

    // init pxn_nic_paths_
    const int gpu_count = (int)pxn_cuda_list_.size();
    const int nic_count = (int)pxn_hca_list_.size();
    pxn_nic_paths_.assign(gpu_count, std::vector<int>(nic_count, -1));

    // whether loaded nvidia_peermem
    const bool peermem_loaded = PxnCheckNvidiaPeermemLoaded();

    for (int src_gpu_idx = 0; src_gpu_idx < gpu_count; ++src_gpu_idx) {
        for (int nic_idx = 0; nic_idx < nic_count; ++nic_idx) {
            // pxn_hca_list_ group subscript
            // affinity pxn_cuda_list_ group subscript
            int proxy_gpu_idx = pxn_nic_affinity_gpu_[nic_idx];
            // current nic and gpu is affinity,not belong pxn
            if (proxy_gpu_idx < 0 || proxy_gpu_idx == src_gpu_idx) continue;

            int src_cuda_dev = pxn_cuda_list_[src_gpu_idx].pxn_cuda_device_id;
            int proxy_cuda_dev =
                pxn_cuda_list_[proxy_gpu_idx].pxn_cuda_device_id;
            // input cuda physics id, judge has nvlink
            if (!PxnHasNvLink(src_cuda_dev, proxy_cuda_dev)) continue;
            // have nvidia_peermem, and support gdr
            if (!peermem_loaded) continue;
            if (!PxnGpuSupportsGdr(proxy_cuda_dev)) {
                LOG(ERROR) << "PXN: proxy GPU cuda:" << proxy_cuda_dev
                           << " does not support GDR; skipping "
                           << "src cuda:" << src_cuda_dev << " -> nic "
                           << pxn_hca_list_[nic_idx].pxn_nic_name;
                continue;
            }

            // pxn_hca_list_ and pxn_cuda_list_ group id
            pxn_nic_paths_[src_gpu_idx][nic_idx] = proxy_gpu_idx;
            LOG(INFO) << "PXN: nic_paths src_gpu_physics_idx:" << src_cuda_dev
                      << " nic_name:" << pxn_hca_list_[nic_idx].pxn_nic_name
                      << " proxy_gpu_physics_idx:" << proxy_cuda_dev
                      << " src_gpu_idx:" << src_gpu_idx
                      << " nic_idx:" << nic_idx
                      << " proxy_gpu_idx:" << proxy_gpu_idx;
        }
    }

    LOG(INFO) << "PXN: topology built. " << gpu_count << " GPUs, " << nic_count
              << " NICs.";
    pxn_built_ = true;
    return 0;
}

// must input pxn_hca_list_ and pxn_cuda_list_ group subscript
int PxnTopologyManager::PxnLookupProxyGpu(int src_gpu, int nic_id) const {
    if (src_gpu < 0 || src_gpu >= (int)pxn_nic_paths_.size()) return -1;
    if (nic_id < 0 || nic_id >= (int)pxn_nic_paths_[src_gpu].size()) return -1;
    int idx = pxn_nic_paths_[src_gpu][nic_id];
    if (idx < 0) return -1;
    // return cuda device physics id
    return pxn_cuda_list_[idx].pxn_cuda_device_id;
}

// must input pxn_hca_list_ group subscript
int PxnTopologyManager::PxnLookupNicAffinityGpu(int nic_id) const {
    if (nic_id < 0 || nic_id >= (int)pxn_nic_affinity_gpu_.size()) return -1;
    // get affinity pxn_cuda_list_ group subscript
    int idx = pxn_nic_affinity_gpu_[nic_id];
    if (idx < 0) return -1;
    // return cuda device physics id
    return pxn_cuda_list_[idx].pxn_cuda_device_id;
}

// nic name -> pxn_hca_list_ group subscript
int PxnTopologyManager::PxnHcaNameToIndex(const std::string& hca_name) const {
    auto it = pxn_hca_name_to_index_.find(hca_name);
    return it == pxn_hca_name_to_index_.end() ? -1 : it->second;
}

std::string PxnTopologyManager::PxnIndexToHcaName(int hca_index) const {
    return pxn_hca_list_[hca_index].pxn_nic_name;
}

// cuda physics id -> pxn_cuda_list_ group subscript
int PxnTopologyManager::PxnCudaDeviceToIndex(int cuda_device_id) const {
    auto it = pxn_cuda_id_to_index_.find(cuda_device_id);
    return it == pxn_cuda_id_to_index_.end() ? -1 : it->second;
}

int PxnTopologyManager::PxnGetGpuIdFromAddr(const void* addr) {
    if (addr == nullptr) return -1;
    cudaPointerAttributes attr = {};
    cudaError_t err = cudaPointerGetAttributes(&attr, addr);
    if (err != cudaSuccess) {
        // Not a CUDA-known pointer; clear the sticky error.
        cudaGetLastError();
        return -1;
    }
    if (attr.type != cudaMemoryTypeDevice) return -1;
    return attr.device;
}

// ---- Private helpers -------------------------------------------------

// get pcie distance minimum gpu device
int PxnTopologyManager::PxnComputeAffinityGpu(int nic_id) const {
    const char* nic_pci = pxn_hca_list_[nic_id].pxn_pci_bus_id.c_str();
    int best_gpu_idx = -1;
    int best_distance = INT_MAX;
    for (int gpu_idx = 0; gpu_idx < (int)pxn_cuda_list_.size(); ++gpu_idx) {
        if (PxnCrossesHostBridge(
                nic_pci, pxn_cuda_list_[gpu_idx].pxn_pci_bus_id.c_str())) {
            continue;
        }
        int distance = PxnPciDistance(
            nic_pci, pxn_cuda_list_[gpu_idx].pxn_pci_bus_id.c_str());
        if (distance < 0) continue;
        if (distance < best_distance) {
            best_distance = distance;
            best_gpu_idx = gpu_idx;
        }
    }
    LOG(INFO) << "  -> best_gpu_idx=" << best_gpu_idx
              << " distance=" << best_distance;
    return best_gpu_idx;
}

// whether have nvlik between gpus
bool PxnTopologyManager::PxnHasNvLink(int src_cuda, int proxy_cuda) const {
    if (src_cuda == proxy_cuda) return false;
    int can_access = 0;
    cudaError_t err =
        cudaDeviceCanAccessPeer(&can_access, src_cuda, proxy_cuda);
    if (err != cudaSuccess) {
        cudaGetLastError();
        return false;
    }
    return can_access == 1;
}

bool PxnTopologyManager::PxnCheckNvidiaPeermemLoaded() {
    static bool cached = []() {
        struct stat st;
        if (stat("/sys/module/nvidia_peermem", &st) == 0 &&
            S_ISDIR(st.st_mode)) {
            return true;
        }
        if (stat("/sys/module/nv_peer_mem", &st) == 0 && S_ISDIR(st.st_mode)) {
            return true;
        }
        LOG(WARNING) << "PXN: nvidia_peermem kernel module not loaded; "
                        "GDR unavailable, PXN will fall back to non-PXN paths";
        return false;
    }();
    return cached;
}

// just check gpu support gdr
bool PxnTopologyManager::PxnGpuSupportsGdr(int cuda_device_id) {
    if (cuda_device_id < 0) return false;
    int val = 0;
    cudaError_t err = cudaDeviceGetAttribute(
        &val, cudaDevAttrGPUDirectRDMASupported, cuda_device_id);
    if (err != cudaSuccess) {
        cudaGetLastError();
        return false;
    }
    return val == 1;
}

}  // namespace pxn
}  // namespace mooncake
