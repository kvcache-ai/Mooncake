// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef MOONCAKE_PXN_TOPOLOGY_MANAGER_H_
#define MOONCAKE_PXN_TOPOLOGY_MANAGER_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {
namespace pxn {

class PxnTopologyManager {
   public:
    struct PxnNicRecord {
        // hca name and pci bus id
        std::string pxn_nic_name;
        std::string pxn_pci_bus_id;
    };

    // cuda physics id + PCIe bus id
    struct PxnGpuRecord {
        int pxn_cuda_device_id = -1;
        std::string pxn_pci_bus_id;
    };

    // Construct topology pxn manager. If `pxn_mgr` is non-null and its
    // cached hca_list matches the new hca_list, leaves `pxn_mgr`
    // unchanged (avoids repeated CUDA/nvml/realpath syscalls). Otherwise
    // rebuilds and writes the new manager back through `pxn_mgr`.
    // Returns 0 on success, non-zero on failure.
    static int PxnCreateAndBuild(
        std::unique_ptr<PxnTopologyManager>& pxn_mgr,
        const std::vector<std::string>& hca_list);

    // PxnCreateAndBuild call PxnBuild
    int PxnBuild(const std::vector<PxnNicRecord>& hca_records);

    bool PxnIsBuilt() const { return pxn_built_; }

    // find proxy gpu physics id,input group id
    int PxnLookupProxyGpu(int src_gpu, int nic_id) const;

    int PxnLookupNicAffinityGpu(int nic_id) const;

    // HCA name -> hca_list_ group index
    int PxnHcaNameToIndex(const std::string& hca_name) const;

    std::string PxnIndexToHcaName(int hca_index) const;

    // cuda physics id -> cuda_list_ group index
    int PxnCudaDeviceToIndex(int cuda_device_id) const;

    // gpu memory -> gpu physics id
    static int PxnGetGpuIdFromAddr(const void* addr);

    int PxnNumGpus() const { return (int)pxn_cuda_list_.size(); }
    int PxnNumNics() const { return (int)pxn_hca_list_.size(); }

   private:
    std::vector<PxnGpuRecord> pxn_cuda_list_;
    std::vector<PxnNicRecord> pxn_hca_list_;
    std::unordered_map<std::string, int> pxn_hca_name_to_index_;
    std::unordered_map<int, int> pxn_cuda_id_to_index_;
    // pxn_nic_affinity_gpu_[nic group index] = gpu group index
    std::vector<int> pxn_nic_affinity_gpu_;
    // 2D route table:pxn_nic_paths_[src_gpu][nic] = proxy_gpu group index
    std::vector<std::vector<int>> pxn_nic_paths_;
    bool pxn_built_ = false;

    int PxnComputeAffinityGpu(int nic_id) const;
    bool PxnHasNvLink(int src_cuda_id, int proxy_cuda_id) const;

    static bool PxnCheckNvidiaPeermemLoaded();
    static bool PxnGpuSupportsGdr(int cuda_device_id);
};

}  // namespace pxn
}  // namespace mooncake

#endif  // MOONCAKE_PXN_TOPOLOGY_MANAGER_H_
