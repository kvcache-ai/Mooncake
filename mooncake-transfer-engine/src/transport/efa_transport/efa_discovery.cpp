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

// Must come before any other include that could pull in <rdma/fabric.h>.
#include "transport/efa_transport/efa_fabric.h"

#include "transport/efa_transport/efa_discovery.h"

#include <link.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <mutex>
#include <set>

#include "cuda_alike.h"
#include "transport/efa_transport/efa_neuron.h"

namespace mooncake {

// GPU builds also run on hosts without a GPU (e.g. the CUDA wheel on a CPU
// instance). Request FI_HMEM and use the device-memory paths only when a
// device is present; otherwise the EFA provider rejects the FI_HMEM hints.
bool efaHasGpuDevice() {
    static const bool present = [] {
        int count = 0;
#if defined(USE_CUDA)
        return cudaGetDeviceCount(&count) == cudaSuccess && count > 0;
#elif defined(USE_HIP)
        return hipGetDeviceCount(&count) == hipSuccess && count > 0;
#else
        (void)count;
        return false;
#endif
    }();
    return present;
}

void prepareEfaLibfabricEnv() {
    static std::once_flag once;
    std::call_once(once, [] {
        if (!efaHasGpuDevice()) {
            // Without GPU memory to register (CPU build or no device), prevent
            // libfabric's EFA provider from dlopen-ing libcudart/libcuda at
            // fi_getinfo/fi_domain time. That initialization creates a CUDA
            // primary context on GPU 0 and leaks ~616 MiB of device memory
            // even when no GPU memory is ever registered. Only set if the user
            // hasn't explicitly configured FI_HMEM.
            //
            // On a Neuron host the same guard has to name "neuron" instead:
            // FI_HMEM takes exactly one interface, not a list, so "system" (or
            // even "system,neuron") makes fi_getinfo() report -FI_ENODATA once
            // FI_MR_HMEM is in mr_mode.  Selecting "neuron" still leaves host
            // memory registrable -- FI_HMEM_SYSTEM is always available -- and
            // still keeps libfabric away from libcudart, which is the point of
            // the guard.
            setenv("FI_HMEM", neuronAvailable() ? "neuron" : "system", 0);
        }

        // Keep libfabric's SHM sub-provider out of the way on Neuron hosts.
        //
        // For same-host peers the EFA provider hands the transfer to an
        // internal shm endpoint (efa_shm_info_create() in
        // prov/efa/src/efa_shm.c), and shm cannot move Neuron HBM.
        // libfabric's FI_HMEM_NEURON op table (src/hmem.c) implements
        // copy_to_hmem only: copy_from_hmem is neuron_copy_from_dev(), whose
        // whole body is
        //   FI_WARN_ONCE("Copies from AWS Neuron to host memory are not
        //                 supported."); return -FI_ENOSYS;
        // and every IPC entry is a ofi_hmem_no_* stub, so smr_ep.c's
        //   use_ipc = ofi_hmem_is_ipc_enabled(iface) && ...
        // is always false and shm has no protocol left that could read out of
        // a source Neuron buffer.  The shm provider never uses dmabuf either.
        //
        // Left to itself shm therefore treats the /dev/neuron mapping as
        // ordinary host memory and memcpy()s across what is really an
        // uncached PCIe BAR: correct bytes, 0.07 GB/s.  Disabling shm sends
        // intra-node traffic back over the EFA device instead, measured at
        // 19.23 GB/s -- the same as the inter-node rate for one chip, i.e. no
        // loss at all.
        //
        // Declaring FI_HMEM in hints->caps, which is what the CUDA path does
        // and what efa_shm.c keys FI_MR_HMEM off, would be worse than this:
        // it makes shm HMEM-aware and the transfer then fails outright on
        // that ENOSYS rather than merely crawling.
        //
        // Overwrite is 0, so an explicit FI_EFA_ENABLE_SHM_TRANSFER from the
        // environment still wins.  This is process-global and also takes host
        // memory off the shm path on Neuron hosts; that is an acceptable
        // trade, since intra-node host transfers loop back over EFA just as
        // well.
        if (neuronAvailable()) setenv("FI_EFA_ENABLE_SHM_TRANSFER", "0", 0);
    });
}

struct fi_info* allocEfaHints() {
    const bool gpu = efaHasGpuDevice();
    struct fi_info* hints = fi_allocinfo();
    if (!hints) return nullptr;

    hints->caps =
        FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    if (gpu) {
        // Declare FI_HMEM so the provider wires up HMEM-aware copy routines
        // (cudaMemcpy) on every data path, including the intra-node SHM SAR
        // segmentation/reassembly path. Registering device memory via
        // FI_MR_HMEM alone is NOT enough: without FI_HMEM in caps the SHM
        // sub-provider initializes its copy callbacks in plain-host mode and
        // does a host memcpy() straight into a CUDA device VA during SAR,
        // which SIGSEGVs in __memcpy_avx512_unaligned_erms.
        // See https://github.com/ofiwg/libfabric/issues/12328.
        hints->caps |= FI_HMEM;
    }
    hints->mode = FI_CONTEXT;
    hints->ep_attr->type = FI_EP_RDM;  // EFA uses RDM endpoints
    hints->fabric_attr->prov_name = strdup("efa");
    hints->domain_attr->mr_mode =
        FI_MR_LOCAL | FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY;
    if (gpu) hints->domain_attr->mr_mode |= FI_MR_HMEM;
    // FI_MR_HMEM is mandatory for FI_HMEM_NEURON: without it the EFA provider
    // matches no fi_info at all (fi_getinfo -> -FI_ENODATA) rather than merely
    // refusing the later fi_mr_regattr().  Like the GPU check above this is a
    // runtime decision -- Neuron support costs no build flag, see efa_neuron.h.
    //
    // Note that FI_HMEM is deliberately NOT added to caps for Neuron, unlike
    // the GPU path above.  See the FI_EFA_ENABLE_SHM_TRANSFER guard in
    // prepareEfaLibfabricEnv() for why declaring it would turn a slow
    // intra-node path into a broken one.
    if (neuronAvailable()) hints->domain_attr->mr_mode |= FI_MR_HMEM;
    hints->domain_attr->threading = FI_THREAD_SAFE;
    return hints;
}

bool listEfaNics(std::vector<EfaNic>& nics, std::string* error) {
    nics.clear();
    if (!loadLibfabric(error)) return false;
    prepareEfaLibfabricEnv();

    struct fi_info* hints = allocEfaHints();
    if (!hints) {
        if (error) *error = "fi_allocinfo failed";
        return false;
    }
    struct fi_info* info = nullptr;
    // Same API version as EfaContext::construct(), see the comment there.
    int ret = fi_getinfo(kEfaRequiredFabricVersion, nullptr, nullptr, 0, hints,
                         &info);
    fi_freeinfo(hints);
    if (ret) {
        if (error) *error = std::string("fi_getinfo: ") + fi_strerror(-ret);
        return false;
    }

    static const std::string kRdmSuffix = "-rdm";
    std::set<std::string> seen;
    for (struct fi_info* cur = info; cur; cur = cur->next) {
        if (!cur->domain_attr || !cur->domain_attr->name) continue;
        std::string domain = cur->domain_attr->name;
        if (domain.size() <= kRdmSuffix.size() ||
            domain.compare(domain.size() - kRdmSuffix.size(), kRdmSuffix.size(),
                           kRdmSuffix) != 0) {
            continue;
        }
        EfaNic nic;
        nic.name = domain.substr(0, domain.size() - kRdmSuffix.size());
        // A device can appear once per fabric that accepted the hints.
        if (!seen.insert(nic.name).second) continue;
        if (cur->nic && cur->nic->bus_attr &&
            cur->nic->bus_attr->bus_type == FI_BUS_PCI) {
            const auto& pci = cur->nic->bus_attr->attr.pci;
            char bdf[32];
            snprintf(bdf, sizeof(bdf), "%04x:%02x:%02x.%x", pci.domain_id,
                     pci.bus_id, pci.device_id, pci.function_id);
            nic.pci_bus_id = bdf;
            std::ifstream("/sys/bus/pci/devices/" + nic.pci_bus_id +
                          "/numa_node") >>
                nic.numa_node;
        }
        nics.push_back(std::move(nic));
    }
    fi_freeinfo(info);
    return true;
}

std::string loadedLibibverbsPath() {
    std::string path;
    dl_iterate_phdr(
        [](struct dl_phdr_info* info, size_t, void* data) {
            if (info->dlpi_name && strstr(info->dlpi_name, "libibverbs.so")) {
                *static_cast<std::string*>(data) = info->dlpi_name;
                return 1;
            }
            return 0;
        },
        &path);
    return path;
}

}  // namespace mooncake
