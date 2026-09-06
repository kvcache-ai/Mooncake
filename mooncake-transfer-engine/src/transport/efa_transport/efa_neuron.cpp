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

#include "transport/efa_transport/efa_neuron.h"

#include <dirent.h>
#include <dlfcn.h>
#include <glog/logging.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cctype>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {

namespace {

// One /dev/neuron<index> mapping as reported by /proc/self/maps.
struct NeuronRegion {
    uintptr_t start;
    uintptr_t end;
    int device_index;
};

std::mutex g_neuron_mutex;
std::vector<NeuronRegion> g_neuron_regions;

// Parses /proc/self/maps and keeps only the /dev/neuron<N> mappings.  Called
// with g_neuron_mutex held.
void refreshNeuronRegionsLocked() {
    std::vector<NeuronRegion> regions;
    std::ifstream maps("/proc/self/maps");
    if (!maps.is_open()) {
        LOG(WARNING) << "Neuron: cannot open /proc/self/maps, Neuron device "
                        "memory will be treated as host memory";
        return;
    }

    const std::string kDevPrefix = "/dev/neuron";
    std::string line;
    while (std::getline(maps, line)) {
        auto dev_pos = line.find(kDevPrefix);
        if (dev_pos == std::string::npos) continue;

        // The ordinal is the digit run right after "/dev/neuron"; stop at the
        // first non-digit so a "(deleted)" suffix does not confuse us.
        size_t digit_pos = dev_pos + kDevPrefix.size();
        if (digit_pos >= line.size() || !isdigit(line[digit_pos])) continue;
        int device_index = 0;
        while (digit_pos < line.size() && isdigit(line[digit_pos])) {
            device_index = device_index * 10 + (line[digit_pos] - '0');
            ++digit_pos;
        }

        uintptr_t start = 0, end = 0;
        if (sscanf(line.c_str(), "%lx-%lx", &start, &end) != 2) continue;
        if (end <= start) continue;

        regions.push_back(NeuronRegion{start, end, device_index});
    }

    g_neuron_regions = std::move(regions);
}

// Looks up a range in the cached snapshot.  Called with g_neuron_mutex held.
bool findRegionLocked(uintptr_t start, uintptr_t end, int* device_index) {
    for (const auto& region : g_neuron_regions) {
        if (start >= region.start && end <= region.end) {
            if (device_index) *device_index = region.device_index;
            return true;
        }
    }
    return false;
}

// Maps the Neuron runtime into this process, preferring a copy that is already
// there, and returns a handle to it (nullptr if it cannot be found).  Loading
// it does not initialise it -- nrt_init() is the framework's business, not
// ours.
//
// Doing this eagerly is not an optimisation, it is what makes FI_HMEM_NEURON
// work at all.  libfabric's EFA provider reaches the runtime with
// dlopen("libnrt.so.1") while initialising its HMEM interfaces, and the Neuron
// SDK installs libnrt under /opt/aws/neuron/lib without adding that directory
// to the loader path -- so on a default install that dlopen fails, the
// FI_HMEM_NEURON op table is left empty, and the only symptom is
// fi_mr_regattr() returning -FI_ENOSYS much later.  Because libfabric asks by
// soname and libnrt's SONAME is "libnrt.so.1", a copy we have already mapped
// satisfies its dlopen() without any filesystem search, which is why this works
// where setting LD_LIBRARY_PATH from inside the process would not (glibc parses
// that variable once, at startup).
void* dlopenNeuronRuntime() {
    static const char* kNames[] = {"libnrt.so.1",
                                   "/opt/aws/neuron/lib/libnrt.so.1"};
    static void* const handle = []() -> void* {
        // Pass 0 only adopts an already-mapped libnrt: inside a Neuron
        // inference worker the framework has loaded and initialised the runtime
        // long before we get here, and a second independent copy of a device
        // runtime in one process is a known source of device-arbitration
        // failures. Pass 1 may load one, asking by soname first so that a
        // framework-configured location (LD_LIBRARY_PATH, rpath) still wins
        // over the SDK's default install path.
        for (int pass = 0; pass < 2; ++pass) {
            int flags = RTLD_LAZY | RTLD_GLOBAL | (pass == 0 ? RTLD_NOLOAD : 0);
            for (const char* name : kNames) {
                void* h = dlopen(name, flags);
                if (h) return h;
            }
        }
        return nullptr;
    }();
    return handle;
}

// nec_get_device_pci_bdf(neuron_dev, &domain, &bus, &slot, &func) -- see nec.h
// in the Neuron runtime headers.  Returns NRT_SUCCESS (0).  This is the only
// way to get from a /dev/neuron<N> ordinal to a PCI address: the driver exposes
// neuron devices as *virtual* class devices
// (/sys/devices/virtual/neuron_device) with no link back to the PCI device, and
// the ordinals are not in BDF order -- on trn2.48xlarge neuron0 is 0000:cc:00.0
// while neuron1 is 0000:b5:00.0.
using NecGetDevicePciBdfFn = int (*)(int, uint32_t*, uint32_t*, uint8_t*,
                                     uint8_t*);

// "<domain>:<bus>" of the bridge a PCI device hangs off, e.g. "0000:b9" for a
// device behind port 0000:b9:02.1.  Empty if it cannot be determined.
//
// This is the grouping that expresses PCIe-switch locality: on trn2.48xlarge
// each switch carries two Neuron devices and two EFA NICs, and every device
// under it shares this prefix.
std::string pciParentBus(const std::string& bdf) {
    char resolved[PATH_MAX];
    if (!realpath(("/sys/bus/pci/devices/" + bdf).c_str(), resolved)) {
        return std::string();
    }
    // .../0000:b9:02.1/0000:cc:00.0 -> parent component "0000:b9:02.1"
    char* last = strrchr(resolved, '/');
    if (!last) return std::string();
    *last = '\0';
    char* parent = strrchr(resolved, '/');
    if (!parent) return std::string();
    std::string port(parent + 1);

    // Keep "<domain>:<bus>", dropping the ":<slot>.<func>" of the port itself.
    auto first = port.find(':');
    if (first == std::string::npos) return std::string();
    auto second = port.find(':', first + 1);
    if (second == std::string::npos) return std::string();
    return port.substr(0, second);
}

// Groups the EFA NICs of this host by the PCIe bridge they sit behind.
std::map<std::string, std::vector<std::string>> efaNicsByParentBus() {
    std::map<std::string, std::vector<std::string>> by_bus;
    DIR* dir = opendir("/sys/class/infiniband");
    if (!dir) return by_bus;

    struct dirent* entry;
    while ((entry = readdir(dir))) {
        if (entry->d_name[0] == '.') continue;
        std::string name = entry->d_name;
        char resolved[PATH_MAX];
        if (!realpath(("/sys/class/infiniband/" + name + "/device").c_str(),
                      resolved)) {
            continue;
        }
        const char* last = strrchr(resolved, '/');
        if (!last) continue;
        std::string bus = pciParentBus(last + 1);
        if (!bus.empty()) by_bus[bus].push_back(name);
    }
    (void)closedir(dir);
    return by_bus;
}

}  // namespace

bool neuronAvailable() {
    static const bool available = []() -> bool {
        const char* disable = getenv("MC_EFA_DISABLE_NEURON");
        if (disable && (disable[0] == '1' || disable[0] == 't' ||
                        disable[0] == 'T' || disable[0] == 'y')) {
            LOG(INFO) << "Neuron support disabled by MC_EFA_DISABLE_NEURON";
            return false;
        }
        if (access("/dev/neuron0", F_OK) != 0) return false;

#ifndef MOONCAKE_HAVE_FI_HMEM_NEURON
        // Built against a libfabric with no FI_HMEM_NEURON, so this binary
        // cannot register Neuron HBM however the hardware is arranged.  Report
        // the device as unsupported and let registration fail in fi_mr_reg()
        // rather than claim it here: the alternative -- passing device memory
        // off as host memory -- would be silently wrong.  Warned about only on
        // hosts that actually have a device, hence the ordering.
        LOG(WARNING) << "Neuron devices are present but this binary was built "
                        "against a libfabric whose rdma/fi_domain.h has no "
                        "FI_HMEM_NEURON, so Neuron HBM cannot be registered. "
                        "Rebuild against a libfabric that defines it (the AWS "
                        "EFA installer's copy under /opt/amazon/efa does; "
                        "older distro libfabric-dev packages do not).";
        return false;
#else
        // Deliberately still true when libnrt is missing.  The hardware is
        // there, so calling Neuron HBM host memory would be a lie -- and an
        // expensive one, since "cpu:N" is what opts a buffer into CPU pre-touch
        // stores.  Better to keep the honest answer and let registration fail
        // loudly, with this line explaining why.
        if (!dlopenNeuronRuntime()) {
            LOG(WARNING)
                << "Neuron devices are present but libnrt.so.1 could not be "
                   "loaded ("
                << dlerror()
                << "); libfabric will refuse to register Neuron HBM "
                   "(fi_mr_regattr -> ENOSYS). Add the Neuron SDK library "
                   "directory (usually /opt/aws/neuron/lib) to "
                   "LD_LIBRARY_PATH.";
        }
        LOG(INFO) << "Neuron device detected, EFA transport will register "
                     "Neuron HBM with FI_HMEM_NEURON";
        return true;
#endif
    }();
    return available;
}

bool neuronProbeAddress(const void* addr, size_t length, int* device_index) {
    if (!neuronAvailable() || addr == nullptr || length == 0) return false;

    const uintptr_t start = reinterpret_cast<uintptr_t>(addr);
    const uintptr_t end = start + length;
    if (end < start) return false;  // overflow

    std::lock_guard<std::mutex> guard(g_neuron_mutex);
    if (findRegionLocked(start, end, device_index)) return true;

    // A miss is re-checked against a fresh snapshot rather than answered from
    // the cache: mappings appear as the Neuron runtime grows its pools, and
    // misreporting device memory as host memory is not a slow path but a
    // correctness bug -- the caller would hand it to fi_mr_reg() as
    // FI_HMEM_SYSTEM and pre-touch it with CPU stores.  The cost is one read of
    // /proc/self/maps per registration of a non-Neuron buffer, and only on
    // hosts that actually have Neuron devices.
    refreshNeuronRegionsLocked();
    return findRegionLocked(start, end, device_index);
}

std::map<int, std::vector<std::string>> neuronNicAffinity() {
    std::map<int, std::vector<std::string>> affinity;
    if (!neuronAvailable()) return affinity;

    void* handle = dlopenNeuronRuntime();
    if (!handle) return affinity;
    auto get_bdf = reinterpret_cast<NecGetDevicePciBdfFn>(
        dlsym(handle, "nec_get_device_pci_bdf"));
    if (!get_bdf) {
        LOG(WARNING)
            << "Neuron: libnrt has no nec_get_device_pci_bdf, cannot "
               "work out which EFA NICs share a PCIe switch with each "
               "device; Neuron transfers will use all NICs and may run "
               "several times slower than they could";
        return affinity;
    }

    const auto nics_by_bus = efaNicsByParentBus();
    if (nics_by_bus.empty()) return affinity;

    // Scanned rather than counted, and gaps are skipped rather than treated as
    // the end: the ordinals a container sees need not start at 0 or be
    // contiguous.  The bound is generous -- trn2.48xlarge, the largest today,
    // exposes 16 -- and each miss is one access().
    constexpr int kMaxNeuronDevices = 64;
    for (int index = 0; index < kMaxNeuronDevices; ++index) {
        const std::string dev = "/dev/neuron" + std::to_string(index);
        if (access(dev.c_str(), F_OK) != 0) continue;

        uint32_t domain = 0, bus = 0;
        uint8_t slot = 0, func = 0;
        if (get_bdf(index, &domain, &bus, &slot, &func) != 0) continue;

        char bdf[32];
        snprintf(bdf, sizeof(bdf), "%04x:%02x:%02x.%x", domain, bus, slot,
                 func);
        const std::string parent_bus = pciParentBus(bdf);
        if (parent_bus.empty()) continue;

        auto it = nics_by_bus.find(parent_bus);
        if (it == nics_by_bus.end()) continue;

        affinity[index] = it->second;
        VLOG(1) << "Neuron device " << index << " (" << bdf
                << ") shares PCIe switch " << parent_bus << " with "
                << it->second.size() << " EFA NIC(s)";
    }

    if (affinity.empty()) {
        LOG(WARNING)
            << "Neuron: no device could be matched to an EFA NIC on the "
               "same PCIe switch; Neuron transfers will use all NICs";
    }
    return affinity;
}

std::string neuronLocationName(int device_index) {
    return "neuron:" + std::to_string(device_index);
}

}  // namespace mooncake
