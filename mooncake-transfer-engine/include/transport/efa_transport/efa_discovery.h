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

#ifndef EFA_DISCOVERY_H
#define EFA_DISCOVERY_H

#include <cstdint>
#include <string>
#include <vector>

struct fi_info;

// EFA device discovery through libfabric. This header does not include the
// libfabric headers, so the topology code can use it.
namespace mooncake {

struct EfaNic {
    std::string name;        // e.g. "rdmap79s0", without the "-rdm" suffix
    std::string pci_bus_id;  // e.g. "0000:4f:00.0"
    int numa_node = -1;
};

// Whether GPU memory can be registered, i.e. a GPU build with a device.
bool efaHasGpuDevice();

// Sets the environment that must be in place before the first fi_getinfo()
// call in this process, because libfabric reads it only once. Idempotent.
void prepareEfaLibfabricEnv();

// fi_getinfo() hints for the EFA RDM endpoints this transport opens, without
// a domain name. Free with fi_freeinfo(). libfabric must be loaded.
struct fi_info* allocEfaHints();

// Lists the EFA NICs that libfabric's "efa" fabric offers for RDM endpoints,
// i.e. the devices EfaContext can open. Returns false, with a reason in
// `error`, if libfabric cannot be loaded or fi_getinfo() fails.
bool listEfaNics(std::vector<EfaNic>& nics, std::string* error);

// Path of the libibverbs this process has loaded, for diagnostics. Empty if
// it cannot be determined.
std::string loadedLibibverbsPath();

}  // namespace mooncake

#endif  // EFA_DISCOVERY_H
