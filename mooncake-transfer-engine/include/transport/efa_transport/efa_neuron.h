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

#ifndef EFA_NEURON_H
#define EFA_NEURON_H

#include <cstddef>
#include <map>
#include <string>
#include <vector>

// AWS Neuron (Trainium / Inferentia) device-memory support for the EFA
// transport.
//
// Neuron HBM reaches user space as a shared mmap of the /dev/neuron<N>
// character device, so both questions the transport needs answered -- "is this
// pointer device memory" and "which device is it on" -- fall straight out of
// /proc/self/maps.  That is why this layer needs no Neuron SDK headers and no
// link-time dependency on libnrt: a stock USE_EFA build gains Neuron support at
// runtime, and the same binary still runs unchanged on hosts with no Neuron
// device.
//
// The actual registration is libfabric's job.  Its EFA provider already
// implements the FI_HMEM_NEURON interface (it is how the Neuron collectives
// stack moves HBM between instances), so all the transport has to do is hand
// fi_mr_regattr() the right iface and device ordinal.

namespace mooncake {

// True when this host has Neuron devices and Neuron support has not been
// switched off with MC_EFA_DISABLE_NEURON=1.  Evaluated once and cached; cheap
// enough to call on any path.
bool neuronAvailable();

// Reports whether [addr, addr + length) lies entirely inside one Neuron HBM
// mapping.  On success *device_index receives the /dev/neuron<N> ordinal.
//
// A buffer straddling two mappings is rejected rather than registered as host
// memory: passing device memory to fi_mr_reg() as FI_HMEM_SYSTEM would have the
// provider walk it with CPU loads.
bool neuronProbeAddress(const void* addr, size_t length, int* device_index);

// Maps each Neuron device ordinal to the EFA NICs sharing its PCIe switch, e.g.
// {0: {"rdmap201s0", "rdmap202s0"}}.  Empty when the mapping cannot be worked
// out, in which case the caller should leave the topology alone.
//
// This is not a micro-optimisation.  Neuron HBM registers fine on any NIC, but
// reaching it from a NIC on another switch measured 2.5 GB/s against 19.2 GB/s
// for the switch-local pair on trn2.48xlarge -- a 7.7x penalty, and enough to
// put an unpinned Neuron transfer (2.6 GB/s across all 16 NICs, dominated by
// the 14 distant ones) far below plain host DRAM.
//
// Two properties make this usable while the topology is still being built,
// which is the only point where adding entries cannot renumber HCA indices: it
// needs no device allocation, and it needs no nrt_init(), so it will not claim
// a NeuronCore out from under the framework sharing the process.
std::map<int, std::vector<std::string>> neuronNicAffinity();

// Location string Mooncake uses for Neuron memory, matching the "neuron:<N>"
// device naming that vLLM's Neuron platform already reports.
std::string neuronLocationName(int device_index);

}  // namespace mooncake

#endif  // EFA_NEURON_H
