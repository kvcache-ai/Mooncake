// Copyright 2025 KVCache.AI
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

#ifndef TENT_TRANSPORT_SLOTS_H
#define TENT_TRANSPORT_SLOTS_H

#include <cstdint>
#include <string>

#include "tent/common/types.h"
#include "tent/transport_plugin.h"

namespace mooncake {
namespace tent {

// Process-wide table of the plugin slots [kFirstPluginSlot,
// kNumTransportTypes). The transport plugin loader writes each slot at most
// once; transportTypeName() and parseTransportType() read it without locking,
// so they are safe to call before, during and after plugin loading.

enum class SlotAssignResult {
    kAssigned,
    kDuplicateName,
    kNoFreeSlot,
};

// Assigns the lowest free plugin slot to `name` and records `flags`
// (TENT_XPORT_FLAG_* from tent/transport_plugin.h). On success `slot` is set.
SlotAssignResult assignPluginTransportSlot(const std::string& name,
                                           uint32_t flags, TransportType& slot);

// TENT_XPORT_FLAG_* recorded for `slot`, or 0 if the slot is unused.
uint32_t pluginTransportSlotFlags(int slot);

// Number of assigned plugin slots.
int pluginTransportSlotCount();

// True if `type` is an assigned plugin slot whose flags include `flag`.
inline bool pluginSlotHasFlag(int type, uint32_t flag) {
    return isPluginTransportSlot(type) &&
           (pluginTransportSlotFlags(type) & flag) != 0;
}

// Locality rules of a plugin slot; built-in types are not restricted here
// (their rules stay hard-coded at the call sites).
inline bool pluginSlotAllowsRoute(int type, bool local_segment,
                                  bool same_machine) {
    if (!isPluginTransportSlot(type)) return true;
    const uint32_t flags = pluginTransportSlotFlags(type);
    if ((flags & TENT_XPORT_FLAG_LOCAL_SEGMENT_ONLY) && !local_segment)
        return false;
    if ((flags & TENT_XPORT_FLAG_SAME_MACHINE_ONLY) && !same_machine)
        return false;
    return true;
}

// Test hook: forgets every assignment. Names already handed out stay valid
// (they are never freed), but a slot number may afterwards be given to
// another name, so callers must not keep slot numbers across a reset.
void resetPluginTransportSlotsForTest();

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_SLOTS_H
