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

#include "tent/common/transport_slots.h"

#include <atomic>
#include <cstring>
#include <mutex>
#include <vector>

namespace mooncake {
namespace tent {

namespace {

struct SlotEntry {
    // Published last with release order; flags is valid once name is seen.
    std::atomic<const char*> name{nullptr};
    std::atomic<uint32_t> flags{0};
};

SlotEntry g_slots[kNumPluginSlots];
std::mutex g_assign_mutex;

SlotEntry* entryOf(int slot) {
    if (slot < kFirstPluginSlot || slot >= kNumTransportTypes) return nullptr;
    return &g_slots[slot - kFirstPluginSlot];
}

}  // namespace

const char* pluginTransportSlotName(int slot) {
    auto* entry = entryOf(slot);
    if (!entry) return "unknown";
    const char* name = entry->name.load(std::memory_order_acquire);
    return name ? name : "unknown";
}

TransportType pluginTransportSlotByName(const std::string& name) {
    for (int i = 0; i < kNumPluginSlots; ++i) {
        const char* slot_name = g_slots[i].name.load(std::memory_order_acquire);
        if (slot_name && name == slot_name)
            return static_cast<TransportType>(kFirstPluginSlot + i);
    }
    return UNSPEC;
}

SlotAssignResult assignPluginTransportSlot(const std::string& name,
                                           uint32_t flags,
                                           TransportType& slot) {
    std::lock_guard<std::mutex> lock(g_assign_mutex);
    if (pluginTransportSlotByName(name) != UNSPEC)
        return SlotAssignResult::kDuplicateName;
    for (int i = 0; i < kNumPluginSlots; ++i) {
        auto& entry = g_slots[i];
        if (entry.name.load(std::memory_order_relaxed)) continue;
        entry.flags.store(flags, std::memory_order_relaxed);
        // Intentionally leaked: readers may hold the pointer indefinitely.
        entry.name.store(strdup(name.c_str()), std::memory_order_release);
        slot = static_cast<TransportType>(kFirstPluginSlot + i);
        return SlotAssignResult::kAssigned;
    }
    return SlotAssignResult::kNoFreeSlot;
}

uint32_t pluginTransportSlotFlags(int slot) {
    auto* entry = entryOf(slot);
    if (!entry || !entry->name.load(std::memory_order_acquire)) return 0;
    return entry->flags.load(std::memory_order_relaxed);
}

bool isPluginTransportSlot(int type) {
    auto* entry = entryOf(type);
    return entry && entry->name.load(std::memory_order_acquire) != nullptr;
}

int pluginTransportSlotCount() {
    int count = 0;
    for (auto& entry : g_slots)
        if (entry.name.load(std::memory_order_acquire)) ++count;
    return count;
}

void resetPluginTransportSlotsForTest() {
    std::lock_guard<std::mutex> lock(g_assign_mutex);
    // Names handed out before the reset must stay valid.
    static auto* retired = new std::vector<const char*>();
    for (auto& entry : g_slots) {
        const char* name =
            entry.name.exchange(nullptr, std::memory_order_acq_rel);
        if (name) retired->push_back(name);
        entry.flags.store(0, std::memory_order_relaxed);
    }
}

}  // namespace tent
}  // namespace mooncake
