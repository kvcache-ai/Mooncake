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

#ifndef TENT_TRANSPORT_WIRE_CODEC_H
#define TENT_TRANSPORT_WIRE_CODEC_H

#include "tent/runtime/segment.h"

namespace mooncake {
namespace tent {

// Plugin slots are process-local, so a segment descriptor never carries them
// as integers. On the way out, every plugin-slot entry of
// BufferDesc::transports / transport_attrs and MemorySegmentDesc::
// transport_attrs moves to the name-keyed plugin_transports /
// plugin_transport_attrs fields (DeviceDesc entries are dropped). A
// descriptor that uses no plugin slot serializes exactly as before.
json segmentDescToWireJson(const SegmentDesc& desc);

// Applied to a descriptor parsed from a peer: drops transport integers that
// are not built-in transports here (e.g. from a newer peer), maps plugin
// names to this process's slots, and drops names that are not loaded here.
void decodeSegmentDescFromWire(SegmentDesc& desc);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_WIRE_CODEC_H
