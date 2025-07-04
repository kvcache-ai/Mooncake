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

#include "config.h"
#include "transfer_metadata.h"

namespace mooncake {
void TransferMetadata::SegmentDesc::dump() const {
    LOG(INFO) << "  segment name: " << name;
    LOG(INFO) << "  protocol: " << protocol;
    LOG(INFO) << "  topology: " << topology.toString();
    LOG(INFO) << "  devices: ";
    for (auto &device : devices) {
        LOG(INFO) << "    device name " << device.name << ", lid " << device.lid
                  << ", " << device.gid;
    }
    LOG(INFO) << "  buffers: ";
    for (auto &buffer : buffers) {
        LOG(INFO) << "    buffer type " << buffer.name << ", address "
                  << (void *)buffer.addr << "--"
                  << (void *)(buffer.addr + buffer.length);
    }
    LOG(INFO) << "  nvmeof buffers: " << nvmeof_buffers.size() << " items";
    LOG(INFO) << "  timestamp: " << timestamp;
}

void TransferMetadata::dumpMetadataContent(const std::string &segment_name,
                                           uint64_t offset, uint64_t length) {
    thread_local uint64_t last_ts = 0;
    uint64_t current_ts = getCurrentTimeInNano();
    const static uint64_t kMinDisplayThreshold = 500000000;  // 0.5 sec

    auto segment_locked = segment_lock_.tryLockShared();
    auto rpc_meta_locked = rpc_meta_lock_.tryLockShared();
    if (!segment_locked || !rpc_meta_locked) {
        return;
    }

    if (current_ts - last_ts > kMinDisplayThreshold || globalConfig().trace) {
        LOG(INFO) << "Failed to get segment descriptor for segment "
                  << segment_name << " address " << (void *)offset << "--"
                  << (void *)(offset + length);
        dumpMetadataContentUnlocked();
        last_ts = current_ts;
    }

    if (rpc_meta_locked) rpc_meta_lock_.unlockShared();
    if (segment_locked) segment_lock_.unlockShared();
}

void TransferMetadata::dumpMetadataContentUnlocked() {
    LOG(INFO) << "-----------------------------------------------------------";
    LOG(INFO) << "TransferMetadata::dumpMetadataContent";
    LOG(INFO) << "-----------------------------------------------------------";
    LOG(INFO) << "=== Cached Segment Descriptors ===";
    for (auto &entry : segment_id_to_desc_map_) {
        auto &desc = entry.second;
        if (!desc) {
            LOG(INFO) << "segment id: " << entry.first << ", ref object nil";
        } else {
            LOG(INFO) << "segment id: " << entry.first << ", ref object "
                      << &desc;
            desc->dump();
        }
    }
    LOG(INFO) << "=== Local RPC Route ===";
    LOG(INFO) << "location: " << local_rpc_meta_.ip_or_host_name << ":"
              << local_rpc_meta_.rpc_port;
    LOG(INFO) << "=== Remote RPC Routes ===";
    for (auto &entry : rpc_meta_map_) {
        LOG(INFO) << "segment name: " << entry.first
                  << ", location: " << entry.second.ip_or_host_name << ":"
                  << entry.second.rpc_port;
    }
}
}  // namespace mooncake