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

#include <atomic>

#include "config.h"
#include "common.h"
#include "transfer_metadata.h"

namespace mooncake {
void TransferMetadata::SegmentDesc::dump() const {
    LOG(INFO) << "  segment name: " << name;
    if (!rdma_server_name.empty() && rdma_server_name != name) {
        LOG(INFO) << "  rdma server name: " << rdma_server_name;
    }
    LOG(INFO) << "  protocol: " << protocol;
    LOG(INFO) << "  metadata version: " << metadata_version << " ("
              << formatEpochMicroseconds(metadata_version) << ")";
    if (!tcp_data_host.empty()) {
        LOG(INFO) << "  tcp data endpoint: " << tcp_data_host << ":"
                  << tcp_data_port;
    }
    LOG(INFO) << "  topology: " << topology.toString();
    LOG(INFO) << "  devices: ";
    for (auto& device : devices) {
        LOG(INFO) << "    device name " << device.name << ", lid " << device.lid
                  << ", " << device.gid;
    }
    LOG(INFO) << "  buffers: ";
    for (auto& buffer : buffers) {
        LOG(INFO) << "    buffer type " << buffer.name << ", address "
                  << (void*)buffer.addr << "--"
                  << (void*)(buffer.addr + buffer.length);
    }
    LOG(INFO) << "  nvmeof buffers: " << nvmeof_buffers.size() << " items";
    LOG(INFO) << "  timestamp: " << timestamp;
}

void TransferMetadata::dumpMetadataContent(
    const std::shared_ptr<const SegmentDesc>& desc, uint64_t offset,
    uint64_t length) {
    const uint64_t now_ns = getCurrentTimeInNano();
    static constexpr uint64_t kMinDisplayThreshold = 500000000;  // 0.5 sec

    // Runs synchronously on the transfer worker (CQ polling) thread, so the
    // per-event record must be fixed-size and capped process-wide: at most
    // one line per window, with a suppressed-event counter keeping the
    // failure rate observable without flooding.
    static std::atomic<uint64_t> g_last_log_ns{0};
    static std::atomic<uint64_t> g_suppressed_count{0};

    uint64_t prev = g_last_log_ns.load(std::memory_order_relaxed);
    if (!globalConfig().trace &&
        !(now_ns - prev > kMinDisplayThreshold &&
          g_last_log_ns.compare_exchange_strong(prev, now_ns,
                                                std::memory_order_relaxed))) {
        g_suppressed_count.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    // The caller holds the descriptor the failed device/buffer selection
    // matched against; log that exact snapshot (its metadata_version is the
    // failure-relevant one) rather than re-looking-up the segment cache,
    // which may already hold a newer descriptor installed by a concurrent
    // syncSegmentCache.  No metadata locks are needed for a held snapshot.
    const uint64_t suppressed =
        g_suppressed_count.exchange(0, std::memory_order_relaxed);
    if (desc) {
        LOG(INFO) << "Failed to select peer device/buffer: segment "
                  << desc->name << ", address " << (void*)offset << "--"
                  << (void*)(offset + length) << ", metadata_version "
                  << desc->metadata_version << ", buffers "
                  << desc->buffers.size() << ", suppressed " << suppressed
                  << " similar events in the last window";
    } else {
        LOG(INFO) << "Failed to select peer device/buffer: address "
                  << (void*)offset << "--" << (void*)(offset + length)
                  << ", no descriptor held, suppressed " << suppressed
                  << " similar events in the last window";
    }

    // The full descriptor body (one line per registered buffer) is unbounded
    // work; keep it trace-mode only, printed from the caller-held snapshot.
    if (globalConfig().trace && desc) {
        desc->dump();
    }
}

void TransferMetadata::dumpMetadataContentUnlocked() {
    LOG(INFO) << "-----------------------------------------------------------";
    LOG(INFO) << "TransferMetadata::dumpMetadataContent";
    LOG(INFO) << "-----------------------------------------------------------";
    LOG(INFO) << "=== Cached Segment Descriptors ===";
    for (auto& entry : segment_id_to_desc_map_) {
        auto& desc = entry.second;
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
              << local_rpc_meta_.rpc_port
              << ", metadata version: " << local_rpc_meta_.metadata_version
              << " ("
              << formatEpochMicroseconds(local_rpc_meta_.metadata_version)
              << ")";
    LOG(INFO) << "=== Remote RPC Routes ===";
    for (auto& entry : rpc_meta_map_) {
        LOG(INFO) << "segment name: " << entry.first
                  << ", location: " << entry.second.ip_or_host_name << ":"
                  << entry.second.rpc_port
                  << ", metadata version: " << entry.second.metadata_version
                  << " ("
                  << formatEpochMicroseconds(entry.second.metadata_version)
                  << ")";
    }
}
}  // namespace mooncake
