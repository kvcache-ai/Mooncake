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

// Provider-independent pieces of the libfabric transport: the metadata it
// publishes, how a buffer is cut into registrations, and how a request is cut
// into operations. None of this touches libfabric, so it is unit-tested on
// its own.

#ifndef TENT_FABRIC_ATTRS_H
#define TENT_FABRIC_ATTRS_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

inline constexpr int kFabricAttrVersion = 1;

// One NIC of a peer, as published in MemorySegmentDesc::transport_attrs.
struct FabricNicAttr {
    std::string name;     // libfabric domain name
    std::string address;  // raw fi_getname() bytes
    int numa_node = -1;
};

// Segment-level attributes. A peer can only be reached with the same
// provider, and `virt_addr` says how its keys are addressed: by virtual
// address (FI_MR_VIRT_ADDR) or by offset from the start of each
// registration.
struct FabricPeerAttr {
    int version = kFabricAttrVersion;
    std::string provider;
    bool virt_addr = true;
    std::vector<FabricNicAttr> nics;
};

// One registration of a buffer. `nics` indexes FabricPeerAttr::nics and
// `keys` runs parallel to it; a chunk need not be registered on every NIC.
struct FabricChunkAttr {
    uint64_t offset = 0;  // from BufferDesc::addr
    uint64_t length = 0;
    std::vector<int> nics;
    std::vector<uint64_t> keys;
    // Subset of nics closest to the memory (same PCIe switch or NUMA node),
    // which peers should target. Empty means no preference.
    std::vector<int> near;

    // Returns false if the chunk is not registered on `nic`.
    bool keyFor(int nic, uint64_t& key) const;
};

// Buffer-level attributes, in BufferDesc::transport_attrs. Keys are 64-bit,
// so they cannot use BufferDesc::rkey.
struct FabricBufferAttr {
    std::vector<FabricChunkAttr> chunks;  // ascending, contiguous from 0
};

std::string encodeFabricPeerAttr(const FabricPeerAttr& attr);
Status decodeFabricPeerAttr(const std::string& text, FabricPeerAttr& attr);
std::string encodeFabricBufferAttr(const FabricBufferAttr& attr);
Status decodeFabricBufferAttr(const std::string& text, uint64_t buffer_length,
                              FabricBufferAttr& attr);

std::string hexEncode(const std::string& bytes);
bool hexDecode(const std::string& hex, std::string& bytes);

struct FabricChunkRange {
    uint64_t offset;
    uint64_t length;
};

// Cuts [0, length) into registrations of at most `chunk_limit` bytes. A
// limit of 0 means one registration.
std::vector<FabricChunkRange> planFabricChunks(uint64_t length,
                                               uint64_t chunk_limit);

// Chooses the NICs each chunk is registered on. `pte_budget` is the number of
// pages one NIC can map (0 = unlimited). When the whole buffer fits every
// NIC's budget, every chunk goes on every NIC. Otherwise chunks are
// partitioned across NICs (fewer chunks than NICs) or dealt round-robin (more
// chunks than NICs); InvalidArgument if even that exceeds a NIC's budget.
Status assignFabricChunkNics(const std::vector<FabricChunkRange>& chunks,
                             size_t num_nics, uint64_t page_size,
                             uint64_t pte_budget,
                             std::vector<std::vector<int>>& assignment);

// A piece of a request that stays inside one local and one remote chunk.
struct FabricSpan {
    uint64_t offset;  // from the start of the request
    uint64_t length;
    size_t local_chunk;
    size_t remote_chunk;
};

// Cuts a request of `length` bytes into spans of at most `max_span` bytes
// that never cross a registration boundary on either side (a single
// operation must stay inside one MR). `local_start`/`remote_start` are the
// request's offsets inside the local and remote buffers. Returns false if the
// request runs past either buffer's chunks.
bool cutFabricSpans(uint64_t length, uint64_t local_start,
                    const std::vector<FabricChunkRange>& local_chunks,
                    uint64_t remote_start,
                    const std::vector<FabricChunkRange>& remote_chunks,
                    uint64_t max_span, std::vector<FabricSpan>& spans);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_FABRIC_ATTRS_H
