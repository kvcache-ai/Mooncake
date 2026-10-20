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

#include "tent/transport/fabric/fabric_attrs.h"

#include <algorithm>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {

using json = nlohmann::json;

bool FabricChunkAttr::keyFor(int nic, uint64_t& key) const {
    for (size_t i = 0; i < nics.size(); ++i) {
        if (nics[i] == nic) {
            key = keys[i];
            return true;
        }
    }
    return false;
}

std::string hexEncode(const std::string& bytes) {
    static const char kDigits[] = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) {
        out.push_back(kDigits[c >> 4]);
        out.push_back(kDigits[c & 0xf]);
    }
    return out;
}

static int hexValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

bool hexDecode(const std::string& hex, std::string& bytes) {
    if (hex.size() % 2) return false;
    bytes.clear();
    bytes.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        int hi = hexValue(hex[i]), lo = hexValue(hex[i + 1]);
        if (hi < 0 || lo < 0) return false;
        bytes.push_back(static_cast<char>((hi << 4) | lo));
    }
    return true;
}

std::string encodeFabricPeerAttr(const FabricPeerAttr& attr) {
    json nics = json::array();
    for (const auto& nic : attr.nics) {
        nics.push_back({{"name", nic.name},
                        {"addr", hexEncode(nic.address)},
                        {"numa", nic.numa_node}});
    }
    return json{{"version", attr.version},
                {"provider", attr.provider},
                {"virt_addr", attr.virt_addr},
                {"nics", std::move(nics)}}
        .dump();
}

Status decodeFabricPeerAttr(const std::string& text, FabricPeerAttr& attr) {
    try {
        auto j = json::parse(text);
        attr.version = j.at("version").get<int>();
        if (attr.version != kFabricAttrVersion) {
            return Status::InvalidEntry("Unsupported fabric attr version " +
                                        std::to_string(attr.version) +
                                        LOC_MARK);
        }
        attr.provider = j.at("provider").get<std::string>();
        attr.virt_addr = j.at("virt_addr").get<bool>();
        attr.nics.clear();
        for (const auto& item : j.at("nics")) {
            FabricNicAttr nic;
            nic.name = item.at("name").get<std::string>();
            if (!hexDecode(item.at("addr").get<std::string>(), nic.address) ||
                nic.address.empty()) {
                return Status::MalformedJson("Bad fabric NIC address" LOC_MARK);
            }
            nic.numa_node = item.value("numa", -1);
            attr.nics.push_back(std::move(nic));
        }
    } catch (const std::exception& e) {
        return Status::MalformedJson(std::string("Bad fabric peer attr: ") +
                                     e.what() + LOC_MARK);
    }
    return Status::OK();
}

std::string encodeFabricBufferAttr(const FabricBufferAttr& attr) {
    json chunks = json::array();
    for (const auto& chunk : attr.chunks) {
        json entry = {{"off", chunk.offset},
                      {"len", chunk.length},
                      {"nics", chunk.nics},
                      {"keys", chunk.keys}};
        if (!chunk.near.empty()) entry["near"] = chunk.near;
        chunks.push_back(std::move(entry));
    }
    return json{{"chunks", std::move(chunks)}}.dump();
}

Status decodeFabricBufferAttr(const std::string& text, uint64_t buffer_length,
                              FabricBufferAttr& attr) {
    try {
        auto j = json::parse(text);
        attr.chunks.clear();
        uint64_t expected = 0;
        for (const auto& item : j.at("chunks")) {
            FabricChunkAttr chunk;
            chunk.offset = item.at("off").get<uint64_t>();
            chunk.length = item.at("len").get<uint64_t>();
            chunk.nics = item.at("nics").get<std::vector<int>>();
            chunk.keys = item.at("keys").get<std::vector<uint64_t>>();
            if (item.contains("near"))
                chunk.near = item.at("near").get<std::vector<int>>();
            if (chunk.offset != expected || chunk.length == 0 ||
                chunk.nics.size() != chunk.keys.size() || chunk.nics.empty()) {
                return Status::MalformedJson(
                    "Inconsistent fabric buffer chunks" LOC_MARK);
            }
            for (int nic : chunk.near) {
                if (std::find(chunk.nics.begin(), chunk.nics.end(), nic) ==
                    chunk.nics.end()) {
                    return Status::MalformedJson(
                        "Fabric chunk prefers an unregistered NIC" LOC_MARK);
                }
            }
            expected += chunk.length;
            attr.chunks.push_back(std::move(chunk));
        }
        if (expected != buffer_length) {
            return Status::MalformedJson(
                "Fabric buffer chunks do not cover the buffer" LOC_MARK);
        }
    } catch (const std::exception& e) {
        return Status::MalformedJson(std::string("Bad fabric buffer attr: ") +
                                     e.what() + LOC_MARK);
    }
    return Status::OK();
}

std::vector<FabricChunkRange> planFabricChunks(uint64_t length,
                                               uint64_t chunk_limit) {
    std::vector<FabricChunkRange> chunks;
    if (chunk_limit == 0 || length <= chunk_limit) {
        chunks.push_back({0, length});
        return chunks;
    }
    for (uint64_t offset = 0; offset < length; offset += chunk_limit) {
        chunks.push_back({offset, std::min(chunk_limit, length - offset)});
    }
    return chunks;
}

Status assignFabricChunkNics(const std::vector<FabricChunkRange>& chunks,
                             size_t num_nics, uint64_t page_size,
                             uint64_t pte_budget,
                             std::vector<std::vector<int>>& assignment) {
    assignment.assign(chunks.size(), {});
    if (num_nics == 0) {
        return Status::DeviceNotFound("No fabric NIC to register on" LOC_MARK);
    }
    if (page_size == 0) page_size = 4096;
    auto pages = [&](uint64_t bytes) {
        return (bytes + page_size - 1) / page_size;
    };
    uint64_t total_pages = 0;
    for (const auto& chunk : chunks) total_pages += pages(chunk.length);

    if (pte_budget == 0 || total_pages <= pte_budget) {
        for (auto& nics : assignment) {
            for (size_t n = 0; n < num_nics; ++n) nics.push_back((int)n);
        }
        return Status::OK();
    }

    if (chunks.size() <= num_nics) {
        // Disjoint partition: each chunk gets its own group of NICs.
        const size_t per = num_nics / chunks.size();
        const size_t extra = num_nics % chunks.size();
        size_t next = 0;
        for (size_t c = 0; c < chunks.size(); ++c) {
            const size_t count = per + (c < extra ? 1 : 0);
            for (size_t k = 0; k < count; ++k)
                assignment[c].push_back((int)next++);
        }
        for (const auto& chunk : chunks) {
            if (pages(chunk.length) > pte_budget) {
                return Status::InvalidArgument(
                    "Fabric chunk exceeds the per-NIC page budget" LOC_MARK);
            }
        }
        return Status::OK();
    }

    std::vector<uint64_t> used(num_nics, 0);
    for (size_t c = 0; c < chunks.size(); ++c) {
        const size_t nic = c % num_nics;
        used[nic] += pages(chunks[c].length);
        if (used[nic] > pte_budget) {
            return Status::InvalidArgument(
                "Buffer exceeds the page budget of all fabric NICs "
                "combined" LOC_MARK);
        }
        assignment[c].push_back((int)nic);
    }
    return Status::OK();
}

// Index of the chunk holding `offset`, or chunks.size() if none does.
static size_t findChunk(const std::vector<FabricChunkRange>& chunks,
                        uint64_t offset) {
    auto it = std::upper_bound(
        chunks.begin(), chunks.end(), offset,
        [](uint64_t off, const FabricChunkRange& c) { return off < c.offset; });
    if (it == chunks.begin()) return chunks.size();
    --it;
    if (offset - it->offset >= it->length) return chunks.size();
    return static_cast<size_t>(it - chunks.begin());
}

bool cutFabricSpans(uint64_t length, uint64_t local_start,
                    const std::vector<FabricChunkRange>& local_chunks,
                    uint64_t remote_start,
                    const std::vector<FabricChunkRange>& remote_chunks,
                    uint64_t max_span, std::vector<FabricSpan>& spans) {
    spans.clear();
    if (max_span == 0) max_span = UINT64_MAX;
    uint64_t done = 0;
    while (done < length) {
        const uint64_t local_off = local_start + done;
        const uint64_t remote_off = remote_start + done;
        const size_t lc = findChunk(local_chunks, local_off);
        const size_t rc = findChunk(remote_chunks, remote_off);
        if (lc == local_chunks.size() || rc == remote_chunks.size())
            return false;
        const auto& l = local_chunks[lc];
        const auto& r = remote_chunks[rc];
        uint64_t span = length - done;
        span = std::min(span, l.offset + l.length - local_off);
        span = std::min(span, r.offset + r.length - remote_off);
        span = std::min(span, max_span);
        spans.push_back({done, span, lc, rc});
        done += span;
    }
    return true;
}

}  // namespace tent
}  // namespace mooncake
