// Copyright(C) 2026 Advanced Micro Devices, Inc. All rights reserved.
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

#include "transport/hip_transport/hip_share_handle.h"

#include <cstring>
#include <stdexcept>

#include "common/serialization.h"

namespace mooncake {
namespace hip_share {

namespace {

constexpr uint8_t kMagic[4] = {'H', 'S', 'H', 'R'};
constexpr uint16_t kVersion = 1;
constexpr uint32_t kMaxChunks = 1u << 20;

template <typename T>
void put(std::vector<uint8_t> &out, T value) {
    uint8_t bytes[sizeof(T)];
    memcpy(bytes, &value, sizeof(T));
    out.insert(out.end(), bytes, bytes + sizeof(T));
}

class Reader {
   public:
    Reader(const uint8_t *data, size_t size) : data_(data), size_(size) {}

    template <typename T>
    bool get(T &value) {
        if (size_ - pos_ < sizeof(T)) return false;
        memcpy(&value, data_ + pos_, sizeof(T));
        pos_ += sizeof(T);
        return true;
    }

    bool bytes(size_t n, const uint8_t *&ptr) {
        if (size_ - pos_ < n) return false;
        ptr = data_ + pos_;
        pos_ += n;
        return true;
    }

    bool done() const { return pos_ == size_; }

   private:
    const uint8_t *data_;
    size_t size_;
    size_t pos_ = 0;
};

bool decodeLegacy(const uint8_t *data, size_t size, Descriptor &desc) {
    desc = Descriptor();
    desc.legacy = true;
    Chunk chunk;
    chunk.handle.assign(data, data + size);
    if (size == kLegacyIpcBytes) {
        desc.kind = Kind::kIpc;
    } else {
        desc.kind = Kind::kPosixFd;
        int32_t fd_pid[2];
        memcpy(fd_pid, data, sizeof(fd_pid));
        desc.owner_pid = fd_pid[1];
    }
    desc.chunks.push_back(std::move(chunk));
    return true;
}

}  // namespace

const char *kindName(Kind kind) {
    switch (kind) {
        case Kind::kIpc:
            return "ipc";
        case Kind::kPosixFd:
            return "posix_fd";
        case Kind::kFabric:
            return "fabric";
        default:
            return "unknown";
    }
}

std::vector<uint8_t> encode(const Descriptor &desc) {
    std::vector<uint8_t> out;
    out.reserve(32 + desc.ppod_id.size() +
                desc.chunks.size() * (18 + kMaxHandleBytes));
    out.insert(out.end(), kMagic, kMagic + sizeof(kMagic));
    put<uint16_t>(out, kVersion);
    put<uint8_t>(out, static_cast<uint8_t>(desc.kind));
    put<uint8_t>(out, 0);
    put<int32_t>(out, desc.owner_pid);
    put<int32_t>(out, desc.owner_device);
    put<uint32_t>(out, desc.vpod_id);
    if (desc.ppod_id.size() > UINT16_MAX)
        throw std::invalid_argument("ppod_id too long");
    put<uint16_t>(out, static_cast<uint16_t>(desc.ppod_id.size()));
    out.insert(out.end(), desc.ppod_id.begin(), desc.ppod_id.end());
    put<uint32_t>(out, static_cast<uint32_t>(desc.chunks.size()));
    for (const auto &chunk : desc.chunks) {
        if (chunk.handle.size() > kMaxHandleBytes)
            throw std::invalid_argument("share handle too long");
        put<uint64_t>(out, chunk.offset);
        put<uint64_t>(out, chunk.size);
        put<uint16_t>(out, static_cast<uint16_t>(chunk.handle.size()));
        out.insert(out.end(), chunk.handle.begin(), chunk.handle.end());
    }
    return out;
}

bool decode(const uint8_t *data, size_t size, Descriptor &desc) {
    if (!data) return false;
    // The typed layout is never exactly 64 or 8 bytes long.
    if (size == kLegacyIpcBytes || size == kLegacyPosixFdBytes)
        return decodeLegacy(data, size, desc);

    Reader reader(data, size);
    const uint8_t *magic = nullptr;
    if (!reader.bytes(sizeof(kMagic), magic) ||
        memcmp(magic, kMagic, sizeof(kMagic)) != 0)
        return false;

    Descriptor out;
    uint16_t version = 0;
    uint8_t kind = 0, reserved = 0;
    uint16_t ppod_len = 0;
    uint32_t nchunks = 0;
    if (!reader.get(version) || version != kVersion) return false;
    if (!reader.get(kind) || !reader.get(reserved)) return false;
    if (kind < static_cast<uint8_t>(Kind::kIpc) ||
        kind > static_cast<uint8_t>(Kind::kFabric))
        return false;
    out.kind = static_cast<Kind>(kind);
    if (!reader.get(out.owner_pid) || !reader.get(out.owner_device) ||
        !reader.get(out.vpod_id) || !reader.get(ppod_len))
        return false;
    const uint8_t *ppod = nullptr;
    if (!reader.bytes(ppod_len, ppod)) return false;
    out.ppod_id.assign(reinterpret_cast<const char *>(ppod), ppod_len);
    if (!reader.get(nchunks) || nchunks == 0 || nchunks > kMaxChunks)
        return false;
    out.chunks.resize(nchunks);
    uint64_t expected_offset = 0;
    for (auto &chunk : out.chunks) {
        uint16_t hlen = 0;
        const uint8_t *handle = nullptr;
        if (!reader.get(chunk.offset) || !reader.get(chunk.size) ||
            !reader.get(hlen) || hlen > kMaxHandleBytes ||
            !reader.bytes(hlen, handle))
            return false;
        // Chunks tile the buffer in order without gaps.
        if (chunk.offset != expected_offset || chunk.size == 0) return false;
        expected_offset += chunk.size;
        chunk.handle.assign(handle, handle + hlen);
    }
    if (!reader.done()) return false;
    desc = std::move(out);
    return true;
}

bool decode(const std::vector<uint8_t> &bytes, Descriptor &desc) {
    return decode(bytes.data(), bytes.size(), desc);
}

std::string encodeToShmName(const Descriptor &desc) {
    auto bytes = encode(desc);
    return serializeBinaryData(bytes.data(), bytes.size());
}

bool decodeFromShmName(const std::string &shm_name, Descriptor &desc) {
    std::vector<unsigned char> bytes;
    try {
        deserializeBinaryData(shm_name, bytes);
    } catch (const std::exception &) {
        return false;
    }
    return decode(bytes.data(), bytes.size(), desc);
}

}  // namespace hip_share
}  // namespace mooncake
