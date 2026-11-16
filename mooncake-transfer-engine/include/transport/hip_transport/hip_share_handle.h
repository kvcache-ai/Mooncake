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

#ifndef HIP_SHARE_HANDLE_H_
#define HIP_SHARE_HANDLE_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mooncake {
namespace hip_share {

// How a registered HIP buffer is shared with peers. Published (hex-encoded)
// in BufferDesc::shm_name.
enum class Kind : uint8_t {
    kUnknown = 0,
    kIpc = 1,      // hipIpcMemHandle_t, same host only
    kPosixFd = 2,  // POSIX fd + owner pid (pidfd_getfd), same host only
    kFabric = 3,   // hipMemFabricHandle_t, reachable within one UALink vPOD
};

const char *kindName(Kind kind);

constexpr size_t kMaxHandleBytes = 64;
// Pre-typed formats were told apart by blob length only.
constexpr size_t kLegacyIpcBytes = 64;
constexpr size_t kLegacyPosixFdBytes = 8;

// One physical allocation backing [offset, offset + size) of the buffer.
struct Chunk {
    uint64_t offset = 0;
    uint64_t size = 0;
    std::vector<uint8_t> handle;
};

struct Descriptor {
    Kind kind = Kind::kUnknown;
    bool legacy = false;
    int32_t owner_pid = -1;
    int32_t owner_device = -1;
    // UALink fabric domain of owner_device; empty ppod_id and vpod_id ==
    // kNoVpod mean "unknown", which only matches same-host peers.
    std::string ppod_id;
    uint32_t vpod_id = kNoVpod;
    std::vector<Chunk> chunks;

    static constexpr uint32_t kNoVpod = 0xffffffffu;
};

// Typed binary layout (little endian):
//   magic "HSHR" | u16 version | u8 kind | u8 reserved | i32 owner_pid
//   | i32 owner_device | u32 vpod_id | u16 ppod_len | ppod bytes
//   | u32 nchunks | { u64 offset | u64 size | u16 hlen | handle bytes }*
std::vector<uint8_t> encode(const Descriptor &desc);

// Accepts the typed layout and both legacy layouts (64-byte IPC handle,
// 8-byte {fd, pid}). Returns false on malformed input.
bool decode(const uint8_t *data, size_t size, Descriptor &desc);
bool decode(const std::vector<uint8_t> &bytes, Descriptor &desc);

// Hex form used in BufferDesc::shm_name.
std::string encodeToShmName(const Descriptor &desc);
bool decodeFromShmName(const std::string &shm_name, Descriptor &desc);

}  // namespace hip_share
}  // namespace mooncake

#endif  // HIP_SHARE_HANDLE_H_
