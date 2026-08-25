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

#ifndef RDMA_MSG_HEADER_H_
#define RDMA_MSG_HEADER_H_

#include <cstdint>
#include <cstring>

#include "error.h"

namespace mooncake {

// Msg QP payload header (host endian). Fixed 32 bytes.
// | magic:u32 | ver:u8 | type:u8 | flags:u8 | slice_seq:u8 |
// | task_id:u64 | dest_addr:u64 | length:u32 | total_chunks:u32 |
constexpr uint32_t kMsgMagic = 0x47534D4Du;  // 'MSGM' LE
constexpr uint8_t kMsgVersion = 1;
constexpr size_t kMsgHeaderSize = 32;

enum class MsgType : uint8_t {
    DATA_WRITE = 1,
    READ_REQ = 2,
    READ_RESP = 3,
};

struct MsgHeader {
    uint8_t version = kMsgVersion;
    MsgType type = MsgType::DATA_WRITE;
    uint8_t flags = 0;
    uint8_t slice_seq = 0;
    uint64_t task_id = 0;
    uint64_t dest_addr = 0;
    uint32_t length = 0;
    // Chunks the whole task is split into, so the receiver knows when it has
    // seen all of them and can drop the task's ACK bookkeeping. Order-agnostic,
    // unlike a last-chunk flag. Zero means the sender did not report it.
    uint32_t total_chunks = 0;
};

inline int encodeMsgHeader(const MsgHeader &h, void *out, size_t out_len) {
    if (!out || out_len < kMsgHeaderSize) return ERR_INVALID_ARGUMENT;
    auto *p = static_cast<uint8_t *>(out);
    std::memset(p, 0, kMsgHeaderSize);
    std::memcpy(p + 0, &kMsgMagic, 4);
    p[4] = h.version;
    p[5] = static_cast<uint8_t>(h.type);
    p[6] = h.flags;
    p[7] = h.slice_seq;
    std::memcpy(p + 8, &h.task_id, 8);
    std::memcpy(p + 16, &h.dest_addr, 8);
    std::memcpy(p + 24, &h.length, 4);
    std::memcpy(p + 28, &h.total_chunks, 4);
    return 0;
}

inline int decodeMsgHeader(const void *in, size_t in_len, MsgHeader &h) {
    if (!in || in_len < kMsgHeaderSize) return ERR_INVALID_ARGUMENT;
    auto *p = static_cast<const uint8_t *>(in);
    uint32_t magic = 0;
    std::memcpy(&magic, p, 4);
    if (magic != kMsgMagic) return ERR_INVALID_ARGUMENT;
    h.version = p[4];
    h.type = static_cast<MsgType>(p[5]);
    h.flags = p[6];
    h.slice_seq = p[7];
    std::memcpy(&h.task_id, p + 8, 8);
    std::memcpy(&h.dest_addr, p + 16, 8);
    std::memcpy(&h.length, p + 24, 4);
    std::memcpy(&h.total_chunks, p + 28, 4);
    return 0;
}

}  // namespace mooncake

#endif  // RDMA_MSG_HEADER_H_
