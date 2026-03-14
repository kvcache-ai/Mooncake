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

#ifndef TCP_HP_PROTOCOL_H_
#define TCP_HP_PROTOCOL_H_

#include <cstdint>
#include <endian.h>

namespace mooncake {
namespace tent {
namespace tcp_hp {

// "MOONTCPH" in ASCII = 0x4D4F4F4E'54435048
static constexpr uint64_t kMagic = 0x4D4F4F4E54435048ULL;
static constexpr uint32_t kProtocolVersion = 1;

// Sent by client immediately after TCP connect.
struct __attribute__((packed)) HandshakeReq {
    uint64_t magic;     // kMagic
    uint32_t version;   // kProtocolVersion
    uint32_t flags;     // reserved, set to 0
};

// Sent by server in response to HandshakeReq.
struct __attribute__((packed)) HandshakeAck {
    uint16_t status;    // 0 = OK, non-zero = error
    uint16_t flags;     // reserved
};

// Status codes for HandshakeAck.
static constexpr uint16_t kHandshakeOK = 0;
static constexpr uint16_t kHandshakeBadMagic = 1;
static constexpr uint16_t kHandshakeBadVersion = 2;

// Data transfer header, sent before each transfer payload.
struct __attribute__((packed)) DataHeader {
    uint8_t  opcode;    // Request::WRITE=1 or Request::READ=0
    uint8_t  flags;     // reserved
    uint16_t reserved;  // alignment padding
    uint64_t addr;      // destination address (little-endian)
    uint64_t length;    // transfer length (little-endian)

    void encode(uint8_t op, uint64_t a, uint64_t len) {
        opcode = op;
        flags = 0;
        reserved = 0;
        addr = htole64(a);
        length = htole64(len);
    }

    uint64_t decodedAddr() const { return le64toh(addr); }
    uint64_t decodedLength() const { return le64toh(length); }
};

static_assert(sizeof(HandshakeReq) == 16, "HandshakeReq must be 16 bytes");
static_assert(sizeof(HandshakeAck) == 4, "HandshakeAck must be 4 bytes");
static_assert(sizeof(DataHeader) == 20, "DataHeader must be 20 bytes (packed)");

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake

#endif  // TCP_HP_PROTOCOL_H_
