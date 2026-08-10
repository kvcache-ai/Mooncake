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

#ifndef RDMA_CTRL_FRAME_H_
#define RDMA_CTRL_FRAME_H_

#include <cstdint>
#include <string>
#include <vector>

#include "transfer_metadata.h"

namespace mooncake {

// Wire header (little-endian on the wire, host endian locally):
// | magic:u32 | ver:u8 | type:u8 | flags:u16 | session:u64 | epoch:u64 |
// | seq:u64 | ack_seq:u64 | payload_len:u32 | payload... |
constexpr uint32_t kCtrlFrameMagic = 0x434B434Du;  // 'MCKC' LE
constexpr uint8_t kCtrlFrameVersion = 1;
constexpr size_t kCtrlFrameHeaderSize = 44;

enum class CtrlFrameType : uint8_t {
    CREDIT_GRANT = 1,
    CREDIT_PROGRESS = 2,
    CREDIT_REQUEST = 3,
    DATA_ACK = 4,
    SESSION_OPEN = 5,
    SESSION_CLOSE = 6,
    FENCE = 7,
    DRAIN_ACK = 8,
    CTRL_ACK = 9,
    NOTIFY_COMPAT = 10,
};

enum class CtrlFrameFlags : uint16_t {
    None = 0,
    NeedsAck = 1 << 0,
};

enum class CreditResource : uint16_t {
    DataBytes = 1,
    RequestSlots = 2,
    BounceBytes = 3,
    BounceSlots = 4,
};

struct CreditAmount {
    CreditResource resource = CreditResource::DataBytes;
    uint64_t grant_total = 0;
};

struct DataAckEntry {
    uint64_t task_id = 0;
    uint64_t acked_bytes = 0;
};

struct CtrlFrame {
    uint8_t version = kCtrlFrameVersion;
    CtrlFrameType type = CtrlFrameType::NOTIFY_COMPAT;
    uint16_t flags = 0;
    uint64_t session = 0;
    uint64_t epoch = 0;
    uint64_t seq = 0;
    uint64_t ack_seq = 0;
    std::vector<uint8_t> payload;
};

// Payload helpers -----------------------------------------------------------

int encodeNotifyCompatPayload(const TransferMetadata::NotifyDesc &notify,
                              std::vector<uint8_t> &out);
int decodeNotifyCompatPayload(const uint8_t *data, size_t len,
                              TransferMetadata::NotifyDesc &notify);

int encodeCreditGrantPayload(const std::vector<CreditAmount> &grants,
                             std::vector<uint8_t> &out);
int decodeCreditGrantPayload(const uint8_t *data, size_t len,
                             std::vector<CreditAmount> &grants);

int encodeDataAckPayload(const std::vector<DataAckEntry> &acks,
                         std::vector<uint8_t> &out);
int decodeDataAckPayload(const uint8_t *data, size_t len,
                         std::vector<DataAckEntry> &acks);

// SESSION_OPEN payload: bounce_slots:u32 | bounce_slot_size:u32
int encodeSessionOpenPayload(uint32_t bounce_slots, uint32_t bounce_slot_size,
                             std::vector<uint8_t> &out);
int decodeSessionOpenPayload(const uint8_t *data, size_t len,
                             uint32_t &bounce_slots,
                             uint32_t &bounce_slot_size);

// Full frame ---------------------------------------------------------------

bool isCtrlFrameMagic(const uint8_t *data, size_t len);

// Encode frame into out. Returns 0 on success.
int encodeCtrlFrame(const CtrlFrame &frame, std::vector<uint8_t> &out);

// Decode frame from buffer. Returns 0 on success.
int decodeCtrlFrame(const uint8_t *data, size_t len, CtrlFrame &frame);

}  // namespace mooncake

#endif  // RDMA_CTRL_FRAME_H_
