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

#include "transport/rdma_twosided/ctrl_frame.h"

#include <cstring>

#include "error.h"

namespace mooncake {

namespace {

template <typename T>
void appendPod(std::vector<uint8_t> &out, const T &v) {
    const auto *p = reinterpret_cast<const uint8_t *>(&v);
    out.insert(out.end(), p, p + sizeof(T));
}

template <typename T>
int readPod(const uint8_t *&ptr, const uint8_t *end, T &v) {
    if (static_cast<size_t>(end - ptr) < sizeof(T)) return ERR_INVALID_ARGUMENT;
    std::memcpy(&v, ptr, sizeof(T));
    ptr += sizeof(T);
    return 0;
}

int requireExactEnd(const uint8_t *ptr, const uint8_t *end) {
    return ptr == end ? 0 : ERR_INVALID_ARGUMENT;
}

}  // namespace

int encodeNotifyCompatPayload(const TransferMetadata::NotifyDesc &notify,
                              std::vector<uint8_t> &out) {
    if (notify.name.size() > UINT32_MAX ||
        notify.notify_msg.size() > UINT32_MAX)
        return ERR_INVALID_ARGUMENT;
    uint32_t name_len = static_cast<uint32_t>(notify.name.size());
    uint32_t msg_len = static_cast<uint32_t>(notify.notify_msg.size());
    out.clear();
    out.reserve(8 + name_len + msg_len);
    appendPod(out, name_len);
    out.insert(out.end(), notify.name.begin(), notify.name.end());
    appendPod(out, msg_len);
    out.insert(out.end(), notify.notify_msg.begin(), notify.notify_msg.end());
    return 0;
}

int decodeNotifyCompatPayload(const uint8_t *data, size_t len,
                              TransferMetadata::NotifyDesc &notify) {
    if (!data || len < 8) return ERR_INVALID_ARGUMENT;
    const uint8_t *ptr = data;
    const uint8_t *end = data + len;
    uint32_t name_len = 0, msg_len = 0;
    if (readPod(ptr, end, name_len)) return ERR_INVALID_ARGUMENT;
    if (static_cast<size_t>(end - ptr) < name_len) return ERR_INVALID_ARGUMENT;
    // Preflight remaining length before mutating `notify`.
    const uint8_t *name_ptr = ptr;
    ptr += name_len;
    if (readPod(ptr, end, msg_len)) return ERR_INVALID_ARGUMENT;
    if (static_cast<size_t>(end - ptr) < msg_len) return ERR_INVALID_ARGUMENT;
    const uint8_t *msg_ptr = ptr;
    ptr += msg_len;
    if (requireExactEnd(ptr, end)) return ERR_INVALID_ARGUMENT;

    TransferMetadata::NotifyDesc decoded;
    decoded.name.assign(reinterpret_cast<const char *>(name_ptr), name_len);
    decoded.notify_msg.assign(reinterpret_cast<const char *>(msg_ptr), msg_len);
    notify = std::move(decoded);
    return 0;
}

int encodeCreditGrantPayload(const std::vector<CreditAmount> &grants,
                             std::vector<uint8_t> &out) {
    if (grants.size() > UINT16_MAX) return ERR_INVALID_ARGUMENT;
    out.clear();
    uint16_t count = static_cast<uint16_t>(grants.size());
    appendPod(out, count);
    for (const auto &g : grants) {
        appendPod(out, static_cast<uint16_t>(g.resource));
        appendPod(out, g.grant_total);
    }
    return 0;
}

int decodeCreditGrantPayload(const uint8_t *data, size_t len,
                             std::vector<CreditAmount> &grants) {
    if (!data || len < sizeof(uint16_t)) return ERR_INVALID_ARGUMENT;
    const uint8_t *ptr = data;
    const uint8_t *end = data + len;
    uint16_t count = 0;
    if (readPod(ptr, end, count)) return ERR_INVALID_ARGUMENT;
    constexpr size_t kEntrySize = sizeof(uint16_t) + sizeof(uint64_t);
    if (count > 4) return ERR_INVALID_ARGUMENT;
    const size_t need = static_cast<size_t>(count) * kEntrySize;
    if (static_cast<size_t>(end - ptr) != need) return ERR_INVALID_ARGUMENT;

    std::vector<CreditAmount> decoded;
    decoded.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        uint16_t resource = 0;
        uint64_t total = 0;
        if (readPod(ptr, end, resource) || readPod(ptr, end, total))
            return ERR_INVALID_ARGUMENT;
        if (resource < 1 || resource > 4) return ERR_INVALID_ARGUMENT;
        decoded.push_back({static_cast<CreditResource>(resource), total});
    }
    if (requireExactEnd(ptr, end)) return ERR_INVALID_ARGUMENT;
    grants = std::move(decoded);
    return 0;
}

int encodeDataAckPayload(const std::vector<DataAckEntry> &acks,
                         std::vector<uint8_t> &out) {
    if (acks.size() > UINT16_MAX) return ERR_INVALID_ARGUMENT;
    out.clear();
    uint16_t count = static_cast<uint16_t>(acks.size());
    appendPod(out, count);
    for (const auto &a : acks) {
        appendPod(out, a.task_id);
        appendPod(out, a.acked_bytes);
    }
    return 0;
}

int decodeDataAckPayload(const uint8_t *data, size_t len,
                         std::vector<DataAckEntry> &acks) {
    if (!data || len < sizeof(uint16_t)) return ERR_INVALID_ARGUMENT;
    const uint8_t *ptr = data;
    const uint8_t *end = data + len;
    uint16_t count = 0;
    if (readPod(ptr, end, count)) return ERR_INVALID_ARGUMENT;
    constexpr size_t kEntrySize = sizeof(uint64_t) * 2;
    const size_t need = static_cast<size_t>(count) * kEntrySize;
    if (need / kEntrySize != count) return ERR_INVALID_ARGUMENT;  // overflow
    if (static_cast<size_t>(end - ptr) != need) return ERR_INVALID_ARGUMENT;

    std::vector<DataAckEntry> decoded;
    decoded.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        DataAckEntry e;
        if (readPod(ptr, end, e.task_id) || readPod(ptr, end, e.acked_bytes))
            return ERR_INVALID_ARGUMENT;
        decoded.push_back(e);
    }
    if (requireExactEnd(ptr, end)) return ERR_INVALID_ARGUMENT;
    acks = std::move(decoded);
    return 0;
}

int encodeSessionOpenPayload(uint32_t bounce_slots, uint32_t bounce_slot_size,
                             std::vector<uint8_t> &out) {
    out.clear();
    appendPod(out, bounce_slots);
    appendPod(out, bounce_slot_size);
    return 0;
}

int decodeSessionOpenPayload(const uint8_t *data, size_t len,
                             uint32_t &bounce_slots,
                             uint32_t &bounce_slot_size) {
    if (!data || len != 8) return ERR_INVALID_ARGUMENT;
    const uint8_t *ptr = data;
    const uint8_t *end = data + len;
    uint32_t slots = 0, size = 0;
    if (readPod(ptr, end, slots) || readPod(ptr, end, size))
        return ERR_INVALID_ARGUMENT;
    if (requireExactEnd(ptr, end)) return ERR_INVALID_ARGUMENT;
    bounce_slots = slots;
    bounce_slot_size = size;
    return 0;
}

bool isCtrlFrameMagic(const uint8_t *data, size_t len) {
    if (!data || len < sizeof(uint32_t)) return false;
    uint32_t magic = 0;
    std::memcpy(&magic, data, sizeof(magic));
    return magic == kCtrlFrameMagic;
}

int encodeCtrlFrame(const CtrlFrame &frame, std::vector<uint8_t> &out) {
    if (frame.version != kCtrlFrameVersion) return ERR_INVALID_ARGUMENT;
    if (!isKnownCtrlFrameType(static_cast<uint8_t>(frame.type)))
        return ERR_INVALID_ARGUMENT;
    if ((frame.flags & ~kCtrlFrameKnownFlagsMask) != 0)
        return ERR_INVALID_ARGUMENT;
    if (frame.payload.size() > UINT32_MAX) return ERR_INVALID_ARGUMENT;
    out.clear();
    out.reserve(kCtrlFrameHeaderSize + frame.payload.size());
    appendPod(out, kCtrlFrameMagic);
    appendPod(out, frame.version);
    appendPod(out, static_cast<uint8_t>(frame.type));
    appendPod(out, frame.flags);
    appendPod(out, frame.session);
    appendPod(out, frame.epoch);
    appendPod(out, frame.seq);
    appendPod(out, frame.ack_seq);
    uint32_t payload_len = static_cast<uint32_t>(frame.payload.size());
    appendPod(out, payload_len);
    out.insert(out.end(), frame.payload.begin(), frame.payload.end());
    return 0;
}

int decodeCtrlFrame(const uint8_t *data, size_t len, CtrlFrame &frame) {
    if (!data || len < kCtrlFrameHeaderSize) return ERR_INVALID_ARGUMENT;
    const uint8_t *ptr = data;
    const uint8_t *end = data + len;
    uint32_t magic = 0;
    uint8_t version = 0;
    uint8_t type = 0;
    uint16_t flags = 0;
    uint64_t session = 0, epoch = 0, seq = 0, ack_seq = 0;
    uint32_t payload_len = 0;
    if (readPod(ptr, end, magic) || magic != kCtrlFrameMagic)
        return ERR_INVALID_ARGUMENT;
    if (readPod(ptr, end, version) || readPod(ptr, end, type) ||
        readPod(ptr, end, flags) || readPod(ptr, end, session) ||
        readPod(ptr, end, epoch) || readPod(ptr, end, seq) ||
        readPod(ptr, end, ack_seq) || readPod(ptr, end, payload_len))
        return ERR_INVALID_ARGUMENT;
    if (version != kCtrlFrameVersion) return ERR_INVALID_ARGUMENT;
    if (!isKnownCtrlFrameType(type)) return ERR_INVALID_ARGUMENT;
    if ((flags & ~kCtrlFrameKnownFlagsMask) != 0) return ERR_INVALID_ARGUMENT;
    if (static_cast<size_t>(end - ptr) != payload_len)
        return ERR_INVALID_ARGUMENT;

    CtrlFrame decoded;
    decoded.version = version;
    decoded.type = static_cast<CtrlFrameType>(type);
    decoded.flags = flags;
    decoded.session = session;
    decoded.epoch = epoch;
    decoded.seq = seq;
    decoded.ack_seq = ack_seq;
    decoded.payload.assign(ptr, ptr + payload_len);
    frame = std::move(decoded);
    return 0;
}

}  // namespace mooncake
