#pragma once

#include <cstdint>

// The frame a coro_rpc client puts on the wire before the serialized payload.
// A recorder stores the payload and the function id; a replayer has to rebuild
// the rest of the frame, so every tool that writes one shares this layout.
namespace mooncake::traffic::wire {

constexpr std::uint32_t kRequestMagic = 21;
constexpr std::uint32_t kProtocolVersion = 0;
constexpr std::uint32_t kMaxResponseBytes = 512U * 1024U * 1024U;

#pragma pack(push, 1)
struct RequestHeader {
    std::uint8_t magic;
    std::uint8_t version;
    std::uint8_t serialize_type;
    std::uint8_t msg_type;
    std::uint32_t seq_num;
    std::uint32_t function_id;
    std::uint32_t length;
    std::uint32_t attach_length;
};
static_assert(sizeof(RequestHeader) == 20,
              "the recorded payload is replayed behind this exact header");

struct ResponseHeader {
    std::uint8_t magic;
    std::uint8_t version;
    std::uint8_t err_code;
    std::uint8_t msg_type;
    std::uint32_t seq_num;
    std::uint32_t length;
    std::uint32_t attach_length;
};
static_assert(sizeof(ResponseHeader) == 16,
              "every replayer parses responses with this exact header");
#pragma pack(pop)

}  // namespace mooncake::traffic::wire
