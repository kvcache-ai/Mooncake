#pragma once
#include <cstdint>
#include <string_view>

namespace cinatra {
using namespace std::string_view_literals;

#if defined(_WIN32)
#define __SWAP_LONGLONG(l)                                                     \
  ((((l) >> 56) & 0x00000000000000FFLL) |                                      \
   (((l) >> 40) & 0x000000000000FF00LL) |                                      \
   (((l) >> 24) & 0x0000000000FF0000LL) |                                      \
   (((l) >> 8) & 0x00000000FF000000LL) | (((l) << 8) & 0x000000FF00000000LL) | \
   (((l) << 24) & 0x0000FF0000000000LL) |                                      \
   (((l) << 40) & 0x00FF000000000000LL) |                                      \
   (((l) << 56) & 0xFF00000000000000LL))

inline uint64_t htobe64(uint64_t val) {
  const uint64_t ret = __SWAP_LONGLONG(val);
  return ret;
}

inline uint64_t be64toh(uint64_t val) {
  const uint64_t ret = __SWAP_LONGLONG(val);
  return ret;
}

#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define be64toh(x) OSSwapBigToHostInt64(x)
#define htobe64(x) OSSwapHostToBigInt64(x)
#endif  //_WIN32

enum opcode : std::uint8_t {
  cont = 0,
  text = 1,
  binary = 2,
  rsv3 = 3,
  rsv4 = 4,
  rsv5 = 5,
  rsv6 = 6,
  rsv7 = 7,
  close = 8,
  ping = 9,
  pong = 10,
  crsvb = 11,
  crsvc = 12,
  crsvd = 13,
  crsve = 14,
  crsvf = 15
};

/** Close status codes.
These codes accompany close frames.
@see <a href="https://tools.ietf.org/html/rfc6455#section-7.4.1">RFC 6455 7.4.1
Defined Status Codes</a>
*/
enum close_code : std::uint16_t {
  /// Normal closure; the connection successfully completed whatever purpose for
  /// which it was created.
  normal = 1000,

  /// The endpoint is going away, either because of a server failure or because
  /// the browser is navigating away from the page that opened the connection.
  going_away = 1001,

  /// The endpoint is terminating the connection due to a protocol error.
  protocol_error = 1002,

  /// The connection is being terminated because the endpoint received data of a
  /// type it cannot accept (for example, a text-only endpoint received binary
  /// data).
  unknown_data = 1003,

  /// The endpoint is terminating the connection because a message was received
  /// that contained inconsistent data (e.g., non-UTF-8 data within a text
  /// message).
  bad_payload = 1007,

  /// The endpoint is terminating the connection because it received a message
  /// that violates its policy. This is a generic status code, used when codes
  /// 1003 and 1009 are not suitable.
  policy_error = 1008,

  /// The endpoint is terminating the connection because a data frame was
  /// received that is too large.
  too_big = 1009,

  /// The client is terminating the connection because it expected the server to
  /// negotiate one or more extension, but the server didn't.
  needs_extension = 1010,

  /// The server is terminating the connection because it encountered an
  /// unexpected condition that prevented it from fulfilling the request.
  internal_error = 1011,

  /// The server is terminating the connection because it is restarting.
  service_restart = 1012,

  /// The server is terminating the connection due to a temporary condition,
  /// e.g. it is overloaded and is casting off some of its clients.
  try_again_later = 1013,

  //----
  //
  // The following are illegal on the wire
  //

  /** Used internally to mean "no error"
  This code is reserved and may not be sent.
  */
  none = 0,

  /** Reserved for future use by the WebSocket standard.
  This code is reserved and may not be sent.
  */
  reserved1 = 1004,

  /** No status code was provided even though one was expected.
  This code is reserved and may not be sent.
  */
  no_status = 1005,

  /** Connection was closed without receiving a close frame

  This code is reserved and may not be sent.
  */
  abnormal = 1006,

  /** Reserved for future use by the WebSocket standard.

  This code is reserved and may not be sent.
  */
  reserved2 = 1014,

  /** Reserved for future use by the WebSocket standard.

  This code is reserved and may not be sent.
  */
  reserved3 = 1015

  //
  //----

  // last = 5000 // satisfy warnings
};

enum class ws_frame_type {
  WS_ERROR_FRAME = 0xFF00,
  WS_INCOMPLETE_FRAME = 0xFE00,

  WS_OPENING_FRAME = 0x3300,
  WS_CLOSING_FRAME = 0x3400,

  WS_INCOMPLETE_TEXT_FRAME = 0x01,
  WS_INCOMPLETE_BINARY_FRAME = 0x02,

  WS_TEXT_FRAME = 0x81,    // 128 + 1 == WS_FRAGMENT_FIN | WS_OPCODE_TEXT
  WS_BINARY_FRAME = 0x82,  // 128 + 2
  WS_RSV3_FRAME = 0x83,    // 128 + 3
  WS_RSV4_FRAME = 0x84,
  WS_RSV5_FRAME = 0x85,
  WS_RSV6_FRAME = 0x86,
  WS_RSV7_FRAME = 0x87,
  WS_CLOSE_FRAME = 0x88,
  WS_PING_FRAME = 0x89,
  WS_PONG_FRAME = 0x8A,
};

struct close_frame {
  uint16_t code;
  char *message;
  size_t length;
};

enum ws_head_len {
  SHORT_HEADER = 6,
  MEDIUM_HEADER = 8,
  LONG_HEADER = 14,
  INVALID_HEADER
};

enum client_ws_head_len {
  CLIENT_SHORT_HEADER = 2,
  CLIENT_MEDIUM_HEADER = 4,
  CLIENT_LONG_HEADER = 10,
  CLIENT_INVALID_HEADER
};

enum ws_send_state {
  SND_CONTINUATION = 1,
  SND_NO_FIN = 2,
  SND_COMPRESSED = 64
};

struct frame_header {
  uint8_t opcode : 4;
  uint8_t rsv3 : 1;
  uint8_t rsv2 : 1;
  uint8_t rsv1 : 1;
  uint8_t fin : 1;
  uint8_t len : 7;
  uint8_t mask : 1;
};

#define WEBSOCKET_FRAME_MAXLEN 16384
#define WEBSOCKET_PAYLOAD_SINGLE 125
#define WEBSOCKET_PAYLOAD_EXTEND_1 126
#define WEBSOCKET_PAYLOAD_EXTEND_2 127

inline constexpr const size_t MAX_CLOSE_PAYLOAD = 123;

inline constexpr const std::string_view WEBSOCKET = "websocket"sv;
inline constexpr const std::string_view UPGRADE = "Upgrade"sv;
inline constexpr const char ws_guid[] = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
}  // namespace cinatra