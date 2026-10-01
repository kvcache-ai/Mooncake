#pragma once

// Internal binary OpLog batch record codec (P02).
//
// This header is deliberately outside the public codec surface: production
// callers keep using DecodeOpLogBatchRecord() from oplog_batch_codec.h, which
// owns format/version dispatch. The functions below are exported so the
// production dispatch and the test-only fixture encoder can live in separate
// translation units without exposing the wire details to other subsystems.
//
// Wire shape (local proposal, not an upstream-approved standard):
//   offset  width  meaning
//   0       8      magic = 89 4D 43 4F 50 4C 47 0A ("\x89MCOPLG\n")
//   8       1      envelope_version = 1
//   9       1      codec_id = 1 (MessagePack body)
//   10      2      flags = 0, unsigned big-endian
//   12      4      body_length, unsigned big-endian
//   16      N      MessagePack body
//
// The MessagePack body is the same logical six-element batch projection the
// JSON writer emits, but with payloads carried as MessagePack BIN instead of
// base64 text:
//   [schema_version: uint, batch_id: uint, first_seq: uint, last_seq: uint,
//    entries: [[op_type: uint, tenant_id: str, object_key: str,
//               payload: bin], ...],
//    batch_checksum: uint]
//
// The batch_checksum is the same canonical logical checksum the JSON writer
// records, so the two physical formats agree on the logical batch value.

#include <cstdint>
#include <string>
#include <string_view>

namespace mooncake {

// Reserved envelope constants for this prototype. The magic, envelope version,
// and codec id are local placeholders, not project-assigned identifiers.
inline constexpr char kOpLogBatchEnvelopeMagic[8] = {'\x89', 'M', 'C', 'O',
                                                     'P',    'L', 'G', '\n'};
inline constexpr size_t kOpLogBatchEnvelopeMagicSize = 8;
inline constexpr uint8_t kOpLogBatchEnvelopeVersion = 1;
inline constexpr uint8_t kOpLogBatchCodecMessagePack = 1;
inline constexpr size_t kOpLogBatchEnvelopeHeaderSize = 16;

// True when the input starts with the full binary envelope magic. A truncated
// magic prefix is not a match; callers distinguish that case explicitly.
bool HasOpLogBatchBinaryMagic(std::string_view value);

// True when the input is a non-empty proper prefix of the envelope magic.
bool IsTruncatedOpLogBatchBinaryMagic(std::string_view value);

// Decodes a framed binary batch record. The output is assigned only after
// every framing, type, range, shape, and checksum check succeeds; on failure
// the caller's record is left untouched.
struct OpLogBatchRecord;
bool DecodeOpLogBatchRecordBinary(const std::string& value,
                                  OpLogBatchRecord* batch,
                                  std::string* reason = nullptr);

// Test-only fixture encoder. It is exported for the reader tests so a fixed
// binary golden can be produced independently of the decoder, but it is not
// wired into any production writer; the production writer still emits JSON.
std::string EncodeOpLogBatchRecordBinaryForTest(const OpLogBatchRecord& batch);

}  // namespace mooncake
