#pragma once

#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace mooncake::traffic {

inline constexpr std::uint32_t kTrafficFileMagic = 0x3152434d;    // MCR1
inline constexpr std::uint32_t kTrafficBlockMagic = 0x4b42434d;   // MCBK
inline constexpr std::uint32_t kTrafficFooterMagic = 0x4745434d;  // MCEG
inline constexpr std::uint16_t kTrafficVersionMajor = 1;
inline constexpr std::uint16_t kTrafficVersionMinor = 0;
inline constexpr std::size_t kTrafficFileHeaderSize = 64;
inline constexpr std::size_t kTrafficBlockHeaderSize = 16;
inline constexpr std::size_t kTrafficRecordHeaderSize = 32;
inline constexpr std::size_t kTrafficFooterSize = 40;

// A block is written and read whole, so both ends share one bound: the writer
// refuses a larger block_bytes, and the reader stops before allocating more
// than this for a corrupt length.
inline constexpr std::uint32_t kMaxBlockBytes = 64U * 1024U * 1024U;

enum class RedactionMode : std::uint8_t {
    kNone = 0,
    kMetadata = 1,
    kRedacted = 2,
};

#pragma pack(push, 1)

struct TrafficFileHeader {
    std::uint32_t magic = kTrafficFileMagic;
    std::uint16_t version_major = kTrafficVersionMajor;
    std::uint16_t version_minor = kTrafficVersionMinor;
    std::uint32_t header_size = kTrafficFileHeaderSize;
    // Start time, pid, and a per-process sequence, in that order. Every field
    // in this file is little-endian.
    std::uint8_t session_id[16]{};
    std::uint64_t segment_seq = 0;
    std::uint64_t start_ms = 0;
    std::uint8_t redaction = 0;
    std::uint8_t reserved[19]{};
};

struct TrafficBlockHeader {
    std::uint32_t magic = kTrafficBlockMagic;
    std::uint32_t block_bytes = 0;
    std::uint32_t record_count = 0;
    std::uint32_t crc32 = 0;
};

struct TrafficRecordHeader {
    std::uint32_t total_len = 0;
    std::uint32_t route_key = 0;
    std::uint64_t conn_id = 0;
    std::uint32_t seq_num = 0;
    std::uint32_t payload_len = 0;
    std::uint64_t timestamp_ms = 0;
};

struct TrafficFooter {
    std::uint32_t magic = kTrafficFooterMagic;
    std::uint32_t block_count = 0;
    std::uint64_t record_count = 0;
    std::uint64_t record_bytes = 0;
    std::uint64_t start_ms = 0;
    std::uint64_t end_ms = 0;
};

#pragma pack(pop)

static_assert(sizeof(TrafficFileHeader) == kTrafficFileHeaderSize);
static_assert(sizeof(TrafficBlockHeader) == kTrafficBlockHeaderSize);
static_assert(sizeof(TrafficRecordHeader) == kTrafficRecordHeaderSize);
static_assert(sizeof(TrafficFooter) == kTrafficFooterSize);

// Session files store these structs as they sit in memory, so the on-disk
// format is little-endian by construction. Every platform Mooncake builds on
// is little-endian; a big-endian port has to encode the fields field by field
// here and decode them in the reader.
static_assert(std::endian::native == std::endian::little,
              "the traffic session format is little-endian");

inline std::uint64_t TrafficNowMs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

}  // namespace mooncake::traffic
