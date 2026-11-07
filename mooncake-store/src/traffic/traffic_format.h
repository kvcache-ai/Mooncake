#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace mooncake::traffic {

inline constexpr std::uint32_t kTrafficFileMagic = 0x3152434d;  // MCR1
inline constexpr std::uint32_t kTrafficBlockMagic = 0x4b42434d;  // MCBK
inline constexpr std::uint32_t kTrafficFooterMagic = 0x4745434d;  // MCEG
inline constexpr std::uint16_t kTrafficVersionMajor = 1;
inline constexpr std::uint16_t kTrafficVersionMinor = 0;
inline constexpr std::uint32_t kTrafficBlockData = 0;
inline constexpr std::uint32_t kTrafficBlockRouteTable = 1;
inline constexpr std::size_t kTrafficFileHeaderSize = 64;
inline constexpr std::size_t kTrafficBlockHeaderSize = 20;
inline constexpr std::size_t kTrafficRecordHeaderSize = 32;
inline constexpr std::size_t kTrafficFooterSize = 48;

#pragma pack(push, 1)

struct TrafficFileHeader {
    std::uint32_t magic = kTrafficFileMagic;
    std::uint16_t version_major = kTrafficVersionMajor;
    std::uint16_t version_minor = kTrafficVersionMinor;
    std::uint32_t header_size = kTrafficFileHeaderSize;
    std::uint8_t session_uuid[16]{};
    std::uint64_t segment_seq = 0;
    std::uint64_t start_ms = 0;
    std::uint32_t route_count = 0;
    std::uint32_t reserved = 0;
    std::uint64_t reserved2 = 0;
    std::uint32_t reserved3 = 0;
};

struct TrafficBlockHeader {
    std::uint32_t magic = kTrafficBlockMagic;
    std::uint32_t block_bytes = 0;
    std::uint32_t record_count = 0;
    std::uint32_t crc32 = 0;
    std::uint32_t block_type = kTrafficBlockData;
};

struct TrafficRecordHeader {
    std::uint32_t total_len = 0;
    std::uint32_t route_key = 0;
    std::uint64_t conn_id = 0;
    std::uint32_t seq_num = 0;
    std::uint16_t flags = 0;
    std::uint16_t reserved = 0;
    std::uint32_t payload_len = 0;
    std::uint32_t timestamp_ms = 0;
};

struct TrafficFooter {
    std::uint32_t magic = kTrafficFooterMagic;
    std::uint32_t block_count = 0;
    std::uint64_t record_count = 0;
    std::uint64_t payload_bytes = 0;
    std::uint64_t start_ms = 0;
    std::uint64_t end_ms = 0;
    std::uint32_t index_crc32 = 0;
    std::uint32_t reserved = 0;
};

#pragma pack(pop)

static_assert(sizeof(TrafficFileHeader) == kTrafficFileHeaderSize);
static_assert(sizeof(TrafficBlockHeader) == kTrafficBlockHeaderSize);
static_assert(sizeof(TrafficRecordHeader) == kTrafficRecordHeaderSize);
static_assert(sizeof(TrafficFooter) == kTrafficFooterSize);

inline std::uint64_t TrafficNowMs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

}  // namespace mooncake::traffic
