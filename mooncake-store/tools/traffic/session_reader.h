#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "traffic_format.h"

namespace mooncake::traffic {

// One recorded request read from a session segment.
struct Record {
    TrafficRecordHeader header{};
    std::vector<char> payload;
};

const char* RedactionName(std::uint8_t value);

// Reads the segment files a traffic session produced. The reader validates the
// file header, every block checksum, the record lengths and the footer counters
// so a corrupted session is reported instead of replayed. A process that dies
// before it can write a footer leaves the last segment without one; the blocks
// it did write are intact, so they are read and incomplete() reports the cut.
class SessionReader {
   public:
    enum class NextResult { kRecord, kDone, kError };

    // Opens a session directory, or a directory that holds exactly one
    // session-* subdirectory.
    bool Open(const std::string& dir, std::string* error);
    NextResult Next(Record* record, std::string* error);
    // Consumes the remaining records so the footer is validated. Returns false
    // when the session is corrupt, with the reason in *error.
    bool Finish(std::string* error);

    std::uint64_t records() const { return records_; }
    std::uint64_t payload_bytes() const { return payload_bytes_; }
    std::uint64_t block_count() const { return block_count_; }
    std::size_t segment_count() const { return segment_count_; }
    std::uint8_t redaction() const { return redaction_; }
    // True when a segment ended without a footer, which happens when the
    // recorder did not get to close the session.
    bool incomplete() const { return incomplete_; }
    // Time from the first record to the last, in milliseconds. Zero when the
    // session holds fewer than two records.
    std::int64_t duration_ms() const {
        return last_timestamp_ms_ - first_timestamp_ms_;
    }
    const std::filesystem::path& session_dir() const { return session_dir_; }
    const std::map<std::uint32_t, std::uint64_t>& routes() const {
        return routes_;
    }

   private:
    enum class OpenResult { kOpened, kDone, kError };
    OpenResult OpenNextFile(std::string* error);
    bool ReadBlock(std::uint32_t magic, std::string* error);
    bool ReadFooter(std::uint32_t magic, std::string* error);
    NextResult ReadRecord(Record* record, std::string* error);

    std::vector<std::filesystem::path> files_;
    std::size_t next_file_ = 0;
    std::filesystem::path current_path_;
    std::ifstream input_;
    std::array<std::uint8_t, 16> session_id_{};
    std::uint8_t redaction_ = 0;
    bool footer_seen_ = false;
    bool incomplete_ = false;
    std::vector<char> block_;
    std::size_t block_offset_ = 0;
    std::uint32_t block_records_left_ = 0;
    std::uint64_t file_block_count_ = 0;
    std::uint64_t file_record_count_ = 0;
    // Size of the record headers and payloads in the current segment.
    std::uint64_t file_record_bytes_ = 0;
    std::uint64_t records_ = 0;
    std::uint64_t payload_bytes_ = 0;
    std::uint64_t block_count_ = 0;
    std::size_t segment_count_ = 0;
    std::int64_t first_timestamp_ms_ = 0;
    std::int64_t last_timestamp_ms_ = 0;
    std::map<std::uint32_t, std::uint64_t> routes_;
    std::filesystem::path session_dir_;
};

}  // namespace mooncake::traffic
