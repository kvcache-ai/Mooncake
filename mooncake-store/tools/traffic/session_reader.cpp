#include "session_reader.h"

#include <algorithm>
#include <cstring>

#include "crc32c.h"

namespace mooncake::traffic {

const char* RedactionName(std::uint8_t value) {
    switch (value) {
        case 0:
            return "none";
        case 1:
            return "metadata";
        case 2:
            return "redacted";
        default:
            return "unknown";
    }
}

bool SessionReader::Open(const std::string& dir, std::string* error) {
    std::error_code ec;
    auto collect = [&](const std::filesystem::path& root) {
        files_.clear();
        session_dir_ = root;
        for (const auto& entry :
             std::filesystem::directory_iterator(root, ec)) {
            // An entry that cannot be inspected is skipped on its own; only an
            // error from the iteration itself fails the open.
            std::error_code entry_ec;
            if (!entry.is_regular_file(entry_ec)) {
                continue;
            }
            const std::string name = entry.path().filename().string();
            if (name.rfind("segment-", 0) == 0 &&
                entry.path().extension() == ".mcr") {
                files_.push_back(entry.path());
            }
        }
    };
    collect(dir);
    if (ec) {
        *error = "cannot read " + dir + ": " + ec.message();
        return false;
    }
    if (files_.empty()) {
        std::vector<std::filesystem::path> sessions;
        for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
            std::error_code entry_ec;
            if (!entry.is_directory(entry_ec)) {
                continue;
            }
            const std::string name = entry.path().filename().string();
            if (name.rfind("session-", 0) == 0) {
                sessions.push_back(entry.path());
            }
        }
        if (!ec && sessions.size() == 1) {
            collect(sessions.front());
        }
        if (files_.empty()) {
            if (!ec && sessions.size() > 1) {
                std::sort(sessions.begin(), sessions.end());
                *error = "several sessions in " + dir +
                         "; pass one session directory, for example " +
                         sessions.front().string();
                return false;
            }
            *error = "no segment-*.mcr files in " + dir;
            return false;
        }
    }
    std::sort(files_.begin(), files_.end());
    return OpenNextFile(error) == OpenResult::kOpened;
}

SessionReader::NextResult SessionReader::Next(Record* record,
                                              std::string* error) {
    while (true) {
        if (!input_.is_open()) {
            switch (OpenNextFile(error)) {
                case OpenResult::kOpened:
                    break;
                case OpenResult::kDone:
                    return NextResult::kDone;
                case OpenResult::kError:
                    return NextResult::kError;
            }
        }
        if (block_records_left_ > 0) {
            return ReadRecord(record, error);
        }
        if (footer_seen_) {
            input_.close();
            continue;
        }

        std::uint32_t magic = 0;
        input_.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        if (input_.gcount() == 0) {
            // Ending exactly where the next block header would start means the
            // recorder never wrote a footer: the process was killed, or the
            // disk refused the write. Every block already read is intact, so
            // keep them and report the session as cut short.
            input_.close();
            incomplete_ = true;
            continue;
        }
        if (input_.gcount() != static_cast<std::streamsize>(sizeof(magic))) {
            // A short read only happens at the end of the file, so a block
            // header that stops inside its first four bytes is the same kind of
            // cut: the process died while writing.
            input_.close();
            incomplete_ = true;
            return NextResult::kDone;
        }
        if (magic == kTrafficFooterMagic) {
            if (!ReadFooter(magic, error)) {
                return NextResult::kError;
            }
            continue;
        }
        if (magic != kTrafficBlockMagic) {
            *error = "bad block magic in " + current_path_.string();
            return NextResult::kError;
        }
        if (!ReadBlock(magic, error)) {
            return NextResult::kError;
        }
        if (cut_short_) {
            input_.close();
            incomplete_ = true;
            return NextResult::kDone;
        }
    }
}

SessionReader::OpenResult SessionReader::OpenNextFile(std::string* error) {
    if (next_file_ >= files_.size()) {
        return OpenResult::kDone;
    }
    current_path_ = files_[next_file_++];
    input_.open(current_path_, std::ios::binary);
    if (!input_) {
        *error = "cannot open " + current_path_.string();
        return OpenResult::kError;
    }
    TrafficFileHeader header{};
    input_.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (input_.gcount() != static_cast<std::streamsize>(sizeof(header))) {
        *error = "truncated file header: " + current_path_.string();
        return OpenResult::kError;
    }
    if (header.magic != kTrafficFileMagic ||
        header.header_size != sizeof(TrafficFileHeader) ||
        header.version_major != kTrafficVersionMajor ||
        header.version_minor != kTrafficVersionMinor || header.redaction > 2) {
        *error = "unsupported file header: " + current_path_.string();
        return OpenResult::kError;
    }
    if (segment_count_ == 0) {
        std::memcpy(session_id_.data(), header.session_id,
                    session_id_.size());
        redaction_ = header.redaction;
    } else if (redaction_ != header.redaction ||
               !std::equal(session_id_.begin(), session_id_.end(),
                           header.session_id)) {
        *error =
            "segment header does not match session: " + current_path_.string();
        return OpenResult::kError;
    }
    file_block_count_ = 0;
    file_record_count_ = 0;
    file_record_bytes_ = 0;
    footer_seen_ = false;
    block_records_left_ = 0;
    block_offset_ = 0;
    block_.clear();
    ++segment_count_;
    return OpenResult::kOpened;
}

bool SessionReader::Finish(std::string* error) {
    Record record;
    while (true) {
        switch (Next(&record, error)) {
            case NextResult::kRecord:
                continue;
            case NextResult::kDone:
                return true;
            case NextResult::kError:
                return false;
        }
    }
}

bool SessionReader::ReadBlock(std::uint32_t magic, std::string* error) {
    TrafficBlockHeader header{};
    header.magic = magic;
    input_.read(reinterpret_cast<char*>(&header) + sizeof(magic),
                sizeof(header) - sizeof(magic));
    if (input_.gcount() !=
        static_cast<std::streamsize>(sizeof(header) - sizeof(magic))) {
        // A short read only happens at the end of the file: the process died
        // while writing this header.
        cut_short_ = true;
        return true;
    }
    if (header.block_bytes > kMaxBlockBytes) {
        *error = "block too large: " + current_path_.string();
        return false;
    }
    block_.resize(header.block_bytes);
    if (header.block_bytes > 0) {
        input_.read(block_.data(), header.block_bytes);
        if (input_.gcount() !=
            static_cast<std::streamsize>(header.block_bytes)) {
            // A kill or a power loss in the middle of the block leaves a
            // complete header and part of the payload. The blocks before it
            // are still intact, so the read stops at the cut.
            cut_short_ = true;
            return true;
        }
    }
    Crc32c crc;
    crc.Extend(block_.data(), block_.size());
    if (crc.Final() != header.crc32) {
        *error = "block crc mismatch: " + current_path_.string();
        return false;
    }
    block_offset_ = 0;
    block_records_left_ = header.record_count;
    ++block_count_;
    ++file_block_count_;
    if (block_records_left_ == 0 && !block_.empty()) {
        *error = "block has bytes but no records: " + current_path_.string();
        return false;
    }
    return true;
}

bool SessionReader::ReadFooter(std::uint32_t magic, std::string* error) {
    TrafficFooter footer{};
    footer.magic = magic;
    input_.read(reinterpret_cast<char*>(&footer) + sizeof(magic),
                sizeof(footer) - sizeof(magic));
    if (input_.gcount() !=
        static_cast<std::streamsize>(sizeof(footer) - sizeof(magic))) {
        *error = "truncated footer: " + current_path_.string();
        return false;
    }
    if (footer.block_count != file_block_count_ ||
        footer.record_count != file_record_count_ ||
        footer.record_bytes != file_record_bytes_) {
        *error = "footer counters mismatch: " + current_path_.string();
        return false;
    }
    footer_seen_ = true;
    return true;
}

SessionReader::NextResult SessionReader::ReadRecord(Record* record,
                                                    std::string* error) {
    if (block_offset_ + sizeof(TrafficRecordHeader) > block_.size()) {
        *error = "truncated record header: " + current_path_.string();
        return NextResult::kError;
    }
    std::memcpy(&record->header, block_.data() + block_offset_,
                sizeof(TrafficRecordHeader));
    const auto& header = record->header;
    if (header.total_len < sizeof(TrafficRecordHeader) ||
        header.payload_len != header.total_len - sizeof(TrafficRecordHeader) ||
        block_offset_ + header.total_len > block_.size()) {
        *error = "bad record length: " + current_path_.string();
        return NextResult::kError;
    }
    record->payload.assign(
        block_.data() + block_offset_ + sizeof(TrafficRecordHeader),
        block_.data() + block_offset_ + header.total_len);
    block_offset_ += header.total_len;
    --block_records_left_;
    if (block_records_left_ == 0 && block_offset_ != block_.size()) {
        *error = "mismatched block record count: " + current_path_.string();
        return NextResult::kError;
    }
    ++records_;
    payload_bytes_ += header.payload_len;
    ++file_record_count_;
    file_record_bytes_ += header.total_len;
    ++routes_[header.route_key];
    if (records_ == 1) {
        first_timestamp_ms_ = header.timestamp_ms;
    }
    last_timestamp_ms_ = header.timestamp_ms;
    return NextResult::kRecord;
}

}  // namespace mooncake::traffic
