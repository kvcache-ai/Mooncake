#include "traffic/file_sink.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <system_error>

#include <glog/logging.h>

#include "crc32c.h"
#include "traffic/traffic_format.h"

namespace mooncake::traffic {
namespace {

std::string MakeSessionDir(const std::string& output_dir,
                           std::uint64_t start_ms) {
    std::error_code ec;
    std::filesystem::create_directories(output_dir, ec);
    if (ec) {
        LOG(ERROR) << "traffic: create output directory failed: "
                   << ec.message();
        return {};
    }
    if (::chmod(output_dir.c_str(), 0700) != 0) {
        LOG(ERROR) << "traffic: chmod output directory failed: " << output_dir;
        return {};
    }
    const std::string base = output_dir + "/session-" +
                             std::to_string(start_ms) + "-" +
                             std::to_string(::getpid());
    std::string candidate = base;
    for (int suffix = 1; std::filesystem::exists(candidate); ++suffix) {
        candidate = base + "-" + std::to_string(suffix);
    }
    std::filesystem::create_directory(candidate, ec);
    if (ec) {
        LOG(ERROR) << "traffic: create session directory failed: "
                   << ec.message();
        return {};
    }
    if (::chmod(candidate.c_str(), 0700) != 0) {
        LOG(ERROR) << "traffic: chmod session directory failed: " << candidate;
        return {};
    }
    return candidate;
}

}  // namespace

FileSink::FileSink(std::uint64_t segment_bytes,
                   std::uint64_t segment_seconds,
                   std::uint64_t max_total_bytes)
    : segment_bytes_(segment_bytes),
      segment_seconds_(segment_seconds),
      max_total_bytes_(max_total_bytes) {}

FileSink::~FileSink() { Close(); }

bool FileSink::OpenSession(const std::string& output_dir,
                           std::uint64_t start_ms) {
    Close();
    failed_ = false;
    segment_seq_ = 0;
    block_count_ = 0;
    record_count_ = 0;
    payload_bytes_ = 0;
    bytes_written_ = 0;
    segments_ = 0;
    segment_start_ms_ = start_ms;
    session_dir_ = MakeSessionDir(output_dir, start_ms);
    if (session_dir_.empty()) {
        failed_ = true;
        return false;
    }
    return OpenNextSegment();
}

bool FileSink::OpenNextSegment() {
    if (max_total_bytes_ > 0 &&
        bytes_written_ + kTrafficFileHeaderSize + kTrafficFooterSize >
            max_total_bytes_) {
        LOG(ERROR) << "traffic: max_total_bytes exceeded before segment";
        failed_ = true;
        return false;
    }
    if (segment_seq_ != 0) {
        segment_start_ms_ = TrafficNowMs();
    }
    char name[64];
    std::snprintf(name, sizeof(name), "segment-%06llu.mcr",
                  static_cast<unsigned long long>(segment_seq_));
    segment_path_ = session_dir_ + "/" + name;
    const int fd = ::open(segment_path_.c_str(),
                          O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) {
        LOG(ERROR) << "traffic: open segment failed: " << segment_path_;
        failed_ = true;
        return false;
    }
    file_ = ::fdopen(fd, "wb");
    if (file_ == nullptr) {
        ::close(fd);
        LOG(ERROR) << "traffic: fdopen segment failed: " << segment_path_;
        failed_ = true;
        return false;
    }

    TrafficFileHeader header;
    header.segment_seq = segment_seq_;
    header.start_ms = segment_start_ms_;
    const std::uint64_t seed = segment_start_ms_ ^ (segment_seq_ + 1);
    for (std::size_t i = 0; i < sizeof(header.session_uuid); ++i) {
        header.session_uuid[i] = static_cast<std::uint8_t>(seed >> ((i % 8) * 8));
    }
    if (std::fwrite(&header, sizeof(header), 1, file_) != 1) {
        LOG(ERROR) << "traffic: write segment header failed: "
                   << segment_path_;
        failed_ = true;
        std::fclose(file_);
        file_ = nullptr;
        return false;
    }
    segment_bytes_written_ = sizeof(header);
    block_count_ = 0;
    record_count_ = 0;
    payload_bytes_ = 0;
    bytes_written_ += sizeof(header);
    ++segments_;
    return true;
}

bool FileSink::WriteBlock(const void* data, std::uint32_t bytes,
                          std::uint32_t records,
                          std::uint32_t block_type) {
    std::lock_guard<std::mutex> lock(mu_);
    if (file_ == nullptr || failed_) {
        return false;
    }
    const std::uint64_t block_total =
        kTrafficBlockHeaderSize + static_cast<std::uint64_t>(bytes);
    if (max_total_bytes_ > 0 &&
        bytes_written_ + block_total + kTrafficFooterSize > max_total_bytes_) {
        LOG(ERROR) << "traffic: max_total_bytes exceeded";
        failed_ = true;
        return false;
    }
    const bool rotate_by_bytes =
        segment_bytes_ > 0 && block_count_ > 0 &&
        segment_bytes_written_ + block_total + kTrafficFooterSize >
            segment_bytes_;
    const bool rotate_by_time =
        segment_seconds_ > 0 && block_count_ > 0 &&
        TrafficNowMs() - segment_start_ms_ >= segment_seconds_ * 1000;
    if (rotate_by_bytes || rotate_by_time) {
        if (!CloseSegment(false) || !OpenNextSegment()) {
            return false;
        }
    }

    TrafficBlockHeader header;
    header.block_bytes = bytes;
    header.record_count = records;
    header.block_type = block_type;
    header.crc32 = mooncake::Crc32cValue(data, bytes);
    if (std::fwrite(&header, sizeof(header), 1, file_) != 1 ||
        (bytes > 0 && std::fwrite(data, bytes, 1, file_) != 1)) {
        LOG(ERROR) << "traffic: write block failed: " << segment_path_;
        failed_ = true;
        return false;
    }
    segment_bytes_written_ += block_total;
    bytes_written_ += block_total;
    ++block_count_;
    if (block_type == kTrafficBlockData) {
        record_count_ += records;
        payload_bytes_ += bytes;
    }
    return true;
}

bool FileSink::WriteRecords(const struct iovec* iov, int iov_count,
                            std::uint64_t bytes, std::uint32_t records) {
    std::lock_guard<std::mutex> lock(mu_);
    if (file_ == nullptr || failed_ || bytes > 0xffffffffu) {
        return false;
    }
    if (max_total_bytes_ > 0 &&
        bytes_written_ + kTrafficBlockHeaderSize + bytes + kTrafficFooterSize >
            max_total_bytes_) {
        LOG(ERROR) << "traffic: max_total_bytes exceeded";
        failed_ = true;
        return false;
    }
    const std::uint64_t block_total =
        kTrafficBlockHeaderSize + static_cast<std::uint64_t>(bytes);
    const bool rotate_by_bytes =
        segment_bytes_ > 0 && block_count_ > 0 &&
        segment_bytes_written_ + block_total + kTrafficFooterSize >
            segment_bytes_;
    const bool rotate_by_time =
        segment_seconds_ > 0 && block_count_ > 0 &&
        TrafficNowMs() - segment_start_ms_ >= segment_seconds_ * 1000;
    if (rotate_by_bytes || rotate_by_time) {
        if (!CloseSegment(false) || !OpenNextSegment()) {
            return false;
        }
    }

    mooncake::Crc32c crc;
    for (int i = 0; i < iov_count; ++i) {
        crc.Extend(iov[i].iov_base,
                   static_cast<std::size_t>(iov[i].iov_len));
    }
    TrafficBlockHeader header;
    header.block_bytes = static_cast<std::uint32_t>(bytes);
    header.record_count = records;
    header.block_type = kTrafficBlockData;
    header.crc32 = crc.Final();
    if (std::fwrite(&header, sizeof(header), 1, file_) != 1 ||
        std::fflush(file_) != 0) {
        LOG(ERROR) << "traffic: write block header failed: " << segment_path_;
        failed_ = true;
        return false;
    }
    int remaining = iov_count;
    struct iovec* vec = const_cast<struct iovec*>(iov);
    while (remaining > 0) {
        const ssize_t written = ::writev(::fileno(file_), vec, remaining);
        if (written <= 0) {
            LOG(ERROR) << "traffic: write records failed: " << segment_path_;
            failed_ = true;
            return false;
        }
        std::size_t left = static_cast<std::size_t>(written);
        while (remaining > 0 && left >= vec->iov_len) {
            left -= vec->iov_len;
            ++vec;
            --remaining;
        }
        if (remaining > 0 && left > 0) {
            vec->iov_base = static_cast<char*>(vec->iov_base) + left;
            vec->iov_len -= left;
        }
    }
    segment_bytes_written_ += block_total;
    bytes_written_ += block_total;
    ++block_count_;
    record_count_ += records;
    payload_bytes_ += bytes;
    return true;
}

bool FileSink::Flush() {
    if (file_ == nullptr) {
        return false;
    }
    if (std::fflush(file_) != 0) {
        failed_ = true;
        return false;
    }
    return true;
}

bool FileSink::CloseSegment(bool sync) {
    if (file_ == nullptr) {
        return true;
    }
    TrafficFooter footer;
    footer.block_count = static_cast<std::uint32_t>(block_count_);
    footer.record_count = record_count_;
    footer.payload_bytes = payload_bytes_;
    footer.start_ms = segment_start_ms_;
    footer.end_ms = TrafficNowMs();
    if (std::fwrite(&footer, sizeof(footer), 1, file_) != 1) {
        failed_ = true;
    }
    if (std::fflush(file_) != 0) {
        failed_ = true;
    }
    if (sync && ::fdatasync(::fileno(file_)) != 0) {
        failed_ = true;
    }
    if (std::fclose(file_) != 0) {
        failed_ = true;
    }
    file_ = nullptr;
    bytes_written_ += sizeof(footer);
    ++segment_seq_;
    return !failed_;
}

void FileSink::Close() {
    if (file_ != nullptr) {
        CloseSegment(true);
    }
}

}  // namespace mooncake::traffic
