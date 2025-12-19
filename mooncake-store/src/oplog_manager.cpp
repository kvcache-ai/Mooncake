#include "oplog_manager.h"

#include <algorithm>
#include <chrono>
#include <functional>

namespace mooncake {

OpLogManager::OpLogManager() = default;

uint64_t OpLogManager::Append(OpType type, const std::string& key,
                              const std::string& payload) {
    OpLogEntry entry;
    entry.op_type = type;
    entry.object_key = key;
    entry.payload = payload;
    entry.timestamp_ms = NowMs();
    entry.checksum = ComputeChecksum(entry.payload);
    entry.prefix_hash = ComputePrefixHash(entry.object_key);

    std::unique_lock<std::shared_mutex> lock(mutex_);
    entry.sequence_id = ++last_seq_id_;

    if (buffer_.size() >= kMaxBufferEntries_) {
        buffer_.pop_front();
        ++first_seq_id_;
    }

    buffer_.emplace_back(std::move(entry));
    return last_seq_id_;
}

std::vector<OpLogEntry> OpLogManager::GetEntriesSince(uint64_t since_seq_id,
                                                      size_t limit) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<OpLogEntry> result;
    if (buffer_.empty() || since_seq_id >= last_seq_id_) {
        return result;
    }

    result.reserve(std::min(limit, buffer_.size()));
    for (const auto& e : buffer_) {
        if (e.sequence_id > since_seq_id) {
            result.push_back(e);
            if (result.size() >= limit) {
                break;
            }
        }
    }
    return result;
}

uint64_t OpLogManager::GetLastSequenceId() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return last_seq_id_;
}

void OpLogManager::TruncateBefore(uint64_t min_seq_to_keep) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    while (!buffer_.empty() && buffer_.front().sequence_id < min_seq_to_keep) {
        buffer_.pop_front();
        ++first_seq_id_;
    }
}

size_t OpLogManager::GetEntryCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return buffer_.size();
}

uint64_t OpLogManager::NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch())
        .count();
}

uint32_t OpLogManager::ComputeChecksum(const std::string& data) {
    // NOTE: For now we use a simple hash as a placeholder. This can be
    // replaced with a real CRC32 implementation later if needed.
    return static_cast<uint32_t>(std::hash<std::string>{}(data));
}

uint32_t OpLogManager::ComputePrefixHash(const std::string& key) {
    if (key.empty()) {
        return 0;
    }
    // Use at most first 8 characters to compute a simple hash.
    const size_t prefix_len = std::min<size_t>(8, key.size());
    return static_cast<uint32_t>(
        std::hash<std::string_view>{}(std::string_view(key.data(), prefix_len)));
}

}  // namespace mooncake


