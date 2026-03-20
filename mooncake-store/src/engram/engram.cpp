#include "engram/engram.h"

#include <array>
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <future>
#include <limits>
#include <numeric>
#include <random>
#include <sstream>
#include <set>
#include <string_view>
#include <type_traits>
#include <vector>

#include "engram/engram_config.h"

namespace mooncake {
namespace engram {

namespace {

constexpr char kRemoteGatherSubmitName[] = "engram_remote_gather_submit_v2";
constexpr char kRemoteGatherAckName[] = "engram_remote_gather_ack_v2";
constexpr auto kRemoteGatherPollInterval = std::chrono::microseconds(50);
constexpr auto kRemoteGatherAckTimeout = std::chrono::milliseconds(1000);
constexpr uint32_t kRemoteGatherWireMagic = 0x4d435347U;  // MCSG
constexpr uint16_t kRemoteGatherWireVersion = 2;
constexpr size_t kDefaultRequestRingSlotSize = 256 * 1024;
constexpr size_t kDefaultRequestRingSlotCount = 64;
constexpr size_t kDefaultRequestRingMaxSlotSize = 1024 * 1024;
constexpr size_t kRemoteGatherFixedHeaderBytes =
    sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint16_t) +
    sizeof(uint16_t);
constexpr size_t kRemoteGatherHeadHeaderMinBytes = 5;
constexpr size_t kRemoteGatherRangeMinBytes = 2;

enum class RemoteGatherWireKind : uint16_t {
    kRequest = 1,
    kDoorbell = 2,
    kAck = 3,
};

bool load_bool_env(const char* name, bool default_value = false) {
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return default_value;
    }
    std::string_view view(value);
    if (view == "0" || view == "false" || view == "False" || view == "FALSE" ||
        view == "off" || view == "OFF" || view == "no" || view == "NO") {
        return false;
    }
    return true;
}

size_t load_size_env(const char* name, size_t default_value) {
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return default_value;
    }
    char* end = nullptr;
    errno = 0;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0) {
        LOG(WARNING) << "Invalid " << name << "=" << value
                     << ", using default " << default_value;
        return default_value;
    }
    return static_cast<size_t>(parsed);
}

size_t round_up_to_power_of_two(size_t value) {
    if (value <= 1) {
        return 1;
    }

    size_t rounded = 1;
    while (rounded < value) {
        if (rounded > std::numeric_limits<size_t>::max() / 2) {
            return value;
        }
        rounded <<= 1;
    }
    return rounded;
}

template <typename T>
void append_wire_value(std::string& out, const T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    size_t old_size = out.size();
    out.resize(old_size + sizeof(T));
    std::memcpy(out.data() + old_size, &value, sizeof(T));
}

template <typename T>
bool read_wire_value(std::string_view input, size_t& offset, T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    if (offset + sizeof(T) > input.size()) {
        return false;
    }
    std::memcpy(&value, input.data() + offset, sizeof(T));
    offset += sizeof(T);
    return true;
}

bool append_wire_bytes(std::string& out, std::string_view value) {
    const uint32_t len = static_cast<uint32_t>(value.size());
    append_wire_value(out, len);
    out.append(value.data(), value.size());
    return true;
}

bool read_wire_bytes(std::string_view input, size_t& offset, std::string& out) {
    uint32_t len = 0;
    if (!read_wire_value(input, offset, len) || offset + len > input.size()) {
        return false;
    }
    out.assign(input.data() + offset, len);
    offset += len;
    return true;
}

size_t varuint_size(uint64_t value) {
    size_t bytes = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++bytes;
    }
    return bytes;
}

bool read_wire_varuint(std::string_view input, size_t& offset, uint64_t& value) {
    value = 0;
    unsigned shift = 0;
    while (offset < input.size() && shift <= 63) {
        const uint8_t byte =
            static_cast<uint8_t>(input[static_cast<size_t>(offset++)]);
        value |= static_cast<uint64_t>(byte & 0x7fU) << shift;
        if ((byte & 0x80U) == 0) {
            return true;
        }
        shift += 7;
    }
    return false;
}

template <typename T>
bool append_wire_buffer_value(char* out, size_t capacity, size_t& offset,
                              const T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    if (offset + sizeof(T) > capacity) {
        return false;
    }
    std::memcpy(out + offset, &value, sizeof(T));
    offset += sizeof(T);
    return true;
}

bool append_wire_buffer_varuint(char* out, size_t capacity, size_t& offset,
                                uint64_t value) {
    while (value >= 0x80) {
        if (offset >= capacity) {
            return false;
        }
        out[offset++] = static_cast<char>((value & 0x7fU) | 0x80U);
        value >>= 7;
    }
    if (offset >= capacity) {
        return false;
    }
    out[offset++] = static_cast<char>(value);
    return true;
}

struct RemoteGatherRange {
    int64_t start_row = 0;
    int64_t rows = 0;
    uint64_t dest_offset = 0;
};

struct RemoteGatherHeadRequest {
    int32_t head_index = 0;
    AllocatedBuffer::Descriptor src_buffer;
    AllocatedBuffer::Descriptor dst_buffer;
    std::vector<RemoteGatherRange> ranges;
};

struct RemoteGatherRequest {
    uint64_t request_id = 0;
    uint32_t row_bytes = 0;
    std::vector<RemoteGatherHeadRequest> heads;
};

struct RemoteGatherAck {
    uint64_t request_id = 0;
    int32_t status = 0;
    std::string error;
};

struct RemoteGatherDoorbell {
    uint64_t request_id = 0;
    uint64_t descriptor_bytes = 0;
    uint64_t slot_buffer_size = 0;
    uint64_t slot_buffer_address = 0;
    std::string requester_agent;
};

struct RemoteGatherClientState {
    struct PendingAck {
        RemoteGatherAck ack;
    };

    std::mutex mu;
    std::condition_variable cv;
    std::unordered_map<uint64_t, PendingAck> pending_acks;
    std::atomic<uint64_t> expected_ack_count{0};
    bool poll_in_progress = false;
    size_t service_threads = 0;
};

std::mutex g_remote_gather_client_states_mu;
std::unordered_map<const Client*, std::weak_ptr<RemoteGatherClientState>>
    g_remote_gather_client_states;

std::shared_ptr<RemoteGatherClientState> get_remote_gather_client_state(
    const std::shared_ptr<Client>& client) {
    if (!client) {
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(g_remote_gather_client_states_mu);
    auto& weak_state = g_remote_gather_client_states[client.get()];
    auto state = weak_state.lock();
    if (!state) {
        state = std::make_shared<RemoteGatherClientState>();
        weak_state = state;
    }
    return state;
}

bool try_begin_remote_gather_poll(
    const std::shared_ptr<RemoteGatherClientState>& state) {
    std::lock_guard<std::mutex> lock(state->mu);
    if (state->poll_in_progress) {
        return false;
    }
    state->poll_in_progress = true;
    return true;
}

void finish_remote_gather_poll(
    const std::shared_ptr<RemoteGatherClientState>& state) {
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->poll_in_progress = false;
    }
    state->cv.notify_all();
}

void register_remote_gather_service_thread(
    const std::shared_ptr<RemoteGatherClientState>& state) {
    {
        std::lock_guard<std::mutex> lock(state->mu);
        ++state->service_threads;
    }
    state->cv.notify_all();
}

void unregister_remote_gather_service_thread(
    const std::shared_ptr<RemoteGatherClientState>& state) {
    {
        std::lock_guard<std::mutex> lock(state->mu);
        if (state->service_threads > 0) {
            --state->service_threads;
        }
    }
    state->cv.notify_all();
}

void enqueue_remote_gather_ack(
    const std::shared_ptr<RemoteGatherClientState>& state,
    RemoteGatherAck ack) {
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->pending_acks[ack.request_id] = {std::move(ack)};
    }
    state->cv.notify_all();
}

// Generic stable radix sort for sparse row-planning workloads keyed by
// non-negative integer IDs. It avoids comparator-heavy O(n log n) sorting on
// the hot path and is reusable for other batched gather/scatter planners.
template <typename T, typename KeyFn>
void radix_sort_by_u64_key(std::vector<T>& values, std::vector<T>& scratch,
                           KeyFn key_fn) {
    if (values.size() < 2) {
        return;
    }

    uint64_t max_key = 0;
    for (const auto& value : values) {
        max_key = std::max(max_key, key_fn(value));
    }
    if (max_key == 0) {
        return;
    }

    constexpr unsigned kBitsPerPass = 11;
    constexpr size_t kBucketCount = 1u << kBitsPerPass;
    constexpr uint64_t kBucketMask = kBucketCount - 1;
    std::array<size_t, kBucketCount> bucket_offsets;

    scratch.resize(values.size());
    auto* src = &values;
    auto* dst = &scratch;
    for (unsigned shift = 0; (max_key >> shift) != 0; shift += kBitsPerPass) {
        bucket_offsets.fill(0);
        for (const auto& value : *src) {
            const size_t bucket =
                static_cast<size_t>((key_fn(value) >> shift) & kBucketMask);
            ++bucket_offsets[bucket];
        }

        size_t next_offset = 0;
        for (size_t& bucket_offset : bucket_offsets) {
            const size_t count = bucket_offset;
            bucket_offset = next_offset;
            next_offset += count;
        }

        for (const auto& value : *src) {
            const size_t bucket =
                static_cast<size_t>((key_fn(value) >> shift) & kBucketMask);
            (*dst)[bucket_offsets[bucket]++] = value;
        }
        std::swap(src, dst);
    }

    if (src != &values) {
        values.swap(scratch);
    }
}

size_t estimate_remote_gather_head_request_bytes(
    const RemoteGatherHeadRequest& head) {
    size_t encoded_bytes = varuint_size(static_cast<uint64_t>(head.head_index)) +
                           varuint_size(head.ranges.size()) +
                           varuint_size(head.src_buffer.size_) +
                           varuint_size(head.src_buffer.buffer_address_) +
                           varuint_size(head.dst_buffer.buffer_address_);
    int64_t prev_end = 0;
    for (const auto& range : head.ranges) {
        if (range.start_row < prev_end || range.rows <= 0) {
            return 0;
        }
        const uint64_t gap = static_cast<uint64_t>(range.start_row - prev_end);
        encoded_bytes +=
            varuint_size(gap) + varuint_size(static_cast<uint64_t>(range.rows));
        prev_end = range.start_row + range.rows;
    }
    return encoded_bytes;
}

size_t estimate_remote_gather_request_bytes(const RemoteGatherRequest& request) {
    size_t encoded_bytes = kRemoteGatherFixedHeaderBytes +
                           sizeof(request.request_id) +
                           varuint_size(request.row_bytes) +
                           varuint_size(request.heads.size());
    for (const auto& head : request.heads) {
        const size_t head_bytes = estimate_remote_gather_head_request_bytes(head);
        if (head_bytes == 0) {
            return 0;
        }
        encoded_bytes += head_bytes;
    }
    return encoded_bytes;
}

bool encode_remote_gather_request_to_buffer(const RemoteGatherRequest& request,
                                            char* out, size_t capacity,
                                            size_t& encoded_bytes) {
    encoded_bytes = 0;
    if (!append_wire_buffer_value(out, capacity, encoded_bytes,
                                  kRemoteGatherWireMagic) ||
        !append_wire_buffer_value(out, capacity, encoded_bytes,
                                  kRemoteGatherWireVersion) ||
        !append_wire_buffer_value(
            out, capacity, encoded_bytes,
            static_cast<uint16_t>(RemoteGatherWireKind::kRequest)) ||
        !append_wire_buffer_value(out, capacity, encoded_bytes,
                                  static_cast<uint16_t>(0)) ||
        !append_wire_buffer_value(out, capacity, encoded_bytes,
                                  request.request_id) ||
        !append_wire_buffer_varuint(out, capacity, encoded_bytes,
                                    request.row_bytes) ||
        !append_wire_buffer_varuint(out, capacity, encoded_bytes,
                                    request.heads.size())) {
        return false;
    }

    for (const auto& head : request.heads) {
        if (!append_wire_buffer_varuint(
                out, capacity, encoded_bytes,
                static_cast<uint64_t>(head.head_index)) ||
            !append_wire_buffer_varuint(out, capacity, encoded_bytes,
                                        head.ranges.size()) ||
            !append_wire_buffer_varuint(out, capacity, encoded_bytes,
                                        head.src_buffer.size_) ||
            !append_wire_buffer_varuint(out, capacity, encoded_bytes,
                                        head.src_buffer.buffer_address_) ||
            !append_wire_buffer_varuint(out, capacity, encoded_bytes,
                                        head.dst_buffer.buffer_address_)) {
            return false;
        }

        int64_t prev_end = 0;
        for (const auto& range : head.ranges) {
            if (range.start_row < prev_end || range.rows <= 0) {
                return false;
            }
            const uint64_t gap = static_cast<uint64_t>(range.start_row - prev_end);
            if (!append_wire_buffer_varuint(out, capacity, encoded_bytes, gap) ||
                !append_wire_buffer_varuint(
                    out, capacity, encoded_bytes,
                    static_cast<uint64_t>(range.rows))) {
                return false;
            }
            prev_end = range.start_row + range.rows;
        }
    }
    return true;
}

bool decode_remote_gather_request_v1(std::string_view input, size_t offset,
                                     uint64_t request_id,
                                     RemoteGatherRequest& request) {
    request = RemoteGatherRequest{};
    request.request_id = request_id;

    uint32_t head_count = 0;
    uint32_t total_ranges = 0;
    if (!read_wire_value(input, offset, head_count) ||
        !read_wire_value(input, offset, total_ranges)) {
        return false;
    }
    request.heads.reserve(head_count);

    uint32_t decoded_ranges = 0;
    uint64_t request_row_bytes = 0;
    for (uint32_t i = 0; i < head_count; ++i) {
        RemoteGatherHeadRequest head;
        uint32_t range_count = 0;
        if (!read_wire_value(input, offset, head.head_index) ||
            !read_wire_value(input, offset, range_count) ||
            !read_wire_value(input, offset, head.src_buffer.size_) ||
            !read_wire_value(input, offset, head.src_buffer.buffer_address_) ||
            !read_wire_value(input, offset, head.dst_buffer.size_) ||
            !read_wire_value(input, offset, head.dst_buffer.buffer_address_)) {
            return false;
        }
        head.ranges.reserve(range_count);
        for (uint32_t j = 0; j < range_count; ++j) {
            RemoteGatherRange range;
            if (!read_wire_value(input, offset, range.start_row) ||
                !read_wire_value(input, offset, range.rows) ||
                !read_wire_value(input, offset, range.dest_offset)) {
                return false;
            }
            head.ranges.push_back(range);
        }
        uint64_t head_rows = 0;
        for (const auto& range : head.ranges) {
            if (range.rows <= 0) {
                return false;
            }
            head_rows += static_cast<uint64_t>(range.rows);
        }
        if (head_rows == 0 || head.dst_buffer.size_ % head_rows != 0) {
            return false;
        }
        const uint64_t head_row_bytes = head.dst_buffer.size_ / head_rows;
        if (head_row_bytes == 0 ||
            head_row_bytes > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        if (request_row_bytes == 0) {
            request_row_bytes = head_row_bytes;
        } else if (request_row_bytes != head_row_bytes) {
            return false;
        }
        decoded_ranges += range_count;
        request.heads.push_back(std::move(head));
    }

    if (decoded_ranges != total_ranges || offset != input.size() ||
        request_row_bytes == 0) {
        return false;
    }
    request.row_bytes = static_cast<uint32_t>(request_row_bytes);
    return true;
}

bool decode_remote_gather_request_v2(std::string_view input, size_t offset,
                                     uint64_t request_id,
                                     RemoteGatherRequest& request) {
    request = RemoteGatherRequest{};
    request.request_id = request_id;

    uint64_t row_bytes = 0;
    uint64_t head_count = 0;
    if (!read_wire_varuint(input, offset, row_bytes) ||
        !read_wire_varuint(input, offset, head_count) ||
        row_bytes == 0 ||
        row_bytes > std::numeric_limits<uint32_t>::max() ||
        head_count > std::numeric_limits<size_t>::max()) {
        return false;
    }
    request.row_bytes = static_cast<uint32_t>(row_bytes);
    request.heads.reserve(static_cast<size_t>(head_count));

    for (uint64_t i = 0; i < head_count; ++i) {
        RemoteGatherHeadRequest head;
        uint64_t head_index = 0;
        uint64_t range_count = 0;
        uint64_t src_buffer_size = 0;
        uint64_t src_buffer_address = 0;
        uint64_t dst_buffer_address = 0;
        if (!read_wire_varuint(input, offset, head_index) ||
            !read_wire_varuint(input, offset, range_count) ||
            !read_wire_varuint(input, offset, src_buffer_size) ||
            !read_wire_varuint(input, offset, src_buffer_address) ||
            !read_wire_varuint(input, offset, dst_buffer_address) ||
            head_index > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
            return false;
        }

        head.head_index = static_cast<int32_t>(head_index);
        head.src_buffer.size_ = static_cast<size_t>(src_buffer_size);
        head.src_buffer.buffer_address_ =
            static_cast<uintptr_t>(src_buffer_address);
        head.dst_buffer.buffer_address_ =
            static_cast<uintptr_t>(dst_buffer_address);
        head.ranges.reserve(static_cast<size_t>(range_count));

        uint64_t total_rows = 0;
        int64_t next_start_row = 0;
        size_t next_dest_offset = 0;
        for (uint64_t j = 0; j < range_count; ++j) {
            uint64_t gap = 0;
            uint64_t rows = 0;
            if (!read_wire_varuint(input, offset, gap) ||
                !read_wire_varuint(input, offset, rows) || rows == 0) {
                return false;
            }
            if (gap >
                    static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
                rows >
                    static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
                total_rows >
                    static_cast<uint64_t>(std::numeric_limits<size_t>::max()) /
                        request.row_bytes ||
                rows >
                    (static_cast<uint64_t>(std::numeric_limits<size_t>::max()) -
                     next_dest_offset) /
                        request.row_bytes) {
                return false;
            }

            if (next_start_row >
                std::numeric_limits<int64_t>::max() -
                    static_cast<int64_t>(gap)) {
                return false;
            }
            next_start_row += static_cast<int64_t>(gap);
            RemoteGatherRange range;
            range.start_row = next_start_row;
            range.rows = static_cast<int64_t>(rows);
            range.dest_offset = next_dest_offset;
            head.ranges.push_back(range);

            const size_t range_bytes =
                static_cast<size_t>(rows) * request.row_bytes;
            next_dest_offset += range_bytes;
            if (next_start_row >
                std::numeric_limits<int64_t>::max() - range.rows) {
                return false;
            }
            next_start_row += range.rows;
            total_rows += rows;
        }
        head.dst_buffer.size_ = next_dest_offset;
        request.heads.push_back(std::move(head));
    }
    return offset == input.size();
}

bool decode_remote_gather_request(std::string_view input,
                                  RemoteGatherRequest& request) {
    request = RemoteGatherRequest{};
    size_t offset = 0;
    uint32_t magic = 0;
    uint16_t version = 0;
    uint16_t kind = 0;
    uint16_t reserved = 0;
    uint64_t request_id = 0;
    if (!read_wire_value(input, offset, magic) ||
        !read_wire_value(input, offset, version) ||
        !read_wire_value(input, offset, kind) ||
        !read_wire_value(input, offset, reserved) ||
        !read_wire_value(input, offset, request_id)) {
        return false;
    }
    if (magic != kRemoteGatherWireMagic ||
        kind != static_cast<uint16_t>(RemoteGatherWireKind::kRequest)) {
        return false;
    }
    if (version == 1) {
        return decode_remote_gather_request_v1(input, offset, request_id, request);
    }
    if (version == 2) {
        return decode_remote_gather_request_v2(input, offset, request_id, request);
    }
    return false;
}

bool encode_remote_gather_doorbell(const RemoteGatherDoorbell& doorbell,
                                   std::string& out) {
    out.clear();
    append_wire_value(out, kRemoteGatherWireMagic);
    append_wire_value(out, kRemoteGatherWireVersion);
    append_wire_value(out,
                      static_cast<uint16_t>(RemoteGatherWireKind::kDoorbell));
    append_wire_value(out, static_cast<uint16_t>(0));
    append_wire_value(out, doorbell.request_id);
    append_wire_value(out, doorbell.descriptor_bytes);
    append_wire_value(out, doorbell.slot_buffer_size);
    append_wire_value(out, doorbell.slot_buffer_address);
    return append_wire_bytes(out, doorbell.requester_agent);
}

bool decode_remote_gather_doorbell(std::string_view input,
                                   RemoteGatherDoorbell& doorbell) {
    doorbell = RemoteGatherDoorbell{};
    size_t offset = 0;
    uint32_t magic = 0;
    uint16_t version = 0;
    uint16_t kind = 0;
    uint16_t reserved = 0;
    if (!read_wire_value(input, offset, magic) ||
        !read_wire_value(input, offset, version) ||
        !read_wire_value(input, offset, kind) ||
        !read_wire_value(input, offset, reserved) ||
        !read_wire_value(input, offset, doorbell.request_id) ||
        !read_wire_value(input, offset, doorbell.descriptor_bytes) ||
        !read_wire_value(input, offset, doorbell.slot_buffer_size) ||
        !read_wire_value(input, offset, doorbell.slot_buffer_address) ||
        !read_wire_bytes(input, offset, doorbell.requester_agent)) {
        return false;
    }
    return magic == kRemoteGatherWireMagic &&
           (version == 1 || version == kRemoteGatherWireVersion) &&
           kind == static_cast<uint16_t>(RemoteGatherWireKind::kDoorbell) &&
           offset == input.size();
}

bool encode_remote_gather_ack(const RemoteGatherAck& ack, std::string& out) {
    out.clear();
    append_wire_value(out, kRemoteGatherWireMagic);
    append_wire_value(out, kRemoteGatherWireVersion);
    append_wire_value(out, static_cast<uint16_t>(RemoteGatherWireKind::kAck));
    append_wire_value(out, static_cast<uint16_t>(0));
    append_wire_value(out, ack.request_id);
    append_wire_value(out, ack.status);
    return append_wire_bytes(out, ack.error);
}

bool decode_remote_gather_ack(std::string_view input, RemoteGatherAck& ack) {
    ack = RemoteGatherAck{};
    size_t offset = 0;
    uint32_t magic = 0;
    uint16_t version = 0;
    uint16_t kind = 0;
    uint16_t reserved = 0;
    if (!read_wire_value(input, offset, magic) ||
        !read_wire_value(input, offset, version) ||
        !read_wire_value(input, offset, kind) ||
        !read_wire_value(input, offset, reserved) ||
        !read_wire_value(input, offset, ack.request_id) ||
        !read_wire_value(input, offset, ack.status) ||
        !read_wire_bytes(input, offset, ack.error)) {
        return false;
    }
    return magic == kRemoteGatherWireMagic &&
           (version == 1 || version == kRemoteGatherWireVersion) &&
           kind == static_cast<uint16_t>(RemoteGatherWireKind::kAck) &&
           offset == input.size();
}

}  // namespace

// -----------------------------------------------------------------------------
// Hash mapping (inlined from NgramHashMapping)
// -----------------------------------------------------------------------------

bool Engram::is_prime(int64_t n) {
    if (n < 2) return false;
    if (n == 2) return true;
    if (n % 2 == 0) return false;
    for (int64_t i = 3; i * i <= n; i += 2) {
        if (n % i == 0) return false;
    }
    return true;
}

int64_t Engram::find_next_prime(int64_t start,
                                std::unordered_set<int64_t>& seen) {
    int64_t candidate = start + 1;
    while (true) {
        if (is_prime(candidate) && seen.find(candidate) == seen.end()) {
            return candidate;
        }
        candidate++;
    }
}

void Engram::hash_flat_(
    const std::vector<std::vector<int64_t>>& input_ids, int layer_id,
    std::vector<int64_t>& flat_hash_ids,
    std::vector<std::vector<LookupRowRef>>* head_refs) const {
    const int B = static_cast<int>(input_ids.size());
    if (B == 0) {
        flat_hash_ids.clear();
        if (head_refs != nullptr) {
            head_refs->clear();
        }
        return;
    }
    const int L = static_cast<int>(input_ids[0].size());
    const int num_heads = static_cast<int>(list_of_N_.size());

    flat_hash_ids.resize(static_cast<size_t>(B) * L * num_heads);
    if (head_refs != nullptr) {
        head_refs->assign(num_heads, {});
        const size_t refs_per_head = static_cast<size_t>(B) * L;
        for (auto& refs : *head_refs) {
            refs.reserve(refs_per_head);
        }
    }

    const auto& multipliers = layer_multipliers_.at(layer_id);
    const auto& head_vocab_sizes = vocab_size_across_layers_.at(layer_id);
    const size_t token_stride = static_cast<size_t>(num_heads);
    const size_t output_stride = token_stride * embed_D_;

    for (int b = 0; b < B; ++b) {
        const auto& row = input_ids[b];
        for (int l = 0; l < L; ++l) {
            int64_t mix = row[l] * multipliers[0];
            const size_t token_index = static_cast<size_t>(b) * L + l;
            const size_t token_hash_base = token_index * token_stride;
            const size_t token_output_base = token_index * output_stride;
            int head_base = 0;

            for (int n = 2; n <= max_ngram_size_; ++n) {
                const int shift = n - 1;
                const int64_t prev_token = (l >= shift) ? row[l - shift] : pad_id_;
                mix ^= (prev_token * multipliers[shift]);

                const auto& ngram_vocab_sizes = head_vocab_sizes[n - 2];
                for (int j = 0; j < n_head_per_ngram_; ++j) {
                    const int head_index = head_base + j;
                    const int64_t mod = ngram_vocab_sizes[j];
                    int64_t idx = mix % mod;
                    if (idx < 0) {
                        idx += mod;
                    }
                    flat_hash_ids[token_hash_base + head_index] = idx;
                    if (head_refs != nullptr) {
                        (*head_refs)[head_index].push_back(LookupRowRef{
                            idx, token_output_base +
                                     static_cast<size_t>(head_index) * embed_D_});
                    }
                }
                head_base += n_head_per_ngram_;
            }
        }
    }
}

bool Engram::remote_gather_enabled() const {
    return store_ != nullptr && store_->has_live_client() &&
           store_protocol_ == "rdma" &&
           load_bool_env("MC_ENGRAM_REMOTE_GATHER", true);
}

int Engram::ensure_request_ring(size_t min_slot_size) const {
    if (store_ == nullptr) {
        return -1;
    }

    const size_t configured_slot_size =
        load_size_env("MC_ENGRAM_SG_RING_SLOT_BYTES",
                      kDefaultRequestRingSlotSize);
    size_t slot_size = configured_slot_size;
    const size_t slot_count =
        load_size_env("MC_ENGRAM_SG_RING_SLOTS", kDefaultRequestRingSlotCount);
    size_t max_slot_size =
        load_size_env("MC_ENGRAM_SG_RING_MAX_SLOT_BYTES",
                      kDefaultRequestRingMaxSlotSize);
    if (max_slot_size != 0) {
        max_slot_size = std::max(max_slot_size, configured_slot_size);
    }
    if (min_slot_size > slot_size) {
        slot_size = round_up_to_power_of_two(min_slot_size);
        if (max_slot_size > 0) {
            slot_size = std::min(slot_size, max_slot_size);
        }
    }
    const size_t total_bytes = slot_size * slot_count;

    std::lock_guard<std::mutex> lock(request_ring_mu_);
    if (request_ring_registered_ && request_ring_slot_size_ >= slot_size &&
        request_ring_slot_count_ >= slot_count) {
        return 0;
    }

    if (request_ring_registered_ && store_->has_live_client()) {
        for (void* buf : request_ring_buffers_) {
            store_->unregister_buffer(buf);
        }
    }
    request_ring_registered_ = false;
    request_ring_workspace_.clear();
    request_ring_buffers_.clear();
    request_ring_descriptors_.clear();
    request_ring_in_use_.clear();
    request_ring_request_to_slot_.clear();

    if (slot_size == 0 || slot_count == 0) {
        return -1;
    }

    request_ring_workspace_.assign(total_bytes, 0);
    request_ring_buffers_.reserve(slot_count);
    request_ring_descriptors_.reserve(slot_count);
    request_ring_in_use_.assign(slot_count, false);
    request_ring_slot_size_ = slot_size;
    request_ring_slot_count_ = slot_count;

    if (!store_->has_live_client()) {
        return -1;
    }

    const std::string& local_agent = local_agent_name_;
    if (local_agent.empty()) {
        return -1;
    }

    char* base = request_ring_workspace_.data();
    for (size_t i = 0; i < slot_count; ++i) {
        void* slot_ptr = base + i * slot_size;
        request_ring_buffers_.push_back(slot_ptr);
        int ret = store_->register_buffer(slot_ptr, slot_size);
        if (ret != 0) {
            for (size_t j = 0; j < i; ++j) {
                store_->unregister_buffer(request_ring_buffers_[j]);
            }
            request_ring_workspace_.clear();
            request_ring_buffers_.clear();
            request_ring_descriptors_.clear();
            request_ring_in_use_.clear();
            request_ring_slot_size_ = 0;
            request_ring_slot_count_ = 0;
            return -1;
        }
        AllocatedBuffer::Descriptor desc;
        desc.size_ = slot_size;
        desc.buffer_address_ = reinterpret_cast<uintptr_t>(slot_ptr);
        desc.protocol_ = store_protocol_;
        desc.transport_endpoint_ = local_agent;
        request_ring_descriptors_.push_back(std::move(desc));
    }

    request_ring_registered_ = true;
    return 0;
}

void Engram::release_request_ring() const {
    std::lock_guard<std::mutex> lock(request_ring_mu_);
    if (store_ != nullptr && request_ring_registered_ && store_->has_live_client()) {
        for (void* buf : request_ring_buffers_) {
            store_->unregister_buffer(buf);
        }
    }
    request_ring_registered_ = false;
    request_ring_workspace_.clear();
    request_ring_buffers_.clear();
    request_ring_descriptors_.clear();
    request_ring_in_use_.clear();
    request_ring_request_to_slot_.clear();
    request_ring_slot_size_ = 0;
    request_ring_slot_count_ = 0;
}

int Engram::ensure_remote_gather_workspace(
    const std::vector<size_t>& head_sizes) {
    if (store_ == nullptr) {
        return -1;
    }

    const size_t required_bytes =
        std::accumulate(head_sizes.begin(), head_sizes.end(),
                        static_cast<size_t>(0));

    std::lock_guard<std::mutex> lock(remote_gather_workspace_mu_);
    bool reuse_existing =
        remote_gather_workspace_registered_ &&
        remote_gather_workspace_.size() >= required_bytes;
    if (reuse_existing) {
        remote_gather_buffer_sizes_ = head_sizes;
        remote_gather_buffers_.clear();
        remote_gather_buffers_.reserve(head_sizes.size());
        char* base = remote_gather_workspace_.data();
        for (size_t size : head_sizes) {
            remote_gather_buffers_.push_back(base);
            base += size;
        }
        return 0;
    }

    if (remote_gather_workspace_registered_ && store_->has_live_client()) {
        store_->unregister_buffer(remote_gather_workspace_.data());
    }
    remote_gather_workspace_registered_ = false;
    remote_gather_workspace_.clear();
    remote_gather_buffers_.clear();
    remote_gather_buffer_sizes_.clear();

    if (required_bytes == 0) {
        return 0;
    }

    remote_gather_workspace_.assign(required_bytes, 0);
    remote_gather_buffers_.reserve(head_sizes.size());
    remote_gather_buffer_sizes_ = head_sizes;

    char* base = remote_gather_workspace_.data();
    for (size_t size : head_sizes) {
        remote_gather_buffers_.push_back(base);
        base += size;
    }

    int ret = store_->register_buffer(remote_gather_workspace_.data(),
                                      remote_gather_workspace_.size());
    if (ret != 0) {
        remote_gather_workspace_.clear();
        remote_gather_buffers_.clear();
        remote_gather_buffer_sizes_.clear();
        return -1;
    }

    remote_gather_workspace_registered_ = true;
    return 0;
}

int Engram::ensure_remote_descriptor_workspace(size_t required_bytes) {
    if (store_ == nullptr) {
        return -1;
    }

    std::lock_guard<std::mutex> lock(remote_descriptor_workspace_mu_);
    if (remote_descriptor_workspace_registered_ &&
        remote_descriptor_workspace_.size() >= required_bytes) {
        return 0;
    }

    if (remote_descriptor_workspace_registered_ && store_->has_live_client()) {
        store_->unregister_buffer(remote_descriptor_workspace_.data());
    }
    remote_descriptor_workspace_registered_ = false;
    remote_descriptor_workspace_.clear();

    if (required_bytes == 0) {
        return 0;
    }

    remote_descriptor_workspace_.assign(required_bytes, 0);
    int ret = store_->register_buffer(remote_descriptor_workspace_.data(),
                                      remote_descriptor_workspace_.size());
    if (ret != 0) {
        remote_descriptor_workspace_.clear();
        return -1;
    }
    remote_descriptor_workspace_registered_ = true;
    return 0;
}

void Engram::release_remote_descriptor_workspace() {
    std::lock_guard<std::mutex> lock(remote_descriptor_workspace_mu_);
    if (store_ != nullptr && remote_descriptor_workspace_registered_ &&
        store_->has_live_client()) {
        store_->unregister_buffer(remote_descriptor_workspace_.data());
    }
    remote_descriptor_workspace_registered_ = false;
    remote_descriptor_workspace_.clear();
}

void Engram::release_remote_gather_workspace() {
    std::lock_guard<std::mutex> lock(remote_gather_workspace_mu_);
    if (store_ != nullptr && remote_gather_workspace_registered_ &&
        store_->has_live_client()) {
        store_->unregister_buffer(remote_gather_workspace_.data());
    }
    remote_gather_workspace_registered_ = false;
    remote_gather_workspace_.clear();
    remote_gather_buffers_.clear();
    remote_gather_buffer_sizes_.clear();
}

void Engram::ensure_remote_gather_service() {
    if (!remote_gather_enabled()) {
        return;
    }

    std::lock_guard<std::mutex> lock(remote_gather_service_mu_);
    if (remote_gather_service_running_.load(std::memory_order_acquire)) {
        return;
    }
    remote_gather_service_running_.store(true, std::memory_order_release);
    remote_gather_service_thread_ =
        std::thread(&Engram::remote_gather_service_loop, this);
}

void Engram::stop_remote_gather_service() {
    {
        std::lock_guard<std::mutex> lock(remote_gather_service_mu_);
        remote_gather_service_running_.store(false, std::memory_order_release);
    }
    if (remote_gather_service_thread_.joinable()) {
        remote_gather_service_thread_.join();
    }
}

void Engram::remote_gather_service_loop() {
    if (store_ == nullptr) {
        return;
    }

    auto client = store_->get_client_shared();
    if (!client) {
        return;
    }
    auto client_state = get_remote_gather_client_state(client);
    if (!client_state) {
        return;
    }

    const size_t hot_poll_budget =
        load_size_env("MC_ENGRAM_REMOTE_GATHER_HOT_POLLS", 64);
    size_t hot_polls_remaining = hot_poll_budget;

    register_remote_gather_service_thread(client_state);
    while (remote_gather_service_running_.load(std::memory_order_acquire)) {
        if (!try_begin_remote_gather_poll(client_state)) {
            std::unique_lock<std::mutex> lock(client_state->mu);
            client_state->cv.wait_for(lock, kRemoteGatherPollInterval, [&] {
                return !remote_gather_service_running_.load(
                           std::memory_order_acquire) ||
                       !client_state->poll_in_progress;
            });
            continue;
        }
        std::vector<TransferMetadata::NotifyDesc> notifies;
        int rc = client->GetTransferNotifies(notifies);
        finish_remote_gather_poll(client_state);
        if (rc != 0) {
            LOG_EVERY_N(WARNING, 100) << "remote_gather_get_notifies_failed rc="
                                      << rc;
            hot_polls_remaining = 0;
            std::unique_lock<std::mutex> lock(client_state->mu);
            client_state->cv.wait_for(lock, kRemoteGatherPollInterval, [&] {
                return !remote_gather_service_running_.load(
                           std::memory_order_acquire) ||
                       client_state->expected_ack_count.load(
                           std::memory_order_relaxed) > 0;
            });
            continue;
        }
        if (notifies.empty()) {
            if (hot_polls_remaining > 0) {
                --hot_polls_remaining;
                continue;
            }
            if (client_state->expected_ack_count.load(
                    std::memory_order_relaxed) > 0) {
                continue;
            }
            std::unique_lock<std::mutex> lock(client_state->mu);
            client_state->cv.wait_for(lock, kRemoteGatherPollInterval, [&] {
                return !remote_gather_service_running_.load(
                           std::memory_order_acquire) ||
                       client_state->expected_ack_count.load(
                           std::memory_order_relaxed) > 0;
            });
            continue;
        }
        hot_polls_remaining = hot_poll_budget;

        for (const auto& notify : notifies) {
            if (notify.name == kRemoteGatherAckName) {
                RemoteGatherAck ack;
                if (decode_remote_gather_ack(notify.notify_msg, ack)) {
                    enqueue_remote_gather_ack(client_state, std::move(ack));
                }
                continue;
            }
            if (notify.name != kRemoteGatherSubmitName) {
                continue;
            }

            RemoteGatherDoorbell doorbell;
            if (!decode_remote_gather_doorbell(notify.notify_msg, doorbell)) {
                LOG(WARNING) << "remote_gather_decode_doorbell_failed";
                continue;
            }

            RemoteGatherAck ack;
            ack.request_id = doorbell.request_id;
            const std::string reply_agent = doorbell.requester_agent;
            RemoteGatherRequest request;

            if (reply_agent.empty()) {
                ack.status = -1;
                ack.error = "empty reply agent";
            } else if (doorbell.descriptor_bytes == 0 ||
                       doorbell.descriptor_bytes > doorbell.slot_buffer_size) {
                ack.status = -1;
                ack.error = "invalid descriptor bytes";
            } else {
                if (ensure_remote_descriptor_workspace(
                        static_cast<size_t>(doorbell.descriptor_bytes)) != 0) {
                    ack.status = -1;
                    ack.error = "failed to prepare descriptor workspace";
                } else {
                    AllocatedBuffer::Descriptor descriptor_src;
                    descriptor_src.size_ = doorbell.descriptor_bytes;
                    descriptor_src.buffer_address_ = doorbell.slot_buffer_address;
                    descriptor_src.protocol_ = store_protocol_;
                    descriptor_src.transport_endpoint_ = reply_agent;
                    void* descriptor_dst = nullptr;
                    {
                        std::lock_guard<std::mutex> lock(
                            remote_descriptor_workspace_mu_);
                        descriptor_dst = remote_descriptor_workspace_.data();
                    }
                    ErrorCode fetch_err = client->BatchTransferReadBuffers(
                        {descriptor_src}, {descriptor_dst},
                        {static_cast<size_t>(doorbell.descriptor_bytes)});
                    if (fetch_err != ErrorCode::OK) {
                        ack.status = -1;
                        ack.error = toString(fetch_err);
                    } else {
                        bool decode_ok = false;
                        {
                            std::lock_guard<std::mutex> lock(
                                remote_descriptor_workspace_mu_);
                            std::string_view descriptor_bytes(
                                remote_descriptor_workspace_.data(),
                                static_cast<size_t>(doorbell.descriptor_bytes));
                            decode_ok = decode_remote_gather_request(
                                descriptor_bytes, request);
                        }
                        if (!decode_ok) {
                            ack.status = -1;
                            ack.error = "failed to decode request";
                        } else if (request.request_id != doorbell.request_id) {
                            ack.status = -1;
                            ack.error = "request id mismatch";
                        }
                    }
                }
            }

            if (ack.status == 0) {
                if (request.heads.empty()) {
                    ack.status = -1;
                    ack.error = "empty request";
                } else {
                    const std::string& local_agent = local_agent_name_;
                    for (auto& head : request.heads) {
                        head.src_buffer.protocol_ = store_protocol_;
                        head.src_buffer.transport_endpoint_ = local_agent;
                        head.dst_buffer.protocol_ = store_protocol_;
                        head.dst_buffer.transport_endpoint_ = reply_agent;
                    }

                    std::vector<size_t> head_sizes;
                    head_sizes.reserve(request.heads.size());
                    bool valid_request = true;
                    const size_t row_bytes =
                        static_cast<size_t>(request.row_bytes);
                    const size_t expected_row_bytes =
                        static_cast<size_t>(embed_D_) * sizeof(float);

                    if (row_bytes == 0 || row_bytes != expected_row_bytes) {
                        valid_request = false;
                        ack.error = "unexpected row size";
                    }
                    for (const auto& head : request.heads) {
                        if (!valid_request) {
                            break;
                        }
                        if (head.ranges.empty()) {
                            valid_request = false;
                            ack.error = "head has no ranges";
                            break;
                        }

                        size_t next_dest_offset = 0;
                        for (const auto& range : head.ranges) {
                            if (range.start_row < 0 || range.rows <= 0) {
                                valid_request = false;
                                ack.error = "invalid range";
                                break;
                            }
                            size_t range_bytes =
                                static_cast<size_t>(range.rows) * row_bytes;
                            size_t src_offset =
                                static_cast<size_t>(range.start_row) * row_bytes;
                            if (src_offset + range_bytes > head.src_buffer.size_) {
                                valid_request = false;
                                ack.error = "source range out of bounds";
                                break;
                            }
                            if (static_cast<size_t>(range.dest_offset) !=
                                next_dest_offset) {
                                valid_request = false;
                                ack.error = "destination layout must be packed";
                                break;
                            }
                            next_dest_offset += range_bytes;
                        }
                        if (!valid_request) {
                            break;
                        }
                        if (next_dest_offset == 0 ||
                            head.dst_buffer.size_ != next_dest_offset) {
                            valid_request = false;
                            ack.error = "destination layout must be packed";
                            break;
                        }
                        head_sizes.push_back(next_dest_offset);
                    }

                    if (valid_request &&
                        ensure_remote_gather_workspace(head_sizes) != 0) {
                        valid_request = false;
                        ack.error = "failed to prepare gather workspace";
                    }

                    if (valid_request) {
                        std::vector<void*> staging_buffers;
                        {
                            std::lock_guard<std::mutex> lock(
                                remote_gather_workspace_mu_);
                            staging_buffers = remote_gather_buffers_;
                        }

                        std::vector<AllocatedBuffer::Descriptor> dest_buffers;
                        std::vector<void*> src_buffers;
                        std::vector<size_t> sizes;
                        dest_buffers.reserve(request.heads.size());
                        src_buffers.reserve(request.heads.size());
                        sizes.reserve(request.heads.size());

                        for (size_t i = 0; i < request.heads.size(); ++i) {
                            const auto& head = request.heads[i];
                            auto src_ptr = client->ResolveLocalBufferAddress(
                                head.src_buffer, head.src_buffer.size_);
                            if (!src_ptr) {
                                valid_request = false;
                                ack.error =
                                    "failed to resolve local source buffer";
                                break;
                            }

                            const char* src_base =
                                static_cast<const char*>(src_ptr.value());
                            char* dst_base =
                                static_cast<char*>(staging_buffers[i]);
                            for (const auto& range : head.ranges) {
                                size_t range_bytes =
                                    static_cast<size_t>(range.rows) * row_bytes;
                                size_t src_offset =
                                    static_cast<size_t>(range.start_row) *
                                    row_bytes;
                                std::memcpy(dst_base + range.dest_offset,
                                            src_base + src_offset, range_bytes);
                            }

                            dest_buffers.push_back(head.dst_buffer);
                            src_buffers.push_back(dst_base);
                            sizes.push_back(head_sizes[i]);
                        }

                        if (valid_request) {
                            ErrorCode err = client->BatchTransferWriteBuffers(
                                dest_buffers, src_buffers, sizes);
                            if (err != ErrorCode::OK) {
                                valid_request = false;
                                ack.error = toString(err);
                            }
                        }
                    }

                    ack.status = valid_request ? 0 : -1;
                }
            }
            if (!reply_agent.empty()) {
                std::string ack_payload;
                if (encode_remote_gather_ack(ack, ack_payload)) {
                    int notify_rc = client->SendTransferNotifyByName(
                        reply_agent, kRemoteGatherAckName, ack_payload);
                    if (notify_rc != 0) {
                        LOG(WARNING) << "remote_gather_send_ack_failed req="
                                     << ack.request_id
                                     << " rc=" << notify_rc;
                    }
                }
            }
        }
    }
    unregister_remote_gather_service_thread(client_state);
}

// -----------------------------------------------------------------------------
// Embedding lookup / populate (inlined from MooncakeEngramEmbedding)
// -----------------------------------------------------------------------------

int Engram::embedding_lookup(
    const std::vector<int64_t>& flat_hash_ids, int B, int L,
    void* output_buffer, size_t output_size,
    const std::vector<void*>* table_buffers,
    const std::vector<size_t>* table_sizes,
    bool buffers_are_pre_registered,
    std::vector<std::vector<LookupRowRef>>* prepared_head_refs) const {
    if (store_ == nullptr) return -1;
    if (flat_hash_ids.empty() || B <= 0 || L <= 0)
        return -1;

    const int num_heads = static_cast<int>(list_of_N_.size());
    if (flat_hash_ids.size() !=
        static_cast<size_t>(B) * L * static_cast<size_t>(num_heads)) {
        return -1;
    }
    const size_t row_bytes = static_cast<size_t>(embed_D_) * sizeof(float);

    size_t expected_size =
        static_cast<size_t>(B) * L * num_heads * embed_D_ * sizeof(float);
    if (output_size < expected_size) return -1;

    float* output = static_cast<float*>(output_buffer);
    const int64_t* hash_data = flat_hash_ids.data();
    auto hash_index = [L, num_heads](int b, int l, int h) -> size_t {
        return (static_cast<size_t>(b) * L + l) * num_heads + h;
    };
    bool use_zero_copy =
        (table_buffers != nullptr && table_sizes != nullptr &&
         static_cast<int>(table_buffers->size()) == num_heads &&
         static_cast<int>(table_sizes->size()) == num_heads);

    if (use_zero_copy) {
        const int64_t max_rows_per_head =
            std::max<int64_t>(1, static_cast<int64_t>(B) * L);
        for (int h = 0; h < num_heads; ++h) {
            size_t required =
                static_cast<size_t>(
                    std::min<int64_t>(list_of_N_[h], max_rows_per_head)) *
                row_bytes;
            if ((*table_sizes)[h] < required) {
                use_zero_copy = false;
                break;
            }
        }
    }

    std::vector<const float*> tables(num_heads, nullptr);
    std::vector<size_t> registered_indices;
    std::vector<tl::expected<QueryResult, ErrorCode>> query_results;
    bool have_query_results = false;
    auto client = store_ != nullptr ? store_->get_client_shared() : nullptr;

    if (use_zero_copy && !buffers_are_pre_registered) {
        for (int h = 0; h < num_heads; ++h) {
            int ret = store_->register_buffer((*table_buffers)[h], (*table_sizes)[h]);
            if (ret != 0) {
                for (int j : registered_indices)
                    store_->unregister_buffer((*table_buffers)[j]);
                use_zero_copy = false;
                break;
            }
            registered_indices.push_back(h);
        }
    }

    if (store_ != nullptr && client != nullptr) {
        query_results = get_query_results();
        have_query_results =
            (query_results.size() == static_cast<size_t>(num_heads));
        if (have_query_results) {
            for (int h = 0; h < num_heads; ++h) {
                if (!query_results[h]) {
                    continue;
                }
                size_t table_bytes =
                    static_cast<size_t>(list_of_N_[h]) * row_bytes;
                auto local_ptr =
                    client->ResolveLocalMemoryAddress(query_results[h].value(),
                                                      table_bytes);
                if (local_ptr) {
                    tables[h] = static_cast<const float*>(local_ptr.value());
                }
            }
        }
    }

    bool all_tables_local = true;
    for (int h = 0; h < num_heads; ++h) {
        if (tables[h] == nullptr) {
            all_tables_local = false;
            break;
        }
    }
    if (all_tables_local) {
        use_zero_copy = false;
    }

    if (use_zero_copy) {
        auto zero_output = [&]() { std::memset(output_buffer, 0, expected_size); };

        if (!have_query_results || client == nullptr) {
            zero_output();
            if (!buffers_are_pre_registered) {
                for (int h : registered_indices) {
                    store_->unregister_buffer((*table_buffers)[h]);
                }
            }
            return -1;
        }

        std::vector<std::vector<LookupRowRef>> owned_head_refs;
        std::vector<std::vector<LookupPackedRange>> packed_ranges(num_heads);
        std::vector<LookupRowRef> row_ref_sort_scratch;
        int total_ranges = 0;
        bool prep_ok = true;
        char* workspace_base = static_cast<char*>((*table_buffers)[0]);

        auto* head_refs = prepared_head_refs;
        if (head_refs != nullptr &&
            static_cast<int>(head_refs->size()) != num_heads) {
            head_refs = nullptr;
        }

        if (head_refs == nullptr) {
            owned_head_refs.assign(num_heads, {});
            head_refs = &owned_head_refs;
        }
        row_ref_sort_scratch.reserve(static_cast<size_t>(B) * L);
        for (int h = 0; h < num_heads; ++h) {
            if (tables[h] != nullptr) {
                continue;
            }
            if (!query_results[h]) {
                prep_ok = false;
                break;
            }

            auto& refs = (*head_refs)[h];
            if (refs.empty()) {
                refs.reserve(static_cast<size_t>(B) * L);
                int64_t vocab_size_h = list_of_N_[h];
                for (int b = 0; b < B; ++b) {
                    for (int l = 0; l < L; ++l) {
                        int64_t idx = hash_data[hash_index(b, l, h)];
                        if (idx < 0) idx = 0;
                        if (idx >= vocab_size_h) idx = vocab_size_h - 1;
                        refs.push_back(LookupRowRef{
                            idx,
                            static_cast<size_t>(
                                (b * L * num_heads + l * num_heads + h) *
                                embed_D_),
                        });
                    }
                }
            }
            if (refs.empty()) {
                continue;
            }

            packed_ranges[h].reserve(refs.size());
            radix_sort_by_u64_key(
                refs, row_ref_sort_scratch, [](const LookupRowRef& ref) {
                    return static_cast<uint64_t>(ref.idx);
                });

            size_t head_cursor = 0;
            size_t ref_begin = 0;
            while (ref_begin < refs.size()) {
                int64_t range_start = refs[ref_begin].idx;
                int64_t last_idx = range_start;
                size_t ref_end = ref_begin + 1;
                while (ref_end < refs.size()) {
                    int64_t idx = refs[ref_end].idx;
                    if (idx == last_idx) {
                        ++ref_end;
                        continue;
                    }
                    if (idx == last_idx + 1) {
                        last_idx = idx;
                        ++ref_end;
                        continue;
                    }
                    break;
                }

                int64_t range_end = last_idx + 1;
                size_t read_size =
                    static_cast<size_t>(range_end - range_start) * row_bytes;
                if (head_cursor + read_size > (*table_sizes)[h]) {
                    prep_ok = false;
                    break;
                }

                packed_ranges[h].push_back(LookupPackedRange{
                    range_start,
                    range_end,
                    head_cursor,
                    ref_begin,
                    ref_end,
                });
                head_cursor += read_size;
                total_ranges++;
                ref_begin = ref_end;
            }

            if (!prep_ok) {
                break;
            }
        }

        bool zero_copy_ok = prep_ok;
        bool fallback_to_range_read = true;

        if (zero_copy_ok && total_ranges > 0 && remote_gather_enabled()) {
            const_cast<Engram*>(this)->ensure_remote_gather_service();

            struct RemoteEndpointRequest {
                std::string remote_agent;
                RemoteGatherRequest request;
                size_t estimated_bytes = 0;
                size_t encoded_bytes = 0;
                std::string doorbell_payload;
            };

            std::vector<RemoteEndpointRequest> remote_requests;
            std::unordered_map<std::string, size_t> endpoint_to_active_request;
            bool remote_request_build_ok = true;
            bool remote_gather_sent = false;
            std::vector<uint64_t> pending_request_ids;
            std::vector<uint64_t> unsent_request_ids;
            std::vector<Replica::Descriptor> selected_remote_replicas(
                num_heads);
            std::unordered_map<std::string, size_t> endpoint_payload_bytes;
            auto release_request_slot = [&](uint64_t request_id) {
                std::lock_guard<std::mutex> lock(request_ring_mu_);
                auto it = request_ring_request_to_slot_.find(request_id);
                if (it == request_ring_request_to_slot_.end()) {
                    return;
                }
                request_ring_in_use_[it->second] = false;
                request_ring_request_to_slot_.erase(it);
                request_ring_cv_.notify_one();
            };

            const std::string& local_agent = local_agent_name_;
            const uint32_t request_row_bytes =
                static_cast<uint32_t>(row_bytes);
            const size_t request_base_estimate_bytes =
                kRemoteGatherFixedHeaderBytes + sizeof(uint64_t) +
                varuint_size(request_row_bytes) + 1;
            auto fit_head_chunk = [&](int head_index,
                                      const AllocatedBuffer::Descriptor& src_desc,
                                      uintptr_t dst_buffer_address,
                                      const auto& head_ranges,
                                      size_t range_begin,
                                      size_t available_bytes) {
                const size_t head_index_bytes =
                    varuint_size(static_cast<uint64_t>(head_index));
                const size_t src_size_bytes = varuint_size(src_desc.size_);
                const size_t src_addr_bytes =
                    varuint_size(src_desc.buffer_address_);
                const size_t dst_addr_bytes =
                    varuint_size(dst_buffer_address);
                size_t range_payload_bytes = 0;
                size_t best_encoded_bytes = 0;
                size_t best_range_count = 0;
                int64_t prev_end = 0;

                while (range_begin + best_range_count < head_ranges.size()) {
                    const auto& range = head_ranges[range_begin + best_range_count];
                    if (range.end <= range.start || range.start < prev_end) {
                        return std::pair<size_t, size_t>(0, 0);
                    }
                    const uint64_t row_gap =
                        static_cast<uint64_t>(range.start - prev_end);
                    const uint64_t row_count =
                        static_cast<uint64_t>(range.end - range.start);
                    const size_t candidate_payload_bytes =
                        range_payload_bytes + varuint_size(row_gap) +
                        varuint_size(row_count);
                    const size_t candidate_encoded_bytes =
                        head_index_bytes + varuint_size(best_range_count + 1) +
                        src_size_bytes + src_addr_bytes + dst_addr_bytes +
                        candidate_payload_bytes;
                    if (candidate_encoded_bytes > available_bytes) {
                        break;
                    }
                    range_payload_bytes = candidate_payload_bytes;
                    best_encoded_bytes = candidate_encoded_bytes;
                    ++best_range_count;
                    prev_end = range.end;
                }
                return std::pair<size_t, size_t>(best_range_count,
                                                 best_encoded_bytes);
            };
            auto start_remote_request = [&](const std::string& remote_agent) {
                size_t request_index = remote_requests.size();
                remote_requests.push_back(RemoteEndpointRequest{
                    remote_agent,
                    RemoteGatherRequest{0, request_row_bytes, {}},
                    request_base_estimate_bytes,
                    0,
                    {},
                });
                endpoint_to_active_request[remote_agent] = request_index;
                return request_index;
            };

            for (int h = 0; remote_request_build_ok && h < num_heads; ++h) {
                if (tables[h] != nullptr || packed_ranges[h].empty()) {
                    continue;
                }

                const QueryResult& qr = query_results[h].value();
                auto replica_res = client->GetPreferredReplica(qr.replicas);
                if (!replica_res || !replica_res.value().is_memory_replica()) {
                    zero_copy_ok = false;
                    fallback_to_range_read = false;
                    remote_request_build_ok = false;
                    break;
                }

                const auto& src_desc =
                    replica_res.value().get_memory_descriptor()
                        .buffer_descriptor;
                if (src_desc.transport_endpoint_.empty()) {
                    remote_request_build_ok = false;
                    break;
                }

                selected_remote_replicas[h] = replica_res.value();
                size_t& endpoint_bytes =
                    endpoint_payload_bytes[src_desc.transport_endpoint_];
                if (endpoint_bytes == 0) {
                    endpoint_bytes = request_base_estimate_bytes;
                }
                char* head_base = static_cast<char*>((*table_buffers)[h]);
                const auto [head_range_count, head_encoded_bytes] =
                    fit_head_chunk(h, src_desc,
                                   reinterpret_cast<uintptr_t>(head_base),
                                   packed_ranges[h], 0,
                                   std::numeric_limits<size_t>::max());
                if (head_range_count != packed_ranges[h].size() ||
                    head_encoded_bytes == 0) {
                    remote_request_build_ok = false;
                    break;
                }
                endpoint_bytes += head_encoded_bytes;
            }

            size_t request_slot_bytes = 0;
            if (remote_request_build_ok) {
                size_t min_request_slot_bytes = 0;
                for (const auto& [unused_endpoint, endpoint_bytes] :
                     endpoint_payload_bytes) {
                    min_request_slot_bytes =
                        std::max(min_request_slot_bytes, endpoint_bytes);
                }
                if (local_agent.empty() ||
                    ensure_request_ring(min_request_slot_bytes) != 0) {
                    remote_request_build_ok = false;
                } else {
                    std::lock_guard<std::mutex> lock(request_ring_mu_);
                    request_slot_bytes = request_ring_slot_size_;
                }
                if (request_slot_bytes <
                    request_base_estimate_bytes +
                        kRemoteGatherHeadHeaderMinBytes +
                        kRemoteGatherRangeMinBytes) {
                    remote_request_build_ok = false;
                }
            }

            for (int h = 0; remote_request_build_ok && h < num_heads; ++h) {
                if (tables[h] != nullptr || packed_ranges[h].empty()) {
                    continue;
                }

                const auto& src_desc =
                    selected_remote_replicas[h].get_memory_descriptor()
                        .buffer_descriptor;
                size_t head_bytes = 0;
                const auto& head_ranges = packed_ranges[h];
                for (const auto& range : head_ranges) {
                    size_t read_size =
                        static_cast<size_t>(range.end - range.start) *
                        row_bytes;
                    head_bytes += read_size;
                }
                if (head_bytes == 0) {
                    continue;
                }

                size_t next_range = 0;
                char* head_base = static_cast<char*>((*table_buffers)[h]);
                while (next_range < head_ranges.size()) {
                    auto it = endpoint_to_active_request.find(
                        src_desc.transport_endpoint_);
                    size_t request_index =
                        (it == endpoint_to_active_request.end())
                            ? start_remote_request(src_desc.transport_endpoint_)
                            : it->second;
                    auto& request = remote_requests[request_index];
                    size_t available_bytes =
                        request_slot_bytes > request.estimated_bytes
                            ? request_slot_bytes - request.estimated_bytes
                            : 0;
                    if (available_bytes <
                        kRemoteGatherHeadHeaderMinBytes +
                            kRemoteGatherRangeMinBytes) {
                        request_index =
                            start_remote_request(src_desc.transport_endpoint_);
                        auto& next_request = remote_requests[request_index];
                        available_bytes =
                            request_slot_bytes - next_request.estimated_bytes;
                        if (available_bytes <
                            kRemoteGatherHeadHeaderMinBytes +
                                kRemoteGatherRangeMinBytes) {
                            remote_request_build_ok = false;
                            break;
                        }
                    }

                    auto& target_request = remote_requests[request_index];
                    RemoteGatherHeadRequest head_request;
                    head_request.head_index = h;
                    head_request.src_buffer = src_desc;
                    head_request.dst_buffer.protocol_ = store_protocol_;
                    head_request.dst_buffer.transport_endpoint_ = local_agent;
                    const size_t chunk_begin_offset =
                        head_ranges[next_range].buffer_offset;
                    const auto [range_count, head_encoded_bytes] =
                        fit_head_chunk(
                            h, src_desc,
                            reinterpret_cast<uintptr_t>(head_base +
                                                        chunk_begin_offset),
                            head_ranges, next_range, available_bytes);
                    if (range_count == 0 || head_encoded_bytes == 0) {
                        remote_request_build_ok = false;
                        break;
                    }
                    size_t chunk_bytes = 0;
                    head_request.ranges.reserve(range_count);
                    for (size_t i = 0; i < range_count; ++i) {
                        const auto& range = head_ranges[next_range + i];
                        size_t read_size =
                            static_cast<size_t>(range.end - range.start) *
                            row_bytes;
                        head_request.ranges.push_back(RemoteGatherRange{
                            range.start,
                            range.end - range.start,
                            static_cast<uint64_t>(range.buffer_offset -
                                                  chunk_begin_offset),
                        });
                        chunk_bytes += read_size;
                    }
                    head_request.dst_buffer.size_ = chunk_bytes;
                    head_request.dst_buffer.buffer_address_ =
                        reinterpret_cast<uintptr_t>(head_base +
                                                    chunk_begin_offset);
                    target_request.request.heads.push_back(
                        std::move(head_request));
                    target_request.estimated_bytes += head_encoded_bytes;
                    next_range += range_count;
                }
            }

            if (zero_copy_ok && remote_request_build_ok && !remote_requests.empty()) {
                pending_request_ids.reserve(remote_requests.size());
                unsent_request_ids.reserve(remote_requests.size());

                for (auto& remote_request : remote_requests) {
                    remote_request.request.request_id =
                        remote_gather_request_seq_.fetch_add(
                            1, std::memory_order_relaxed);
                    const size_t descriptor_bytes =
                        estimate_remote_gather_request_bytes(
                            remote_request.request);
                    if (descriptor_bytes == 0) {
                        remote_request_build_ok = false;
                        break;
                    }
                    size_t slot_index = std::numeric_limits<size_t>::max();
                    void* slot_ptr = nullptr;
                    AllocatedBuffer::Descriptor slot_desc;
                    {
                        std::unique_lock<std::mutex> lock(request_ring_mu_);
                        request_ring_cv_.wait(lock, [&] {
                            if (!request_ring_registered_) {
                                return true;
                            }
                            for (bool in_use : request_ring_in_use_) {
                                if (!in_use) {
                                    return true;
                                }
                            }
                            return false;
                        });
                        if (!request_ring_registered_ ||
                            descriptor_bytes > request_ring_slot_size_) {
                            remote_request_build_ok = false;
                        } else {
                            for (size_t i = 0; i < request_ring_in_use_.size();
                                 ++i) {
                                if (!request_ring_in_use_[i]) {
                                    request_ring_in_use_[i] = true;
                                    slot_index = i;
                                    slot_ptr = request_ring_buffers_[i];
                                    slot_desc = request_ring_descriptors_[i];
                                    request_ring_request_to_slot_
                                        [remote_request.request.request_id] = i;
                                    break;
                                }
                            }
                            remote_request_build_ok = (slot_ptr != nullptr);
                        }
                    }
                    if (!remote_request_build_ok) {
                        if (slot_index != std::numeric_limits<size_t>::max()) {
                            release_request_slot(
                                remote_request.request.request_id);
                        }
                        break;
                    }
                    if (!encode_remote_gather_request_to_buffer(
                            remote_request.request,
                            static_cast<char*>(slot_ptr), slot_desc.size_,
                            remote_request.encoded_bytes)) {
                        release_request_slot(remote_request.request.request_id);
                        remote_request_build_ok = false;
                        break;
                    }
                    RemoteGatherDoorbell doorbell;
                    doorbell.request_id = remote_request.request.request_id;
                    doorbell.descriptor_bytes = remote_request.encoded_bytes;
                    doorbell.slot_buffer_size = slot_desc.size_;
                    doorbell.slot_buffer_address = slot_desc.buffer_address_;
                    doorbell.requester_agent = local_agent;
                    if (!encode_remote_gather_doorbell(
                            doorbell, remote_request.doorbell_payload)) {
                        release_request_slot(remote_request.request.request_id);
                        remote_request_build_ok = false;
                        break;
                    }
                    unsent_request_ids.push_back(
                        remote_request.request.request_id);
                }

                if (remote_request_build_ok) {
                    for (auto& remote_request : remote_requests) {
                        int notify_rc = client->SendTransferNotifyByName(
                            remote_request.remote_agent,
                            kRemoteGatherSubmitName,
                            remote_request.doorbell_payload);
                        if (notify_rc != 0) {
                            zero_copy_ok = false;
                            fallback_to_range_read = false;
                            remote_request_build_ok = false;
                            break;
                        }
                        remote_gather_sent = true;
                        pending_request_ids.push_back(
                            remote_request.request.request_id);
                        unsent_request_ids.erase(
                            std::remove(unsent_request_ids.begin(),
                                        unsent_request_ids.end(),
                                        remote_request.request.request_id),
                            unsent_request_ids.end());
                    }
                }
                for (uint64_t request_id : unsent_request_ids) {
                    release_request_slot(request_id);
                }
                if (remote_gather_sent) {
                    fallback_to_range_read = false;
                    auto remote_client_state =
                        get_remote_gather_client_state(client);
                    bool expected_acks_registered = false;
                    if (remote_client_state != nullptr &&
                        !pending_request_ids.empty()) {
                        remote_client_state->expected_ack_count.fetch_add(
                            pending_request_ids.size(),
                            std::memory_order_relaxed);
                        remote_client_state->cv.notify_all();
                        expected_acks_registered = true;
                    }
                    std::unordered_map<uint64_t, RemoteGatherAck> ack_map;
                    ack_map.reserve(pending_request_ids.size());
                    const auto deadline =
                        std::chrono::steady_clock::now() +
                        kRemoteGatherAckTimeout;

                    while (ack_map.size() < pending_request_ids.size()) {
                        std::vector<uint64_t> ready_request_ids;
                        if (remote_client_state != nullptr) {
                            std::lock_guard<std::mutex> lock(
                                remote_client_state->mu);
                            for (uint64_t request_id : pending_request_ids) {
                                if (ack_map.count(request_id) != 0) {
                                    continue;
                                }
                                auto pending_it =
                                    remote_client_state->pending_acks.find(
                                        request_id);
                                if (pending_it ==
                                    remote_client_state->pending_acks.end()) {
                                    continue;
                                }
                                ack_map.emplace(
                                    request_id,
                                    std::move(pending_it->second.ack));
                                remote_client_state->pending_acks.erase(
                                    pending_it);
                                ready_request_ids.push_back(request_id);
                            }
                        }
                        for (uint64_t request_id : ready_request_ids) {
                            release_request_slot(request_id);
                        }
                        if (ack_map.size() >= pending_request_ids.size()) {
                            break;
                        }
                        if (std::chrono::steady_clock::now() >= deadline) {
                            zero_copy_ok = false;
                            break;
                        }

                        bool wait_for_existing_poller = false;
                        if (remote_client_state != nullptr) {
                            std::unique_lock<std::mutex> lock(
                                remote_client_state->mu);
                            if (remote_client_state->service_threads > 0 ||
                                remote_client_state->poll_in_progress) {
                                wait_for_existing_poller = true;
                                remote_client_state->cv.wait_until(
                                    lock, deadline, [&] {
                                        return ack_map.size() >=
                                                   pending_request_ids.size() ||
                                               !remote_client_state
                                                    ->pending_acks.empty() ||
                                               (!remote_client_state
                                                     ->poll_in_progress &&
                                                remote_client_state
                                                        ->service_threads ==
                                                    0);
                                    });
                            } else {
                                remote_client_state->poll_in_progress = true;
                            }
                        }
                        if (wait_for_existing_poller) {
                            continue;
                        }

                        std::vector<TransferMetadata::NotifyDesc> notifies;
                        int notify_rc = client->GetTransferNotifies(notifies);
                        if (remote_client_state != nullptr) {
                            finish_remote_gather_poll(remote_client_state);
                        }
                        if (notify_rc != 0) {
                            zero_copy_ok = false;
                            break;
                        }
                        bool consumed_ack = false;
                        for (const auto& notify : notifies) {
                            if (notify.name != kRemoteGatherAckName) {
                                continue;
                            }
                            RemoteGatherAck ack;
                            if (!decode_remote_gather_ack(
                                    notify.notify_msg, ack)) {
                                continue;
                            }
                            if (remote_client_state != nullptr) {
                                enqueue_remote_gather_ack(
                                    remote_client_state, std::move(ack));
                            } else {
                                ack_map[ack.request_id] = std::move(ack);
                                release_request_slot(ack.request_id);
                            }
                            consumed_ack = true;
                        }

                        if (!consumed_ack) {
                            std::this_thread::sleep_for(
                                kRemoteGatherPollInterval);
                        }
                    }
                    for (uint64_t request_id : pending_request_ids) {
                        if (ack_map.count(request_id) == 0) {
                            release_request_slot(request_id);
                        }
                    }
                    if (expected_acks_registered) {
                        remote_client_state->expected_ack_count.fetch_sub(
                            pending_request_ids.size(),
                            std::memory_order_relaxed);
                    }

                    if (zero_copy_ok && remote_request_build_ok) {
                        for (uint64_t request_id : pending_request_ids) {
                            auto it = ack_map.find(request_id);
                            if (it == ack_map.end() || it->second.status != 0) {
                                zero_copy_ok = false;
                                break;
                            }
                        }
                    } else {
                        zero_copy_ok = false;
                    }
                }
            }
        }

        if (zero_copy_ok && total_ranges > 0 && fallback_to_range_read) {
            std::vector<
                std::pair<Replica::Descriptor,
                          std::vector<std::tuple<size_t, size_t, size_t>>>>
                key_ranges;
            key_ranges.reserve(num_heads);

            for (int h = 0; h < num_heads; ++h) {
                if (tables[h] != nullptr || packed_ranges[h].empty()) {
                    continue;
                }

                const QueryResult& qr = query_results[h].value();
                auto replica_res = client->GetPreferredReplica(qr.replicas);
                if (!replica_res || !replica_res.value().is_memory_replica()) {
                    zero_copy_ok = false;
                    break;
                }

                char* head_base = static_cast<char*>((*table_buffers)[h]);
                size_t head_base_offset =
                    static_cast<size_t>(head_base - workspace_base);
                std::vector<std::tuple<size_t, size_t, size_t>> ranges;
                ranges.reserve(packed_ranges[h].size());
                for (const auto& range : packed_ranges[h]) {
                    size_t read_size =
                        static_cast<size_t>(range.end - range.start) *
                        row_bytes;
                    ranges.emplace_back(
                        head_base_offset + range.buffer_offset,
                        static_cast<size_t>(range.start) * row_bytes,
                        read_size);
                }
                key_ranges.emplace_back(replica_res.value(), std::move(ranges));
            }

            if (zero_copy_ok && !key_ranges.empty()) {
                ErrorCode err =
                    client->BatchTransferReadRanges(workspace_base, key_ranges);
                zero_copy_ok = (err == ErrorCode::OK);
            }
        }

        if (zero_copy_ok) {
            for (int h = 0; h < num_heads; ++h) {
                if (tables[h] != nullptr || packed_ranges[h].empty()) {
                    continue;
                }

                float* buf = static_cast<float*>((*table_buffers)[h]);
                const auto& refs = (*head_refs)[h];
                for (const auto& range : packed_ranges[h]) {
                    const float* range_base =
                        reinterpret_cast<const float*>(
                            reinterpret_cast<const char*>(buf) +
                            range.buffer_offset);
                    for (size_t i = range.ref_begin; i < range.ref_end; ++i) {
                        const auto& ref = refs[i];
                        const float* src =
                            range_base +
                            static_cast<size_t>(ref.idx - range.start) *
                                embed_D_;
                        float* dst = output + ref.output_offset;
                        std::memcpy(dst, src, row_bytes);
                    }
                }
            }
        } else {
            zero_output();
        }

        if (zero_copy_ok) {
            for (int h = 0; h < num_heads; ++h) {
                if (tables[h] == nullptr) {
                    continue;
                }

                const float* table = tables[h];
                int64_t vocab_size_h = list_of_N_[h];
                for (int b = 0; b < B; ++b) {
                    for (int l = 0; l < L; ++l) {
                        int64_t idx = hash_data[hash_index(b, l, h)];
                        if (idx < 0) idx = 0;
                        if (idx >= vocab_size_h) idx = vocab_size_h - 1;
                        float* dst =
                            output +
                            (b * L * num_heads + l * num_heads + h) *
                                embed_D_;
                        const float* src =
                            table + static_cast<size_t>(idx) * embed_D_;
                        std::memcpy(dst, src, row_bytes);
                    }
                }
            }
        }

        if (!buffers_are_pre_registered) {
            for (int h : registered_indices) {
                store_->unregister_buffer((*table_buffers)[h]);
            }
        }

        if (!zero_copy_ok) {
            return -1;
        }
        return 0;
    }
    if (!use_zero_copy) {
        bool need_fetch = false;
        for (int h = 0; h < num_heads; ++h) {
            if (tables[h] == nullptr) {
                need_fetch = true;
                break;
            }
        }
        if (need_fetch) {
            std::vector<std::shared_ptr<BufferHandle>> buffers =
                store_->batch_get_buffer(embed_keys_);
            if (buffers.size() != embed_keys_.size()) {
                return -1;
            }
            for (int h = 0; h < num_heads; ++h) {
                if (buffers[h] &&
                    buffers[h]->size() >=
                        static_cast<size_t>(list_of_N_[h]) * embed_D_ *
                            sizeof(float)) {
                    tables[h] =
                        static_cast<const float*>(buffers[h]->ptr());
                }
            }
        }
    }

    for (int h = 0; h < num_heads; ++h) {
        if (tables[h] == nullptr) {
            std::memset(output_buffer, 0, expected_size);
            return -1;
        }
    }

    for (int h = 0; h < num_heads; ++h) {
        if (tables[h] == nullptr) continue;
        const float* table = tables[h];
        int64_t vocab_size_h = list_of_N_[h];
        for (int b = 0; b < B; ++b) {
            for (int l = 0; l < L; ++l) {
                int64_t idx = hash_data[hash_index(b, l, h)];
                if (idx < 0) idx = 0;
                if (idx >= vocab_size_h) idx = vocab_size_h - 1;
                float* dst =
                    output + (b * L * num_heads + l * num_heads + h) * embed_D_;
                const float* src = table + idx * embed_D_;
                std::memcpy(dst, src, embed_D_ * sizeof(float));
            }
        }
    }
    return 0;
}

int Engram::embedding_populate_from_tensors(
    const std::vector<void*>& embedding_data,
    const std::vector<size_t>& sizes) {
    if (embedding_data.size() != embed_keys_.size() ||
        sizes.size() != embed_keys_.size()) {
        return -1;
    }

    for (size_t i = 0; i < sizes.size(); ++i) {
        size_t expected =
            static_cast<size_t>(list_of_N_[i]) * embed_D_ * sizeof(float);
        if (sizes[i] < expected) {
            return -1;
        }
    }

    for (size_t i = 0; i < embedding_data.size(); ++i) {
        int ret = store_->register_buffer(embedding_data[i], sizes[i]);
        if (ret != 0) {
            for (size_t j = 0; j < i; ++j)
                store_->unregister_buffer(embedding_data[j]);
            return -1;
        }
    }

    std::vector<int> put_results =
        store_->batch_put_from(embed_keys_, embedding_data, sizes);
    bool ok = std::all_of(put_results.begin(), put_results.end(),
                          [](int r) { return r == 0; });

    for (void* buf : embedding_data) store_->unregister_buffer(buf);
    return ok ? 0 : -1;
}

// -----------------------------------------------------------------------------
// Engram: constructor, query, populate
// -----------------------------------------------------------------------------

Engram::Engram(int layer_id, const EngramConfig& config,
               const BackboneConfig& backbone_cfg,
               std::shared_ptr<PyClient> store)
    : layer_id_(layer_id),
      config_(config),
      backbone_cfg_(backbone_cfg),
      store_(store),
      vocab_size_per_ngram_(config.engram_vocab_size),
      max_ngram_size_(config.max_ngram_size),
      n_embed_per_ngram_(config.n_embed_per_ngram),
      n_head_per_ngram_(config.n_head_per_ngram),
      pad_id_(config.pad_id),
      layer_ids_(config.layer_ids),
      tokenizer_vocab_size_(static_cast<int>(
          std::max(*std::max_element(config.engram_vocab_size.begin(),
                                     config.engram_vocab_size.end()) *
                       2,
                   static_cast<int64_t>(1)))) {
    const int64_t PRIME_1 = 10007;
    const int64_t max_long = std::numeric_limits<int64_t>::max();
    const int64_t M_max = max_long / std::max(1, tokenizer_vocab_size_);
    const int64_t half_bound = std::max(static_cast<int64_t>(1), M_max / 2);

    for (int lid : layer_ids_) {
        std::mt19937_64 gen(static_cast<uint64_t>(config.seed + PRIME_1 * lid));
        std::uniform_int_distribution<int64_t> dis(0, half_bound - 1);
        std::vector<int64_t> multipliers;
        for (int i = 0; i < max_ngram_size_; ++i) {
            multipliers.push_back(dis(gen) * 2 + 1);
        }
        layer_multipliers_[lid] = multipliers;
    }

    std::unordered_set<int64_t> seen_primes;
    for (int lid : layer_ids_) {
        std::vector<std::vector<int64_t>> all_ngram;
        for (int n = 2; n <= max_ngram_size_; ++n) {
            std::vector<int64_t> heads;
            int64_t vocab = vocab_size_per_ngram_[n - 2];
            int64_t start = std::max(static_cast<int64_t>(0), vocab - 1);
            for (int h = 0; h < n_head_per_ngram_; ++h) {
                int64_t p = find_next_prime(start, seen_primes);
                seen_primes.insert(p);
                heads.push_back(p);
                start = p;
            }
            all_ngram.push_back(heads);
        }
        vocab_size_across_layers_[lid] = all_ngram;
    }

    const auto& vs = vocab_size_across_layers_[layer_id];
    for (const auto& ngram_sizes : vs) {
        for (int64_t s : ngram_sizes) {
            list_of_N_.push_back(s);
        }
    }
    if (list_of_N_.empty()) {
        for (int n = 2; n <= config.max_ngram_size; ++n) {
            for (int h = 0; h < config.n_head_per_ngram; ++h) {
                list_of_N_.push_back(config.engram_vocab_size[n - 2]);
            }
        }
    }

    embed_D_ = config.n_embed_per_ngram / config.n_head_per_ngram;
    for (size_t h = 0; h < list_of_N_.size(); ++h) {
        std::ostringstream oss;
        oss << "engram:l" << layer_id_ << ":h" << h;
        embed_keys_.push_back(oss.str());
    }
    if (store_ != nullptr) {
        local_agent_name_ = store_->get_hostname();
        auto client = store_->get_client_shared();
        if (client != nullptr) {
            store_protocol_ = client->GetProtocol();
        }
    }
}

Engram::~Engram() {
    stop_remote_gather_service();
    release_remote_descriptor_workspace();
    release_remote_gather_workspace();
    release_request_ring();
    release_internal_workspace();
}

size_t Engram::get_embedding_tables_workspace_size() const {
    size_t total = 0;
    for (int64_t N : list_of_N_) {
        total += static_cast<size_t>(N) * embed_D_ * sizeof(float);
    }
    return total;
}

std::vector<size_t> Engram::get_query_table_buffer_sizes(int B, int L) const {
    const int64_t max_rows_per_head =
        std::max<int64_t>(1, static_cast<int64_t>(B) * L);
    const size_t row_bytes = static_cast<size_t>(embed_D_) * sizeof(float);

    std::vector<size_t> sizes;
    sizes.reserve(list_of_N_.size());
    for (int64_t N_h : list_of_N_) {
        const int64_t rows = std::min<int64_t>(N_h, max_rows_per_head);
        sizes.push_back(static_cast<size_t>(rows) * row_bytes);
    }
    return sizes;
}

size_t Engram::get_query_workspace_size(int B, int L) const {
    size_t total = 0;
    for (size_t sz : get_query_table_buffer_sizes(B, L)) {
        total += sz;
    }
    return total;
}

int Engram::ensure_internal_workspace(int B, int L) const {
    if (store_ == nullptr) return -1;

    const std::vector<size_t> table_sizes = get_query_table_buffer_sizes(B, L);
    const size_t required_bytes = std::accumulate(
        table_sizes.begin(), table_sizes.end(), static_cast<size_t>(0));

    bool reuse_existing =
        internal_workspace_registered_ &&
        internal_workspace_.size() >= required_bytes &&
        internal_table_sizes_.size() == table_sizes.size();
    if (reuse_existing) {
        for (size_t i = 0; i < table_sizes.size(); ++i) {
            if (internal_table_sizes_[i] < table_sizes[i]) {
                reuse_existing = false;
                break;
            }
        }
    }
    if (reuse_existing) return 0;

    release_internal_workspace();

    internal_workspace_.assign(required_bytes, 0);
    internal_table_buffers_.clear();
    internal_table_sizes_ = table_sizes;
    internal_table_buffers_.reserve(table_sizes.size());

    char* base = internal_workspace_.data();
    for (size_t sz : table_sizes) {
        internal_table_buffers_.push_back(base);
        base += sz;
    }

    for (size_t i = 0; i < internal_table_buffers_.size(); ++i) {
        int ret =
            store_->register_buffer(internal_table_buffers_[i], table_sizes[i]);
        if (ret != 0) {
            for (size_t j = 0; j < i; ++j) {
                store_->unregister_buffer(internal_table_buffers_[j]);
            }
            internal_workspace_.clear();
            internal_table_buffers_.clear();
            internal_table_sizes_.clear();
            internal_workspace_registered_ = false;
            return -1;
        }
    }

    internal_workspace_registered_ = true;
    return 0;
}

void Engram::release_internal_workspace() const {
    if (store_ != nullptr && internal_workspace_registered_ &&
        store_->has_live_client()) {
        for (void* buf : internal_table_buffers_) {
            store_->unregister_buffer(buf);
        }
    }
    internal_workspace_registered_ = false;
    internal_workspace_.clear();
    internal_table_buffers_.clear();
    internal_table_sizes_.clear();
}

std::vector<tl::expected<QueryResult, ErrorCode>> Engram::get_query_results()
    const {
    if (store_ == nullptr) return {};

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(query_result_cache_mu_);
        if (query_result_cache_valid_ &&
            cached_query_results_.size() == embed_keys_.size()) {
            bool cache_valid = true;
            for (const auto& result : cached_query_results_) {
                if (result.lease_timeout <= now) {
                    cache_valid = false;
                    break;
                }
            }
            if (cache_valid) {
                std::vector<tl::expected<QueryResult, ErrorCode>> query_results;
                query_results.reserve(cached_query_results_.size());
                for (const auto& result : cached_query_results_) {
                    query_results.emplace_back(result);
                }
                return query_results;
            }
        }
    }

    auto query_results = store_->batch_query(embed_keys_);
    {
        std::lock_guard<std::mutex> lock(query_result_cache_mu_);
        cached_query_results_.clear();
        query_result_cache_valid_ = false;
        bool can_cache = (query_results.size() == embed_keys_.size());
        if (can_cache) {
            cached_query_results_.reserve(query_results.size());
            for (const auto& result : query_results) {
                if (!result) {
                    can_cache = false;
                    break;
                }
                cached_query_results_.push_back(result.value());
            }
        }
        if (can_cache) {
            query_result_cache_valid_ = true;
        } else {
            cached_query_results_.clear();
        }
    }
    return query_results;
}

void Engram::invalidate_query_result_cache() const {
    std::lock_guard<std::mutex> lock(query_result_cache_mu_);
    cached_query_results_.clear();
    query_result_cache_valid_ = false;
}

std::vector<std::vector<std::vector<int64_t>>> Engram::hash_input_ids(
    const std::vector<std::vector<int64_t>>& input_ids) const {
    const int B = static_cast<int>(input_ids.size());
    if (B == 0) {
        return {};
    }
    const int L = static_cast<int>(input_ids[0].size());
    const int num_heads = static_cast<int>(list_of_N_.size());

    std::vector<int64_t> flat_hash_ids;
    hash_flat_(input_ids, layer_id_, flat_hash_ids, nullptr);

    std::vector<std::vector<std::vector<int64_t>>> result(
        B,
        std::vector<std::vector<int64_t>>(L, std::vector<int64_t>(num_heads)));
    for (int b = 0; b < B; ++b) {
        for (int l = 0; l < L; ++l) {
            const size_t offset =
                (static_cast<size_t>(b) * L + l) * num_heads;
            std::copy_n(flat_hash_ids.begin() + offset, num_heads,
                        result[b][l].begin());
        }
    }
    return result;
}

std::vector<int64_t> Engram::get_table_vocab_sizes() const {
    return list_of_N_;
}

std::vector<std::string> Engram::get_store_keys() const {
    return embed_keys_;
}

int Engram::get_num_heads() const {
    return static_cast<int>(list_of_N_.size());
}

int Engram::get_embedding_dim() const {
    return embed_D_;
}

int Engram::remove_from_store(bool force) {
    if (store_ == nullptr) {
        return static_cast<int>(ErrorCode::INVALID_PARAMS);
    }

    constexpr int kObjectNotFound =
        static_cast<int>(ErrorCode::OBJECT_NOT_FOUND);
    int removed = 0;
    int first_error = 0;

    for (const auto& key : embed_keys_) {
        int exists = store_->isExist(key);
        if (exists < 0) {
            if (first_error == 0) first_error = exists;
            continue;
        }
        if (exists == 0) {
            continue;
        }

        int rc = store_->remove(key, force);
        if (rc == 0) {
            removed++;
            continue;
        }
        if (rc == kObjectNotFound) {
            continue;
        }
        if (first_error == 0) first_error = rc;
    }

    invalidate_query_result_cache();
    return first_error != 0 ? first_error : removed;
}

int Engram::query_embeddings(
    const std::vector<std::vector<int64_t>>& input_ids, void* output,
    size_t output_size, void* workspace, size_t workspace_size) const {
    if (output == nullptr || store_ == nullptr) {
        return -1;
    }

    int B = static_cast<int>(input_ids.size());
    if (B == 0) {
        return -1;
    }
    int L = static_cast<int>(input_ids[0].size());

    size_t expected = static_cast<size_t>(B) * L * list_of_N_.size() *
                      embed_D_ * sizeof(float);
    if (output_size < expected) {
        return -1;
    }

    std::unique_lock<std::mutex> internal_workspace_lock;
    std::vector<void*> table_buffers;
    std::vector<size_t> table_sizes;
    bool buffers_are_pre_registered = false;
    const size_t query_workspace_bytes = get_query_workspace_size(B, L);
    const bool caller_workspace_ready =
        (workspace != nullptr && workspace_size >= query_workspace_bytes);
    if (store_ != nullptr) {
        if (caller_workspace_ready) {
            internal_workspace_lock = std::unique_lock<std::mutex>(
                internal_workspace_mu_, std::try_to_lock);
        } else {
            internal_workspace_lock =
                std::unique_lock<std::mutex>(internal_workspace_mu_);
        }
        if (internal_workspace_lock.owns_lock() &&
            ensure_internal_workspace(B, L) == 0) {
            table_buffers = internal_table_buffers_;
            table_sizes = internal_table_sizes_;
            buffers_are_pre_registered = true;
        }
    }

    if (table_buffers.empty() && caller_workspace_ready) {
        char* tables_base = static_cast<char*>(workspace);
        table_sizes = get_query_table_buffer_sizes(B, L);
        table_buffers.reserve(table_sizes.size());
        for (size_t sz : table_sizes) {
            table_buffers.push_back(tables_base);
            tables_base += sz;
        }
    }

    const bool prepare_head_refs = !table_buffers.empty();
    std::vector<int64_t> flat_hash_ids;
    std::vector<std::vector<LookupRowRef>> head_refs;
    hash_flat_(input_ids, layer_id_, flat_hash_ids,
               prepare_head_refs ? &head_refs : nullptr);

    const std::vector<void*>* tbl_bufs =
        table_buffers.empty() ? nullptr : &table_buffers;
    const std::vector<size_t>* tbl_szs =
        table_sizes.empty() ? nullptr : &table_sizes;

    return embedding_lookup(flat_hash_ids, B, L, output, output_size, tbl_bufs,
                            tbl_szs, buffers_are_pre_registered,
                            head_refs.empty() ? nullptr : &head_refs);
}

int Engram::populate_store_from_buffers(
    const std::vector<void*>& embedding_buffers,
    const std::vector<size_t>& buffer_sizes) {
    if (store_ == nullptr) return -1;
    int ret = embedding_populate_from_tensors(embedding_buffers, buffer_sizes);
    if (ret == 0) {
        invalidate_query_result_cache();
        ensure_remote_gather_service();
    }
    return ret;
}

}  // namespace engram
}  // namespace mooncake
