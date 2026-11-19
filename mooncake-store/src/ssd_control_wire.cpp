// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_control_wire.h"
#include <limits>
#include <type_traits>

namespace mooncake::ssd_offload {
namespace {
constexpr uint32_t magic = 0x53534443, reply_magic = magic | 0x80000000;
bool ValidPeer(PeerIdentity p) {
    return (p.client_id_high || p.client_id_low) && p.boot_epoch;
}
bool HasRequest(SsdControlCommand c) {
    return c == SsdControlCommand::kSubmit ||
           c == SsdControlCommand::kResolve ||
           c == SsdControlCommand::kReadInto ||
           c == SsdControlCommand::kRecoverReadInto;
}
bool KnownCommand(uint32_t c) { return c >= 1 && c <= 6; }
class IO {
   public:
    explicit IO(std::string_view input) : input(input) {}
    IO() : writing(true) {}
    void Fail(DestinationError e = DestinationError::kInvalidArgument) {
        if (error == DestinationError::kOk) error = e;
    }
    bool Room(size_t n) {
        if (error != DestinationError::kOk) return false;
        if (n > (writing ? kSsdControlMaxWireBytes - output.size()
                         : input.size() - position)) {
            Fail(writing ? DestinationError::kLimit
                         : DestinationError::kInvalidArgument);
            return false;
        }
        return true;
    }
    template <typename T>
    void Number(T& value) {
        static_assert(std::is_unsigned_v<T>);
        if (!Room(sizeof(T))) return;
        if (writing)
            for (size_t i = 0; i < sizeof(T); ++i)
                output.push_back(char(value >> (i * 8)));
        else {
            value = 0;
            for (size_t i = 0; i < sizeof(T); ++i)
                value |= T(static_cast<unsigned char>(input[position++]))
                         << (i * 8);
        }
    }
    void Flag(bool& value) {
        uint8_t byte = value ? 1 : 0;
        Number(byte);
        if (byte > 1) Fail();
        value = byte == 1;
    }
    void Count(uint32_t& count, size_t cap, size_t minimum_bytes) {
        Number(count);
        if (count > cap) Fail(DestinationError::kLimit);
        if (!writing && minimum_bytes &&
            count > (input.size() - position) / minimum_bytes)
            Fail();
    }
    void String(std::string_view& value, size_t cap) {
        if (writing && value.size() > UINT32_MAX) {
            Fail(DestinationError::kLimit);
            return;
        }
        uint32_t length = writing ? uint32_t(value.size()) : 0;
        Number(length);
        if (length > cap) {
            Fail(DestinationError::kLimit);
            return;
        }
        if (!Room(length)) return;
        if (writing)
            output.append(value);
        else {
            value = input.substr(position, length);
            position += length;
        }
    }
    bool writing{false};
    DestinationError error{DestinationError::kOk};
    std::string_view input;
    size_t position{0};
    std::string output;
};
void Peer(IO& io, PeerIdentity& value) {
    io.Number(value.client_id_high);
    io.Number(value.client_id_low);
    io.Number(value.boot_epoch);
    if (!ValidPeer(value)) io.Fail();
}
void Attempt(IO& io, AttemptIdentity& value) {
    Peer(io, value.owner);
    io.Number(value.request_id);
    io.Number(value.attempt_id);
    if (!value.request_id || !value.attempt_id) io.Fail();
}
void Credential(IO& io, WriteCredential& value) {
    io.Number(value.high);
    io.Number(value.low);
    if (value == WriteCredential{}) io.Fail(DestinationError::kUnauthorized);
}
void Grant(IO& io, SsdGrantReferenceV1& value) {
    Peer(io, value.requester);
    Attempt(io, value.attempt);
    Credential(io, value.credential);
}
void Header(IO& io, bool reply, SsdControlCommand& command) {
    uint32_t m = reply ? reply_magic : magic, version = 1,
             c = uint32_t(command);
    io.Number(m);
    io.Number(version);
    io.Number(c);
    if (m != (reply ? reply_magic : magic)) io.Fail();
    if (version != 1) io.Fail(DestinationError::kNotSupported);
    if (!KnownCommand(c)) io.Fail();
    command = SsdControlCommand(c);
}
void CheckLimits(IO& io, SsdReadProtocolLimits limits) {
    if (!limits.max_objects || limits.max_objects > 65536 ||
        !limits.max_ranges || limits.max_ranges > 1048576 ||
        !limits.max_fragments || limits.max_fragments > 1048576 ||
        !limits.max_key_bytes || !limits.max_payload_bytes ||
        !limits.max_timeout_ms)
        io.Fail();
}
void Request(IO& io, const SsdReadRequestV1* source,
             SsdReadRequestV1* destination, SsdReadProtocolLimits limits,
             const SsdGrantReferenceV1& grant) {
    uint32_t version = source ? source->protocol_version : 0,
             priority = source ? source->priority : 0;
    PeerIdentity requester = source ? source->requester : PeerIdentity{};
    AttemptIdentity attempt = source ? source->attempt : AttemptIdentity{};
    uint64_t timeout = source ? source->relative_timeout_ms : 0;
    io.Number(version);
    Peer(io, requester);
    Attempt(io, attempt);
    if (version != 1) io.Fail(DestinationError::kNotSupported);
    if (requester != grant.requester || attempt != grant.attempt)
        io.Fail(DestinationError::kWrongAttempt);
    std::string_view tenant =
        source ? std::string_view(source->tenant_id) : std::string_view{};
    io.String(tenant, std::min(size_t{256}, limits.max_key_bytes));
    if (tenant.empty()) io.Fail();
    io.Number(timeout);
    io.Number(priority);
    if (!timeout || priority > 2) io.Fail();
    if (timeout > limits.max_timeout_ms) io.Fail(DestinationError::kLimit);
    uint32_t count = source ? uint32_t(source->objects.size()) : 0;
    io.Count(count, limits.max_objects, 18);
    if (!count) io.Fail();
    if (io.error != DestinationError::kOk) return;
    if (destination) {
        destination->protocol_version = version;
        destination->requester = requester;
        destination->attempt = attempt;
        destination->tenant_id = tenant;
        destination->relative_timeout_ms = timeout;
        destination->priority = priority;
        destination->objects.resize(count);
    }
    size_t key_bytes = tenant.size(), ranges = 0, fragments = 0;
    uint64_t payload = 0;
    for (size_t i = 0; i < count && io.error == DestinationError::kOk; ++i) {
        const auto* object = source ? &source->objects[i] : nullptr;
        auto* decoded = destination ? &destination->objects[i] : nullptr;
        std::string_view key =
            object ? std::string_view(object->key) : std::string_view{};
        io.String(key, limits.max_key_bytes - key_bytes);
        key_bytes += key.size();
        if (key.empty()) io.Fail();
        uint64_t size = object ? object->expected_object_size : 0;
        io.Number(size);
        if (size > uint64_t(INT64_MAX)) io.Fail();
        bool has_checksum =
            object && object->expected_object_checksum.has_value();
        uint64_t checksum =
            has_checksum ? *object->expected_object_checksum : 0;
        io.Flag(has_checksum);
        if (has_checksum) io.Number(checksum);
        uint32_t range_count = object ? uint32_t(object->ranges.size()) : 0;
        io.Count(range_count, limits.max_ranges - ranges, 12);
        ranges += range_count;
        if (io.error != DestinationError::kOk) return;
        if (decoded) {
            decoded->key = key;
            decoded->expected_object_size = size;
            if (has_checksum) decoded->expected_object_checksum = checksum;
            decoded->ranges.resize(range_count);
        }
        for (size_t j = 0; j < range_count && io.error == DestinationError::kOk;
             ++j) {
            const auto* range = object ? &object->ranges[j] : nullptr;
            auto* decoded_range = decoded ? &decoded->ranges[j] : nullptr;
            uint64_t offset = range ? range->source_offset : 0, range_bytes = 0;
            io.Number(offset);
            if (offset > size) io.Fail();
            uint32_t destination_count =
                range ? uint32_t(range->destinations.size()) : 0;
            io.Count(destination_count, limits.max_fragments - fragments, 72);
            fragments += destination_count;
            if (io.error != DestinationError::kOk) return;
            if (decoded_range) {
                decoded_range->source_offset = offset;
                decoded_range->destinations.resize(destination_count);
            }
            for (size_t k = 0;
                 k < destination_count && io.error == DestinationError::kOk;
                 ++k) {
                RegisteredDestination value =
                    range ? range->destinations[k] : RegisteredDestination{};
                Peer(io, value.region.requester);
                io.Number(value.region.region_id);
                io.Number(value.region.generation);
                Credential(io, value.region.credential);
                io.Number(value.offset);
                io.Number(value.length);
                if (value.region.requester != requester ||
                    !value.region.region_id || !value.region.generation)
                    io.Fail();
                if (value.length > UINT64_MAX - value.offset)
                    io.Fail(DestinationError::kOverflow);
                if (value.length > size - offset - range_bytes) io.Fail();
                if (value.length > limits.max_payload_bytes - payload)
                    io.Fail(DestinationError::kLimit);
                if (io.error != DestinationError::kOk) return;
                range_bytes += value.length;
                payload += value.length;
                if (decoded_range) decoded_range->destinations[k] = value;
            }
        }
    }
}
void Result(IO& io, const SsdReadResultV1* source, SsdReadResultV1* destination,
            SsdReadProtocolLimits limits) {
    uint32_t version = source ? source->protocol_version : 0;
    AttemptIdentity attempt = source ? source->attempt : AttemptIdentity{};
    PeerIdentity requester = source ? source->requester : PeerIdentity{};
    bool drained = source && source->dma_drained;
    io.Number(version);
    Attempt(io, attempt);
    Peer(io, requester);
    io.Flag(drained);
    if (version != 1) io.Fail(DestinationError::kNotSupported);
    if (!drained) io.Fail(DestinationError::kNotDrained);
    uint32_t count = source ? uint32_t(source->objects.size()) : 0;
    io.Count(count, limits.max_objects, 21);
    if (io.error != DestinationError::kOk) return;
    if (destination) {
        destination->protocol_version = version;
        destination->attempt = attempt;
        destination->requester = requester;
        destination->dma_drained = drained;
        destination->objects.resize(count);
    }
    uint64_t bytes = 0;
    for (size_t i = 0; i < count && io.error == DestinationError::kOk; ++i) {
        SsdObjectReadResultV1 object =
            source ? source->objects[i] : SsdObjectReadResultV1{};
        uint32_t outcome = uint32_t(object.outcome);
        io.Number(object.original_index);
        io.Number(outcome);
        io.Number(object.valid_bytes);
        bool checksum =
            source && source->objects[i].object_checksum.has_value();
        uint64_t checksum_value =
            checksum ? *source->objects[i].object_checksum : 0;
        io.Flag(checksum);
        if (checksum) io.Number(checksum_value);
        if (outcome > uint32_t(AttemptOutcome::kRejected) ||
            object.original_index >= count)
            io.Fail();
        if (object.valid_bytes > limits.max_payload_bytes - bytes)
            io.Fail(DestinationError::kLimit);
        if (io.error != DestinationError::kOk) return;
        bytes += object.valid_bytes;
        object.outcome = AttemptOutcome(outcome);
        if (checksum) object.object_checksum = checksum_value;
        if (destination) destination->objects[i] = object;
    }
}
void Views(IO& io, const std::vector<ResolvedDestination>* source,
           std::vector<ResolvedDestination>* destination,
           SsdReadProtocolLimits limits) {
    uint32_t count = source ? uint32_t(source->size()) : 0;
    io.Count(count, limits.max_fragments, 25);
    if (io.error != DestinationError::kOk) return;
    if (destination) destination->resize(count);
    uint64_t bytes = 0;
    for (size_t i = 0; i < count && io.error == DestinationError::kOk; ++i) {
        const auto* view = source ? &(*source)[i] : nullptr;
        std::string_view endpoint =
            view ? std::string_view(view->transport_endpoint)
                 : std::string_view{};
        uint64_t address = view ? view->address : 0,
                 length = view ? view->length : 0;
        uint32_t memory = view ? uint32_t(view->memory) : 0;
        io.String(endpoint, 4096);
        io.Number(address);
        io.Number(length);
        io.Number(memory);
        if (endpoint.empty() || endpoint.find('\0') != std::string_view::npos ||
            !address || memory > uint32_t(DestinationMemory::kDevice))
            io.Fail();
        if (length > UINT64_MAX - address) io.Fail(DestinationError::kOverflow);
        if (length > limits.max_payload_bytes - bytes)
            io.Fail(DestinationError::kLimit);
        if (io.error != DestinationError::kOk) return;
        bytes += length;
        if (destination)
            (*destination)[i] = {std::string(endpoint), address, length,
                                 DestinationMemory(memory)};
    }
}
void Message(IO& io, const SsdControlMessage* source,
             SsdControlMessage* destination, SsdReadProtocolLimits limits) {
    CheckLimits(io, limits);
    auto command = source ? source->command : SsdControlCommand::kQuery;
    auto grant = source ? source->reference : SsdGrantReferenceV1{};
    Header(io, false, command);
    Grant(io, grant);
    if (io.error != DestinationError::kOk) return;
    if (source && source->request.has_value() != HasRequest(command)) {
        io.Fail();
        return;
    }
    if (destination) {
        destination->command = command;
        destination->reference = grant;
    }
    if (HasRequest(command)) {
        if (destination) destination->request.emplace();
        Request(io, source ? &*source->request : nullptr,
                destination ? &*destination->request : nullptr, limits, grant);
    }
    const bool direct = command == SsdControlCommand::kReadInto ||
                        command == SsdControlCommand::kRecoverReadInto;
    if (direct)
        Views(io, source ? &source->destinations : nullptr,
              destination ? &destination->destinations : nullptr, limits);
    else if (source && !source->destinations.empty())
        io.Fail();
}
void Reply(IO& io, const SsdControlReply* source, SsdControlReply* destination,
           SsdReadProtocolLimits limits) {
    CheckLimits(io, limits);
    auto command = source ? source->command : SsdControlCommand::kQuery;
    uint32_t error = source ? uint32_t(source->error) : 0;
    Header(io, true, command);
    io.Number(error);
    if (error > uint32_t(DestinationError::kTransportError)) io.Fail();
    bool snapshot = source && source->snapshot.has_value();
    io.Flag(snapshot);
    if (destination) {
        destination->command = command;
        destination->error = DestinationError(error);
    }
    if (snapshot) {
        uint32_t state = source ? uint32_t(source->snapshot->state) : 0;
        bool result = source && source->snapshot->result.has_value();
        io.Number(state);
        io.Flag(result);
        if (state > uint32_t(SsdOwnerRequestState::kDrained) ||
            result != (state == uint32_t(SsdOwnerRequestState::kDrained)))
            io.Fail();
        if (destination)
            destination->snapshot.emplace(SsdOwnerRequestSnapshot{
                DestinationError::kOk, SsdOwnerRequestState(state), {}});
        if (result) {
            if (destination) destination->snapshot->result.emplace();
            Result(io, source ? &*source->snapshot->result : nullptr,
                   destination ? &*destination->snapshot->result : nullptr,
                   limits);
        }
    }
    uint32_t count = source ? uint32_t(source->destinations.size()) : 0;
    io.Count(count, limits.max_fragments, 25);
    if (io.error != DestinationError::kOk) return;
    if (destination) destination->destinations.resize(count);
    uint64_t bytes = 0;
    for (size_t i = 0; i < count && io.error == DestinationError::kOk; ++i) {
        const auto* view = source ? &source->destinations[i] : nullptr;
        std::string_view endpoint =
            view ? std::string_view(view->transport_endpoint)
                 : std::string_view{};
        uint64_t address = view ? view->address : 0,
                 length = view ? view->length : 0;
        uint32_t memory = view ? uint32_t(view->memory) : 0;
        io.String(endpoint, 4096);
        io.Number(address);
        io.Number(length);
        io.Number(memory);
        if (endpoint.empty() || endpoint.find('\0') != std::string_view::npos ||
            !address || memory > uint32_t(DestinationMemory::kDevice))
            io.Fail();
        if (length > UINT64_MAX - address) io.Fail(DestinationError::kOverflow);
        if (length > limits.max_payload_bytes - bytes)
            io.Fail(DestinationError::kLimit);
        if (io.error != DestinationError::kOk) return;
        bytes += length;
        if (destination)
            destination->destinations[i] = {std::string(endpoint), address,
                                            length, DestinationMemory(memory)};
    }
    const bool success = error == uint32_t(DestinationError::kOk),
               duplicate = error == uint32_t(DestinationError::kDuplicate);
    if (command == SsdControlCommand::kResolve) {
        if (snapshot || (!success && count)) io.Fail();
    } else if (command == SsdControlCommand::kForget) {
        if (snapshot || count) io.Fail();
    } else if (count ||
               snapshot !=
                   (success || ((command == SsdControlCommand::kSubmit ||
                                 command == SsdControlCommand::kReadInto) &&
                                duplicate)))
        io.Fail();
}
template <typename T, typename Walk>
DestinationError Decode(std::string_view bytes, T& output,
                        SsdReadProtocolLimits limits, Walk walk) {
    if (bytes.size() > kSsdControlMaxWireBytes) return DestinationError::kLimit;
    IO preflight(bytes);
    walk(preflight, nullptr, nullptr, limits);
    if (preflight.error != DestinationError::kOk) return preflight.error;
    if (preflight.position != bytes.size())
        return DestinationError::kInvalidArgument;
    T value;
    IO reader(bytes);
    walk(reader, nullptr, &value, limits);
    if (reader.error != DestinationError::kOk) return reader.error;
    if constexpr (std::is_same_v<T, SsdControlMessage>) {
        if (value.request) {
            auto error = ValidateSsdReadRequestV1(
                *value.request, value.reference.attempt.owner, limits);
            if (error != DestinationError::kOk) return error;
        }
        if (value.command == SsdControlCommand::kReadInto ||
            value.command == SsdControlCommand::kRecoverReadInto) {
            size_t index = 0;
            for (const auto& object : value.request->objects)
                for (const auto& range : object.ranges)
                    for (const auto& d : range.destinations)
                        if (index >= value.destinations.size() ||
                            value.destinations[index++].length != d.length)
                            return DestinationError::kInvalidArgument;
            if (index != value.destinations.size())
                return DestinationError::kInvalidArgument;
        }
    }
    output = std::move(value);
    return DestinationError::kOk;
}
}  // namespace
SsdControlEncoded EncodeSsdControlMessage(const SsdControlMessage& message,
                                          SsdReadProtocolLimits limits) {
    if (message.request) {
        const auto error = ValidateSsdReadRequestV1(
            *message.request, message.reference.attempt.owner, limits);
        if (error != DestinationError::kOk) return {error, {}};
    }
    if (message.command == SsdControlCommand::kReadInto ||
        message.command == SsdControlCommand::kRecoverReadInto) {
        if (!message.request) return {DestinationError::kInvalidArgument, {}};
        size_t index = 0;
        for (const auto& object : message.request->objects)
            for (const auto& range : object.ranges)
                for (const auto& d : range.destinations)
                    if (index >= message.destinations.size() ||
                        message.destinations[index++].length != d.length)
                        return {DestinationError::kInvalidArgument, {}};
        if (index != message.destinations.size())
            return {DestinationError::kInvalidArgument, {}};
    }
    IO writer;
    Message(writer, &message, nullptr, limits);
    return {writer.error, writer.error == DestinationError::kOk
                              ? std::move(writer.output)
                              : std::string{}};
}
SsdControlEncoded EncodeSsdControlReply(const SsdControlReply& reply,
                                        SsdReadProtocolLimits limits) {
    IO writer;
    Reply(writer, &reply, nullptr, limits);
    return {writer.error, writer.error == DestinationError::kOk
                              ? std::move(writer.output)
                              : std::string{}};
}
DestinationError DecodeSsdControlMessage(std::string_view bytes,
                                         SsdControlMessage& message,
                                         SsdReadProtocolLimits limits) {
    return Decode(bytes, message, limits, Message);
}
DestinationError DecodeSsdControlReply(std::string_view bytes,
                                       SsdControlReply& reply,
                                       SsdReadProtocolLimits limits) {
    return Decode(bytes, reply, limits, Reply);
}
}  // namespace mooncake::ssd_offload
