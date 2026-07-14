// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_protocol.h"

#include <array>
#include <limits>
#include <utility>

namespace mooncake::tent {
namespace {

constexpr uint32_t kPullRequestMagic = 0x54435251;   // "TCRQ"
constexpr uint32_t kPullResponseMagic = 0x54435250;  // "TCRP"

void append16(std::string& output, uint16_t value) {
    output.push_back(static_cast<char>(value >> 8));
    output.push_back(static_cast<char>(value));
}

void append32(std::string& output, uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8)
        output.push_back(static_cast<char>(value >> shift));
}

void append64(std::string& output, uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8)
        output.push_back(static_cast<char>(value >> shift));
}

uint16_t read16(std::string_view input, size_t& position) {
    uint16_t value = 0;
    for (int i = 0; i < 2; ++i)
        value = static_cast<uint16_t>((value << 8) |
                                      static_cast<uint8_t>(input[position++]));
    return value;
}

uint32_t read32(std::string_view input, size_t& position) {
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i)
        value = (value << 8) | static_cast<uint8_t>(input[position++]);
    return value;
}

uint64_t read64(std::string_view input, size_t& position) {
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i)
        value = (value << 8) | static_cast<uint8_t>(input[position++]);
    return value;
}

bool isKnownResource(CreditResource resource, size_t& index) {
    const uint16_t raw = static_cast<uint16_t>(resource);
    if (raw < 1 || raw > kCreditResourceCount) return false;
    index = raw - 1;
    return true;
}

bool isKnownStatus(ReceiverCreditPullStatus status) {
    switch (status) {
        case ReceiverCreditPullStatus::Granted:
        case ReceiverCreditPullStatus::Retry:
        case ReceiverCreditPullStatus::SessionChanged:
        case ReceiverCreditPullStatus::Unsupported:
        case ReceiverCreditPullStatus::Rejected:
            return true;
    }
    return false;
}

bool isZeroPayload(const ReceiverCreditPullResponseV1& response) {
    const auto& activation = response.activation;
    const auto& update = response.update;
    return activation.schema_version == 0 && activation.chosen_version == 0 &&
           activation.receiver_session_id.high == 0 &&
           activation.receiver_session_id.low == 0 && activation.epoch == 0 &&
           activation.freshness_ttl_ms == 0 && update.schema_version == 0 &&
           update.flags == 0 && update.qos_class == 0 &&
           update.receiver_session_id.high == 0 &&
           update.receiver_session_id.low == 0 && update.epoch == 0 &&
           update.sequence == 0 && update.freshness_ttl_ms == 0 &&
           update.grants.empty();
}

}  // namespace

Status ReceiverCreditPullRequestCodecV1::validate(
    const ReceiverCreditPullRequestV1& request) {
    if (request.schema_version != 1 || request.flags != 0 ||
        request.sender_peer == 0 || request.request_sequence == 0 ||
        request.resources.empty() ||
        request.resources.size() > kCreditResourceCount)
        return Status::InvalidArgument(
            "invalid receiver credit pull request header" LOC_MARK);

    const bool zero_session = request.expected_receiver_session_id.high == 0 &&
                              request.expected_receiver_session_id.low == 0;
    if (zero_session != (request.expected_epoch == 0))
        return Status::InvalidArgument(
            "partial receiver credit session expectation" LOC_MARK);

    std::array<bool, kCreditResourceCount> present{};
    for (const auto& usage : request.resources) {
        size_t index = 0;
        if (!isKnownResource(usage.resource, index) || present[index])
            return Status::InvalidArgument(
                "invalid or duplicate receiver credit pull resource" LOC_MARK);
        present[index] = true;
        if (usage.completed_total > usage.consumed_total ||
            usage.minimum_available > usage.desired_available ||
            usage.desired_available >
                std::numeric_limits<uint64_t>::max() - usage.consumed_total)
            return Status::InvalidArgument(
                "invalid or overflowing receiver credit pull usage" LOC_MARK);
    }
    return Status::OK();
}

Status ReceiverCreditPullRequestCodecV1::encode(
    const ReceiverCreditPullRequestV1& request, std::string& wire) {
    CHECK_STATUS(validate(request));
    std::string encoded;
    encoded.reserve(kHeaderBytes + request.resources.size() * kResourceBytes);
    append32(encoded, kPullRequestMagic);
    append16(encoded, request.schema_version);
    append16(encoded, request.flags);
    append64(encoded, request.sender_peer);
    append32(encoded, request.qos_class);
    append64(encoded, request.expected_receiver_session_id.high);
    append64(encoded, request.expected_receiver_session_id.low);
    append64(encoded, request.expected_epoch);
    append64(encoded, request.request_sequence);
    append64(encoded, request.last_update_sequence);
    append16(encoded, static_cast<uint16_t>(request.resources.size()));
    append16(encoded, 0);
    for (const auto& usage : request.resources) {
        append16(encoded, static_cast<uint16_t>(usage.resource));
        append16(encoded, 0);
        append64(encoded, usage.consumed_total);
        append64(encoded, usage.completed_total);
        append64(encoded, usage.minimum_available);
        append64(encoded, usage.desired_available);
    }
    wire.swap(encoded);
    return Status::OK();
}

Status ReceiverCreditPullRequestCodecV1::decode(
    std::string_view wire, ReceiverCreditPullRequestV1& request) {
    if (wire.size() < kHeaderBytes || wire.size() > kMaxWireBytes)
        return Status::InvalidArgument(
            "invalid receiver credit pull request length" LOC_MARK);
    size_t position = 0;
    if (read32(wire, position) != kPullRequestMagic)
        return Status::InvalidArgument(
            "invalid receiver credit pull request magic" LOC_MARK);

    ReceiverCreditPullRequestV1 decoded;
    decoded.schema_version = read16(wire, position);
    decoded.flags = read16(wire, position);
    decoded.sender_peer = read64(wire, position);
    decoded.qos_class = read32(wire, position);
    decoded.expected_receiver_session_id.high = read64(wire, position);
    decoded.expected_receiver_session_id.low = read64(wire, position);
    decoded.expected_epoch = read64(wire, position);
    decoded.request_sequence = read64(wire, position);
    decoded.last_update_sequence = read64(wire, position);
    const uint16_t count = read16(wire, position);
    const uint16_t reserved = read16(wire, position);
    if (reserved != 0 || count == 0 || count > kCreditResourceCount ||
        wire.size() != kHeaderBytes + count * kResourceBytes)
        return Status::InvalidArgument(
            "invalid receiver credit pull request framing" LOC_MARK);

    decoded.resources.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        ReceiverCreditResourceUsageV1 usage;
        usage.resource = static_cast<CreditResource>(read16(wire, position));
        const uint16_t resource_reserved = read16(wire, position);
        usage.consumed_total = read64(wire, position);
        usage.completed_total = read64(wire, position);
        usage.minimum_available = read64(wire, position);
        usage.desired_available = read64(wire, position);
        if (resource_reserved != 0)
            return Status::InvalidArgument(
                "nonzero receiver credit pull resource reserved "
                "field" LOC_MARK);
        decoded.resources.push_back(usage);
    }
    CHECK_STATUS(validate(decoded));
    request = std::move(decoded);
    return Status::OK();
}

Status ReceiverCreditPullResponseCodecV1::validate(
    const ReceiverCreditPullResponseV1& response) {
    if (response.schema_version != 1 || response.flags != 0 ||
        !isKnownStatus(response.status))
        return Status::InvalidArgument(
            "invalid receiver credit pull response header" LOC_MARK);
    if ((response.status == ReceiverCreditPullStatus::Retry) !=
        (response.retry_after_us != 0))
        return Status::InvalidArgument(
            "invalid receiver credit pull retry policy" LOC_MARK);

    const bool zero_payload_allowed =
        response.status == ReceiverCreditPullStatus::Unsupported ||
        response.status == ReceiverCreditPullStatus::Rejected;
    if (zero_payload_allowed && isZeroPayload(response)) return Status::OK();

    const auto& activation = response.activation;
    const auto& update = response.update;
    if (activation.schema_version != 1 || activation.chosen_version != 1 ||
        (activation.receiver_session_id.high == 0 &&
         activation.receiver_session_id.low == 0) ||
        activation.epoch == 0 || activation.freshness_ttl_ms == 0 ||
        update.schema_version != 1 || update.flags != 0 ||
        !(update.receiver_session_id == activation.receiver_session_id) ||
        update.epoch != activation.epoch || update.sequence == 0 ||
        update.freshness_ttl_ms != activation.freshness_ttl_ms ||
        update.grants.size() != kCreditResourceCount)
        return Status::InvalidArgument(
            "invalid receiver credit pull activation or update" LOC_MARK);

    std::array<bool, kCreditResourceCount> present{};
    for (const auto& grant : update.grants) {
        size_t index = 0;
        if (!isKnownResource(grant.resource, index) || present[index])
            return Status::InvalidArgument(
                "invalid receiver credit pull response grant" LOC_MARK);
        present[index] = true;
    }
    return Status::OK();
}

Status ReceiverCreditPullResponseCodecV1::makeZeroPayload(
    ReceiverCreditPullStatus status, ReceiverCreditPullResponseV1& response) {
    if (status != ReceiverCreditPullStatus::Unsupported &&
        status != ReceiverCreditPullStatus::Rejected)
        return Status::InvalidArgument(
            "zero receiver credit payload is not allowed for status" LOC_MARK);
    ReceiverCreditPullResponseV1 empty;
    empty.status = status;
    empty.activation.schema_version = 0;
    empty.activation.chosen_version = 0;
    empty.update.schema_version = 0;
    response = std::move(empty);
    return Status::OK();
}

Status ReceiverCreditPullResponseCodecV1::encode(
    const ReceiverCreditPullResponseV1& response, std::string& wire) {
    CHECK_STATUS(validate(response));
    std::string encoded;
    encoded.reserve(kWireBytes);
    append32(encoded, kPullResponseMagic);
    append16(encoded, response.schema_version);
    append16(encoded, response.flags);
    append16(encoded, static_cast<uint16_t>(response.status));
    append16(encoded, 0);
    append32(encoded, response.retry_after_us);

    append16(encoded, response.activation.schema_version);
    append16(encoded, response.activation.chosen_version);
    append64(encoded, response.activation.receiver_session_id.high);
    append64(encoded, response.activation.receiver_session_id.low);
    append64(encoded, response.activation.epoch);
    append32(encoded, response.activation.freshness_ttl_ms);
    append32(encoded, 0);

    append16(encoded, response.update.schema_version);
    append16(encoded, response.update.flags);
    append32(encoded, response.update.qos_class);
    append64(encoded, response.update.receiver_session_id.high);
    append64(encoded, response.update.receiver_session_id.low);
    append64(encoded, response.update.epoch);
    append64(encoded, response.update.sequence);
    append32(encoded, response.update.freshness_ttl_ms);
    append16(encoded, static_cast<uint16_t>(response.update.grants.size()));
    append16(encoded, 0);
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (i < response.update.grants.size()) {
            const auto& grant = response.update.grants[i];
            append16(encoded, static_cast<uint16_t>(grant.resource));
            append16(encoded, 0);
            append64(encoded, grant.grant_total);
        } else {
            append16(encoded, 0);
            append16(encoded, 0);
            append64(encoded, 0);
        }
    }
    wire.swap(encoded);
    return Status::OK();
}

Status ReceiverCreditPullResponseCodecV1::decode(
    std::string_view wire, ReceiverCreditPullResponseV1& response) {
    if (wire.size() != kWireBytes)
        return Status::InvalidArgument(
            "invalid receiver credit pull response length" LOC_MARK);
    size_t position = 0;
    if (read32(wire, position) != kPullResponseMagic)
        return Status::InvalidArgument(
            "invalid receiver credit pull response magic" LOC_MARK);

    ReceiverCreditPullResponseV1 decoded;
    decoded.schema_version = read16(wire, position);
    decoded.flags = read16(wire, position);
    decoded.status =
        static_cast<ReceiverCreditPullStatus>(read16(wire, position));
    const uint16_t response_reserved = read16(wire, position);
    decoded.retry_after_us = read32(wire, position);

    decoded.activation.schema_version = read16(wire, position);
    decoded.activation.chosen_version = read16(wire, position);
    decoded.activation.receiver_session_id.high = read64(wire, position);
    decoded.activation.receiver_session_id.low = read64(wire, position);
    decoded.activation.epoch = read64(wire, position);
    decoded.activation.freshness_ttl_ms = read32(wire, position);
    const uint32_t activation_reserved = read32(wire, position);

    decoded.update.schema_version = read16(wire, position);
    decoded.update.flags = read16(wire, position);
    decoded.update.qos_class = read32(wire, position);
    decoded.update.receiver_session_id.high = read64(wire, position);
    decoded.update.receiver_session_id.low = read64(wire, position);
    decoded.update.epoch = read64(wire, position);
    decoded.update.sequence = read64(wire, position);
    decoded.update.freshness_ttl_ms = read32(wire, position);
    const uint16_t count = read16(wire, position);
    const uint16_t update_reserved = read16(wire, position);
    if (response_reserved != 0 || activation_reserved != 0 ||
        update_reserved != 0 || (count != 0 && count != kCreditResourceCount))
        return Status::InvalidArgument(
            "invalid receiver credit pull response framing" LOC_MARK);

    decoded.update.grants.reserve(count);
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        CreditAmount grant;
        grant.resource = static_cast<CreditResource>(read16(wire, position));
        const uint16_t grant_reserved = read16(wire, position);
        grant.grant_total = read64(wire, position);
        if (grant_reserved != 0 ||
            (count == 0 && (static_cast<uint16_t>(grant.resource) != 0 ||
                            grant.grant_total != 0)))
            return Status::InvalidArgument(
                "invalid receiver credit pull grant or padding" LOC_MARK);
        if (count != 0) decoded.update.grants.push_back(grant);
    }
    CHECK_STATUS(validate(decoded));
    response = std::move(decoded);
    return Status::OK();
}

}  // namespace mooncake::tent
