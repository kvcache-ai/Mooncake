// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_protocol.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <string>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake::tent {
namespace {

using json = nlohmann::json;

void Put16(uint8_t* out, uint16_t value) {
    out[0] = static_cast<uint8_t>(value >> 8U);
    out[1] = static_cast<uint8_t>(value);
}

void Put32(uint8_t* out, uint32_t value) {
    for (size_t i = 0; i < 4; ++i) {
        out[i] = static_cast<uint8_t>(value >> ((3U - i) * 8U));
    }
}

void Put64(uint8_t* out, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) {
        out[i] = static_cast<uint8_t>(value >> ((7U - i) * 8U));
    }
}

uint16_t Get16(const uint8_t* in) {
    return static_cast<uint16_t>((static_cast<uint16_t>(in[0]) << 8U) |
                                 static_cast<uint16_t>(in[1]));
}

uint32_t Get32(const uint8_t* in) {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value = (value << 8U) | in[i];
    }
    return value;
}

uint64_t Get64(const uint8_t* in) {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value = (value << 8U) | in[i];
    }
    return value;
}

Status InvalidFrame(const std::string& detail, HighPerformanceTcpStatus error,
                    HighPerformanceTcpStatus* wire_error) {
    if (wire_error != nullptr) *wire_error = error;
    return Status::InvalidArgument(
        "Invalid high-performance TCP frame: " + detail + LOC_MARK);
}

bool IsHex128(const std::string& value) {
    return value.size() == 32 &&
           std::all_of(value.begin(), value.end(),
                       [](unsigned char ch) { return std::isxdigit(ch) != 0; });
}

Status InvalidAttr(const std::string& detail) {
    return Status::InvalidArgument("Invalid HP TCP attribute: " + detail +
                                   LOC_MARK);
}

bool ReadPositiveUint64(const json& object, const char* key, uint64_t* out) {
    auto it = object.find(key);
    if (it == object.end()) return false;
    if (!it->is_number_unsigned() && !it->is_number_integer()) return false;
    if (it->is_number_integer() && it->get<int64_t>() <= 0) return false;
    const auto value = it->get<uint64_t>();
    if (value == 0) return false;
    *out = value;
    return true;
}

}  // namespace

std::array<uint8_t, kHighPerformanceTcpRequestSize>
EncodeHighPerformanceTcpRequest(const HighPerformanceTcpRequestFrame& frame) {
    std::array<uint8_t, kHighPerformanceTcpRequestSize> bytes{};
    Put32(bytes.data(), kHighPerformanceTcpMagic);
    Put16(bytes.data() + 4, kHighPerformanceTcpVersion);
    bytes[6] = static_cast<uint8_t>(frame.opcode);
    bytes[7] = 0;  // flags, reserved in v1
    Put64(bytes.data() + 8, frame.request_id);
    Put64(bytes.data() + 16, frame.registration_id);
    Put64(bytes.data() + 24, frame.remote_addr);
    Put64(bytes.data() + 32, frame.length);
    Put64(bytes.data() + 40, 0);  // reserved
    return bytes;
}

Status DecodeHighPerformanceTcpRequest(const uint8_t* bytes, size_t size,
                                       HighPerformanceTcpRequestFrame* frame,
                                       HighPerformanceTcpStatus* wire_error) {
    if (wire_error != nullptr) {
        *wire_error = HighPerformanceTcpStatus::kInternalError;
    }
    if (bytes == nullptr || frame == nullptr ||
        size != kHighPerformanceTcpRequestSize) {
        return InvalidFrame("request size",
                            HighPerformanceTcpStatus::kInternalError,
                            wire_error);
    }
    if (Get32(bytes) != kHighPerformanceTcpMagic) {
        return InvalidFrame("magic", HighPerformanceTcpStatus::kInternalError,
                            wire_error);
    }
    if (Get16(bytes + 4) != kHighPerformanceTcpVersion) {
        return InvalidFrame("version", HighPerformanceTcpStatus::kBadVersion,
                            wire_error);
    }
    if (bytes[7] != 0 || Get64(bytes + 40) != 0) {
        return InvalidFrame("flags/reserved",
                            HighPerformanceTcpStatus::kInternalError,
                            wire_error);
    }
    if (bytes[6] != static_cast<uint8_t>(HighPerformanceTcpOpcode::kRead) &&
        bytes[6] != static_cast<uint8_t>(HighPerformanceTcpOpcode::kWrite)) {
        return InvalidFrame("opcode", HighPerformanceTcpStatus::kBadOpcode,
                            wire_error);
    }

    const uint64_t remote_addr = Get64(bytes + 24);
    const uint64_t length = Get64(bytes + 32);
    if (length == 0 ||
        remote_addr > std::numeric_limits<uint64_t>::max() - length) {
        return InvalidFrame("length/range",
                            HighPerformanceTcpStatus::kBadLength, wire_error);
    }

    frame->opcode = static_cast<HighPerformanceTcpOpcode>(bytes[6]);
    frame->request_id = Get64(bytes + 8);
    frame->registration_id = Get64(bytes + 16);
    frame->remote_addr = remote_addr;
    frame->length = length;
    return Status::OK();
}

std::array<uint8_t, kHighPerformanceTcpResponseSize>
EncodeHighPerformanceTcpResponse(const HighPerformanceTcpResponseFrame& frame) {
    std::array<uint8_t, kHighPerformanceTcpResponseSize> bytes{};
    Put32(bytes.data(), kHighPerformanceTcpMagic);
    Put16(bytes.data() + 4, kHighPerformanceTcpVersion);
    Put16(bytes.data() + 6, static_cast<uint16_t>(frame.status));
    Put64(bytes.data() + 8, frame.request_id);
    Put64(bytes.data() + 16, frame.committed_bytes);
    Put64(bytes.data() + 24, 0);
    return bytes;
}

Status DecodeHighPerformanceTcpResponse(
    const uint8_t* bytes, size_t size, HighPerformanceTcpResponseFrame* frame) {
    if (bytes == nullptr || frame == nullptr ||
        size != kHighPerformanceTcpResponseSize) {
        return Status::InvalidArgument(
            "Invalid high-performance TCP response size" LOC_MARK);
    }
    if (Get32(bytes) != kHighPerformanceTcpMagic ||
        Get16(bytes + 4) != kHighPerformanceTcpVersion ||
        Get64(bytes + 24) != 0) {
        return Status::InvalidArgument(
            "Invalid high-performance TCP response header" LOC_MARK);
    }
    const uint16_t status = Get16(bytes + 6);
    if (status >
        static_cast<uint16_t>(HighPerformanceTcpStatus::kInternalError)) {
        return Status::InvalidArgument(
            "Invalid high-performance TCP response status" LOC_MARK);
    }
    frame->status = static_cast<HighPerformanceTcpStatus>(status);
    frame->request_id = Get64(bytes + 8);
    frame->committed_bytes = Get64(bytes + 16);
    return Status::OK();
}

const char* HighPerformanceTcpPermissionName(Permission permission) {
    switch (permission) {
        case kLocalReadWrite:
            return "local_read_write";
        case kGlobalReadOnly:
            return "global_read_only";
        case kGlobalReadWrite:
            return "global_read_write";
    }
    return "unknown";
}

Status EncodeHighPerformanceTcpEndpointAttr(
    const HighPerformanceTcpEndpointAttr& attr, std::string* encoded) {
    if (encoded == nullptr || !IsHex128(attr.incarnation) ||
        attr.endpoints.size() != 1 || attr.endpoints[0].host.empty() ||
        attr.endpoints[0].port == 0 || attr.max_transfer_bytes == 0) {
        return InvalidAttr("endpoint");
    }

    json object = {
        {"protocol", "tent_hp_tcp"},
        {"version", kHighPerformanceTcpVersion},
        {"incarnation", attr.incarnation},
        {"endpoints", json::array({{{"host", attr.endpoints[0].host},
                                    {"port", attr.endpoints[0].port}}})},
        {"max_transfer_bytes", attr.max_transfer_bytes},
    };
    *encoded = object.dump();
    return Status::OK();
}

Status DecodeHighPerformanceTcpEndpointAttr(
    const std::string& encoded, HighPerformanceTcpEndpointAttr* attr) {
    if (attr == nullptr) return InvalidAttr("null endpoint output");
    try {
        const json object = json::parse(encoded);
        if (!object.is_object()) return InvalidAttr("endpoint must be object");
        auto protocol = object.find("protocol");
        auto version = object.find("version");
        auto incarnation = object.find("incarnation");
        auto endpoints = object.find("endpoints");
        if (protocol == object.end() || !protocol->is_string() ||
            protocol->get<std::string>() != "tent_hp_tcp") {
            return InvalidAttr("endpoint protocol");
        }
        if (version == object.end() || !version->is_number_integer() ||
            version->get<int64_t>() != kHighPerformanceTcpVersion) {
            return InvalidAttr("endpoint version");
        }
        if (incarnation == object.end() || !incarnation->is_string() ||
            !IsHex128(incarnation->get<std::string>())) {
            return InvalidAttr("endpoint incarnation");
        }
        if (endpoints == object.end() || !endpoints->is_array() ||
            endpoints->size() != 1 || !(*endpoints)[0].is_object()) {
            return InvalidAttr("v1 requires exactly one endpoint");
        }

        const json& endpoint = (*endpoints)[0];
        auto host = endpoint.find("host");
        auto port = endpoint.find("port");
        if (host == endpoint.end() || !host->is_string() ||
            host->get<std::string>().empty() || port == endpoint.end() ||
            (!port->is_number_unsigned() && !port->is_number_integer())) {
            return InvalidAttr("endpoint host/port");
        }
        if (port->is_number_integer() && port->get<int64_t>() <= 0) {
            return InvalidAttr("endpoint port");
        }
        const uint64_t port_value = port->get<uint64_t>();
        if (port_value == 0 || port_value > 65535) {
            return InvalidAttr("endpoint port");
        }

        uint64_t max_transfer_bytes = 0;
        if (!ReadPositiveUint64(object, "max_transfer_bytes",
                                &max_transfer_bytes)) {
            return InvalidAttr("endpoint max_transfer_bytes");
        }

        HighPerformanceTcpEndpointAttr parsed;
        parsed.incarnation = incarnation->get<std::string>();
        parsed.endpoints.push_back(
            {host->get<std::string>(), static_cast<uint16_t>(port_value)});
        parsed.max_transfer_bytes = max_transfer_bytes;
        *attr = std::move(parsed);
        return Status::OK();
    } catch (const std::exception& error) {
        return Status::MalformedJson(
            std::string("Invalid HP TCP endpoint attribute: ") + error.what() +
            LOC_MARK);
    }
}

Status EncodeHighPerformanceTcpBufferAttr(
    const HighPerformanceTcpBufferAttr& attr, std::string* encoded) {
    if (encoded == nullptr || attr.registration_id == 0 ||
        (attr.permission != "global_read_only" &&
         attr.permission != "global_read_write")) {
        return InvalidAttr("buffer");
    }
    *encoded = json{{"protocol", "tent_hp_tcp"},
                    {"version", kHighPerformanceTcpVersion},
                    {"registration_id", attr.registration_id},
                    {"permission", attr.permission}}
                   .dump();
    return Status::OK();
}

Status DecodeHighPerformanceTcpBufferAttr(const std::string& encoded,
                                          HighPerformanceTcpBufferAttr* attr) {
    if (attr == nullptr) return InvalidAttr("null buffer output");
    try {
        const json object = json::parse(encoded);
        if (!object.is_object()) return InvalidAttr("buffer must be object");
        auto protocol = object.find("protocol");
        auto version = object.find("version");
        auto registration = object.find("registration_id");
        auto permission = object.find("permission");
        if (protocol == object.end() || !protocol->is_string() ||
            protocol->get<std::string>() != "tent_hp_tcp") {
            return InvalidAttr("buffer protocol");
        }
        if (version == object.end() || !version->is_number_integer() ||
            version->get<int64_t>() != kHighPerformanceTcpVersion) {
            return InvalidAttr("buffer version");
        }
        if (registration == object.end() ||
            (!registration->is_number_unsigned() &&
             !registration->is_number_integer()) ||
            (registration->is_number_integer() &&
             registration->get<int64_t>() <= 0)) {
            return InvalidAttr("buffer registration_id");
        }
        const uint64_t registration_id = registration->get<uint64_t>();
        if (registration_id == 0 || permission == object.end() ||
            !permission->is_string()) {
            return InvalidAttr("buffer registration/permission");
        }
        const std::string permission_value = permission->get<std::string>();
        if (permission_value != "global_read_only" &&
            permission_value != "global_read_write") {
            return InvalidAttr("buffer permission");
        }
        *attr = {registration_id, permission_value};
        return Status::OK();
    } catch (const std::exception& error) {
        return Status::MalformedJson(
            std::string("Invalid HP TCP buffer attribute: ") + error.what() +
            LOC_MARK);
    }
}

}  // namespace mooncake::tent
