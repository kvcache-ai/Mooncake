// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_protocol.h"

#include <limits>
#include <stdexcept>

#include "tent/common/types.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake::tent {
namespace {
void Put16(uint8_t* p, uint16_t v) { p[0] = v >> 8; p[1] = v; }
void Put32(uint8_t* p, uint32_t v) { for (int i = 3; i >= 0; --i) p[3-i] = v >> (i * 8); }
void Put64(uint8_t* p, uint64_t v) { for (int i = 7; i >= 0; --i) p[7-i] = v >> (i * 8); }
uint16_t Get16(const uint8_t* p) { return (uint16_t(p[0]) << 8) | p[1]; }
uint32_t Get32(const uint8_t* p) { uint32_t r = 0; for (int i=0;i<4;++i) r=(r<<8)|p[i]; return r; }
uint64_t Get64(const uint8_t* p) { uint64_t r = 0; for (int i=0;i<8;++i) r=(r<<8)|p[i]; return r; }
Status Invalid(const char* what) { return Status::InvalidArgument(std::string("Invalid high-performance TCP frame: ") + what + LOC_MARK); }
}

std::array<uint8_t, kHighPerformanceTcpRequestSize> EncodeHighPerformanceTcpRequest(
    const HighPerformanceTcpRequestFrame& f) {
    std::array<uint8_t, kHighPerformanceTcpRequestSize> b{};
    Put32(b.data(), kHighPerformanceTcpMagic); Put16(b.data()+4, kHighPerformanceTcpVersion);
    b[6] = static_cast<uint8_t>(f.opcode); b[7] = 0;
    Put64(b.data()+8, f.request_id); Put64(b.data()+16, f.registration_id);
    Put64(b.data()+24, f.remote_addr); Put64(b.data()+32, f.length);
    return b;
}
Status DecodeHighPerformanceTcpRequest(const uint8_t* b, size_t n, HighPerformanceTcpRequestFrame* f) {
    if (!b || !f || n != kHighPerformanceTcpRequestSize) return Invalid("request size");
    if (Get32(b) != kHighPerformanceTcpMagic) return Invalid("magic");
    if (Get16(b+4) != kHighPerformanceTcpVersion) return Invalid("version");
    if (b[7] != 0 || Get64(b+40) != 0) return Invalid("reserved/flags");
    if (b[6] != uint8_t(HighPerformanceTcpOpcode::kRead) && b[6] != uint8_t(HighPerformanceTcpOpcode::kWrite)) return Invalid("opcode");
    f->opcode = static_cast<HighPerformanceTcpOpcode>(b[6]); f->request_id = Get64(b+8);
    f->registration_id = Get64(b+16); f->remote_addr = Get64(b+24); f->length = Get64(b+32); return Status::OK();
}
std::array<uint8_t, kHighPerformanceTcpResponseSize> EncodeHighPerformanceTcpResponse(const HighPerformanceTcpResponseFrame& f) {
    std::array<uint8_t, kHighPerformanceTcpResponseSize> b{};
    Put32(b.data(), kHighPerformanceTcpMagic); Put16(b.data()+4, kHighPerformanceTcpVersion);
    Put16(b.data()+6, static_cast<uint16_t>(f.status)); Put64(b.data()+8, f.request_id); Put64(b.data()+16, f.committed_bytes); return b;
}
Status DecodeHighPerformanceTcpResponse(const uint8_t* b, size_t n, HighPerformanceTcpResponseFrame* f) {
    if (!b || !f || n != kHighPerformanceTcpResponseSize) return Invalid("response size");
    if (Get32(b) != kHighPerformanceTcpMagic) return Invalid("magic");
    if (Get16(b+4) != kHighPerformanceTcpVersion || Get64(b+24) != 0) return Invalid("version/reserved");
    auto s = Get16(b+6); if (s > uint16_t(HighPerformanceTcpStatus::kInternalError)) return Invalid("status");
    f->status = static_cast<HighPerformanceTcpStatus>(s); f->request_id = Get64(b+8); f->committed_bytes = Get64(b+16); return Status::OK();
}
const char* HighPerformanceTcpPermissionName(Permission p) {
    switch (p) { case kLocalReadWrite: return "local_read_write"; case kGlobalReadOnly: return "global_read_only"; case kGlobalReadWrite: return "global_read_write"; } return "unknown";
}
Status EncodeHighPerformanceTcpEndpointAttr(const HighPerformanceTcpEndpointAttr& a, std::string* out) {
    if (!out || a.incarnation.size() != 32 || a.endpoints.size() != 1 || a.endpoints[0].host.empty() || !a.endpoints[0].port || !a.max_transfer_bytes) return Status::InvalidArgument("Invalid hp TCP endpoint attribute" LOC_MARK);
    nlohmann::json j{{"protocol","tent_hp_tcp"},{"version",1},{"incarnation",a.incarnation},{"max_transfer_bytes",a.max_transfer_bytes},{"endpoints", {{{"host",a.endpoints[0].host},{"port",a.endpoints[0].port}}}}}; *out=j.dump(); return Status::OK();
}
Status DecodeHighPerformanceTcpEndpointAttr(const std::string& s, HighPerformanceTcpEndpointAttr* a) {
    if (!a) return Status::InvalidArgument("Null hp TCP endpoint attr" LOC_MARK); try { auto j=nlohmann::json::parse(s); if(j.at("protocol")!="tent_hp_tcp"||j.at("version")!=1||!j.at("incarnation").is_string()) return Status::InvalidArgument("Unsupported hp TCP endpoint attr" LOC_MARK); auto e=j.at("endpoints"); if(!e.is_array()||e.size()!=1) return Status::InvalidArgument("HP TCP v1 requires one endpoint" LOC_MARK); a->incarnation=j.at("incarnation").get<std::string>(); a->max_transfer_bytes=j.at("max_transfer_bytes").get<uint64_t>(); a->endpoints={{e[0].at("host").get<std::string>(),e[0].at("port").get<uint16_t>()}}; if(a->incarnation.size()!=32||a->endpoints[0].host.empty()||!a->endpoints[0].port||!a->max_transfer_bytes) return Status::InvalidArgument("Invalid hp TCP endpoint attr" LOC_MARK); return Status::OK(); } catch(const std::exception& e) { return Status::MalformedJson(std::string("Invalid hp TCP endpoint attr: ")+e.what()+LOC_MARK); }
}
Status EncodeHighPerformanceTcpBufferAttr(const HighPerformanceTcpBufferAttr& a,std::string* out) { if(!out||!a.registration_id||(a.permission!="global_read_only"&&a.permission!="global_read_write")) return Status::InvalidArgument("Invalid hp TCP buffer attr" LOC_MARK); *out=nlohmann::json{{"protocol","tent_hp_tcp"},{"version",1},{"registration_id",a.registration_id},{"permission",a.permission}}.dump(); return Status::OK(); }
Status DecodeHighPerformanceTcpBufferAttr(const std::string& s,HighPerformanceTcpBufferAttr* a) { if(!a) return Status::InvalidArgument("Null hp TCP buffer attr" LOC_MARK); try {auto j=nlohmann::json::parse(s); if(j.at("protocol")!="tent_hp_tcp"||j.at("version")!=1) return Status::InvalidArgument("Unsupported hp TCP buffer attr" LOC_MARK); a->registration_id=j.at("registration_id").get<uint64_t>(); a->permission=j.at("permission").get<std::string>(); if(!a->registration_id||(a->permission!="global_read_only"&&a->permission!="global_read_write")) return Status::InvalidArgument("Invalid hp TCP buffer attr" LOC_MARK); return Status::OK();}catch(const std::exception&e){return Status::MalformedJson(std::string("Invalid hp TCP buffer attr: ")+e.what()+LOC_MARK);}}
}  // namespace mooncake::tent
