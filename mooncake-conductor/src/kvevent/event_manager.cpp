#include "conductor/kvevent/event_manager.h"

#include <glog/logging.h>
#include <msgpack.hpp>
#include <csignal>
#include <ylt/coro_http/coro_http_server.hpp>

#include <algorithm>
#include <chrono>
#include <limits>
#include <optional>
#include <set>
#include <thread>
#include <utility>

#include "conductor/prefixindex/hash_strategy.h"

namespace conductor {
namespace kvevent {

namespace {

using coro_http::coro_http_request;
using coro_http::coro_http_response;
using coro_http::status_type;

// Every endpoint speaks msgpack only: request bodies, success responses, and
// error envelopes are all msgpack maps. There is no JSON wire path.
constexpr const char* kApplicationMsgpack = "application/msgpack";

using MsgpackPacker = msgpack::packer<msgpack::sbuffer>;

prefixindex::ContextKey ContextFromService(
    const common::ServiceConfig& service) {
    return {.tenant_id = service.tenant_id,
            .model_name = service.model_name,
            .lora_name = service.lora_name,
            .block_size = service.block_size};
}

prefixindex::HashProfile ProfileFromService(
    const common::ServiceConfig& service) {
    return service.hash_profile;
}

prefixindex::EngineRegistration RegistrationFromService(
    const common::ServiceConfig& service) {
    return {.context = ContextFromService(service),
            .profile = ProfileFromService(service),
            .instance_id = service.instance_id,
            .dp_rank = service.dp_rank,
            .effective_block_size = service.block_size,
            .cache_group = service.cache_group};
}

bool MsgpackInt64(const msgpack::object& value, int64_t* out) {
    if (value.type == msgpack::type::POSITIVE_INTEGER) {
        if (value.via.u64 >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return false;
        }
        *out = static_cast<int64_t>(value.via.u64);
        return true;
    }
    if (value.type == msgpack::type::NEGATIVE_INTEGER) {
        *out = value.via.i64;
        return true;
    }
    return false;
}

// The HTTP registration contract intentionally exposes only the two vLLM v1
// recipes.  Validate this at the request boundary before invoking root
// derivation so an unsupported selector cannot trigger any hash work or state
// mutation, and so the error is attributed to the algorithm field.
bool IsSupportedHashAlgorithm(std::string_view algorithm) {
    return algorithm == "sha256" || algorithm == "sha256_cbor";
}

// ---------------------------------------------------------------------------
// Response writers
// ---------------------------------------------------------------------------

void HttpMsgpack(coro_http_response& resp, status_type status,
                 const msgpack::sbuffer& body) {
    resp.add_header("Content-Type", kApplicationMsgpack);
    resp.set_status_and_content(status, std::string(body.data(), body.size()));
}

// Writes an error response: msgpack map {"error": message}.
void HttpError(coro_http_response& resp, status_type status,
               const std::string& message) {
    msgpack::sbuffer body;
    MsgpackPacker packer(&body);
    packer.pack_map(1);
    packer.pack("error");
    packer.pack(message);
    HttpMsgpack(resp, status, body);
}

void HttpValidationError(coro_http_response& resp, const char* reason,
                         const std::string& message,
                         const char* field = nullptr,
                         std::optional<size_t> index = std::nullopt) {
    msgpack::sbuffer body;
    MsgpackPacker packer(&body);
    uint32_t size = 2;
    if (field != nullptr) {
        ++size;
    }
    if (index.has_value()) {
        ++size;
    }
    packer.pack_map(size);
    packer.pack("error");
    packer.pack(message);
    packer.pack("reason");
    packer.pack(reason);
    if (field != nullptr) {
        packer.pack("field");
        packer.pack(field);
    }
    if (index.has_value()) {
        packer.pack("index");
        packer.pack(static_cast<uint64_t>(*index));
    }
    HttpMsgpack(resp, status_type::bad_request, body);
}

// ---------------------------------------------------------------------------
// Request helpers (msgpack-native)
// ---------------------------------------------------------------------------

std::string_view ObjectStr(const msgpack::object& object) {
    return std::string_view(object.via.str.ptr, object.via.str.size);
}

// Finds a string-keyed member in a msgpack map; nullptr when absent.
const msgpack::object* MapFind(const msgpack::object_map& map,
                               std::string_view key) {
    for (uint32_t i = 0; i < map.size; ++i) {
        const msgpack::object& k = map.ptr[i].key;
        if (k.type == msgpack::type::STR && ObjectStr(k) == key) {
            return &map.ptr[i].val;
        }
    }
    return nullptr;
}

// Parses the request body as a msgpack map. Returns false (and writes the
// error response) on a wrong content type, malformed payload, or non-map root.
// The returned object_map points into the handle's zone; keep handle alive.
bool ParseMsgpackBody(coro_http_request& req, coro_http_response& resp,
                      const char* what, msgpack::object_handle* handle,
                      msgpack::object_map* out) {
    const std::string_view content_type = req.get_header_value("content-type");
    if (content_type.find(kApplicationMsgpack) == std::string_view::npos) {
        HttpValidationError(resp, "unsupported_content_type",
                            "Content-Type must be application/msgpack");
        return false;
    }
    const auto body = req.get_body();
    try {
        *handle = msgpack::unpack(body.data(), body.size());
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to decode " << what
                   << " msgpack err=" << e.what();
        HttpValidationError(resp, "invalid_msgpack", "Invalid msgpack object");
        return false;
    }
    const msgpack::object root = handle->get();
    if (root.type != msgpack::type::MAP) {
        HttpValidationError(resp, "invalid_msgpack",
                            "request body must be a msgpack map");
        return false;
    }
    *out = root.via.map;
    return true;
}

bool RejectUnknownFields(const msgpack::object_map& body,
                         const std::set<std::string>& allowed,
                         coro_http_response& resp) {
    for (uint32_t i = 0; i < body.size; ++i) {
        const msgpack::object& key = body.ptr[i].key;
        if (key.type != msgpack::type::STR) {
            HttpValidationError(resp, "invalid_msgpack",
                                "map keys must be strings");
            return false;
        }
        const std::string name(ObjectStr(key));
        if (!allowed.contains(name)) {
            HttpValidationError(resp, "unknown_field",
                                "unsupported request field: " + name,
                                name.c_str());
            return false;
        }
    }
    return true;
}

bool RequiredString(const msgpack::object_map& body, const char* field,
                    coro_http_response& resp, std::string* out) {
    const msgpack::object* value = MapFind(body, field);
    if (value == nullptr) {
        HttpValidationError(resp, "missing",
                            std::string(field) + " is required", field);
        return false;
    }
    if (value->type != msgpack::type::STR) {
        HttpValidationError(resp, "invalid_type",
                            std::string(field) + " must be a string", field);
        return false;
    }
    *out = std::string(ObjectStr(*value));
    if (out->empty()) {
        HttpValidationError(resp, "invalid_value",
                            std::string(field) + " must not be empty", field);
        return false;
    }
    return true;
}

bool OptionalStringStrict(const msgpack::object_map& body, const char* field,
                          const std::string& fallback, coro_http_response& resp,
                          std::string* out) {
    const msgpack::object* value = MapFind(body, field);
    if (value == nullptr) {
        *out = fallback;
        return true;
    }
    if (value->type != msgpack::type::STR) {
        HttpValidationError(resp, "invalid_type",
                            std::string(field) + " must be a string", field);
        return false;
    }
    *out = std::string(ObjectStr(*value));
    return true;
}

bool RequiredPositiveInt64(const msgpack::object_map& body, const char* field,
                           coro_http_response& resp, int64_t* out) {
    const msgpack::object* value = MapFind(body, field);
    if (value == nullptr) {
        HttpValidationError(resp, "missing",
                            std::string(field) + " is required", field);
        return false;
    }
    if (!MsgpackInt64(*value, out)) {
        HttpValidationError(resp, "invalid_type",
                            std::string(field) + " must be an integer", field);
        return false;
    }
    if (*out <= 0) {
        HttpValidationError(resp, "out_of_range",
                            std::string(field) + " must be greater than zero",
                            field);
        return false;
    }
    return true;
}

bool ParseOptionalCacheGroup(const msgpack::object_map& body,
                             coro_http_response& resp,
                             std::optional<int64_t>* cache_group) {
    const msgpack::object* value = MapFind(body, "cache_group");
    if (value == nullptr || value->type == msgpack::type::NIL) {
        cache_group->reset();
        return true;
    }
    int64_t parsed = 0;
    if (!MsgpackInt64(*value, &parsed)) {
        HttpValidationError(resp, "invalid_type",
                            "cache_group must be an integer or null",
                            "cache_group");
        return false;
    }
    if (parsed != 0) {
        HttpValidationError(resp, "unsupported",
                            "only cache group zero is supported",
                            "cache_group");
        return false;
    }
    *cache_group = parsed;
    return true;
}

bool ParseHashProfileConfig(const msgpack::object_map& body,
                            coro_http_response& resp,
                            common::ResolvedHashProfile* profile) {
    const msgpack::object* value = MapFind(body, "hash_profile");
    if (value == nullptr) {
        HttpValidationError(resp, "missing", "hash_profile is required",
                            "hash_profile");
        return false;
    }
    if (value->type != msgpack::type::MAP) {
        HttpValidationError(resp, "invalid_type", "hash_profile must be a map",
                            "hash_profile");
        return false;
    }
    const msgpack::object_map& profile_map = value->via.map;
    static const std::set<std::string> kAllowedProfileFields = {
        "algorithm", "index_projection", "python_hash_seed", "strategy"};
    if (!RejectUnknownFields(profile_map, kAllowedProfileFields, resp)) {
        return false;
    }

    common::HashProfileConfig source;
    if (!RequiredString(profile_map, "strategy", resp, &source.strategy) ||
        !RequiredString(profile_map, "algorithm", resp, &source.algorithm) ||
        !RequiredString(profile_map, "python_hash_seed", resp,
                        &source.python_hash_seed) ||
        !RequiredString(profile_map, "index_projection", resp,
                        &source.index_projection)) {
        return false;
    }

    if (!IsSupportedHashAlgorithm(source.algorithm)) {
        HttpValidationError(resp, "invalid_value",
                            "unsupported hash algorithm: " + source.algorithm,
                            "algorithm");
        return false;
    }

    // Resolve the selected recipe before ParseServiceConfigRequest returns.
    // SubscribeToService (and thus prefix-index/ZMQ state mutation) is only
    // reached after this derived profile has been validated.
    if (std::string error = prefixindex::ResolveHashProfile(source, profile);
        !error.empty()) {
        const char* field = "python_hash_seed";
        if (source.strategy != "vllm_v1") {
            field = "strategy";
        } else if (!IsSupportedHashAlgorithm(source.algorithm)) {
            field = "algorithm";
        } else if (source.index_projection != "low64_be") {
            field = "index_projection";
        }
        HttpValidationError(resp, "invalid_value", error, field);
        return false;
    }
    return true;
}

std::string ValidateServiceConfig(const common::ServiceConfig& service) {
    if (service.endpoint.empty()) {
        return "endpoint is required";
    }
    if (service.model_name.empty()) {
        return "modelname is required";
    }
    if (service.tenant_id.empty()) {
        return "tenant_id must not be empty after normalization";
    }
    if (service.block_size <= 0) {
        return "block_size must be greater than zero";
    }
    if (service.dp_rank < 0) {
        return "dp_rank must be non-negative";
    }
    if (service.cache_group.has_value() && *service.cache_group != 0) {
        return "only cache group zero is supported";
    }
    if (service.publisher_kind == common::PublisherKind::kVllm) {
        if (service.instance_id.empty()) {
            return "instance_id is required for vLLM";
        }
        return prefixindex::PrefixCacheTable::ValidateRegistration(
                   RegistrationFromService(service))
            .error;
    }
    if (service.publisher_kind == common::PublisherKind::kMooncake) {
        return prefixindex::ValidateHashProfile(ProfileFromService(service));
    }
    return "unsupported publisher kind";
}

struct QueryRequest {
    prefixindex::ContextKey context;
    std::vector<int32_t> token_ids;
    std::optional<std::string> cache_salt;
    std::optional<std::string> instance_filter;
};

// Decodes one little-endian int32 from a msgpack bin token_ids element.
int32_t DecodeLeInt32(const char* p) {
    const uint32_t v =
        static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
        (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
        (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
        (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
    return static_cast<int32_t>(v);
}

// Parses a /query request body encoded as msgpack (Content-Type:
// application/msgpack). Same schema and validation semantics as
// ParseQueryRequest; token_ids may be a msgpack bin of little-endian int32
// (preferred: 4 bytes/token, no per-element parsing) or an array of integers.
// Errors are reported as JSON regardless of request encoding.
bool ParseQueryMsgpackRequest(coro_http_request& req, coro_http_response& resp,
                              QueryRequest* request) {
    const auto body = req.get_body();
    msgpack::object_handle handle;
    try {
        handle = msgpack::unpack(body.data(), body.size());
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to decode query msgpack err=" << e.what();
        HttpValidationError(resp, "invalid_msgpack", "Invalid msgpack object");
        return false;
    }
    const msgpack::object root = handle.get();
    if (root.type != msgpack::type::MAP) {
        HttpValidationError(resp, "invalid_msgpack",
                            "request body must be a msgpack map");
        return false;
    }

    bool have_model = false;
    bool have_block_size = false;
    bool have_token_ids = false;
    request->cache_salt.reset();
    request->instance_filter.reset();

    const msgpack::object_map& map = root.via.map;
    for (uint32_t i = 0; i < map.size; ++i) {
        const msgpack::object_kv& kv = map.ptr[i];
        if (kv.key.type != msgpack::type::STR) {
            HttpValidationError(resp, "invalid_msgpack",
                                "map keys must be strings");
            return false;
        }
        const std::string_view key(kv.key.via.str.ptr, kv.key.via.str.size);
        const msgpack::object& value = kv.val;

        if (key == "model") {
            if (value.type != msgpack::type::STR) {
                HttpValidationError(resp, "invalid_type",
                                    "model must be a string", "model");
                return false;
            }
            request->context.model_name.assign(value.via.str.ptr,
                                               value.via.str.size);
            if (request->context.model_name.empty()) {
                HttpValidationError(resp, "invalid_value",
                                    "model must not be empty", "model");
                return false;
            }
            have_model = true;
        } else if (key == "block_size") {
            if (value.type == msgpack::type::POSITIVE_INTEGER) {
                if (value.via.u64 == 0 ||
                    value.via.u64 > static_cast<uint64_t>(
                                        std::numeric_limits<int64_t>::max())) {
                    HttpValidationError(resp, "out_of_range",
                                        "block_size must be a positive integer",
                                        "block_size");
                    return false;
                }
                request->context.block_size =
                    static_cast<int64_t>(value.via.u64);
                have_block_size = true;
            } else if (value.type == msgpack::type::NEGATIVE_INTEGER) {
                HttpValidationError(resp, "out_of_range",
                                    "block_size must be a positive integer",
                                    "block_size");
                return false;
            } else {
                HttpValidationError(resp, "invalid_type",
                                    "block_size must be an integer",
                                    "block_size");
                return false;
            }
        } else if (key == "token_ids") {
            request->token_ids.clear();
            if (value.type == msgpack::type::BIN) {
                const msgpack::object_bin& bin = value.via.bin;
                if (bin.size % sizeof(int32_t) != 0) {
                    HttpValidationError(
                        resp, "invalid_value",
                        "token_ids bin size must be a multiple of 4",
                        "token_ids");
                    return false;
                }
                const size_t count = bin.size / sizeof(int32_t);
                request->token_ids.reserve(count);
                for (size_t t = 0; t < count; ++t) {
                    request->token_ids.push_back(
                        DecodeLeInt32(bin.ptr + t * sizeof(int32_t)));
                }
            } else if (value.type == msgpack::type::ARRAY) {
                const msgpack::object_array& array = value.via.array;
                request->token_ids.reserve(array.size);
                for (uint32_t t = 0; t < array.size; ++t) {
                    const msgpack::object& element = array.ptr[t];
                    int64_t token_id = 0;
                    if (element.type == msgpack::type::POSITIVE_INTEGER) {
                        if (element.via.u64 >
                            static_cast<uint64_t>(
                                std::numeric_limits<int32_t>::max())) {
                            HttpValidationError(
                                resp, "out_of_range",
                                "token_ids element is outside the int32 range",
                                "token_ids", t);
                            return false;
                        }
                        token_id = static_cast<int64_t>(element.via.u64);
                    } else if (element.type ==
                               msgpack::type::NEGATIVE_INTEGER) {
                        if (element.via.i64 <
                            std::numeric_limits<int32_t>::min()) {
                            HttpValidationError(
                                resp, "out_of_range",
                                "token_ids element is outside the int32 range",
                                "token_ids", t);
                            return false;
                        }
                        token_id = element.via.i64;
                    } else {
                        HttpValidationError(
                            resp, "invalid_type",
                            "token_ids element must be an integer", "token_ids",
                            t);
                        return false;
                    }
                    request->token_ids.push_back(
                        static_cast<int32_t>(token_id));
                }
            } else {
                HttpValidationError(resp, "invalid_type",
                                    "token_ids must be a bin or an array",
                                    "token_ids");
                return false;
            }
            have_token_ids = true;
        } else if (key == "tenant_id") {
            if (value.type != msgpack::type::STR) {
                HttpValidationError(resp, "invalid_type",
                                    "tenant_id must be a string", "tenant_id");
                return false;
            }
            request->context.tenant_id.assign(value.via.str.ptr,
                                              value.via.str.size);
        } else if (key == "lora_name") {
            if (value.type != msgpack::type::STR) {
                HttpValidationError(resp, "invalid_type",
                                    "lora_name must be a string", "lora_name");
                return false;
            }
            request->context.lora_name.assign(value.via.str.ptr,
                                              value.via.str.size);
        } else if (key == "cache_salt") {
            if (value.type == msgpack::type::NIL) {
                // no salt
            } else if (value.type == msgpack::type::STR) {
                const std::string salt(value.via.str.ptr, value.via.str.size);
                if (!salt.empty()) {
                    request->cache_salt = salt;
                }
            } else {
                HttpValidationError(resp, "invalid_type",
                                    "cache_salt must be a string or null",
                                    "cache_salt");
                return false;
            }
        } else if (key == "instance_id") {
            if (value.type != msgpack::type::STR) {
                HttpValidationError(resp, "invalid_type",
                                    "instance_id must be a string",
                                    "instance_id");
                return false;
            }
            request->instance_filter =
                std::string(value.via.str.ptr, value.via.str.size);
        } else {
            const std::string field(key);
            HttpValidationError(resp, "unknown_field",
                                "unsupported request field: " + field,
                                field.c_str());
            return false;
        }
    }

    if (!have_model) {
        HttpValidationError(resp, "missing", "model is required", "model");
        return false;
    }
    if (!have_block_size) {
        HttpValidationError(resp, "missing", "block_size is required",
                            "block_size");
        return false;
    }
    if (!have_token_ids) {
        HttpValidationError(resp, "missing", "token_ids is required",
                            "token_ids");
        return false;
    }
    if (request->context.tenant_id.empty()) {
        request->context.tenant_id = "default";
    }
    return true;
}

bool ParseServiceConfigRequest(const msgpack::object_map& body,
                               coro_http_response& resp,
                               common::ServiceConfig* service) {
    static const std::set<std::string> kAllowedFields = {
        "block_size",      "cache_group", "dp_rank",   "endpoint",
        "hash_profile",    "instance_id", "lora_name", "modelname",
        "replay_endpoint", "tenant_id",   "type"};
    std::string publisher_type;
    if (!RejectUnknownFields(body, kAllowedFields, resp) ||
        !RequiredString(body, "endpoint", resp, &service->endpoint) ||
        !RequiredString(body, "type", resp, &publisher_type) ||
        !RequiredString(body, "modelname", resp, &service->model_name) ||
        !RequiredString(body, "instance_id", resp, &service->instance_id) ||
        !RequiredPositiveInt64(body, "block_size", resp,
                               &service->block_size) ||
        !OptionalStringStrict(body, "replay_endpoint", "", resp,
                              &service->replay_endpoint) ||
        !OptionalStringStrict(body, "lora_name", "", resp,
                              &service->lora_name) ||
        !OptionalStringStrict(body, "tenant_id", "default", resp,
                              &service->tenant_id) ||
        !ParseOptionalCacheGroup(body, resp, &service->cache_group) ||
        !ParseHashProfileConfig(body, resp, &service->hash_profile)) {
        return false;
    }
    const auto publisher_kind = common::ParsePublisherKind(publisher_type);
    if (!publisher_kind.has_value()) {
        HttpValidationError(resp, "invalid_value",
                            "type must be vLLM or Mooncake", "type");
        return false;
    }
    service->publisher_kind = *publisher_kind;
    if (service->tenant_id.empty()) {
        service->tenant_id = "default";
    }

    const msgpack::object* dp_rank_value = MapFind(body, "dp_rank");
    if (dp_rank_value == nullptr) {
        HttpValidationError(resp, "missing", "dp_rank is required", "dp_rank");
        return false;
    }
    int64_t dp_rank = 0;
    if (!MsgpackInt64(*dp_rank_value, &dp_rank)) {
        HttpValidationError(resp, "invalid_type", "dp_rank must be an integer",
                            "dp_rank");
        return false;
    }
    if (dp_rank < 0 ||
        dp_rank > static_cast<int64_t>(std::numeric_limits<int>::max())) {
        HttpValidationError(resp, "out_of_range",
                            "dp_rank must be a non-negative int", "dp_rank");
        return false;
    }
    service->dp_rank = static_cast<int>(dp_rank);

    if (const std::string error = ValidateServiceConfig(*service);
        !error.empty()) {
        HttpValidationError(resp, "invalid_registration", error);
        return false;
    }
    return true;
}

void PackCacheHitResult(MsgpackPacker& packer,
                        const prefixindex::CacheHitResult& result) {
    packer.pack_map(6);
    packer.pack("longest_matched");
    packer.pack(result.longest_match_tokens);
    packer.pack("dp");
    packer.pack_map(static_cast<uint32_t>(result.dp.size()));
    for (const auto& [rank, tokens] : result.dp) {
        // Rank keys keep their decimal-string form from the previous wire
        // contract.
        packer.pack(std::to_string(rank));
        packer.pack(tokens);
    }
    packer.pack("rank_matches");
    packer.pack_map(static_cast<uint32_t>(result.rank_matches.size()));
    for (const auto& [rank, match] : result.rank_matches) {
        packer.pack(std::to_string(rank));
        packer.pack_map(3);
        packer.pack("gpu");
        packer.pack(match.gpu);
        packer.pack("cpu");
        packer.pack(match.cpu);
        packer.pack("disk");
        packer.pack(match.disk);
    }
    packer.pack("gpu");
    packer.pack(result.gpu);
    packer.pack("cpu");
    packer.pack(result.cpu);
    packer.pack("disk");
    packer.pack(result.disk);
}

void PackHashProfile(MsgpackPacker& packer,
                     const common::ResolvedHashProfile& profile) {
    // Keep the resolved profile fields in one place so /services and
    // /global_view cannot drift when a new recipe is added.  root_digest is
    // intentionally emitted here only as a derived diagnostic; it is not
    // accepted by ParseHashProfileConfig as registration input.
    packer.pack_map(5);
    packer.pack("strategy");
    packer.pack(profile.strategy);
    packer.pack("algorithm");
    packer.pack(profile.algorithm);
    packer.pack("python_hash_seed");
    packer.pack(profile.python_hash_seed);
    packer.pack("root_digest");
    packer.pack(profile.root_digest);
    packer.pack("index_projection");
    packer.pack(profile.index_projection);
}

void PackServiceConfig(MsgpackPacker& packer,
                       const common::ServiceConfig& svc) {
    // Field names are the exported struct-field names of
    // common.ServiceConfig (fixed wire contract).
    packer.pack_map(11);
    packer.pack("Endpoint");
    packer.pack(svc.endpoint);
    packer.pack("ReplayEndpoint");
    packer.pack(svc.replay_endpoint);
    packer.pack("Type");
    packer.pack(std::string(common::PublisherKindName(svc.publisher_kind)));
    packer.pack("ModelName");
    packer.pack(svc.model_name);
    packer.pack("LoraName");
    packer.pack(svc.lora_name);
    packer.pack("TenantID");
    packer.pack(svc.tenant_id);
    packer.pack("InstanceID");
    packer.pack(svc.instance_id);
    packer.pack("BlockSize");
    packer.pack(svc.block_size);
    packer.pack("DPRank");
    packer.pack(svc.dp_rank);
    packer.pack("CacheGroup");
    if (svc.cache_group.has_value()) {
        packer.pack(*svc.cache_group);
    } else {
        packer.pack_nil();
    }
    packer.pack("HashProfile");
    PackHashProfile(packer, svc.hash_profile);
}

}  // namespace

std::string MakeServiceKey(const std::string& instance_id,
                           const std::string& tenant_id, int dp_rank) {
    return instance_id + "|" + tenant_id + "|" + std::to_string(dp_rank);
}

EventManager::EventManager(std::vector<common::ServiceConfig> services,
                           int http_server_port)
    : services_(std::move(services)), http_server_port_(http_server_port) {}

EventManager::~EventManager() { Stop(); }

bool EventManager::IsStopped() {
    std::shared_lock lock(mu_);
    return stopped_;
}

void EventManager::Start() {
    LOG(INFO) << "Starting KV Event Manager...";

    std::vector<common::ServiceConfig> services_snapshot;
    {
        std::shared_lock lock(mu_);
        services_snapshot = services_;
    }

    // Subscribe to all services concurrently.
    // mu_ serialises the check-then-act inside SubscribeToService and
    // serialises with concurrent /register HTTP handlers so that
    // subscribers_, active_configs_, and services_ stay consistent.
    std::atomic<int> failure_count{0};
    std::vector<std::thread> workers;
    workers.reserve(services_snapshot.size());
    for (const auto& svc : services_snapshot) {
        workers.emplace_back([this, svc, &failure_count] {
            std::pair<bool, std::string> result;
            {
                std::unique_lock lock(mu_);
                result = SubscribeToService(svc);
            }
            if (!result.second.empty()) {
                LOG(ERROR) << "Failed to initiate subscription service_type="
                           << common::PublisherKindName(svc.publisher_kind)
                           << " instance_id=" << svc.instance_id
                           << " endpoint=" << svc.endpoint
                           << " error=" << result.second;
                failure_count.fetch_add(1);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    const int failed = failure_count.load();
    LOG(INFO) << "Static KV Event Manager started. Subscriptions success="
              << (static_cast<int>(services_snapshot.size()) - failed)
              << " failed=" << failed;
}

void EventManager::Stop() {
    {
        std::unique_lock lock(mu_);
        if (stopped_) {
            stop_cv_.wait(lock, [this] { return stop_complete_; });
            return;
        }
        stopped_ = true;
        for (const auto& [unused_key, handler] : handlers_) {
            (void)unused_key;
            handler->MarkUnavailable();
        }
    }

    LOG(INFO) << "Stopping Conductor KV Event Manager.....";

    // yalantinglibs closes the acceptor and current connections, stops the
    // thread pool, and joins the server thread before stop() returns.
    if (http_server_) {
        LOG(INFO) << "Shutting down HTTP server";
        http_server_->stop();
    }

    // Stop all ZMQ clients. Collect them under the lock but Stop()
    // outside it — same deadlock rule as UnsubscribeFromService.
    std::vector<std::pair<std::string, std::shared_ptr<zmq::ZMQClient>>>
        clients;
    std::vector<std::shared_ptr<KVEventHandler>> handlers;
    {
        std::unique_lock lock(mu_);
        clients.assign(subscribers_.begin(), subscribers_.end());
        handlers.reserve(handlers_.size());
        for (const auto& [unused_key, handler] : handlers_) {
            (void)unused_key;
            handlers.push_back(handler);
        }
    }
    for (auto& [key, client] : clients) {
        client->Stop();
        LOG(INFO) << "Stopped all subscription service_key=" << key;
    }
    for (const auto& handler : handlers) {
        handler->WaitForIdle();
    }

    {
        std::unique_lock lock(mu_);
        stop_complete_ = true;
    }
    stop_cv_.notify_all();
}

std::pair<bool, std::string> EventManager::SubscribeToService(
    const common::ServiceConfig& svc) {
    // The caller holds mu_ exclusively, so read stopped_ directly rather
    // than recursively acquiring the non-recursive shared mutex via
    // IsStopped().
    if (stopped_) {
        return {false, "manager stopped"};
    }

    if (const std::string error = ValidateServiceConfig(svc); !error.empty()) {
        return {false, "invalid service registration: " + error};
    }

    const std::string svc_key =
        MakeServiceKey(svc.instance_id, svc.tenant_id, svc.dp_rank);
    if (unregistering_.contains(svc_key)) {
        return {false, "service is being unregistered: " + svc_key};
    }
    if (cleanup_quarantined_.contains(svc_key)) {
        return {false, "service cleanup is quarantined: " + svc_key};
    }
    if (auto existing = active_configs_.find(svc_key);
        existing != active_configs_.end()) {
        if (existing->second == svc) {
            return {false, ""};
        }
        return {false, "conflicting registration for service key: " + svc_key};
    }
    if (subscribers_.contains(svc_key)) {
        return {false,
                "inconsistent subscriber state for service key: " + svc_key};
    }
    if (auto endpoint = active_endpoints_.find(svc.endpoint);
        endpoint != active_endpoints_.end()) {
        return {false, "conflicting active registration for endpoint: " +
                           svc.endpoint};
    }

    // Use ReplayEndpoint directly, fallback to empty if not provided
    const std::string replay_endpoint = svc.replay_endpoint;

    auto handler = std::make_shared<KVEventHandler>(this, svc);

    // Configure ZMQ Client
    zmq::ZMQClientConfig zmq_config;
    zmq_config.cache_pool_key = svc_key;
    zmq_config.endpoint = svc.endpoint;
    zmq_config.replay_endpoint = replay_endpoint;
    zmq_config.model_name = svc.model_name;
    zmq_config.publisher_kind = svc.publisher_kind;
    zmq_config.poll_timeout = std::chrono::milliseconds(100);
    zmq_config.replay_timeout = std::chrono::seconds(5);
    zmq_config.reconnect_delay = std::chrono::seconds(1);

    if (auto err = zmq::ValidateConfig(zmq_config); !err.empty()) {
        return {false, "invalid ZMQ config: " + err};
    }

    bool inserted_registration = false;
    if (svc.publisher_kind == common::PublisherKind::kVllm) {
        const auto registration_result =
            indexer_.Register(RegistrationFromService(svc));
        if (!registration_result.error.empty()) {
            return {false, "failed to register prefix context: " +
                               registration_result.error};
        }
        inserted_registration = registration_result.inserted;
    } else {
        const std::string binding_error = indexer_.ValidateProfileBinding(
            ContextFromService(svc), ProfileFromService(svc));
        if (!binding_error.empty() &&
            binding_error != "ContextKey is not registered") {
            return {false,
                    "failed to bind shared-cache profile: " + binding_error};
        }
    }

    auto client = std::make_shared<zmq::ZMQClient>(zmq_config, handler);
    if (auto err = client->Start(); !err.empty()) {
        if (inserted_registration) {
            const std::string rollback_error = indexer_.Unregister(
                ContextFromService(svc), svc.instance_id, svc.dp_rank);
            if (!rollback_error.empty()) {
                LOG(ERROR) << "Registration rollback failed service_key="
                           << svc_key << " error=" << rollback_error;
            }
        }
        return {false, "failed to start ZMQ client: " + err};
    }

    subscribers_[svc_key] = client;
    handlers_[svc_key] = handler;
    active_configs_[svc_key] = svc;
    active_endpoints_[svc.endpoint] = svc_key;

    LOG(INFO) << "Successfully subscribed to service publisher_kind="
              << common::PublisherKindName(svc.publisher_kind)
              << " service_key=" << svc_key
              << " instance_id=" << svc.instance_id
              << " tenant_id=" << svc.tenant_id << " endpoint=" << svc.endpoint
              << " replay_endpoint=" << replay_endpoint;

    return {true, ""};
}

std::pair<bool, std::string> EventManager::UnsubscribeFromService(
    const std::string& instance_id, const std::string& tenant_id, int dp_rank) {
    const std::string svc_key = MakeServiceKey(instance_id, tenant_id, dp_rank);

    std::shared_ptr<zmq::ZMQClient> client;
    std::shared_ptr<KVEventHandler> handler;
    common::ServiceConfig service;
    {
        std::unique_lock lock(mu_);
        if (unregistering_.contains(svc_key)) {
            return {false, "service is already being unregistered: " + svc_key};
        }
        auto client_it = subscribers_.find(svc_key);
        auto handler_it = handlers_.find(svc_key);
        auto config_it = active_configs_.find(svc_key);
        if (client_it == subscribers_.end() || handler_it == handlers_.end() ||
            config_it == active_configs_.end()) {
            return {false, "service not found: " + svc_key};
        }
        client = client_it->second;
        handler = handler_it->second;
        service = config_it->second;
        // Keep the maps populated while Stop joins the event loop. This
        // reserves the key and prevents a replacement registration from being
        // removed by this in-flight unregister operation.
        unregistering_.insert(svc_key);
        handler->MarkUnavailable();
    }

    // Stop the ZMQ client OUTSIDE mu_ to avoid deadlock:
    // HandleBatch acquires mu_ (read). If the ZMQ event-loop thread is
    // currently inside HandleBatch (or about to enter it), holding mu_
    // while waiting for that thread to exit via client->Stop() -> join
    // would deadlock.
    client->Stop();
    handler->WaitForIdle();

    std::string index_error = handler->InvalidateEndpoint();
    if (service.publisher_kind == common::PublisherKind::kVllm) {
        const std::string unregister_error = indexer_.Unregister(
            ContextFromService(service), service.instance_id, service.dp_rank);
        if (index_error.empty()) {
            index_error = unregister_error;
        }
    }
    if (!index_error.empty()) {
        std::unique_lock lock(mu_);
        unregistering_.erase(svc_key);
        cleanup_quarantined_.insert(svc_key);
        LOG(ERROR) << "Endpoint cleanup quarantined service_key=" << svc_key
                   << " endpoint=" << service.endpoint
                   << " error=" << index_error;
        return {true, index_error};
    }

    {
        std::unique_lock lock(mu_);
        subscribers_.erase(svc_key);
        handlers_.erase(svc_key);
        active_configs_.erase(svc_key);
        if (auto endpoint = active_endpoints_.find(service.endpoint);
            endpoint != active_endpoints_.end() &&
            endpoint->second == svc_key) {
            active_endpoints_.erase(endpoint);
        }
        if (auto service_it =
                std::find(services_.begin(), services_.end(), service);
            service_it != services_.end()) {
            services_.erase(service_it);
        }
        unregistering_.erase(svc_key);
        cleanup_quarantined_.erase(svc_key);
    }

    LOG(INFO) << "Successfully unsubscribed from service service_key="
              << svc_key << " instance_id=" << instance_id
              << " tenant_id=" << tenant_id;
    return {true, ""};
}

void EventManager::RegisterHttpHandlers() {
    using coro_http::GET;
    using coro_http::POST;
    auto* server = http_server_.get();

    // ---- /query ---------------------------------------------------------
    // msgpack-only protocol (JSON support was dropped during development:
    // JsonCpp DOM parsing dominates cost at long context). token_ids may be
    // a bin of little-endian int32 or an integer array. Responses and error
    // bodies remain JSON.
    server->set_http_handler<POST>(
        "/query", [this](coro_http_request& req, coro_http_response& resp) {
            VLOG(1) << "receive req method=POST path=/query";

            const std::string_view content_type =
                req.get_header_value("content-type");
            if (content_type.find("application/msgpack") ==
                std::string_view::npos) {
                HttpValidationError(resp, "unsupported_content_type",
                                    "Content-Type must be application/msgpack");
                return;
            }

            QueryRequest query;
            if (!ParseQueryMsgpackRequest(req, resp, &query)) {
                return;
            }

            const auto results =
                indexer_.Query(query.context, query.token_ids, query.cache_salt,
                               query.instance_filter);
            msgpack::sbuffer body;
            MsgpackPacker packer(&body);
            packer.pack_map(1);
            packer.pack("instances");
            packer.pack_map(static_cast<uint32_t>(results.size()));
            for (const auto& [instance_id, result] : results) {
                packer.pack(instance_id);
                PackCacheHitResult(packer, result);
            }
            HttpMsgpack(resp, status_type::ok, body);
        });
    server->set_http_handler<GET>("/query", [](coro_http_request&,
                                               coro_http_response& resp) {
        HttpError(resp, status_type::method_not_allowed, "Method not allowed");
    });

    // ---- /register ------------------------------------------------------
    server->set_http_handler<POST>(
        "/register", [this](coro_http_request& req, coro_http_response& resp) {
            msgpack::object_handle handle;
            msgpack::object_map body{};
            if (!ParseMsgpackBody(req, resp, "register", &handle, &body)) {
                return;
            }

            common::ServiceConfig svc;
            if (!ParseServiceConfigRequest(body, resp, &svc)) {
                return;
            }

            {
                std::unique_lock lock(mu_);
                auto [is_new, err] = SubscribeToService(svc);
                if (!err.empty()) {
                    lock.unlock();
                    LOG(ERROR) << "Dynamic register failed instance_id="
                               << svc.instance_id << " err=" << err;
                    if (err.starts_with("failed to start ZMQ client")) {
                        HttpError(resp, status_type::internal_server_error,
                                  "Failed to subscribe: " + err);
                    } else {
                        HttpValidationError(resp, "invalid_registration", err);
                    }
                    return;
                }
                if (is_new) {
                    services_.push_back(svc);
                }
            }

            msgpack::sbuffer response_body;
            MsgpackPacker packer(&response_body);
            packer.pack_map(2);
            packer.pack("status");
            packer.pack("registered successfully");
            packer.pack("instance_id");
            packer.pack(svc.instance_id);
            HttpMsgpack(resp, status_type::ok, response_body);
        });
    server->set_http_handler<GET>("/register", [](coro_http_request&,
                                                  coro_http_response& resp) {
        HttpError(resp, status_type::method_not_allowed, "Method not allowed");
    });

    // ---- /unregister ----------------------------------------------------
    server->set_http_handler<POST>(
        "/unregister",
        [this](coro_http_request& req, coro_http_response& resp) {
            msgpack::object_handle handle;
            msgpack::object_map body{};
            if (!ParseMsgpackBody(req, resp, "unregister", &handle, &body)) {
                return;
            }

            static const std::set<std::string> kAllowedFields = {
                "dp_rank", "instance_id", "tenant_id"};
            if (!RejectUnknownFields(body, kAllowedFields, resp)) {
                return;
            }
            std::string target_tenant;
            std::string instance_id;
            if (!RequiredString(body, "instance_id", resp, &instance_id) ||
                !OptionalStringStrict(body, "tenant_id", "default", resp,
                                      &target_tenant)) {
                return;
            }
            if (target_tenant.empty()) {
                target_tenant = "default";
            }
            const msgpack::object* dp_rank_value = MapFind(body, "dp_rank");
            if (dp_rank_value == nullptr) {
                HttpValidationError(resp, "missing", "dp_rank is required",
                                    "dp_rank");
                return;
            }
            int64_t parsed_rank = 0;
            if (!MsgpackInt64(*dp_rank_value, &parsed_rank) ||
                parsed_rank < 0 ||
                parsed_rank >
                    static_cast<int64_t>(std::numeric_limits<int>::max())) {
                HttpValidationError(resp, "invalid_value",
                                    "dp_rank must be a non-negative int",
                                    "dp_rank");
                return;
            }
            const int dp_rank = static_cast<int>(parsed_rank);
            const std::string target_key =
                MakeServiceKey(instance_id, target_tenant, dp_rank);

            const auto [removed_service, error] =
                UnsubscribeFromService(instance_id, target_tenant, dp_rank);
            if (!removed_service) {
                HttpError(
                    resp, status_type::not_found,
                    error.empty() ? "service not found: " + target_key : error);
                return;
            }
            if (!error.empty()) {
                HttpError(resp, status_type::internal_server_error,
                          "Failed to unregister prefix context: " + error);
                return;
            }

            msgpack::sbuffer response_body;
            MsgpackPacker packer(&response_body);
            packer.pack_map(2);
            packer.pack("status");
            packer.pack("unregistered successfully");
            packer.pack("removed_instances");
            packer.pack_array(1);
            packer.pack(target_key);
            HttpMsgpack(resp, status_type::ok, response_body);
        });
    server->set_http_handler<GET>("/unregister", [](coro_http_request&,
                                                    coro_http_response& resp) {
        HttpError(resp, status_type::method_not_allowed, "Method not allowed");
    });

    // ---- /global_view ---------------------------------------------------
    server->set_http_handler<GET>(
        "/global_view", [this](coro_http_request&, coro_http_response& resp) {
            const auto global_view = indexer_.GetGlobalView();

            msgpack::sbuffer body;
            MsgpackPacker packer(&body);
            packer.pack_map(2);
            packer.pack("context_count");
            packer.pack(static_cast<uint64_t>(global_view.context_count));
            packer.pack("contexts");
            packer.pack_array(
                static_cast<uint32_t>(global_view.contexts.size()));
            for (const auto& view : global_view.contexts) {
                packer.pack_map(7);
                packer.pack("model_name");
                packer.pack(view.context.model_name);
                packer.pack("lora_name");
                packer.pack(view.context.lora_name);
                packer.pack("block_size");
                packer.pack(view.context.block_size);
                packer.pack("tenant_id");
                packer.pack(view.context.tenant_id);
                packer.pack("prefix_count");
                packer.pack(static_cast<uint64_t>(view.prefix_count));
                packer.pack("hash_profile");
                PackHashProfile(packer, view.profile);
                packer.pack("instances");
                packer.pack_map(
                    static_cast<uint32_t>(view.instance_ranks.size()));
                for (const auto& [instance_id, ranks] : view.instance_ranks) {
                    packer.pack(instance_id);
                    packer.pack_array(static_cast<uint32_t>(ranks.size()));
                    for (const int64_t rank : ranks) {
                        packer.pack(rank);
                    }
                }
            }
            HttpMsgpack(resp, status_type::ok, body);
        });
    server->set_http_handler<POST>(
        "/global_view", [](coro_http_request&, coro_http_response& resp) {
            HttpError(resp, status_type::method_not_allowed,
                      "Method not allowed");
        });

    // ---- /services ------------------------------------------------------
    server->set_http_handler<GET>(
        "/services", [this](coro_http_request&, coro_http_response& resp) {
            VLOG(1) << "receive req method=GET path=/services";

            msgpack::sbuffer body;
            MsgpackPacker packer(&body);
            packer.pack_map(2);
            packer.pack("count");
            {
                std::shared_lock lock(mu_);
                packer.pack(static_cast<uint64_t>(active_configs_.size()));
                packer.pack("services");
                packer.pack_array(
                    static_cast<uint32_t>(active_configs_.size()));
                for (const auto& [key, svc] : active_configs_) {
                    PackServiceConfig(packer, svc);
                }
            }
            HttpMsgpack(resp, status_type::ok, body);
        });
    server->set_http_handler<POST>("/services", [](coro_http_request&,
                                                   coro_http_response& resp) {
        HttpError(resp, status_type::method_not_allowed, "Method not allowed");
    });
}

bool EventManager::StartHTTPServer() {
    http_server_ = std::make_unique<coro_http::coro_http_server>(
        /*thread_num=*/4, static_cast<unsigned short>(http_server_port_));
    RegisterHttpHandlers();

    LOG(INFO) << "HTTP server listening port=" << http_server_port_;
    // async_start returns a future that resolves on failure or stop;
    // errors surface asynchronously and are logged rather than returned.
    auto future = http_server_->async_start();
    // Give a synchronous bind failure a brief chance to surface, so
    // callers see startup errors like "address in use".
    if (future.hasResult()) {
        const auto ec = std::move(future).get();
        if (ec) {
            LOG(ERROR) << "HTTP server failed err=" << ec.message();
            return false;
        }
    }
    return true;
}

}  // namespace kvevent
}  // namespace conductor
