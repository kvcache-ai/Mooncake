#include "storage/distributed/s3_adapter.h"

#include <boost/algorithm/string.hpp>

#include <string_view>
#include <utility>

#include <glog/logging.h>

#include "config/s3_adapter_config.h"
#include "storage/distributed/object_storage_signing.h"

namespace mooncake {
namespace {

using object_storage_signing::CanonicalQuery;
using object_storage_signing::Hex;
using object_storage_signing::HmacSha256;
using object_storage_signing::Sha256Hex;
using object_storage_signing::SigningTimestamp;
using object_storage_signing::UriEncode;

constexpr char kUnsignedPayload[] = "UNSIGNED-PAYLOAD";

}  // namespace

tl::expected<void, ErrorCode> S3ObjectStorageAdapter::Init() {
    auto config = S3AdapterConfig::FromEnvironment();
    if (!config.has_value()) {
        return tl::make_unexpected(config.error());
    }
    endpoint_ = std::move(config->endpoint);
    bucket_ = std::move(config->bucket);
    region_ = std::move(config->region);
    access_key_id_ = std::move(config->access_key_id);
    access_key_secret_ = std::move(config->access_key_secret);
    security_token_ = std::move(config->security_token);
    path_style_ = config->path_style;
    anonymous_ = config->anonymous;
    max_connections_ = config->max_connections;
    receive_buffer_size_ = config->receive_buffer_size;
    upload_buffer_size_ = config->upload_buffer_size;
    FinishInit();
    return {};
}

std::string S3ObjectStorageAdapter::HostHeader() const {
    // S3AdapterConfig guarantees an http:// or https:// scheme.
    std::string_view rest = endpoint_;
    bool https = true;
    if (const size_t scheme = rest.find("://"); scheme != std::string::npos) {
        https = boost::algorithm::iequals(rest.substr(0, scheme), "https");
        rest.remove_prefix(scheme + 3);
    }
    if (const size_t slash = rest.find('/'); slash != std::string::npos) {
        rest = rest.substr(0, slash);
    }
    std::string host(rest);
    // curl omits the default port from the Host header it would send; the
    // signed value must match what the server receives.
    const std::string default_port = https ? ":443" : ":80";
    if (boost::algorithm::ends_with(host, default_port)) {
        host.resize(host.size() - default_port.size());
    }
    boost::algorithm::to_lower(host);
    return path_style_ ? host : bucket_ + "." + host;
}

std::string S3ObjectStorageAdapter::CanonicalUri(
    const std::string& physical_key) const {
    // S3 encodes each path segment once (no double encoding, unlike other
    // AWS services). Path-style requests carry the bucket in the path.
    const std::string key = UriEncode(physical_key, true);
    return path_style_ ? "/" + UriEncode(bucket_) + "/" + key : "/" + key;
}

std::vector<std::string> S3ObjectStorageAdapter::BuildSignedHeaders(
    const std::string& method, const std::string& physical_key,
    const std::map<std::string, std::string>& query,
    const std::string& range) const {
    return BuildSignedHeadersAt(method, physical_key, query, range,
                                SigningTimestamp().first);
}

std::vector<std::string> S3ObjectStorageAdapter::BuildSignedHeadersAt(
    const std::string& method, const std::string& physical_key,
    const std::map<std::string, std::string>& query,
    const std::string& /*range*/, const std::string& timestamp) const {
    // A Range header is sent but not signed; Signature V4 does not require
    // it, and S3 needs no extra header for standard range semantics.
    std::map<std::string, std::string> signed_headers{
        {"host", HostHeader()},
        {"x-amz-content-sha256", kUnsignedPayload},
        {"x-amz-date", timestamp},
    };
    if (!security_token_.empty())
        signed_headers["x-amz-security-token"] = security_token_;

    std::vector<std::string> headers;
    std::string canonical_headers;
    std::string signed_header_names;
    for (const auto& [name, value] : signed_headers) {
        // curl takes "Host:" from this list instead of deriving it from the
        // URL, so the signed value is the sent value.
        headers.push_back((name == "host" ? std::string("Host") : name) + ": " +
                          value);
        canonical_headers +=
            name + ":" + boost::algorithm::trim_copy(value) + "\n";
        if (!signed_header_names.empty()) signed_header_names.push_back(';');
        signed_header_names += name;
    }
    if (anonymous_) return headers;

    const std::string date = timestamp.substr(0, 8);
    const std::string canonical_request =
        method + "\n" + CanonicalUri(physical_key) + "\n" +
        CanonicalQuery(query, true) + "\n" + canonical_headers + "\n" +
        signed_header_names + "\n" + kUnsignedPayload;
    const std::string scope = date + "/" + region_ + "/s3/aws4_request";
    const std::string string_to_sign = "AWS4-HMAC-SHA256\n" + timestamp + "\n" +
                                       scope + "\n" +
                                       Sha256Hex(canonical_request);

    const std::string secret = "AWS4" + access_key_secret_;
    auto date_key = HmacSha256(secret.data(), secret.size(), date);
    auto region_key = HmacSha256(date_key.data(), date_key.size(), region_);
    auto service_key = HmacSha256(region_key.data(), region_key.size(), "s3");
    auto signing_key =
        HmacSha256(service_key.data(), service_key.size(), "aws4_request");
    auto signature =
        HmacSha256(signing_key.data(), signing_key.size(), string_to_sign);
    headers.push_back(
        "Authorization: AWS4-HMAC-SHA256 Credential=" + access_key_id_ + "/" +
        scope + ", SignedHeaders=" + signed_header_names +
        ", Signature=" + Hex(signature.data(), signature.size()));
    return headers;
}

}  // namespace mooncake
