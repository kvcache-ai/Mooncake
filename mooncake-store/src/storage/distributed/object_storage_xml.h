#pragma once

// XML response parsing for object storage adapters, built on
// Boost.PropertyTree. ListObjectsV2 responses share one schema across S3 and
// OSS; how the continuation token and keys are encoded is left to each
// adapter.

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

namespace mooncake::object_storage_xml {

struct ListedObject {
    std::string key;  // XML-decoded, otherwise as sent by the service
    size_t size = 0;
};

struct ListObjectsPage {
    std::vector<ListedObject> objects;
    bool is_truncated = false;
    std::string next_continuation_token;  // empty when absent
};

// Parses a ListObjectsV2 ListBucketResult document. A document that is not
// well-formed, or lacks IsTruncated or a Key or Size in any Contents element,
// is an error (with a reason for logging): it may be a cut-off response, and
// a partial listing must not be mistaken for a complete one.
tl::expected<ListObjectsPage, std::string> ParseListObjectsV2(
    std::string_view body);

// Returns "Code: Message" from an Error document, or an empty string when
// the body carries neither.
std::string ErrorCodeAndMessage(std::string_view body);

}  // namespace mooncake::object_storage_xml
