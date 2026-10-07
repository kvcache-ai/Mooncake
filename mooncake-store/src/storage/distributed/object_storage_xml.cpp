#include "storage/distributed/object_storage_xml.h"

#include <boost/algorithm/string.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <charconv>
#include <optional>
#include <sstream>
#include <system_error>
#include <utility>

namespace mooncake::object_storage_xml {
namespace {

namespace ptree = boost::property_tree;

// Parses `body` into a tree with comments dropped; nullopt when the document
// is not well-formed. Entities and CDATA are decoded by the parser, and tags
// may carry whitespace and attributes such as xmlns.
std::optional<ptree::ptree> ReadDocument(std::string_view body,
                                         std::string* error) {
    std::istringstream stream{std::string(body)};
    ptree::ptree document;
    try {
        ptree::read_xml(stream, document, ptree::xml_parser::no_comments);
    } catch (const ptree::xml_parser_error& e) {
        if (error) *error = e.message();
        return std::nullopt;
    }
    return document;
}

// The element named `root`, if it is the document's only top-level element.
// The parser accepts several top-level elements, which XML does not.
const ptree::ptree* Root(const ptree::ptree& document, const char* root) {
    if (document.size() != 1 || document.front().first != root) return nullptr;
    return &document.front().second;
}

// The text of the child element `name`, or nullopt unless there is exactly
// one such element.
std::optional<std::string> OnlyChildText(const ptree::ptree& node,
                                         const char* name) {
    if (node.count(name) != 1) return std::nullopt;
    return node.get_child(name).data();
}

std::optional<size_t> ParseSize(std::string text) {
    boost::algorithm::trim(text);
    size_t value = 0;
    const char* end = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(text.data(), end, value);
    if (ec != std::errc() || ptr != end) return std::nullopt;
    return value;
}

}  // namespace

tl::expected<ListObjectsPage, std::string> ParseListObjectsV2(
    std::string_view body) {
    std::string error;
    const auto document = ReadDocument(body, &error);
    if (!document) return tl::make_unexpected("not well-formed XML: " + error);
    const ptree::ptree* root = Root(*document, "ListBucketResult");
    if (!root)
        return tl::make_unexpected("root element is not ListBucketResult");

    ListObjectsPage page;
    for (const auto& [name, contents] : *root) {
        if (name != "Contents") continue;
        auto key = OnlyChildText(contents, "Key");
        auto size_text = OnlyChildText(contents, "Size");
        if (!key || !size_text)
            return tl::make_unexpected(
                "Contents element without exactly one Key and one Size");
        const auto size = ParseSize(std::move(*size_text));
        if (!size)
            return tl::make_unexpected("Contents element has an invalid Size");
        page.objects.push_back({std::move(*key), *size});
    }

    // IsTruncated is mandatory: without it the end of the listing cannot be
    // told apart from a cut-off response.
    auto truncated = OnlyChildText(*root, "IsTruncated");
    if (truncated) {
        boost::algorithm::trim(*truncated);
        boost::algorithm::to_lower(*truncated);
    }
    if (!truncated || (*truncated != "true" && *truncated != "false"))
        return tl::make_unexpected("no valid IsTruncated element");
    page.is_truncated = *truncated == "true";

    if (root->count("NextContinuationToken") > 1)
        return tl::make_unexpected("more than one NextContinuationToken");
    page.next_continuation_token =
        root->get<std::string>("NextContinuationToken", "");
    return page;
}

std::string ErrorCodeAndMessage(std::string_view body) {
    const auto document = ReadDocument(body, nullptr);
    if (!document) return {};
    const ptree::ptree* root = Root(*document, "Error");
    if (!root) return {};
    const std::string code = root->get<std::string>("Code", "");
    const std::string message = root->get<std::string>("Message", "");
    if (code.empty() && message.empty()) return {};
    return code + ": " + message.substr(0, 256);
}

}  // namespace mooncake::object_storage_xml
