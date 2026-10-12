#include "storage/distributed/object_storage_xml.h"

#include <boost/algorithm/string.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <charconv>
#include <optional>
#include <sstream>
#include <system_error>
#include <utility>
#include <vector>

namespace mooncake::object_storage_xml {
namespace {

namespace ptree = boost::property_tree;

// True when every end tag names the element opened last. `body` must already
// have been accepted by the XML reader, which checks everything else but does
// not compare an end tag's name with its start tag. Without this check
// "<A><Contents>...</A></Contents>" would parse, with the object under A.
bool EndTagsMatch(std::string_view body) {
    constexpr std::string_view kNameEnd = " \t\r\n/>";
    std::vector<std::string_view> open;
    size_t pos = 0;
    while ((pos = body.find('<', pos)) != std::string_view::npos) {
        const std::string_view rest = body.substr(pos);
        // Comments, CDATA sections and processing instructions hold no tags.
        std::string_view terminator;
        if (rest.starts_with("<!--")) {
            terminator = "-->";
        } else if (rest.starts_with("<![CDATA[")) {
            terminator = "]]>";
        } else if (rest.starts_with("<?")) {
            terminator = "?>";
        } else if (rest.starts_with("<!")) {
            terminator = ">";
        }
        if (!terminator.empty()) {
            const size_t end = body.find(terminator, pos);
            if (end == std::string_view::npos) return false;
            pos = end + terminator.size();
            continue;
        }

        const bool is_end_tag = rest.starts_with("</");
        const size_t name_begin = pos + (is_end_tag ? 2 : 1);
        const size_t name_end = body.find_first_of(kNameEnd, name_begin);
        if (name_end == std::string_view::npos) return false;
        const std::string_view name =
            body.substr(name_begin, name_end - name_begin);

        // The tag ends at the first '>' outside a quoted attribute value.
        size_t tag_end = name_end;
        char quote = '\0';
        for (; tag_end < body.size(); ++tag_end) {
            const char c = body[tag_end];
            if (quote != '\0') {
                if (c == quote) quote = '\0';
            } else if (c == '"' || c == '\'') {
                quote = c;
            } else if (c == '>') {
                break;
            }
        }
        if (tag_end == body.size()) return false;

        if (is_end_tag) {
            if (open.empty() || open.back() != name) return false;
            open.pop_back();
        } else if (body[tag_end - 1] != '/') {
            open.push_back(name);
        }
        pos = tag_end + 1;
    }
    return open.empty();
}

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
    if (!EndTagsMatch(body)) {
        if (error) *error = "an end tag does not match its start tag";
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

// The parser stores an element's attributes as a child with this name.
constexpr char kAttributes[] = "<xmlattr>";

// True when `node` has an element named `name` anywhere below it.
bool HasDescendant(const ptree::ptree& node, const char* name) {
    for (const auto& [child_name, child] : node) {
        if (child_name == kAttributes) continue;
        if (child_name == name || HasDescendant(child, name)) return true;
    }
    return false;
}

// The text of the child element `name`, or nullopt unless there is exactly
// one such element and it holds text only.
std::optional<std::string> OnlyChildText(const ptree::ptree& node,
                                         const char* name) {
    if (node.count(name) != 1) return std::nullopt;
    const ptree::ptree& child = node.get_child(name);
    for (const auto& [grandchild_name, unused] : child) {
        if (grandchild_name != kAttributes) return std::nullopt;
    }
    return child.data();
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
        // Objects are listed only as direct children of the root. A Contents
        // element anywhere deeper would be left out of the listing.
        if (HasDescendant(contents, "Contents"))
            return tl::make_unexpected("Contents element nested inside " +
                                       name);
        if (name != "Contents") continue;
        auto key = OnlyChildText(contents, "Key");
        auto size_text = OnlyChildText(contents, "Size");
        if (!key || !size_text)
            return tl::make_unexpected(
                "Contents element without exactly one text-only Key and Size");
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
