#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "../src/storage/distributed/object_storage_xml.h"

namespace mooncake::object_storage_xml {
namespace {

const char kNotTruncated[] = "<IsTruncated>false</IsTruncated>";

std::string Listing(const std::string& contents,
                    const std::string& tail = kNotTruncated) {
    return "<ListBucketResult>" + contents + tail + "</ListBucketResult>";
}

const char kObject[] = "<Contents><Key>p/a</Key><Size>3</Size></Contents>";

TEST(ObjectStorageXmlTest, ParsesListObjectsV2Page) {
    const std::string body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<ListBucketResult "
        "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Name>kv-bucket</Name><Prefix>p/</Prefix><KeyCount>2</KeyCount>"
        "<MaxKeys>1000</MaxKeys><EncodingType>url</EncodingType>"
        "<IsTruncated>true</IsTruncated>"
        "<Contents><Key>p/a</Key><LastModified>2026-10-09T00:00:00.000Z"
        "</LastModified><ETag>&quot;abc&quot;</ETag><Size>3</Size>"
        "<StorageClass>STANDARD</StorageClass></Contents>"
        "<Contents><Key>p/b%2Fc</Key><Size>18446744073709551615</Size>"
        "</Contents>"
        "<NextContinuationToken>1/abc%2Fdef+==</NextContinuationToken>"
        "</ListBucketResult>";
    auto page = ParseListObjectsV2(body);
    ASSERT_TRUE(page.has_value()) << page.error();
    ASSERT_EQ(page->objects.size(), 2u);
    EXPECT_EQ(page->objects[0].key, "p/a");
    EXPECT_EQ(page->objects[0].size, 3u);
    // Keys are returned as sent; URL decoding is the adapter's job.
    EXPECT_EQ(page->objects[1].key, "p/b%2Fc");
    EXPECT_EQ(page->objects[1].size, 18446744073709551615u);
    EXPECT_TRUE(page->is_truncated);
    EXPECT_EQ(page->next_continuation_token, "1/abc%2Fdef+==");
}

// XML allows whitespace before the closing '>' of start and end tags. Exact
// tag matching skipped such an element and returned a shorter listing.
TEST(ObjectStorageXmlTest, AcceptsWhitespaceInTags) {
    const std::string body =
        "<ListBucketResult >\n"
        "  <Contents >\n"
        "    <Key >p/a</Key >\n"
        "    <Size\n>3</Size\t>\n"
        "  </Contents >\n"
        "  <Contents\n><Key>p/b</Key><Size> 4 </Size></Contents\n>\n"
        "  <IsTruncated > false </IsTruncated >\n"
        "</ListBucketResult >\n";
    auto page = ParseListObjectsV2(body);
    ASSERT_TRUE(page.has_value()) << page.error();
    ASSERT_EQ(page->objects.size(), 2u);
    EXPECT_EQ(page->objects[0].key, "p/a");
    EXPECT_EQ(page->objects[0].size, 3u);
    EXPECT_EQ(page->objects[1].key, "p/b");
    EXPECT_EQ(page->objects[1].size, 4u);
    EXPECT_FALSE(page->is_truncated);
}

TEST(ObjectStorageXmlTest, AcceptsAttributesCommentsAndUnknownElements) {
    const std::string body = Listing(
        "<!-- page 1 --><Owner><ID>x</ID></Owner>"
        "<Contents xmlns:x=\"urn:x\" x:extra=\"1\"><Key>p/a</Key>"
        "<Size>3</Size><x:Unknown>y</x:Unknown></Contents>"
        "<CommonPrefixes><Prefix>p/d/</Prefix></CommonPrefixes>");
    auto page = ParseListObjectsV2(body);
    ASSERT_TRUE(page.has_value()) << page.error();
    ASSERT_EQ(page->objects.size(), 1u);
    EXPECT_EQ(page->objects[0].key, "p/a");
}

TEST(ObjectStorageXmlTest, DecodesEntitiesAndCdataInKeys) {
    const std::string body = Listing(
        "<Contents><Key>p/a&amp;b&lt;c&gt;d&quot;e&apos;f&#65;&#x42;</Key>"
        "<Size>1</Size></Contents>"
        "<Contents><Key><![CDATA[p/x<&>y]]></Key><Size>2</Size></Contents>");
    auto page = ParseListObjectsV2(body);
    ASSERT_TRUE(page.has_value()) << page.error();
    ASSERT_EQ(page->objects.size(), 2u);
    EXPECT_EQ(page->objects[0].key, "p/a&b<c>d\"e'fAB");
    EXPECT_EQ(page->objects[1].key, "p/x<&>y");
}

TEST(ObjectStorageXmlTest, AcceptsEmptyListing) {
    auto page = ParseListObjectsV2(Listing("<KeyCount>0</KeyCount>"));
    ASSERT_TRUE(page.has_value()) << page.error();
    EXPECT_TRUE(page->objects.empty());
    EXPECT_FALSE(page->is_truncated);
    EXPECT_TRUE(page->next_continuation_token.empty());
}

// Every one of these must be an error: a cut-off or corrupt LIST response
// returned as a successful, shorter listing would drop existing objects.
TEST(ObjectStorageXmlTest, RejectsMalformedListResponses) {
    const std::string object = kObject;
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"empty body", ""},
        {"not XML", "Service Unavailable"},
        {"truncated before the root closes",
         "<ListBucketResult>" + object + "<IsTruncated>false</IsTruncated>"},
        {"truncated inside a tag",
         "<ListBucketResult>" + object +
             "<IsTruncated>false</IsTruncated></ListBucketResult"},
        {"truncated inside a key", "<ListBucketResult><Contents><Key>p/a"},
        {"unterminated Contents",
         Listing(object + "<Contents><Key>p/b</Key><Size>3</Size>")},
        {"text after the root", Listing(object) + "junk"},
        {"second top-level element", Listing(object) + "<Extra/>"},
        {"wrong root element",
         "<Error>" + object + "<IsTruncated>false</IsTruncated></Error>"},
        {"Contents without Key",
         Listing("<Contents><Size>3</Size></Contents>")},
        {"Contents without Size",
         Listing("<Contents><Key>p/a</Key></Contents>")},
        {"Contents with two Keys",
         Listing("<Contents><Key>p/a</Key><Key>p/b</Key><Size>3</Size>"
                 "</Contents>")},
        {"Contents with two Sizes",
         Listing("<Contents><Key>p/a</Key><Size>3</Size><Size>4</Size>"
                 "</Contents>")},
        {"non-numeric Size",
         Listing("<Contents><Key>p/a</Key><Size>3x</Size></Contents>")},
        {"negative Size",
         Listing("<Contents><Key>p/a</Key><Size>-1</Size></Contents>")},
        {"empty Size",
         Listing("<Contents><Key>p/a</Key><Size></Size></Contents>")},
        {"Size out of range",
         Listing("<Contents><Key>p/a</Key><Size>18446744073709551616</Size>"
                 "</Contents>")},
        {"missing IsTruncated", Listing(object, "")},
        {"invalid IsTruncated",
         Listing(object, "<IsTruncated>maybe</IsTruncated>")},
        {"empty IsTruncated", Listing(object, "<IsTruncated/>")},
        {"two IsTruncated", Listing(object,
                                    "<IsTruncated>false</IsTruncated>"
                                    "<IsTruncated>true</IsTruncated>")},
        {"two NextContinuationTokens",
         Listing(object,
                 "<IsTruncated>true</IsTruncated>"
                 "<NextContinuationToken>a</NextContinuationToken>"
                 "<NextContinuationToken>b</NextContinuationToken>")},
    };
    for (const auto& [name, body] : cases) {
        SCOPED_TRACE(name);
        auto page = ParseListObjectsV2(body);
        EXPECT_FALSE(page.has_value()) << body;
        if (!page) {
            EXPECT_FALSE(page.error().empty());
        }
    }
}

TEST(ObjectStorageXmlTest, ExtractsErrorCodeAndMessage) {
    EXPECT_EQ(ErrorCodeAndMessage(
                  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>"
                  "<Code>SignatureDoesNotMatch</Code><Message>The request "
                  "signature we calculated does not match &apos;x&apos;"
                  "</Message><StringToSign>secret</StringToSign></Error>"),
              "SignatureDoesNotMatch: The request signature we calculated "
              "does not match 'x'");
    EXPECT_EQ(ErrorCodeAndMessage("<Error ><Code >NoSuchKey</Code ></Error >"),
              "NoSuchKey: ");
    EXPECT_EQ(ErrorCodeAndMessage(""), "");
    EXPECT_EQ(ErrorCodeAndMessage("<html>Bad Gateway</html>"), "");
    EXPECT_EQ(ErrorCodeAndMessage("<Error><Code>Slow"), "");
}

}  // namespace
}  // namespace mooncake::object_storage_xml
