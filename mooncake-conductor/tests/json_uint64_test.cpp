// JSON library validation: /global_view serialises uint64 conductor
// hashes as JSON numbers and must not lose precision above 2^53. This
// locks in the jsoncpp choice; if this test ever fails after a jsoncpp
// upgrade, the library must be replaced with one that preserves full
// 64-bit integer precision.

#include <gtest/gtest.h>
#include <json/json.h>

#include <cstdint>
#include <sstream>

namespace {

std::string WriteCompact(const Json::Value& v) {
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    return Json::writeString(wb, v);
}

Json::Value Parse(const std::string& s) {
    Json::CharReaderBuilder rb;
    Json::Value out;
    std::string errs;
    std::istringstream iss(s);
    EXPECT_TRUE(Json::parseFromStream(rb, iss, &out, &errs)) << errs;
    return out;
}

TEST(JsonUint64, BoundaryValuesRoundTrip) {
    const uint64_t cases[] = {
        0,
        1,
        (1ULL << 53) - 1,  // largest double-exact integer
        (1ULL << 53),
        (1ULL << 53) + 1,  // first value a double-based library corrupts
        (1ULL << 63),
        0xFFFFFFFFFFFFFFFFULL,  // uint64 max boundary value
    };
    for (const uint64_t v : cases) {
        Json::Value doc;
        doc["h"] = Json::Value::UInt64(v);
        const std::string text = WriteCompact(doc);
        const Json::Value back = Parse(text);
        ASSERT_TRUE(back["h"].isUInt64()) << "value=" << v;
        EXPECT_EQ(back["h"].asUInt64(), v) << "text=" << text;
    }
}

// The /global_view hashmap serialises uint64 values inside objects keyed
// by decimal strings; verify the exact shape survives a round-trip.
TEST(JsonUint64, HashmapShapeRoundTrip) {
    Json::Value mapping(Json::objectValue);
    mapping["11185915045167517441"] =
        Json::Value::UInt64(9404987693367201964ULL);
    const std::string text = WriteCompact(mapping);
    EXPECT_EQ(text, "{\"11185915045167517441\":9404987693367201964}");
    const Json::Value back = Parse(text);
    EXPECT_EQ(back["11185915045167517441"].asUInt64(), 9404987693367201964ULL);
}

}  // namespace
