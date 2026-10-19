// Unit tests for the MessagePack map helpers shared by the engine decoders.
#include "../src/zmq/msgpack_reader.h"

#include <gtest/gtest.h>

#include <limits>
#include <sstream>
#include <string>

namespace {

using mooncake::conductor::zmq::detail::MapReader;
using mooncake::conductor::zmq::detail::ParseInt32Array;
using mooncake::conductor::zmq::detail::ParseInt64;
using mooncake::conductor::zmq::detail::ParseNullableInt32Array;
using mooncake::conductor::zmq::detail::ParseNullableInt64;
using mooncake::conductor::zmq::detail::ParseNullableString;
using mooncake::conductor::zmq::detail::ParseOptional;
using mooncake::conductor::zmq::detail::ParseRequired;
using mooncake::conductor::zmq::detail::ParseString;
using mooncake::conductor::zmq::detail::ParseUint64;
using Packer = msgpack::packer<std::stringstream>;

// Keeps the object_handle alive for as long as the views into it are used.
class Unpacked {
   public:
    explicit Unpacked(const std::string& payload) {
        handle_ = msgpack::unpack(payload.data(), payload.size());
    }
    const msgpack::object& get() const { return handle_.get(); }

   private:
    msgpack::object_handle handle_;
};

template <typename PackFn>
std::string Pack(PackFn pack) {
    std::stringstream buffer;
    Packer packer(buffer);
    pack(packer);
    return buffer.str();
}

const std::set<std::string_view> kFields = {"a", "b", "c"};

TEST(MsgpackMapReader, RejectsNonMapAndNonStringKeys) {
    const Unpacked array(Pack([](Packer& p) { p.pack_array(0); }));
    const MapReader array_reader(array.get(), kFields);
    EXPECT_NE(array_reader.error().find("expected event map, got array"),
              std::string::npos)
        << array_reader.error();

    const Unpacked int_key(Pack([](Packer& p) {
        p.pack_map(1);
        p.pack_int64(1);
        p.pack_int64(2);
    }));
    const MapReader int_key_reader(int_key.get(), kFields);
    EXPECT_NE(int_key_reader.error().find("must be a string"),
              std::string::npos)
        << int_key_reader.error();
}

TEST(MsgpackMapReader, SkipsUnknownKeysAndRejectsDuplicateRecognizedKeys) {
    const Unpacked unknown(Pack([](Packer& p) {
        p.pack_map(2);
        p.pack(std::string("a"));
        p.pack_int64(1);
        p.pack(std::string("unknown"));
        p.pack_int64(2);
    }));
    const MapReader unknown_reader(unknown.get(), kFields);
    ASSERT_TRUE(unknown_reader.error().empty()) << unknown_reader.error();
    EXPECT_NE(unknown_reader.Get("a"), nullptr);
    EXPECT_EQ(unknown_reader.Get("unknown"), nullptr);
    EXPECT_EQ(unknown_reader.Get("b"), nullptr);

    // A repeated unrecognized key stays harmless; a repeated recognized one is
    // ambiguous and must fail.
    const Unpacked duplicate_unknown(Pack([](Packer& p) {
        p.pack_map(2);
        p.pack(std::string("unknown"));
        p.pack_int64(1);
        p.pack(std::string("unknown"));
        p.pack_int64(2);
    }));
    EXPECT_TRUE(MapReader(duplicate_unknown.get(), kFields).error().empty());

    const Unpacked duplicate(Pack([](Packer& p) {
        p.pack_map(2);
        p.pack(std::string("b"));
        p.pack_int64(1);
        p.pack(std::string("b"));
        p.pack_int64(2);
    }));
    EXPECT_NE(MapReader(duplicate.get(), kFields)
                  .error()
                  .find("duplicate recognized key: b"),
              std::string::npos);
}

TEST(MsgpackMapReader, DistinguishesAbsenceFromNil) {
    const Unpacked payload(Pack([](Packer& p) {
        p.pack_map(1);
        p.pack(std::string("a"));
        p.pack_nil();
    }));
    const MapReader reader(payload.get(), kFields);
    ASSERT_TRUE(reader.error().empty()) << reader.error();

    // Absent optional fields reset the output without invoking the parser;
    // explicit nil still passes through the selected parser.
    std::optional<std::string> value = "unchanged";
    std::string error;
    ASSERT_TRUE(
        ParseOptional(reader, "a", ParseNullableString, &value, &error));
    EXPECT_FALSE(value.has_value());

    value = "kept";
    ASSERT_TRUE(
        ParseOptional(reader, "b", ParseNullableString, &value, &error));
    EXPECT_FALSE(value.has_value());

    std::string required;
    EXPECT_FALSE(ParseRequired(reader, "b", ParseString, &required, &error));
    EXPECT_EQ(error, "missing required key: b");

    // A nullable field that is present as nil is still a nil for a parser that
    // does not accept it.
    EXPECT_FALSE(ParseRequired(reader, "a", ParseString, &required, &error));
    EXPECT_NE(error.find("invalid a: expected string, got nil"),
              std::string::npos)
        << error;
}

TEST(MsgpackReaderScalars, EnforcesTypesAndSignedRange) {
    const Unpacked text(Pack([](Packer& p) { p.pack(std::string("s")); }));
    EXPECT_EQ(*ParseString(text.get()).value, "s");
    EXPECT_FALSE(ParseUint64(text.get()).value.has_value());
    EXPECT_FALSE(ParseInt64(text.get()).value.has_value());

    const Unpacked negative(Pack([](Packer& p) { p.pack_int64(-5); }));
    EXPECT_EQ(*ParseInt64(negative.get()).value, -5);
    // Unsigned parsing rejects a negative integer rather than wrapping it.
    EXPECT_FALSE(ParseUint64(negative.get()).value.has_value());

    // An unsigned value above INT64_MAX does not fit a signed field.
    const Unpacked huge(Pack([](Packer& p) {
        p.pack_uint64(std::numeric_limits<uint64_t>::max());
    }));
    EXPECT_EQ(*ParseUint64(huge.get()).value,
              std::numeric_limits<uint64_t>::max());
    EXPECT_FALSE(ParseInt64(huge.get()).value.has_value());

    const Unpacked boundary(Pack([](Packer& p) {
        p.pack_uint64(
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    }));
    EXPECT_EQ(*ParseInt64(boundary.get()).value,
              std::numeric_limits<int64_t>::max());

    const Unpacked nil(Pack([](Packer& p) { p.pack_nil(); }));
    EXPECT_TRUE(ParseNullableInt64(nil.get()).value.has_value());
    EXPECT_FALSE(*ParseNullableInt64(nil.get()).value);
    EXPECT_FALSE(ParseNullableInt64(text.get()).value.has_value());
}

TEST(MsgpackReaderArrays, ReportsFailingIndexAndNarrowsToInt32) {
    const Unpacked ints(Pack([](Packer& p) {
        p.pack_array(2);
        p.pack_int32(1);
        p.pack_int32(-2);
    }));
    EXPECT_EQ(*ParseInt32Array(ints.get()).value,
              (std::vector<int32_t>{1, -2}));

    const Unpacked overflow(Pack([](Packer& p) {
        p.pack_array(2);
        p.pack_int32(1);
        p.pack_int64(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) +
                     1);
    }));
    const auto overflow_result = ParseInt32Array(overflow.get());
    EXPECT_FALSE(overflow_result.value.has_value());
    EXPECT_NE(overflow_result.error.find("element 1"), std::string::npos)
        << overflow_result.error;
    EXPECT_NE(overflow_result.error.find("outside int32 range"),
              std::string::npos)
        << overflow_result.error;

    const Unpacked not_array(Pack([](Packer& p) { p.pack_int64(1); }));
    EXPECT_NE(ParseInt32Array(not_array.get())
                  .error.find("expected array, got positive integer"),
              std::string::npos);

    const Unpacked nil(Pack([](Packer& p) { p.pack_nil(); }));
    const auto nullable = ParseNullableInt32Array(nil.get());
    ASSERT_TRUE(nullable.value.has_value());
    EXPECT_FALSE(nullable.value->has_value());

    const Unpacked empty(Pack([](Packer& p) { p.pack_array(0); }));
    const auto empty_result = ParseInt32Array(empty.get());
    ASSERT_TRUE(empty_result.value.has_value());
    EXPECT_TRUE(empty_result.value->empty());
}

}  // namespace
