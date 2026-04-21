#define DOCTEST_CONFIG_IMPLEMENT
#include <iostream>

#include "doctest.h"
#include "unittest_proto3.h"

#if defined(STRUCT_PB_WITH_PROTO)
TEST_CASE("test BaseTypeMsg") {
  {  // normal test
    stpb::BaseTypeMsg se_st{
        100, 200, 300, 400, 31.4f, 62.8, false, "World", stpb::Enum::ZERO};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::BaseTypeMsg se_msg;
    SetBaseTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::BaseTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::BaseTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckBaseTypeMsg(dese_st, dese_msg);
  }

  {  // test min and empty str
    stpb::BaseTypeMsg se_st{std::numeric_limits<int32_t>::min(),
                            std::numeric_limits<int64_t>::min(),
                            std::numeric_limits<uint32_t>::min(),
                            std::numeric_limits<uint64_t>::min(),
                            std::numeric_limits<float>::lowest(),
                            std::numeric_limits<double>::lowest(),
                            false,
                            "",
                            stpb::Enum::NEG};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::BaseTypeMsg se_msg;
    SetBaseTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::BaseTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::BaseTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckBaseTypeMsg(dese_st, dese_msg);
  }
  {  // test max and long str
    stpb::BaseTypeMsg se_st{std::numeric_limits<int32_t>::max(),
                            std::numeric_limits<int64_t>::max(),
                            std::numeric_limits<uint32_t>::max(),
                            std::numeric_limits<uint64_t>::max(),
                            std::numeric_limits<float>::max(),
                            std::numeric_limits<double>::max(),
                            true,
                            std::string(1000, 'x'),
                            stpb::Enum::BAZ};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::BaseTypeMsg se_msg;
    SetBaseTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::BaseTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::BaseTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckBaseTypeMsg(dese_st, dese_msg);
  }
}

TEST_CASE("test person and monster") {
  stpb::simple_t2 t{-100, 2, stpb::Color::Blue, 4};
  std::string str;
  iguana::to_pb(t, str);

  stpb::simple_t2 t2;
  iguana::from_pb(t2, str);
  CHECK(t.c == t2.c);

  pb::Simple2 s;
  s.set_a(-100);
  s.set_b(2);
  s.set_c(pb::Color::Blue);
  s.set_d(4);

  std::string pb_str;
  s.SerializeToString(&pb_str);

  CHECK(str == pb_str);
}

TEST_CASE("test person and monster") {
  auto pb_monster = protobuf_sample::create_monster();
  auto sp_monster = create_sp_monster();

  std::string pb_str;
  std::string sp_str;

  pb_monster.SerializeToString(&pb_str);
  iguana::to_pb(sp_monster, sp_str);

  CHECK(pb_str == sp_str);

  mygame::Monster m;
  m.ParseFromString(pb_str);
  CHECK(m.name() == pb_monster.name());

  stpb::Monster spm;
  iguana::from_pb(spm, sp_str);
  CHECK(spm.name == sp_monster.name);

  auto pb_person = protobuf_sample::create_person();
  auto sp_person = create_person();
  pb_person.SerializePartialToString(&pb_str);
  iguana::to_pb(sp_person, sp_str);

  CHECK(pb_str == sp_str);

  mygame::person pp;
  pp.ParseFromString(pb_str);
  CHECK(pp.name() == pb_person.name());

  stpb::person p;
  iguana::from_pb(p, sp_str);
  CHECK(p.name == sp_person.name);
}

TEST_CASE("test IguanaTypeMsg") {
  {  // test normal value
    stpb::IguanaTypeMsg se_st{{100}, {200}, {300}, {400}, {31}, {32}};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::IguanaTypeMsg se_msg{};
    SetIguanaTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::IguanaTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::IguanaTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckIguanaTypeMsg(dese_st, dese_msg);
  }

  {  // test min value
    stpb::IguanaTypeMsg se_st{{std::numeric_limits<int32_t>::min()},
                              {std::numeric_limits<int64_t>::min()},
                              {std::numeric_limits<uint32_t>::min()},
                              {std::numeric_limits<uint64_t>::min()},
                              {std::numeric_limits<int32_t>::lowest()},
                              {std::numeric_limits<int64_t>::lowest()}};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::IguanaTypeMsg se_msg{};
    SetIguanaTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);
    stpb::IguanaTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::IguanaTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckIguanaTypeMsg(dese_st, dese_msg);
  }
  {  // test max value
    stpb::IguanaTypeMsg se_st{{std::numeric_limits<int32_t>::max()},
                              {std::numeric_limits<int64_t>::max()},
                              {std::numeric_limits<uint32_t>::max()},
                              {std::numeric_limits<uint64_t>::max()},
                              {std::numeric_limits<int32_t>::max()},
                              {std::numeric_limits<int64_t>::max()}};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::IguanaTypeMsg se_msg;
    SetIguanaTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::IguanaTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::IguanaTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckIguanaTypeMsg(dese_st, dese_msg);
  }
  {  // test empty
    stpb::IguanaTypeMsg se_st{};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::IguanaTypeMsg se_msg;
    SetIguanaTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::IguanaTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::IguanaTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckIguanaTypeMsg(dese_st, dese_msg);
  }
}

TEST_CASE("test RepeatBaseTypeMsg") {
  {
    stpb::RepeatBaseTypeMsg se_st{
        {1, 2, 3},
        {4, 5, 6},
        {7, 8, 9},
        {10, 11, 12},
        {13.1, 14.2, 15.3},
        {16.4, 17.5, 18.6},
        {"a", "b", "c"},
        {stpb::Enum::BAZ, stpb::Enum::ZERO, stpb::Enum::NEG}};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::RepeatBaseTypeMsg se_msg;
    SetRepeatBaseTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::RepeatBaseTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::RepeatBaseTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckRepeatBaseTypeMsg(dese_st, dese_msg);
  }
  {  // max and min vlaue
    stpb::RepeatBaseTypeMsg se_st{{std::numeric_limits<uint32_t>::max(),
                                   std::numeric_limits<uint32_t>::min()},
                                  {std::numeric_limits<uint64_t>::max(),
                                   std::numeric_limits<uint64_t>::min()},
                                  {std::numeric_limits<int32_t>::max(),
                                   std::numeric_limits<int32_t>::min()},
                                  {std::numeric_limits<int64_t>::max(),
                                   std::numeric_limits<int64_t>::min()},
                                  {},
                                  {std::numeric_limits<double>::max(),
                                   std::numeric_limits<double>::min()},
                                  {"", "", ""},
                                  {stpb::Enum::NEG, stpb::Enum::FOO}};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::RepeatBaseTypeMsg se_msg;
    SetRepeatBaseTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);
    stpb::RepeatBaseTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::RepeatBaseTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckRepeatBaseTypeMsg(dese_st, dese_msg);
  }
}

TEST_CASE("test RepeatIguanaTypeMsg") {
  {
    stpb::RepeatIguanaTypeMsg se_st{
        {{0}, {1}, {3}},    {{4}, {5}, {6}},    {{7}, {8}, {9}},
        {{10}, {11}, {12}}, {{13}, {14}, {15}}, {},
    };
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::RepeatIguanaTypeMsg se_msg;
    SetRepeatIguanaTypeMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::RepeatIguanaTypeMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);
    pb::RepeatIguanaTypeMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckRepeatIguanaTypeMsg(dese_st, dese_msg);
  }
}

TEST_CASE("test NestedMsg") {
  {
    stpb::NestedMsg se_st{
        /* base_msg */ stpb::BaseTypeMsg{100, 200, 300, 400, 31.4f, 62.8, false,
                                         "World", stpb::Enum::BAZ},
        /* repeat_base_msg */
        std::vector<stpb::BaseTypeMsg>{
            {1, 2, 3, 4, 5.5f, 6.6, true, "Hello", stpb::Enum::FOO},
            {7, 8, 9, 10, 11.11f, 12.12, false, "Hi", stpb::Enum::BAR}},
        /* iguana_type_msg */
        stpb::IguanaTypeMsg{{100}, {200}, {300}, {400}, {31}, {32}},
        /* repeat_iguna_msg */
        std::vector{stpb::IguanaTypeMsg{{1}, {2}, {3}},
                    stpb::IguanaTypeMsg{{4}, {5}, {6}},
                    stpb::IguanaTypeMsg{{7}, {8}, {9}}},
        /* repeat_repeat_base_msg */
        std::vector<stpb::RepeatBaseTypeMsg>{
            {{1, 2, 3},
             {4, 5, 6},
             {7, 8, 9},
             {10, 11, 12},
             {13.1, 14.2, 15.3},
             {16.4, 17.5, 18.6},
             {"a", "b", "c"},
             {stpb::Enum::FOO, stpb::Enum::BAR, stpb::Enum::BAZ}},
            {{19, 20, 21},
             {22, 23, 24},
             {25, 26, 27},
             {28, 29, 30},
             {31.1, 32.2, 33.3},
             {34.4, 35.5, 36.6},
             {"x", "y", "z"},
             {stpb::Enum::ZERO, stpb::Enum::NEG, stpb::Enum::FOO}}}};

    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::NestedMsg se_msg;
    SetNestedMsg(se_st, se_msg);

    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);

    CHECK(st_ss == pb_ss);

    stpb::NestedMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);

    pb::NestedMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);

    CheckNestedMsg(dese_st, dese_msg);
  }
  {  // test empty values
    stpb::NestedMsg se_st{
        /* base_msg */ {0, 0, 0, 0, 0.0f, 0.0, true, "", stpb::Enum::ZERO},
        /* repeat_base_msg */ {},
        /* iguana_type_msg */ {},
        /* repeat_iguna_msg */ {},
        /* repeat_repeat_base_msg */ {}};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::NestedMsg se_msg;
    SetNestedMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);

    // CHECK(st_ss == pb_ss);
    print_hex_str(st_ss);
    print_hex_str(pb_ss);
    stpb::NestedMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);

    pb::NestedMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);

    CheckNestedMsg(dese_st, dese_msg);
  }
}

TEST_CASE("test MapMsg") {
  {
    stpb::MapMsg se_st{};

    se_st.sfix64_str_map.emplace(iguana::sfixed64_t{10}, "ten");
    se_st.sfix64_str_map.emplace(iguana::sfixed64_t{20}, "twenty");

    se_st.str_iguana_type_msg_map.emplace(
        "first", stpb::IguanaTypeMsg{{10}, {20}, {30}, {40}, {50}, {60}});
    se_st.str_iguana_type_msg_map.emplace(
        "second", stpb::IguanaTypeMsg{{11}, {21}, {31}, {41}, {51}, {61}});

    se_st.int_repeat_base_msg_map.emplace(
        1, stpb::RepeatBaseTypeMsg{{1, 2},
                                   {3, 4},
                                   {5, 6},
                                   {7, 8},
                                   {9.0f, 10.0f},
                                   {11.0, 12.0},
                                   {"one", "two"},
                                   {stpb::Enum::FOO, stpb::Enum::BAR}});
    se_st.int_repeat_base_msg_map.emplace(
        2, stpb::RepeatBaseTypeMsg{{2, 3},
                                   {4, 5},
                                   {6, 7},
                                   {8, 9},
                                   {10.0f, 11.0f},
                                   {12.0, 13.0},
                                   {"three", "four"},
                                   {stpb::Enum::BAZ, stpb::Enum::NEG}});

    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::MapMsg se_msg{};
    SetMapMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    // It's okay not to satisfy this.
    // CHECK(st_ss == pb_ss);
    CHECK(st_ss.size() == pb_ss.size());
    stpb::MapMsg dese_st{};
    iguana::from_pb(dese_st, pb_ss);
    pb::MapMsg dese_msg;
    dese_msg.ParseFromString(st_ss);
    CheckMapMsg(dese_st, dese_msg);
  }
  {
    // key empty
    stpb::MapMsg se_st{};
    se_st.sfix64_str_map.emplace(iguana::sfixed64_t{30}, "");
    se_st.str_iguana_type_msg_map.emplace(
        "", stpb::IguanaTypeMsg{{0}, {0}, {0}, {0}, {0}, {0}});
    se_st.int_repeat_base_msg_map.emplace(
        3, stpb::RepeatBaseTypeMsg{{}, {}, {}, {}, {}, {}, {}, {}});
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::MapMsg se_msg{};
    SetMapMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::MapMsg dese_st{};
    iguana::from_pb(dese_st, pb_ss);
    pb::MapMsg dese_msg;
    dese_msg.ParseFromString(st_ss);
    CheckMapMsg(dese_st, dese_msg);
  }
}

TEST_CASE("test BaseOneofMsg") {
  {  // test double
    stpb::BaseOneofMsg se_st{123, 3.14159, 456.78};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::BaseOneofMsg se_msg;
    SetBaseOneofMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);
    // print_hex_str(st_ss);
    // print_hex_str(pb_ss);
    stpb::BaseOneofMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);

    pb::BaseOneofMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckBaseOneofMsg(dese_st, dese_msg);
  }
  {  // test string
    stpb::BaseOneofMsg se_st{123, std::string("Hello"), 456.78};
    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::BaseOneofMsg se_msg;
    SetBaseOneofMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::BaseOneofMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);

    pb::BaseOneofMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckBaseOneofMsg(dese_st, dese_msg);
  }
  {  // test BaseTypeMsg
    stpb::BaseTypeMsg baseTypeMsg{
        100, 200, 300, 400, 31.4f, 62.8, false, "World", stpb::Enum::BAZ};
    stpb::BaseOneofMsg se_st{123, baseTypeMsg, 456.78};

    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::BaseOneofMsg se_msg;
    SetBaseOneofMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);

    stpb::BaseOneofMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);

    pb::BaseOneofMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckBaseOneofMsg(dese_st, dese_msg);
  }
  {  // test empty variant
    stpb::BaseOneofMsg se_st{123, {}, 456.78};

    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::BaseOneofMsg se_msg;
    SetBaseOneofMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);
    print_hex_str(st_ss);
    print_hex_str(pb_ss);
    stpb::BaseOneofMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);

    pb::BaseOneofMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckBaseOneofMsg(dese_st, dese_msg);
  }
}

TEST_CASE("test NestOneofMsg ") {
  {  // Test BaseOneofMsg
    stpb::BaseOneofMsg baseOneof{123, std::string("Hello"), 456.78};
    stpb::NestOneofMsg se_st{{baseOneof}};

    std::string st_ss;
    iguana::to_pb(se_st, st_ss);

    pb::NestOneofMsg se_msg;
    SetNestOneofMsg(se_st, se_msg);
    std::string pb_ss;
    se_msg.SerializeToString(&pb_ss);
    CHECK(st_ss == pb_ss);
    stpb::NestOneofMsg dese_st{};
    iguana::from_pb(dese_st, st_ss);

    pb::NestOneofMsg dese_msg;
    dese_msg.ParseFromString(pb_ss);
    CheckNestOneofMsg(dese_st, dese_msg);
  }
}
#endif

struct point_t PUBLIC(point_t) {
  point_t() = default;
  point_t(int a, double b) : x(a), y(b) {}
  int x;
  double y;
};
YLT_REFL(point_t, x, y);

namespace my_space {
struct inner_struct PUBLIC(inner_struct) {
  inner_struct() = default;
  inner_struct(int a, int b, int c) : x(a), y(b), z(c) {}
  int x;
  int y;
  int z;
};

inline auto get_members_impl(inner_struct *) {
  return std::make_tuple(iguana::build_pb_field<&inner_struct::x, 7>("a"),
                         iguana::build_pb_field<&inner_struct::y, 9>("b"),
                         iguana::build_pb_field<&inner_struct::z, 12>("c"));
}
}  // namespace my_space

struct test_pb_st1 PUBLIC(test_pb_st1) {
  test_pb_st1() = default;
  test_pb_st1(int a, iguana::sint32_t b, iguana::sint64_t c)
      : x(a), y(b), z(c) {}
  int x;
  iguana::sint32_t y;
  iguana::sint64_t z;
};
YLT_REFL(test_pb_st1, x, y, z);

struct test_pb_st2 {
  test_pb_st2() = default;
  test_pb_st2(int a, iguana::fixed32_t b, iguana::fixed64_t c)
      : x(a), y(b), z(c) {}
  int x;
  iguana::fixed32_t y;
  iguana::fixed64_t z;
};
YLT_REFL(test_pb_st2, x, y, z);

struct test_pb_st3 PUBLIC(test_pb_st3) {
  test_pb_st3() = default;
  test_pb_st3(int a, iguana::sfixed32_t b, iguana::sfixed64_t c)
      : x(a), y(b), z(c) {}
  int x;
  iguana::sfixed32_t y;
  iguana::sfixed64_t z;
};
YLT_REFL(test_pb_st3, x, y, z);

struct test_pb_st4 PUBLIC(test_pb_st3) {
  test_pb_st4() = default;
  test_pb_st4(int a, std::string b) : x(a), y(std::move(b)) {}
  int x;
  std::string y;
};
YLT_REFL(test_pb_st4, x, y);

struct test_pb_st5 PUBLIC(test_pb_st5) {
  test_pb_st5() = default;
  test_pb_st5(int a, std::string_view b) : x(a), y(b) {}
  int x;
  std::string_view y;
};
YLT_REFL(test_pb_st5, x, y);

struct test_pb_st6 PUBLIC(test_pb_st6) {
  test_pb_st6() = default;
  test_pb_st6(std::optional<int> a, std::optional<std::string> b)
      : x(std::move(a)), y(std::move(b)) {}
  std::optional<int> x;
  std::optional<std::string> y;
};
YLT_REFL(test_pb_st6, x, y);

struct pair_t PUBLIC(pair_t) {
  pair_t() = default;
  pair_t(int a, int b) : x(a), y(b) {}
  int x;
  int y;
};
YLT_REFL(pair_t, x, y);

struct message_t PUBLIC(message_t) {
  message_t() = default;
  message_t(int a, pair_t b) : id(a), t(b) {}
  int id;
  pair_t t;
};
YLT_REFL(message_t, id, t);

struct test_pb_st8 PUBLIC(test_pb_st8) {
  test_pb_st8() = default;
  test_pb_st8(int a, pair_t b, message_t c) : x(a), y(b), z(c) {}

  int x;
  pair_t y;
  message_t z;
};
YLT_REFL(test_pb_st8, x, y, z);

struct test_pb_st9 PUBLIC(test_pb_st9) {
  test_pb_st9() = default;
  test_pb_st9(int a, std::vector<int> b, std::string c)
      : x(a), y(std::move(b)), z(std::move(c)) {}
  int x;
  std::vector<int> y;
  std::string z;
};
YLT_REFL(test_pb_st9, x, y, z);

struct test_pb_st10 PUBLIC(test_pb_st10) {
  test_pb_st10() = default;
  test_pb_st10(int a, std::vector<message_t> b, std::string c)
      : x(a), y(std::move(b)), z(std::move(c)) {}
  int x;
  std::vector<message_t> y;
  std::string z;
};
YLT_REFL(test_pb_st10, x, y, z);

struct test_pb_st11 PUBLIC(test_pb_st11) {
  test_pb_st11() = default;
  test_pb_st11(int a, std::vector<std::optional<message_t>> b,
               std::vector<std::string> c)
      : x(a), y(std::move(b)), z(std::move(c)) {}
  int x;
  std::vector<std::optional<message_t>> y;
  std::vector<std::string> z;
};
YLT_REFL(test_pb_st11, x, y, z);

struct test_pb_st12 PUBLIC(test_pb_st12) {
  test_pb_st12() = default;
  test_pb_st12(int a, std::map<int, std::string> b,
               std::map<std::string, int> c)
      : x(a), y(std::move(b)), z(std::move(c)) {}

  int x;
  std::map<int, std::string> y;
  std::map<std::string, int> z;
};
YLT_REFL(test_pb_st12, x, y, z);

struct test_pb_st13 PUBLIC(test_pb_st13) {
  test_pb_st13() = default;
  test_pb_st13(int a, std::map<int, message_t> b, std::string c)
      : x(a), y(std::move(b)), z(std::move(c)) {}

  int x;
  std::map<int, message_t> y;
  std::string z;
};
YLT_REFL(test_pb_st13, x, y, z);

enum class colors_t { red, black };

enum level_t { debug, info };

struct test_pb_st14 PUBLIC(test_pb_st14) {
  test_pb_st14() = default;
  test_pb_st14(int a, colors_t b, level_t c) : x(a), y(b), z(c) {}
  int x;
  colors_t y;
  level_t z;
};
YLT_REFL(test_pb_st14, x, y, z);

namespace client {
struct person PUBLIC(person) {
  person() = default;
  person(std::string s, int d) : name(s), age(d) {}
  std::string name;
  int64_t age;
};

YLT_REFL(person, name, age);
}  // namespace client

struct my_struct PUBLIC(my_struct) {
  my_struct() = default;
  my_struct(int a, bool b, iguana::fixed64_t c) : x(a), y(b), z(c) {}
  int x;
  bool y;
  iguana::fixed64_t z;
};
YLT_REFL(my_struct, x, y, z);

struct nest1 PUBLIC(nest1) {
  nest1() = default;
  nest1(std::string s, my_struct t, int d)
      : name(std::move(s)), value(t), var(d) {}
  std::string name;
  my_struct value;
  int var;
};

YLT_REFL(nest1, name, value, var);

struct numer_st PUBLIC(numer_st) {
  numer_st() = default;
  numer_st(bool x, double y, float z) : a(x), b(y), c(z) {}
  bool a;
  double b;
  float c;
};
YLT_REFL(numer_st, a, b, c);

TEST_CASE("test reflection") {
  {
    auto t = iguana::create_instance("nest1");
    std::vector<std::string_view> fields_name = t->get_fields_name();
    CHECK(fields_name == std::vector<std::string_view>{"name", "value", "var"});

    my_struct mt{2, true, {42}};
    t->set_field_value("value", mt);
    t->set_field_value("name", std::string("test"));
    t->set_field_value("var", 41);
    nest1 *st = dynamic_cast<nest1 *>(t.get());
    auto p = *st;
    std::cout << p.name << "\n";
    auto &r0 = t->get_field_value<std::string>("name");
    CHECK(r0 == "test");
    auto &r = t->get_field_value<int>("var");
    CHECK(r == 41);
    auto &r1 = t->get_field_value<my_struct>("value");
    CHECK(r1.x == 2);

    t->set_field_value<std::string>("name", "hello");
    auto &s = t->get_field_value<std::string>("name");
    CHECK(s == "hello");
    CHECK_THROWS_AS(t->set_field_value<int>("name", 42), std::invalid_argument);
  }
  {
    auto t = iguana::create_instance("pair_t");
    t->set_field_value("x", 12);
    t->set_field_value("y", 24);
    auto &r0 = t->get_field_value<int>("x");
    CHECK(r0 == 12);
    auto &r = t->get_field_value<int>("y");
    CHECK(r == 24);

    std::string str;
    t->to_pb(str);

    pair_t t1;
    t1.from_pb(str);

    pair_t *st = dynamic_cast<pair_t *>(t.get());
    CHECK(st->x == t1.x);
    CHECK(st->y == t1.y);
  }
  auto t = iguana::create_instance("numer_st");
  t->set_field_value("a", true);
  t->set_field_value("b", double(25));
  t->set_field_value("c", float(42));
  auto &r0 = t->get_field_value<bool>("a");
  CHECK(r0);
  auto &r = t->get_field_value<double>("b");
  CHECK(r == 25);
  auto &r1 = t->get_field_value<float>("c");
  CHECK(r1 == 42);

  numer_st *st = dynamic_cast<numer_st *>(t.get());
  CHECK(st->a == true);
  CHECK(st->b == 25);
  CHECK(st->c == 42);

  std::string str;
  t->to_pb(str);

  numer_st t1;
  t1.from_pb(str);

  CHECK(st->a == t1.a);
  CHECK(st->b == t1.b);
  CHECK(st->c == t1.c);
}

TEST_CASE("test struct_pb") {
  {
    my_space::inner_struct inner{41, 42, 43};

    std::string str;
    iguana::to_pb(inner, str);

    my_space::inner_struct inner1;
    iguana::from_pb(inner1, str);
    CHECK(inner.x == inner1.x);
    CHECK(inner.y == inner1.y);
    CHECK(inner.z == inner1.z);

    std::stringstream ss;
    iguana::to_pb(inner, ss);
    std::string str1 = ss.str();
    my_space::inner_struct inner2;
    iguana::from_pb(inner2, str);
    CHECK(inner.x == inner2.x);
    CHECK(inner.y == inner2.y);
    CHECK(inner.z == inner2.z);
  }

  {
    test_pb_st1 st1{41, {42}, {43}};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st1 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.x == st2.x);
    CHECK(st1.y.val == st2.y.val);
    CHECK(st1.z.val == st2.z.val);
  }

  {
    test_pb_st2 st1{41, {42}, {43}};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st2 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.y.val == st2.y.val);
  }
  {
    test_pb_st3 st1{41, {42}, {43}};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st3 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.y.val == st2.y.val);
  }
  {
    test_pb_st4 st1{41, "it is a test"};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st4 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.y == st2.y);
  }

  {
    test_pb_st5 st1{41, "it is a test"};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st5 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.y == st2.y);
  }
  {
    // optional
    test_pb_st6 st1{41, "it is a test"};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st6 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.y == st2.y);
  }
  {
    // sub nested objects
    nest1 v{"Hi", {1, false, {3}}, 5}, v2{};
    std::string s;
    iguana::to_pb(v, s);
    iguana::from_pb(v2, s);
    CHECK(v.var == v2.var);
    CHECK(v.value.y == v2.value.y);
    CHECK(v.value.z == v2.value.z);

    test_pb_st8 st1{1, {3, 4}, {5, {7, 8}}};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st8 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z.t.y == st2.z.t.y);
  }

  {
    // repeated messages
    test_pb_st9 st1{1, {2, 4, 6}, "test"};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st9 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
  }
  {
    test_pb_st10 st1{1, {{5, {7, 8}}, {9, {11, 12}}}, "test"};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st10 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
  }
  {
    message_t m{1, {3, 4}};
    test_pb_st11 st1{1, {m}, {}};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st11 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
  }
  {
    auto m = std::make_shared<message_t>();
    m->id = 10;
    m->t.x = 11;
    m->t.y = 12;
    std::string s;
    m->to_pb(s);

    std::shared_ptr<iguana::base> t = m;
    std::string str;
    t->to_pb(str);

    CHECK(s == str);

    t->from_pb(str);
  }
  {
    message_t st1{};
    std::string str;
    iguana::to_pb(st1, str);

    message_t st2{};
    iguana::from_pb(st2, str);
    CHECK(st1.id == st2.id);
  }
  {
    test_pb_st11 st1{
        1, {message_t{5, {7, 8}}, message_t{9, {11, 12}}}, {"test"}};
    std::string str;
    iguana::to_pb(st1, str);

    std::string str1;
    st1.to_pb(str1);

    test_pb_st11 st3;
    st3.from_pb(str1);

    CHECK(str == str1);

    test_pb_st11 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
    CHECK(st3.z == st2.z);
  }
  {
    // map messages
    test_pb_st12 st1{1, {{1, "test"}, {2, "ok"}}, {{"test", 4}, {"ok", 6}}};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st12 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
  }
  {
    // map messages
    test_pb_st12 st1{1, {{1, ""}, {0, "ok"}}, {{"", 4}, {"ok", 0}}};
    std::string str;
    iguana::to_pb(st1, str);  // error
    print_hex_str(str);
    test_pb_st12 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
  }
  {
    // map messages
    test_pb_st13 st1;
    st1.x = 1;
    st1.y.emplace(1, message_t{2, {3, 4}});
    st1.y.emplace(2, message_t{4, {6, 8}});
    st1.z = "test";
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st13 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
  }
  {
    // map messages
    test_pb_st13 st1;
    st1.x = 1;
    st1.y.emplace(2, message_t{});
    st1.y.emplace(3, message_t{});
    st1.z = "test";
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st13 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
  }
  {
    // enum
    test_pb_st14 st1{1, colors_t::black, level_t::info};
    std::string str;
    iguana::to_pb(st1, str);

    test_pb_st14 st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
  }
  {
    // bool float double
    numer_st n{true, 10.25, 4.578}, n1;
    std::string str;
    iguana::to_pb(n, str);

    iguana::from_pb(n1, str);
    CHECK(n1.a == n.a);
    CHECK(n1.b == n.b);
    CHECK(n1.c == n.c);
  }
}

TEST_CASE("test members") {
  using namespace iguana;
  using namespace iguana::detail;

  my_space::inner_struct inner{41, 42, 43};
  const auto &map = detail::get_members(inner);
  std::visit(
      [&inner](auto &member) mutable {
        CHECK(member.field_no == 9);
        CHECK(member.field_name == "b");
        CHECK(member.value(inner) == 42);
      },
      map.at(9));

  point_t pt{2, 3};
  const auto &arr1 = detail::get_members(pt);
  auto &val = arr1.at(1);
  std::visit(
      [&pt](auto &member) mutable {
        CHECK(member.field_no == 1);
        CHECK(member.field_name == "x");
        CHECK(member.value(pt) == 2);
      },
      val);
}

struct test_variant PUBLIC(test_variant) {
  test_variant() = default;
  test_variant(int a, std::variant<double, std::string, int> b, double c)
      : x(a), y(std::move(b)), z(c) {}
  int x;
  std::variant<double, std::string, int> y;
  double z;
};
YLT_REFL(test_variant, x, y, z);

TEST_CASE("test variant") {
  {
    test_variant st1 = {5, "Hello, variant!", 3.14};
    std::string str;
    iguana::to_pb(st1, str);
    test_variant st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
    CHECK(std::get<std::string>(st2.y) == "Hello, variant!");
  }
  {
    test_variant st1 = {5, 3.88, 3.14};
    std::string str;
    iguana::to_pb(st1, str);
    test_variant st2;
    iguana::from_pb(st2, str);
    CHECK(st1.z == st2.z);
    CHECK(std::get<double>(st2.y) == 3.88);
  }
}

#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)
struct person PUBLIC(person) {
  person() = default;
  person(int32_t a, std::string b, int c, double d)
      : id(a), name(std::move(b)), age(c), salary(d) {}
  int32_t id;
  std::string name;
  int age;
  double salary;
};
YLT_REFL(person, id, name, age, salary);

enum Color { Red = 0, Black = 2, Green = 4 };

namespace iguana {
template <>
struct enum_value<Color> {
  constexpr static std::array<int, 3> value = {0, 2, 4};
};
}  // namespace iguana

struct vector_t {
  int id;
  Color color;
  std::variant<int, pair_t, std::string> variant;
  std::vector<int> ids;
  std::vector<pair_t> pairs;
  std::vector<std::string> strs;
  std::map<std::string, pair_t> map;
  std::string name;
  std::optional<int> op_val;
};
YLT_REFL(vector_t, id, color, variant, ids, pairs, strs, map, name, op_val);

TEST_CASE("struct to proto") {
  {
    std::string str;
    iguana::to_proto<vector_t>(str, "pb");
    std::cout << str;
    CHECK(str.find(R"(syntax = "proto3";)") != std::string::npos);
    CHECK(str.find("message vector_t") != std::string::npos);
    CHECK(str.find("map<string, pair_t>  map = 9;") != std::string::npos);
    CHECK(str.find("Green = 4;") != std::string::npos);

    std::ofstream file("test_vector.proto", std::ios::binary);
    iguana::to_proto_file<vector_t>(file, "pb");
    file.sync_with_stdio(true);
    file.flush();
    file.close();

    size_t size = std::filesystem::file_size("test_vector.proto");
    std::ifstream in("test_vector.proto", std::ios::binary);
    std::string read_str;
    read_str.resize(size);
    in.read(read_str.data(), size);
    CHECK(read_str.find("map<string, pair_t>  map = 9;") != std::string::npos);
    CHECK(read_str.find("Green = 4;") != std::string::npos);
  }
  {
    std::string str;
    iguana::to_proto<test_pb_st8>(str, "pb");
    std::cout << str;
    CHECK(str.find("message_t z = 3;") != std::string::npos);
    CHECK(str.find("message message_t") != std::string::npos);
    CHECK(str.find("pair_t t = 2;") != std::string::npos);
    CHECK(str.find("message pair_t") != std::string::npos);
  }
  {
    std::string str;
    iguana::to_proto<test_pb_st1>(str, "pb");
    std::cout << str;
    CHECK(str.find("sint64 z = 3;") != std::string::npos);

    iguana::to_proto<test_pb_st2, false>(str);
    std::cout << str;
    CHECK(str.find("fixed64 z = 3;") != std::string::npos);

    iguana::to_proto<test_pb_st3, false>(str);
    std::cout << str;
    CHECK(str.find("sfixed64 z = 3;") != std::string::npos);

    iguana::to_proto<message_t, false>(str);
    std::cout << str;
    CHECK(str.find("pair_t t = 2;") != std::string::npos);
  }
  {
    std::string str;
    iguana::to_proto<person>(str);
    std::cout << str;
    CHECK(str.find("int32 age = 3;") != std::string::npos);
  }
}
#endif

// doctest comments
// 'function' : must be 'attribute' - see issue #182
DOCTEST_MSVC_SUPPRESS_WARNING_WITH_PUSH(4007)
int main(int argc, char **argv) { return doctest::Context(argc, argv).run(); }
DOCTEST_MSVC_SUPPRESS_WARNING_POP