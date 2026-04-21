#pragma once

#include <ylt/struct_pb.hpp>
#if defined(STRUCT_PB_WITH_PROTO)
#include "data_def.pb.h"
#include "unittest_proto3.pb.h"
#endif

#define PUBLIC(T) : public iguana::base_impl<T>

// define the struct as msg in proto
namespace stpb {
enum class Enum {
  ZERO = 0,
  FOO = 1,
  BAR = 2,
  BAZ = 123456,
  NEG = -1,  // Intentionally negative.
};

struct BaseTypeMsg PUBLIC(BaseTypeMsg) {
  BaseTypeMsg() = default;
  BaseTypeMsg(int32_t a, int64_t b, uint32_t c, uint64_t d, float e, double f,
              bool g, std::string h, Enum i)
      : optional_int32(a),
        optional_int64(b),
        optional_uint32(c),
        optional_uint64(d),
        optional_float(e),
        optional_double(f),
        optional_bool(g),
        optional_string(std::move(h)),
        optional_enum(i) {}
  int32_t optional_int32;
  int64_t optional_int64;
  uint32_t optional_uint32;
  uint64_t optional_uint64;
  float optional_float;
  double optional_double;
  bool optional_bool;
  std::string optional_string;
  Enum optional_enum;
  bool operator==(const BaseTypeMsg& other) const {
    return optional_int32 == other.optional_int32 &&
           optional_int64 == other.optional_int64 &&
           optional_uint32 == other.optional_uint32 &&
           optional_uint64 == other.optional_uint64 &&
           optional_float == other.optional_float &&
           optional_double == other.optional_double &&
           optional_bool == other.optional_bool &&
           optional_string == other.optional_string &&
           optional_enum == other.optional_enum;
  }
};
YLT_REFL(BaseTypeMsg, optional_int32, optional_int64, optional_uint32,
         optional_uint64, optional_float, optional_double, optional_bool,
         optional_string, optional_enum);

struct IguanaTypeMsg PUBLIC(IguanaTypeMsg) {
  IguanaTypeMsg() = default;
  IguanaTypeMsg(iguana::sint32_t a, iguana::sint64_t b, iguana::fixed32_t c,
                iguana::fixed64_t d = {}, iguana::sfixed32_t e = {},
                iguana::sfixed64_t f = {})
      : optional_sint32(a),
        optional_sint64(b),
        optional_fixed32(c),
        optional_fixed64(d),
        optional_sfixed32(e),
        optional_sfixed64(f) {}
  iguana::sint32_t optional_sint32;
  iguana::sint64_t optional_sint64;
  iguana::fixed32_t optional_fixed32;
  iguana::fixed64_t optional_fixed64;
  iguana::sfixed32_t optional_sfixed32;
  iguana::sfixed64_t optional_sfixed64;

  bool operator==(const IguanaTypeMsg& other) const {
    return optional_sint32 == other.optional_sint32 &&
           optional_sint64 == other.optional_sint64 &&
           optional_fixed32 == other.optional_fixed32 &&
           optional_fixed64 == other.optional_fixed64 &&
           optional_sfixed32 == other.optional_sfixed32 &&
           optional_sfixed64 == other.optional_sfixed64;
  }
};
YLT_REFL(IguanaTypeMsg, optional_sint32, optional_sint64, optional_fixed32,
         optional_fixed64, optional_sfixed32, optional_sfixed64);

struct RepeatBaseTypeMsg PUBLIC(RepeatBaseTypeMsg) {
  RepeatBaseTypeMsg() = default;
  RepeatBaseTypeMsg(std::vector<uint32_t> a, std::vector<uint64_t> b,
                    std::vector<int32_t> c, std::vector<int64_t> d,
                    std::vector<float> e, std::vector<double> f,
                    std::vector<std::string> g, std::vector<Enum> h)
      : repeated_uint32(std::move(a)),
        repeated_uint64(std::move(b)),
        repeated_int32(std::move(c)),
        repeated_int64(std::move(d)),
        repeated_float(std::move(e)),
        repeated_double(std::move(f)),
        repeated_string(std::move(g)),
        repeated_enum(std::move(h)) {}
  std::vector<uint32_t> repeated_uint32;
  std::vector<uint64_t> repeated_uint64;
  std::vector<int32_t> repeated_int32;
  std::vector<int64_t> repeated_int64;
  std::vector<float> repeated_float;
  std::vector<double> repeated_double;
  std::vector<std::string> repeated_string;
  std::vector<Enum> repeated_enum;
};

YLT_REFL(RepeatBaseTypeMsg, repeated_uint32, repeated_uint64, repeated_int32,
         repeated_int64, repeated_float, repeated_double, repeated_string,
         repeated_enum);

struct RepeatIguanaTypeMsg PUBLIC(RepeatIguanaTypeMsg) {
  RepeatIguanaTypeMsg() = default;
  RepeatIguanaTypeMsg(std::vector<iguana::sfixed32_t> a,
                      std::vector<iguana::sfixed64_t> b,
                      std::vector<iguana::fixed32_t> c,
                      std::vector<iguana::fixed64_t> d,
                      std::vector<iguana::sfixed32_t> e,
                      std::vector<iguana::sfixed64_t> f)
      : repeated_sint32(std::move(a)),
        repeated_sint64(std::move(b)),
        repeated_fixed32(std::move(c)),
        repeated_fixed64(std::move(d)),
        repeated_sfixed32(std::move(e)),
        repeated_sfixed64(std::move(f)) {}
  std::vector<iguana::sfixed32_t> repeated_sint32;
  std::vector<iguana::sfixed64_t> repeated_sint64;
  std::vector<iguana::fixed32_t> repeated_fixed32;
  std::vector<iguana::fixed64_t> repeated_fixed64;
  std::vector<iguana::sfixed32_t> repeated_sfixed32;
  std::vector<iguana::sfixed64_t> repeated_sfixed64;
};

YLT_REFL(RepeatIguanaTypeMsg, repeated_sint32, repeated_sint64,
         repeated_fixed32, repeated_fixed64, repeated_sfixed32,
         repeated_sfixed64);

struct NestedMsg PUBLIC(NestedMsg) {
  NestedMsg() = default;
  NestedMsg(BaseTypeMsg a, std::vector<BaseTypeMsg> b, IguanaTypeMsg c,
            std::vector<IguanaTypeMsg> d, std::vector<RepeatBaseTypeMsg> e)
      : base_msg(std::move(a)),
        repeat_base_msg(std::move(b)),
        iguana_type_msg(std::move(c)),
        repeat_iguna_msg(std::move(d)),
        repeat_repeat_base_msg(std::move(e)) {}
  BaseTypeMsg base_msg;
  std::vector<BaseTypeMsg> repeat_base_msg;
  IguanaTypeMsg iguana_type_msg;
  std::vector<IguanaTypeMsg> repeat_iguna_msg;
  std::vector<RepeatBaseTypeMsg> repeat_repeat_base_msg;
};
YLT_REFL(NestedMsg, base_msg, repeat_base_msg, iguana_type_msg,
         repeat_iguna_msg, repeat_repeat_base_msg);

struct MapMsg PUBLIC(MapMsg) {
  MapMsg() = default;
  MapMsg(std::unordered_map<iguana::sfixed64_t, std::string> a,
         std::unordered_map<std::string, IguanaTypeMsg> b,
         std::map<int, RepeatBaseTypeMsg> c)
      : sfix64_str_map(std::move(a)),
        str_iguana_type_msg_map(std::move(b)),
        int_repeat_base_msg_map(std::move(c)) {}
  std::unordered_map<iguana::sfixed64_t, std::string> sfix64_str_map{};
  std::unordered_map<std::string, IguanaTypeMsg> str_iguana_type_msg_map{};
  std::map<int, RepeatBaseTypeMsg> int_repeat_base_msg_map{};
};
YLT_REFL(MapMsg, sfix64_str_map, str_iguana_type_msg_map,
         int_repeat_base_msg_map);

struct BaseOneofMsg PUBLIC(BaseOneofMsg) {
  BaseOneofMsg() = default;
  BaseOneofMsg(int32_t a, std::variant<double, std::string, BaseTypeMsg> b,
               double c)
      : optional_int32(a), one_of(std::move(b)), optional_double(c) {}
  int32_t optional_int32;
  std::variant<double, std::string, BaseTypeMsg> one_of;
  double optional_double;
};
YLT_REFL(BaseOneofMsg, optional_int32, one_of, optional_double);

struct NestOneofMsg PUBLIC(NestOneofMsg) {
  NestOneofMsg() = default;
  NestOneofMsg(std::variant<std::string, BaseOneofMsg> a)
      : nest_one_of_msg(std::move(a)) {}
  std::variant<std::string, BaseOneofMsg> nest_one_of_msg;
};
YLT_REFL(NestOneofMsg, nest_one_of_msg);

struct simple_t PUBLIC(simple_t) {
  simple_t() = default;
  simple_t(int32_t x, int32_t y, int64_t z, int64_t w, std::string s)
      : a(x), b(y), c(z), d(w), str(std::move(s)) {}
  int32_t a;
  int32_t b;
  int64_t c;
  int64_t d;
  std::string str;
};
YLT_REFL(simple_t, a, b, c, d, str);

struct simple_t1 PUBLIC(simple_t1) {
  simple_t1() = default;
  simple_t1(int32_t x, int32_t y, int64_t z, int64_t w, std::string_view s)
      : a(x), b(y), c(z), d(w), str(s) {}
  int32_t a;
  int32_t b;
  int64_t c;
  int64_t d;
  std::string_view str;
};
YLT_REFL(simple_t1, a, b, c, d, str);

enum Color : uint8_t { Red, Green, Blue };

struct simple_t2 PUBLIC(simple_t2) {
  simple_t2() = default;
  simple_t2(int16_t x, uint8_t y, Color z, int64_t w, std::string_view s = "")
      : a(x), b(y), c(z), d(w), str(s) {}
  int16_t a;
  uint8_t b;
  Color c;
  int64_t d;
  std::string_view str;
};
YLT_REFL(simple_t2, a, b, c, d, str);

struct person {
  person() = default;
  person(int32_t a, std::string b, int c, double d)
      : id(a), name(std::move(b)), age(c), salary(d) {}
  int32_t id;
  std::string name;
  int age;
  double salary;
};
YLT_REFL(person, id, name, age, salary);

struct rect PUBLIC(rect) {
  rect() = default;
  rect(int32_t a, int32_t b, int32_t c, int32_t d)
      : x(a), y(b), width(c), height(d) {}
  int32_t x = 1;
  int32_t y = 0;
  int32_t width = 11;
  int32_t height = 1;
};
YLT_REFL(rect, x, y, width, height);

struct Vec3 PUBLIC(Vec3) {
  Vec3() = default;
  Vec3(float a, float b, float c) : x(a), y(b), z(c) {}
  float x;
  float y;
  float z;

  YLT_REFL(Vec3, x, y, z);
};

struct Weapon PUBLIC(Weapon) {
  Weapon() = default;
  Weapon(std::string a, int32_t b) : name(std::move(a)), damage(b) {}
  std::string name;
  int32_t damage;
};
YLT_REFL(Weapon, name, damage);

struct Monster PUBLIC(Monster) {
  Monster() = default;
  Monster(Vec3 a, int32_t b, int32_t c, std::string d, std::string e, int32_t f,
          std::vector<Weapon> g, Weapon h, std::vector<Vec3> i)
      : pos(a),
        mana(b),
        hp(c),
        name(std::move(d)),
        inventory(std::move(e)),
        color(f),
        weapons(std::move(g)),
        equipped(std::move(h)),
        path(std::move(i)) {}
  Vec3 pos;
  int32_t mana;
  int32_t hp;
  std::string name;
  std::string inventory;
  int32_t color;
  std::vector<Weapon> weapons;
  Weapon equipped;
  std::vector<Vec3> path;
};
YLT_REFL(Monster, pos, mana, hp, name, inventory, color, weapons, equipped,
         path);

struct bench_int32 PUBLIC(bench_int32) {
  bench_int32() = default;
  bench_int32(int32_t x, int32_t y, int32_t z, int32_t u)
      : a(x), b(y), c(z), d(u) {}
  int32_t a;
  int32_t b;
  int32_t c;
  int32_t d;
};
YLT_REFL(bench_int32, a, b, c, d);

}  // namespace stpb

inline auto create_person() {
  stpb::person p{432798, std::string(1024, 'A'), 24, 65536.42};
  return p;
}

inline stpb::Monster create_sp_monster() {
  stpb::Monster m = {
      {1, 2, 3},
      16,
      24,
      "it is a test",
      "\1\2\3\4",
      stpb::Color::Red,
      {{"gun", 42}, {"shotgun", 56}},
      {"air craft", 67},
      {{7, 8, 9}, {71, 81, 91}},
  };
  return m;
}

#if defined(STRUCT_PB_WITH_PROTO)
namespace protobuf_sample {
inline mygame::person create_person() {
  mygame::person p;
  p.set_id(432798);
  p.set_name(std::string(1024, 'A'));
  p.set_age(24);
  p.set_salary(65536.42);
  return p;
}

inline mygame::Monster create_monster() {
  mygame::Monster m;

  auto vec = new mygame::Vec3;
  vec->set_x(1);
  vec->set_y(2);
  vec->set_z(3);
  m.set_allocated_pos(vec);
  m.set_mana(16);
  m.set_hp(24);
  m.set_name("it is a test");
  m.set_inventory("\1\2\3\4");
  m.set_color(::mygame::Monster_Color::Monster_Color_Red);
  auto w1 = m.add_weapons();
  w1->set_name("gun");
  w1->set_damage(42);
  auto w2 = m.add_weapons();
  w2->set_name("shotgun");
  w2->set_damage(56);
  auto w3 = new mygame::Weapon;
  w3->set_name("air craft");
  w3->set_damage(67);
  m.set_allocated_equipped(w3);
  auto p1 = m.add_path();
  p1->set_x(7);
  p1->set_y(8);
  p1->set_z(9);
  auto p2 = m.add_path();
  p2->set_x(71);
  p2->set_y(81);
  p2->set_z(91);

  return m;
}
}  // namespace protobuf_sample

void SetBaseTypeMsg(const stpb::BaseTypeMsg& st, pb::BaseTypeMsg& msg) {
  msg.set_optional_int32(st.optional_int32);
  msg.set_optional_int64(st.optional_int64);
  msg.set_optional_uint32(st.optional_uint32);
  msg.set_optional_uint64(st.optional_uint64);
  msg.set_optional_float(st.optional_float);
  msg.set_optional_double(st.optional_double);
  msg.set_optional_bool(st.optional_bool);
  msg.set_optional_string(st.optional_string);
  msg.set_optional_enum(static_cast<pb::Enum>(st.optional_enum));
}

void CheckBaseTypeMsg(const stpb::BaseTypeMsg& st, const pb::BaseTypeMsg& msg) {
  CHECK(st.optional_int32 == msg.optional_int32());
  CHECK(st.optional_int64 == msg.optional_int64());
  CHECK(st.optional_uint32 == msg.optional_uint32());
  CHECK(st.optional_uint64 == msg.optional_uint64());
  CHECK(st.optional_float == msg.optional_float());
  CHECK(st.optional_double == msg.optional_double());
  CHECK(st.optional_bool == msg.optional_bool());
  CHECK(st.optional_string == msg.optional_string());
  CHECK(static_cast<int32_t>(st.optional_enum) ==
        static_cast<int32_t>(msg.optional_enum()));
}

void SetIguanaTypeMsg(const stpb::IguanaTypeMsg& st, pb::IguanaTypeMsg& msg) {
  msg.set_optional_sint32(st.optional_sint32.val);
  msg.set_optional_sint64(st.optional_sint64.val);
  msg.set_optional_fixed32(st.optional_fixed32.val);
  msg.set_optional_fixed64(st.optional_fixed64.val);
  msg.set_optional_sfixed32(st.optional_sfixed32.val);
  msg.set_optional_sfixed64(st.optional_sfixed64.val);
}

void CheckIguanaTypeMsg(const stpb::IguanaTypeMsg& st,
                        const pb::IguanaTypeMsg& msg) {
  CHECK(st.optional_sint32.val == msg.optional_sint32());
  CHECK(st.optional_sint64.val == msg.optional_sint64());
  CHECK(st.optional_fixed32.val == msg.optional_fixed32());
  CHECK(st.optional_fixed64.val == msg.optional_fixed64());
  CHECK(st.optional_sfixed32.val == msg.optional_sfixed32());
  CHECK(st.optional_sfixed64.val == msg.optional_sfixed64());
}

void SetRepeatBaseTypeMsg(const stpb::RepeatBaseTypeMsg& st,
                          pb::RepeatBaseTypeMsg& msg) {
  for (auto v : st.repeated_uint32) {
    msg.add_repeated_uint32(v);
  }
  for (auto v : st.repeated_uint64) {
    msg.add_repeated_uint64(v);
  }
  for (auto v : st.repeated_int32) {
    msg.add_repeated_int32(v);
  }
  for (auto v : st.repeated_int64) {
    msg.add_repeated_int64(v);
  }
  for (auto v : st.repeated_float) {
    msg.add_repeated_float(v);
  }
  for (auto v : st.repeated_double) {
    msg.add_repeated_double(v);
  }
  for (auto v : st.repeated_string) {
    msg.add_repeated_string(v);
  }
  for (auto v : st.repeated_enum) {
    msg.add_repeated_enum(static_cast<pb::Enum>(v));
  }
}

void CheckRepeatBaseTypeMsg(const stpb::RepeatBaseTypeMsg& st,
                            const pb::RepeatBaseTypeMsg& msg) {
  for (size_t i = 0; i < st.repeated_uint32.size(); ++i) {
    CHECK(st.repeated_uint32[i] == msg.repeated_uint32(i));
  }
  for (size_t i = 0; i < st.repeated_uint64.size(); ++i) {
    CHECK(st.repeated_uint64[i] == msg.repeated_uint64(i));
  }
  for (size_t i = 0; i < st.repeated_int32.size(); ++i) {
    CHECK(st.repeated_int32[i] == msg.repeated_int32(i));
  }
  for (size_t i = 0; i < st.repeated_int64.size(); ++i) {
    CHECK(st.repeated_int64[i] == msg.repeated_int64(i));
  }
  for (size_t i = 0; i < st.repeated_float.size(); ++i) {
    CHECK(st.repeated_float[i] == msg.repeated_float(i));
  }
  for (size_t i = 0; i < st.repeated_double.size(); ++i) {
    CHECK(st.repeated_double[i] == msg.repeated_double(i));
  }
  for (size_t i = 0; i < st.repeated_string.size(); ++i) {
    CHECK(st.repeated_string[i] == msg.repeated_string(i));
  }
  for (size_t i = 0; i < st.repeated_enum.size(); ++i) {
    CHECK(static_cast<int>(st.repeated_enum[i]) ==
          static_cast<int>(msg.repeated_enum(i)));
  }
}

void SetRepeatIguanaTypeMsg(const stpb::RepeatIguanaTypeMsg& st,
                            pb::RepeatIguanaTypeMsg& msg) {
  for (auto v : st.repeated_sint32) {
    msg.add_repeated_sint32(v.val);
  }
  for (auto v : st.repeated_sint64) {
    msg.add_repeated_sint64(v.val);
  }
  for (auto v : st.repeated_fixed32) {
    msg.add_repeated_fixed32(v.val);
  }
  for (auto v : st.repeated_fixed64) {
    msg.add_repeated_fixed64(v.val);
  }
  for (auto v : st.repeated_sfixed32) {
    msg.add_repeated_sfixed32(v.val);
  }
  for (auto v : st.repeated_sfixed64) {
    msg.add_repeated_sfixed64(v.val);
  }
}

void CheckRepeatIguanaTypeMsg(const stpb::RepeatIguanaTypeMsg& st,
                              const pb::RepeatIguanaTypeMsg& msg) {
  for (size_t i = 0; i < st.repeated_sint32.size(); ++i) {
    CHECK(st.repeated_sint32[i].val == msg.repeated_sint32(i));
  }
  for (size_t i = 0; i < st.repeated_sint64.size(); ++i) {
    CHECK(st.repeated_sint64[i].val == msg.repeated_sint64(i));
  }
  for (size_t i = 0; i < st.repeated_fixed32.size(); ++i) {
    CHECK(st.repeated_fixed32[i].val == msg.repeated_fixed32(i));
  }
  for (size_t i = 0; i < st.repeated_fixed64.size(); ++i) {
    CHECK(st.repeated_fixed64[i].val == msg.repeated_fixed64(i));
  }
  for (size_t i = 0; i < st.repeated_sfixed32.size(); ++i) {
    CHECK(st.repeated_sfixed32[i].val == msg.repeated_sfixed32(i));
  }
  for (size_t i = 0; i < st.repeated_sfixed64.size(); ++i) {
    CHECK(st.repeated_sfixed64[i].val == msg.repeated_sfixed64(i));
  }
}

void SetNestedMsg(const stpb::NestedMsg& st, pb::NestedMsg& msg) {
  SetBaseTypeMsg(st.base_msg, *msg.mutable_base_msg());

  for (const auto& base_msg : st.repeat_base_msg) {
    auto* base_msg_ptr = msg.add_repeat_base_msg();
    SetBaseTypeMsg(base_msg, *base_msg_ptr);
  }

  SetIguanaTypeMsg(st.iguana_type_msg, *msg.mutable_iguana_type_msg());

  for (const auto& iguana_type_msg : st.repeat_iguna_msg) {
    auto* iguana_type_msg_ptr = msg.add_repeat_iguna_msg();
    SetIguanaTypeMsg(iguana_type_msg, *iguana_type_msg_ptr);
  }

  for (const auto& repeat_base_msg : st.repeat_repeat_base_msg) {
    auto* repeat_base_msg_ptr = msg.add_repeat_repeat_base_msg();
    SetRepeatBaseTypeMsg(repeat_base_msg, *repeat_base_msg_ptr);
  }
}

void CheckNestedMsg(const stpb::NestedMsg& st, const pb::NestedMsg& msg) {
  CheckBaseTypeMsg(st.base_msg, msg.base_msg());

  CHECK(st.repeat_base_msg.size() == msg.repeat_base_msg_size());
  for (size_t i = 0; i < st.repeat_base_msg.size(); ++i) {
    CheckBaseTypeMsg(st.repeat_base_msg[i], msg.repeat_base_msg(i));
  }

  CheckIguanaTypeMsg(st.iguana_type_msg, msg.iguana_type_msg());

  CHECK(st.repeat_iguna_msg.size() == msg.repeat_iguna_msg_size());
  for (size_t i = 0; i < st.repeat_iguna_msg.size(); ++i) {
    CheckIguanaTypeMsg(st.repeat_iguna_msg[i], msg.repeat_iguna_msg(i));
  }

  CHECK(st.repeat_repeat_base_msg.size() == msg.repeat_repeat_base_msg_size());
  for (size_t i = 0; i < st.repeat_repeat_base_msg.size(); ++i) {
    CheckRepeatBaseTypeMsg(st.repeat_repeat_base_msg[i],
                           msg.repeat_repeat_base_msg(i));
  }
}

void SetMapMsg(const stpb::MapMsg& st, pb::MapMsg& msg) {
  msg.Clear();
  for (const auto& pair : st.sfix64_str_map) {
    (*msg.mutable_sfix64_str_map())[pair.first.val] = pair.second;
  }
  for (const auto& pair : st.str_iguana_type_msg_map) {
    pb::IguanaTypeMsg* it_msg =
        &((*msg.mutable_str_iguana_type_msg_map())[pair.first]);
    SetIguanaTypeMsg(pair.second, *it_msg);
  }
  for (const auto& pair : st.int_repeat_base_msg_map) {
    pb::RepeatBaseTypeMsg* rb_msg =
        &((*msg.mutable_int_repeat_base_msg_map())[pair.first]);
    SetRepeatBaseTypeMsg(pair.second, *rb_msg);
  }
}

void CheckMapMsg(const stpb::MapMsg& st, const pb::MapMsg& msg) {
  CHECK(msg.sfix64_str_map_size() == st.sfix64_str_map.size());
  for (const auto& pair : st.sfix64_str_map) {
    auto it = msg.sfix64_str_map().find(pair.first.val);
    CHECK(it != msg.sfix64_str_map().end());
    CHECK(it->second == pair.second);
  }
  CHECK(msg.str_iguana_type_msg_map_size() ==
        st.str_iguana_type_msg_map.size());
  for (const auto& pair : st.str_iguana_type_msg_map) {
    auto it = msg.str_iguana_type_msg_map().find(pair.first);
    CHECK(it != msg.str_iguana_type_msg_map().end());
    CheckIguanaTypeMsg(pair.second, it->second);
  }

  CHECK(msg.int_repeat_base_msg_map_size() ==
        st.int_repeat_base_msg_map.size());
  for (const auto& pair : st.int_repeat_base_msg_map) {
    auto it = msg.int_repeat_base_msg_map().find(pair.first);
    CHECK(it != msg.int_repeat_base_msg_map().end());
    CheckRepeatBaseTypeMsg(pair.second, it->second);
  }
}

void SetBaseOneofMsg(const stpb::BaseOneofMsg& st, pb::BaseOneofMsg& msg) {
  msg.set_optional_int32(st.optional_int32);
  msg.set_optional_double(st.optional_double);

  std::visit(
      [&](auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, double>) {
          msg.set_one_of_double(value);
        }
        else if constexpr (std::is_same_v<T, std::string>) {
          msg.set_one_of_string(value);
        }
        else if constexpr (std::is_same_v<T, stpb::BaseTypeMsg>) {
          auto* submsg = msg.mutable_one_of_base_type_msg();
          SetBaseTypeMsg(value, *submsg);
        }
      },
      st.one_of);
}

void CheckBaseOneofMsg(const stpb::BaseOneofMsg& st,
                       const pb::BaseOneofMsg& msg) {
  CHECK(st.optional_int32 == msg.optional_int32());
  CHECK(st.optional_double == msg.optional_double());

  std::visit(
      [&](auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, double>) {
          CHECK(value == msg.one_of_double());
        }
        else if constexpr (std::is_same_v<T, std::string>) {
          CHECK(value == msg.one_of_string());
        }
        else if constexpr (std::is_same_v<T, stpb::BaseTypeMsg>) {
          CheckBaseTypeMsg(value, msg.one_of_base_type_msg());
        }
      },
      st.one_of);
}

void SetNestOneofMsg(const stpb::NestOneofMsg& st, pb::NestOneofMsg& msg) {
  std::visit(
      [&](auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
          msg.set_base_one_of_string(value);
        }
        else if constexpr (std::is_same_v<T, stpb::BaseOneofMsg>) {
          auto* submsg = msg.mutable_base_one_of_msg();
          SetBaseOneofMsg(value, *submsg);
        }
      },
      st.nest_one_of_msg);
}

void CheckNestOneofMsg(const stpb::NestOneofMsg& st,
                       const pb::NestOneofMsg& msg) {
  std::visit(
      [&](auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
          CHECK(value == msg.base_one_of_string());
        }
        else if constexpr (std::is_same_v<T, stpb::BaseOneofMsg>) {
          CheckBaseOneofMsg(value, msg.base_one_of_msg());
        }
      },
      st.nest_one_of_msg);
}
#endif

inline void print_hex_str(const std::string& str) {
  std::ostringstream oss;
  oss << std::hex << std::setfill('0');
  for (unsigned char c : str) {
    oss << std::setw(2) << static_cast<int>(c);
  }
  std::cout << oss.str() << std::endl;
}
