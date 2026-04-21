#pragma once
#include <ylt/struct_pb.hpp>

#include "ScopedTimer.hpp"
#include "no_op.h"
#include "sample.hpp"

namespace pb_sample {
struct person : public iguana::base_impl<person> {
  person() = default;
  person(int32_t a, std::string b, int c, double d)
      : id(a), name(std::move(b)), age(c), salary(d) {}
  int32_t id;
  std::string name;
  int age;
  double salary;
};
YLT_REFL(person, id, name, age, salary);

struct persons : public iguana::base_impl<persons> {
  persons() = default;
  explicit persons(std::vector<person> l) : list(std::move(l)) {}
  std::vector<person> list;
};
YLT_REFL(persons, list);

struct rect : public iguana::base_impl<rect> {
  rect() = default;
  rect(int32_t a, int32_t b, int32_t c, int32_t d)
      : x(a), y(b), width(c), height(d) {}
  int32_t x = 1;
  int32_t y = 0;
  int32_t width = 11;
  int32_t height = 1;
};
YLT_REFL(rect, x, y, width, height);

struct rects : public iguana::base_impl<rects> {
  rects() = default;
  explicit rects(std::vector<rect> l) : list(std::move(l)) {}
  std::vector<rect> list;
};
YLT_REFL(rects, list);

struct Vec3 : public iguana::base_impl<Vec3> {
  Vec3() = default;
  Vec3(float a, float b, float c) : x(a), y(b), z(c) {}
  float x;
  float y;
  float z;

  YLT_REFL(Vec3, x, y, z);
};

struct Weapon : public iguana::base_impl<Weapon> {
  Weapon() = default;
  Weapon(std::string s, int32_t d) : name(std::move(s)), damage(d) {}
  std::string name;
  int32_t damage;
};
YLT_REFL(Weapon, name, damage);

enum Color : uint8_t { Red, Green, Blue };

struct Monster : public iguana::base_impl<Monster> {
  Monster() = default;
  Monster(Vec3 a, int32_t b, int32_t c, std::string d, std::string e, Color f,
          std::vector<Weapon> g, Weapon h, std::vector<Vec3> i) {}
  Vec3 pos;
  int32_t mana;
  int32_t hp;
  std::string name;
  std::string inventory;
  Color color;
  std::vector<Weapon> weapons;
  Weapon equipped;
  std::vector<Vec3> path;
};
YLT_REFL(Monster, pos, mana, hp, name, inventory, color, weapons, equipped,
         path);

struct Monsters : public iguana::base_impl<Monsters> {
  Monsters() = default;
  explicit Monsters(std::vector<Monster> l) : list(std::move(l)) {}
  std::vector<Monster> list;
};
YLT_REFL(Monsters, list);

inline auto create_rects(size_t object_count) {
  rect rc{1, 0, 11, 1};
  std::vector<rect> v{};
  for (std::size_t i = 0; i < object_count; i++) {
    v.push_back(rc);
  }
  return v;
}

inline auto create_persons(size_t object_count) {
  std::vector<person> v{};
  person p{432798, std::string(1024, 'A'), 24, 65536.42};

  for (std::size_t i = 0; i < object_count; i++) {
    v.push_back(p);
  }
  return v;
}

inline std::vector<Monster> create_monsters(size_t object_count) {
  std::vector<Monster> v{};
  Monster m = {
      Vec3{1, 2, 3},
      16,
      24,
      "it is a test",
      "\1\2\3\4",
      Color::Red,
      {{"gun", 42}, {"shotgun", 56}},
      {"air craft", 67},
      {{7, 8, 9}, {71, 81, 91}},
  };

  Monster m1 = {
      {11, 22, 33},
      161,
      241,
      "it is a test, ok",
      "\24\25\26\24",
      Color::Red,
      {{"gun", 421}, {"shotgun", 561}},
      {"air craft", 671},
      {{71, 82, 93}, {711, 821, 931}},
  };

  for (std::size_t i = 0; i < object_count / 2; i++) {
    v.push_back(m);
    v.push_back(m1);
  }

  if (object_count % 2 == 1) {
    v.push_back(m);
  }

  return v;
}
}  // namespace pb_sample

struct struct_pb_sample : public base_sample {
  static inline constexpr LibType lib_type = LibType::STRUCT_PB;
  std::string name() const override { return get_lib_name(lib_type); }

  void create_samples() override {
    rects_.list = {pb_sample::create_rects(OBJECT_COUNT)};
    persons_.list = {pb_sample::create_persons(OBJECT_COUNT)};
    monsters_.list = {pb_sample::create_monsters(OBJECT_COUNT)};
  }

  void do_serialization() override {
    serialize(SampleType::RECT, rects_.list[0]);
    struct_pb::to_pb(rects_, buffer_);
    struct_pb::to_pb(rects_, buffer_);
    struct_pb::to_pb(rects_, buffer_);
    std::cout << rects_.list.size() << "\n";
    serialize(SampleType::RECTS, rects_);
    serialize(SampleType::PERSON, persons_.list[0]);
    serialize(SampleType::PERSONS, persons_);
    serialize(SampleType::MONSTER, monsters_.list[0]);
    serialize(SampleType::MONSTERS, monsters_);
  }

  void do_deserialization() override {
    deserialize(SampleType::RECT, rects_.list[0]);
    deserialize(SampleType::RECTS, rects_);
    deserialize(SampleType::PERSON, persons_.list[0]);
    deserialize(SampleType::PERSONS, persons_);
    deserialize(SampleType::MONSTER, monsters_.list[0]);
    deserialize(SampleType::MONSTERS, monsters_);
  }

 private:
  template <typename T>
  void serialize(SampleType sample_type, T &sample) {
    {
      struct_pb::to_pb(sample, buffer_);
      buffer_.clear();

      uint64_t ns = 0;
      std::string bench_name =
          name() + " serialize " + get_sample_name(sample_type);

      {
        ScopedTimer timer(bench_name.data(), ns);
        for (int i = 0; i < ITERATIONS; ++i) {
          struct_pb::to_pb(sample, buffer_);
          no_op(buffer_);
          no_op((char *)&sample);
        }
      }
      ser_time_elapsed_map_.emplace(sample_type, ns);
    }
    buf_size_map_.emplace(sample_type, buffer_.size());
  }
  template <typename T, typename U = T>
  void deserialize(SampleType sample_type, T &sample) {
    buffer_.clear();
    struct_pb::to_pb(sample, buffer_);

    uint64_t ns = 0;
    std::string bench_name =
        name() + " deserialize " + get_sample_name(sample_type);

    {
      ScopedTimer timer(bench_name.data(), ns);
      for (int i = 0; i < ITERATIONS; ++i) {
        U obj;
        struct_pb::from_pb(obj, buffer_);
        no_op((char *)&obj);
        no_op(buffer_);
      }
    }
    deser_time_elapsed_map_.emplace(sample_type, ns);
  }

  pb_sample::rects rects_;
  pb_sample::persons persons_;
  pb_sample::Monsters monsters_;
  std::string buffer_;
};
