#pragma once
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <ylt/struct_pack.hpp>

#include "ScopedTimer.hpp"
#include "config.hpp"
#include "data_def.hpp"
#include "no_op.h"
#include "sample.hpp"

inline auto create_rects(size_t object_count) {
  rect<int32_t> rc{1, 0, 11, 1};
  std::vector<rect<int32_t>> v{};
  for (std::size_t i = 0; i < object_count; i++) {
    v.push_back(rc);
  }
  return v;
}

inline auto create_rect2s(size_t object_count) {
  rect2<int32_t> rc{1, 0, 11, 1};
  std::vector<rect2<int32_t>> v{};
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
      {1, 2, 3},
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

struct struct_pack_sample : public base_sample {
  static inline constexpr LibType lib_type = LibType::STRUCT_PACK;
  std::string name() const override { return get_lib_name(lib_type); }

  void create_samples() override {
    rects_ = create_rects(OBJECT_COUNT);
    persons_ = create_persons(OBJECT_COUNT);
    monsters_ = create_monsters(OBJECT_COUNT);
    rect2s_ = create_rect2s(OBJECT_COUNT);
  }

  void do_serialization() override {
    serialize(SampleType::RECT, rects_[0]);
    serialize(SampleType::RECTS, rects_);
    serialize(SampleType::VAR_RECT, rect2s_[0]);
    serialize(SampleType::VAR_RECTS, rect2s_);
    serialize(SampleType::PERSON, persons_[0]);
    serialize(SampleType::PERSONS, persons_);
    serialize(SampleType::MONSTER, monsters_[0]);
    serialize(SampleType::MONSTERS, monsters_);
  }

  void do_deserialization() override {
    deserialize(SampleType::RECT, rects_[0]);
    deserialize(SampleType::RECTS, rects_);
    deserialize(SampleType::VAR_RECT, rect2s_[0]);
    deserialize(SampleType::VAR_RECTS, rect2s_);
#if __cpp_lib_span >= 202002L
    auto sp = std::span{rects_};
    deserialize(SampleType::ZC_RECTS, sp);
#endif
    deserialize(SampleType::PERSON, persons_[0]);
    deserialize(SampleType::PERSONS, persons_);
    deserialize<std::vector<person>, std::vector<zc_person>>(
        SampleType::ZC_PERSONS, persons_);
    deserialize(SampleType::MONSTER, monsters_[0]);
    deserialize(SampleType::MONSTERS, monsters_);
#if __cpp_lib_span >= 202002L
    deserialize<std::vector<Monster>, std::vector<zc_Monster>>(
        SampleType::ZC_MONSTERS, monsters_);
#endif
  }

 private:
  template <typename T>
  void serialize(SampleType sample_type, T &sample) {
    {
      struct_pack::serialize_to(buffer_, sample);
      buffer_.clear();

      uint64_t ns = 0;
      std::string bench_name =
          name() + " serialize " + get_sample_name(sample_type);

      {
        ScopedTimer timer(bench_name.data(), ns);
        for (int i = 0; i < ITERATIONS; ++i) {
          buffer_.clear();
          struct_pack::serialize_to(buffer_, sample);
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
    // get serialized buffer of sample for deserialize
    buffer_.clear();
    struct_pack::serialize_to(buffer_, sample);

    uint64_t ns = 0;
    std::string bench_name =
        name() + " deserialize " + get_sample_name(sample_type);

    {
      ScopedTimer timer(bench_name.data(), ns);
      for (int i = 0; i < ITERATIONS; ++i) {
        U obj;
        [[maybe_unused]] auto ec = struct_pack::deserialize_to(obj, buffer_);
        no_op((char *)&obj);
        no_op(buffer_);
      }
    }
    deser_time_elapsed_map_.emplace(sample_type, ns);
  }

  std::vector<rect2<int32_t>> rect2s_;
  std::vector<rect<int32_t>> rects_;
  std::vector<person> persons_;
  std::vector<Monster> monsters_;
  std::string buffer_;
};
