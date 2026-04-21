/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <flatbuffers/buffer.h>
#include <flatbuffers/string.h>

#include <cstdint>

#include "ScopedTimer.hpp"
#include "config.hpp"
#include "data_def.hpp"
#include "data_def_generated.h"
#include "no_op.h"
#include "sample.hpp"
using namespace myGame;
namespace flatbuffer_sample {
template <typename T>
void create(flatbuffers::FlatBufferBuilder &builder,
            size_t object_count = OBJECT_COUNT);
template <>
inline void create<fb::rect32s>(flatbuffers::FlatBufferBuilder &builder,
                                size_t object_count) {
  static std::vector<fb::rect32> rects_vector;
  rects_vector.clear();
  rects_vector.assign(object_count, fb::rect32{1, 0, 11, 1});
  auto rects = builder.CreateVectorOfStructs(rects_vector);
  auto orc = fb::Createrect32s(builder, rects);
  builder.Finish(orc);
}

template <>
inline void create<fb::rect32>(flatbuffers::FlatBufferBuilder &builder,
                               size_t object_count) {
  const auto orc = fb::rect32{1, 0, 11, 1};
  auto offset = builder.CreateStruct(orc);
  builder.Finish(offset);
}

template <>
inline void create<fb::persons>(flatbuffers::FlatBufferBuilder &builder,
                                size_t object_count) {
  static std::vector<flatbuffers::Offset<fb::person>> persons_vector;
  persons_vector.clear();
  for (int i = 0; i < object_count; i++) {
    auto name = builder.CreateString(std::string(1024, 'A'));
    auto person = fb::Createperson(builder, 24, name, 432798, 65536.42);
    persons_vector.emplace_back(person);
  }
  auto persons = builder.CreateVector(persons_vector);
  auto orc = fb::Createpersons(builder, persons);
  builder.Finish(orc);
}
template <>
inline void create<fb::person>(flatbuffers::FlatBufferBuilder &builder,
                               size_t object_count) {
  auto name = builder.CreateString("Sword");
  auto orc = fb::Createperson(builder, 65536, name, 65536, 65536.42);
  builder.Finish(orc);
}

template <>
inline void create<fb::Monsters>(flatbuffers::FlatBufferBuilder &builder,
                                 size_t object_count) {
  static std::vector<flatbuffers::Offset<fb::Monster>> vec;
  vec.clear();
  for (int i = 0; i < object_count / 2; i++) {
    {
      auto weapon_one_name = builder.CreateString("gun");
      auto gun = fb::CreateWeapon(builder, weapon_one_name, 42);
      auto weapon_two_name = builder.CreateString("mission");
      auto mission = fb::CreateWeapon(builder, weapon_two_name, 56);
      auto weapon_three_name = builder.CreateString("air craft");
      auto air_craft = fb::CreateWeapon(builder, weapon_three_name, 67);
      flatbuffers::Offset<fb::Weapon> weapons_vector[2] = {gun, mission};
      auto weapons = builder.CreateVector(weapons_vector, 2);
      auto name = builder.CreateString("it is a test");
      const uint8_t inv[] = {1, 2, 3, 4};
      auto inventory = builder.CreateVector(inv, 4);
      const fb::Vec3 points[] = {fb::Vec3{7, 8, 9}, fb::Vec3{71, 81, 91}};
      auto path = builder.CreateVectorOfStructs(points, 2);
      const auto position = fb::Vec3{1.0f, 2.0f, 3.0f};
      auto mst = CreateMonster(builder, &position, 16, 24, name, inventory,
                               fb::Color_Red, weapons, air_craft, path);
      vec.push_back(mst);
    }
    {
      auto weapon_one_name = builder.CreateString("gun");
      auto gun = fb::CreateWeapon(builder, weapon_one_name, 421);
      auto weapon_two_name = builder.CreateString("mission");
      auto mission = fb::CreateWeapon(builder, weapon_two_name, 561);
      auto weapon_three_name = builder.CreateString("air craft");
      auto air_craft = fb::CreateWeapon(builder, weapon_three_name, 671);
      flatbuffers::Offset<fb::Weapon> weapons_vector[2] = {gun, mission};
      auto weapons = builder.CreateVector(weapons_vector, 2);
      auto name = builder.CreateString("it is a test");
      const uint8_t inv[] = {24, 25, 26, 24};
      auto inventory = builder.CreateVector(inv, 4);
      const fb::Vec3 points[] = {fb::Vec3{71, 82, 93}, fb::Vec3{711, 821, 931}};
      auto path = builder.CreateVectorOfStructs(points, 2);
      const auto position = fb::Vec3{11.0f, 22.0f, 33.0f};
      auto mst = CreateMonster(builder, &position, 161, 241, name, inventory,
                               fb::Color_Red, weapons, air_craft, path);
      vec.push_back(mst);
    }
  }

  if (object_count % 2 == 1) {
    {
      auto weapon_one_name = builder.CreateString("gun");
      auto gun = fb::CreateWeapon(builder, weapon_one_name, 421);
      auto weapon_two_name = builder.CreateString("mission");
      auto mission = fb::CreateWeapon(builder, weapon_two_name, 561);
      auto weapon_three_name = builder.CreateString("air craft");
      auto air_craft = fb::CreateWeapon(builder, weapon_three_name, 671);
      flatbuffers::Offset<fb::Weapon> weapons_vector[2] = {gun, mission};
      auto weapons = builder.CreateVector(weapons_vector, 2);
      auto name = builder.CreateString("it is a test");
      const uint8_t inv[] = {24, 25, 26, 24};
      auto inventory = builder.CreateVector(inv, 4);
      const fb::Vec3 points[] = {fb::Vec3{71, 82, 93}, fb::Vec3{711, 821, 931}};
      auto path = builder.CreateVectorOfStructs(points, 2);
      const auto position = fb::Vec3{11.0f, 22.0f, 33.0f};
      auto mst = CreateMonster(builder, &position, 161, 241, name, inventory,
                               fb::Color_Red, weapons, air_craft, path);
      vec.push_back(mst);
    }
  }
  auto offset = builder.CreateVector(vec);
  auto orc = fb::CreateMonsters(builder, offset);
  builder.Finish(orc);
}

template <>
inline void create<fb::Monster>(flatbuffers::FlatBufferBuilder &builder,
                                size_t object_count) {
  auto weapon_one_name = builder.CreateString("gun");
  auto gun = fb::CreateWeapon(builder, weapon_one_name, 42);
  auto weapon_two_name = builder.CreateString("mission");
  auto mission = fb::CreateWeapon(builder, weapon_two_name, 56);
  auto weapon_three_name = builder.CreateString("air craft");
  auto air_craft = fb::CreateWeapon(builder, weapon_three_name, 67);
  flatbuffers::Offset<fb::Weapon> weapons_vector[2] = {gun, mission};
  auto weapons = builder.CreateVector(weapons_vector, 2);
  auto name = builder.CreateString("it is a test");
  const uint8_t inv[] = {1, 2, 3, 4};
  auto inventory = builder.CreateVector(inv, 4);
  const fb::Vec3 points[] = {fb::Vec3{7, 8, 9}, fb::Vec3{71, 81, 91}};
  auto path = builder.CreateVectorOfStructs(points, 2);
  const auto position = fb::Vec3{1.0f, 2.0f, 3.0f};
  auto orc = CreateMonster(builder, &position, 16, 24, name, inventory,
                           fb::Color_Red, weapons, air_craft, path);
  builder.Finish(orc);
}

}  // namespace flatbuffer_sample

struct flatbuffer_sample_t : public base_sample {
  static inline constexpr LibType lib_type = LibType::FLATBUFFER;
  std::string name() const override { return get_lib_name(lib_type); }

  void create_samples() override {}

  void do_serialization() override {
    serialize<fb::rect32>(SampleType::RECT);
    serialize<fb::rect32s>(SampleType::RECTS);
    serialize<fb::person>(SampleType::PERSON);
    serialize<fb::persons>(SampleType::PERSONS);
    serialize<fb::Monster>(SampleType::MONSTER);
    serialize<fb::Monsters>(SampleType::MONSTERS);
  }

  void do_deserialization() override {
    deserialize<fb::rect32>(SampleType::RECT);
    deserialize<fb::rect32s>(SampleType::RECTS);
    deserialize<fb::person>(SampleType::PERSON);
    deserialize<fb::persons>(SampleType::PERSONS);
    deserialize<fb::Monster>(SampleType::MONSTER);
    deserialize<fb::Monsters>(SampleType::MONSTERS);
  }

 private:
  template <typename T>
  void serialize(SampleType sample_type) {
    flatbuffers::FlatBufferBuilder builder;
    {
      flatbuffer_sample::create<T>(builder);

      uint64_t ns = 0;
      std::string bench_name =
          name() + " serialize " + get_sample_name(sample_type);
      {
        ScopedTimer timer(bench_name.data(), ns);
        for (int i = 0; i < ITERATIONS; ++i) {
          builder.Clear();
          flatbuffer_sample::create<T>(builder);
          no_op((char *)builder.GetBufferPointer());
        }
      }
      ser_time_elapsed_map_.emplace(sample_type, ns);
    }
    buf_size_map_.emplace(sample_type, builder.GetSize());
  }
  template <typename T>
  void deserialize(SampleType sample_type) {
    flatbuffers::FlatBufferBuilder builder;
    flatbuffer_sample::create<T>(builder);

    uint64_t ns = 0;
    std::string bench_name =
        name() + " deserialize " + get_sample_name(sample_type);
    {
      ScopedTimer timer(bench_name.data(), ns);
      for (int i = 0; i < ITERATIONS; ++i) {
        auto obj =
            flatbuffers::GetRoot<fb::Monsters>(builder.GetBufferPointer());
        no_op((char *)obj);
        no_op((char *)&builder);
      }
    }
    deser_time_elapsed_map_.emplace(sample_type, ns);
  }
};