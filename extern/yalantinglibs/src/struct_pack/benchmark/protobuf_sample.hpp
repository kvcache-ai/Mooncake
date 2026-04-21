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

#include "ScopedTimer.hpp"
#include "config.hpp"
#ifdef HAVE_PROTOBUF
#include "data_def.pb.h"
#endif
#include "no_op.h"
#include "sample.hpp"
namespace protobuf_sample {
inline auto create_rects(size_t object_count) {
  mygame::rect32s rcs{};
  for (int i = 0; i < object_count; i++) {
    mygame::rect32 *rc = rcs.add_rect32_list();
    rc->set_x(1);
    rc->set_y(0);
    rc->set_width(11);
    rc->set_height(1);
  }
  return rcs;
}

inline mygame::persons create_persons(size_t object_count) {
  mygame::persons ps;
  for (int i = 0; i < object_count; i++) {
    auto *p = ps.add_person_list();
    p->set_id(432798);
    p->set_name(std::string(1024, 'A'));
    p->set_age(24);
    p->set_salary(65536.42);
  }
  return ps;
}

inline mygame::Monsters create_monsters(size_t object_count) {
  mygame::Monsters Monsters;
  for (int i = 0; i < object_count / 2; i++) {
    {
      auto m = Monsters.add_monsters();
      auto vec = new mygame::Vec3;
      vec->set_x(1);
      vec->set_y(2);
      vec->set_z(3);
      m->set_allocated_pos(vec);
      m->set_mana(16);
      m->set_hp(24);
      m->set_name("it is a test");
      m->set_inventory("\1\2\3\4");
      m->set_color(::mygame::Monster_Color::Monster_Color_Red);
      auto w1 = m->add_weapons();
      w1->set_name("gun");
      w1->set_damage(42);
      auto w2 = m->add_weapons();
      w2->set_name("shotgun");
      w2->set_damage(56);
      auto w3 = new mygame::Weapon;
      w3->set_name("air craft");
      w3->set_damage(67);
      m->set_allocated_equipped(w3);
      auto p1 = m->add_path();
      p1->set_x(7);
      p1->set_y(8);
      p1->set_z(9);
      auto p2 = m->add_path();
      p2->set_x(71);
      p2->set_y(81);
      p2->set_z(91);
    }
    {
      auto m = Monsters.add_monsters();
      auto vec = new mygame::Vec3;
      vec->set_x(11);
      vec->set_y(22);
      vec->set_z(33);
      m->set_allocated_pos(vec);
      m->set_mana(161);
      m->set_hp(241);
      m->set_name("it is a test, ok");
      m->set_inventory("\24\25\26\24");
      m->set_color(::mygame::Monster_Color::Monster_Color_Red);
      auto w1 = m->add_weapons();
      w1->set_name("gun");
      w1->set_damage(421);
      auto w2 = m->add_weapons();
      w2->set_name("shotgun");
      w2->set_damage(561);
      auto w3 = new mygame::Weapon;
      w3->set_name("air craft");
      w3->set_damage(671);
      m->set_allocated_equipped(w3);
      auto p1 = m->add_path();
      p1->set_x(71);
      p1->set_y(82);
      p1->set_z(93);
      auto p2 = m->add_path();
      p2->set_x(711);
      p2->set_y(821);
      p2->set_z(931);
    }
  }

  if (object_count % 2 == 1) {
    {
      auto m = Monsters.add_monsters();
      auto vec = new mygame::Vec3;
      vec->set_x(1);
      vec->set_y(2);
      vec->set_z(3);
      m->set_allocated_pos(vec);
      m->set_mana(16);
      m->set_hp(24);
      m->set_name("it is a test");
      m->set_inventory("\1\2\3\4");
      m->set_color(::mygame::Monster_Color::Monster_Color_Red);
      auto w1 = m->add_weapons();
      w1->set_name("gun");
      w1->set_damage(42);
      auto w2 = m->add_weapons();
      w2->set_name("shotgun");
      w2->set_damage(56);
      auto w3 = new mygame::Weapon;
      w3->set_name("air craft");
      w3->set_damage(67);
      m->set_allocated_equipped(w3);
      auto p1 = m->add_path();
      p1->set_x(7);
      p1->set_y(8);
      p1->set_z(9);
      auto p2 = m->add_path();
      p1->set_x(71);
      p1->set_y(81);
      p1->set_z(91);
    }
  }
  return Monsters;
}
}  // namespace protobuf_sample

struct protobuf_sample_t : public base_sample {
  static inline constexpr LibType lib_type = LibType::PROTOBUF;
  std::string name() const override { return get_lib_name(lib_type); }

  void create_samples() override {
    rects_ = protobuf_sample::create_rects(OBJECT_COUNT);
    persons_ = protobuf_sample::create_persons(OBJECT_COUNT);
    monsters_ = protobuf_sample::create_monsters(OBJECT_COUNT);
  }

  void do_serialization() override {
    serialize(SampleType::RECT, *((*rects_.mutable_rect32_list()).begin()));
    serialize(SampleType::RECTS, rects_);
    serialize(SampleType::PERSON, *((*persons_.mutable_person_list()).begin()));
    serialize(SampleType::PERSONS, persons_);
    serialize(SampleType::MONSTER, *((*monsters_.mutable_monsters()).begin()));
    serialize(SampleType::MONSTERS, monsters_);
  }

  void do_deserialization() override {
    deserialize(SampleType::RECT, *((*rects_.mutable_rect32_list()).begin()));
    deserialize(SampleType::RECTS, rects_);
    deserialize(SampleType::PERSON,
                *((*persons_.mutable_person_list()).begin()));
    deserialize(SampleType::PERSONS, persons_);
    deserialize(SampleType::MONSTER,
                *((*monsters_.mutable_monsters()).begin()));
    deserialize(SampleType::MONSTERS, monsters_);
  }

 private:
  template <typename T>
  void serialize(SampleType sample_type, T &sample) {
    {
      sample.SerializeToString(&buffer_);
      buffer_.clear();

      uint64_t ns = 0;
      std::string bench_name =
          name() + " serialize " + get_sample_name(sample_type);

      {
        ScopedTimer timer(bench_name.data(), ns);
        for (int i = 0; i < ITERATIONS; ++i) {
          buffer_.clear();
          sample.SerializeToString(&buffer_);
          no_op(buffer_);
          no_op((char *)&sample);
        }
      }
      ser_time_elapsed_map_.emplace(sample_type, ns);
    }
    buf_size_map_.emplace(sample_type, buffer_.size());
  }

  template <typename T>
  void deserialize(SampleType sample_type, T &sample) {
    // get serialized buffer of sample for deserialize
    buffer_.clear();
    sample.SerializeToString(&buffer_);

    uint64_t ns = 0;
    std::string bench_name =
        name() + " deserialize " + get_sample_name(sample_type);

    {
      ScopedTimer timer(bench_name.data(), ns);
      for (int i = 0; i < ITERATIONS; ++i) {
        T obj;
        obj.ParseFromString(buffer_);
        no_op((char *)&obj);
        no_op(buffer_);
      }
    }
    deser_time_elapsed_map_.emplace(sample_type, ns);
  }

  mygame::rect32s rects_;
  mygame::persons persons_;
  mygame::Monsters monsters_;
  std::string buffer_;
};