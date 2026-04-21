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
#pragma once

#include "ylt/struct_pack.hpp"
#include "ylt/struct_pack/reflection.hpp"
#if __has_include(<msgpack.hpp>)
#define HAVE_MSGPACK 1
#endif

#ifdef HAVE_MSGPACK
#include <msgpack.hpp>
#endif
#include <atomic>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <valarray>
#include <vector>
struct person {
  int32_t id;
  std::string name;
  int age;
  double salary;
#ifdef HAVE_MSGPACK
  MSGPACK_DEFINE(id, name, age, salary);
#endif
};

struct zc_person {
  int32_t id;
  std::string_view name;
  int age;
  double salary;
#ifdef HAVE_MSGPACK
  MSGPACK_DEFINE(id, name, age, salary);
#endif
};

template <typename T>
struct rect {
  T x = 1;
  T y = 0;
  T width = 11;
  T height = 1;
#ifdef HAVE_MSGPACK
  MSGPACK_DEFINE(x, y, width, height);
#endif
};

inline constexpr struct_pack::sp_config set_sp_config(rect<int> *) {
  return struct_pack::DISABLE_ALL_META_INFO;
}

inline constexpr struct_pack::sp_config set_sp_config(
    std::vector<rect<int>> *) {
  return struct_pack::DISABLE_ALL_META_INFO;
}

template <typename T>
struct rect2 {
  T x;
  T y;
  T width;
  T height;
#ifdef HAVE_MSGPACK
  MSGPACK_DEFINE(x, y, width, height);
#endif
};

inline constexpr struct_pack::sp_config set_sp_config(rect2<int32_t> *) {
  return struct_pack::sp_config{struct_pack::sp_config::DISABLE_ALL_META_INFO |
                                struct_pack::sp_config::USE_FAST_VARINT |
                                struct_pack::sp_config::ENCODING_WITH_VARINT};
}

inline constexpr struct_pack::sp_config set_sp_config(
    std::vector<rect2<int32_t>> *) {
  return struct_pack::sp_config{struct_pack::DISABLE_ALL_META_INFO};
}

enum Color : uint8_t { Red, Green, Blue };

struct Vec3 {
  float x;
  float y;
  float z;

  bool operator==(const Vec3 &rhs) const {
    std::valarray<float> lh({x, y, z});
    std::valarray<float> rh({rhs.x, rhs.y, rhs.z});
    return (std::abs(lh - rh) < 0.05f).min();
  };
#ifdef HAVE_MSGPACK
  MSGPACK_DEFINE(x, y, z);
#endif
};

struct Weapon {
  std::string name;
  int16_t damage;

  bool operator==(const Weapon &rhs) const {
    return name == rhs.name && damage == rhs.damage;
  };
#ifdef HAVE_MSGPACK
  MSGPACK_DEFINE(name, damage);
#endif
};

struct Monster {
  Vec3 pos;
  int16_t mana;
  int16_t hp;
  std::string name;
  std::string inventory;
  Color color;
  std::vector<Weapon> weapons;
  Weapon equipped;
  std::vector<Vec3> path;

  bool operator==(const Monster &rhs) const {
    return pos == rhs.pos && mana == rhs.mana && hp == rhs.hp &&
           name == rhs.name && inventory == rhs.inventory &&
           color == rhs.color && weapons == rhs.weapons &&
           equipped == rhs.equipped && path == rhs.path;
  };
#ifdef HAVE_MSGPACK
  MSGPACK_DEFINE(pos, mana, hp, name, inventory, (int &)color, weapons,
                 equipped, path);
#endif
};
#if __cpp_lib_span >= 202002L
struct zc_Weapon {
  std::string_view name;
  int16_t damage;

  bool operator==(const zc_Weapon &rhs) const {
    return name == rhs.name && damage == rhs.damage;
  };
#ifdef HAVE_MSGPACK
  MSGPACK_DEFINE(name, damage);
#endif
};

struct zc_Monster {
  Vec3 pos;
  int16_t mana;
  int16_t hp;
  std::string_view name;
  std::string_view inventory;
  Color color;
  std::vector<zc_Weapon> weapons;
  zc_Weapon equipped;
  std::span<Vec3> path;

  bool operator==(const zc_Monster &rhs) const {
    return pos == rhs.pos && mana == rhs.mana && hp == rhs.hp &&
           name == rhs.name && inventory == rhs.inventory &&
           color == rhs.color && weapons == rhs.weapons &&
           equipped == rhs.equipped;
  };
};
#endif