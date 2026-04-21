# Instructions for using the .proto file to struc_pack header tool

## compile

libprotobuf and libprotoc version is 3.21.0.

```shell
mkdir build
cd build
cmake .. && make
```

## usage

Usage:

```shell
protoc --plugin=protoc-gen-custom=./build/proto_to_struct  data.proto --custom_out=:./protos
```

data.proto is the original file that is intended to be the structure pack file.

`--custom_out=` is followed by the path to the generated file.

data.proto:

```proto
syntax = "proto3";

package mygame;

option optimize_for = SPEED;
option cc_enable_arenas = true;

message Vec3 {
    float x = 1;
    float y = 2;
    float z = 3;
}

message Weapon {
    string name = 1;
    int32 damage = 2;
}

message Monster {
  Vec3 pos = 1;
  int32 mana = 2;
  int32 hp = 3;
  string name = 4;
  bytes inventory = 5;
  enum Color {
        Red = 0;
        Green = 1;
        Blue = 2;
  }
  Color color = 6;
  repeated Weapon weapons = 7;
  Weapon equipped = 8;
  repeated Vec3 path = 9;
}

message Monsters {
    repeated Monster monsters = 1;
}

message person {
    int32 id = 1;
    string name = 2;
    int32 age = 3;
    double salary = 4;
}

message persons {
    repeated person person_list = 1;
}

message bench_int32 {
    int32 a = 1;
    int32 b = 2;
    int32 c = 3;
    int32 d = 4;
}
```

generate struct pack file:

```cpp
#pragma once
#include <ylt/struct_pb.hpp>

enum class Color {
	Red = 0,
	Green = 1,
	Blue = 2,
};

struct Vec3 {
	float x;
	float y;
	float z;
};
YLT_REFL(Vec3, x, y, z);

struct Weapon {
	std::string name;
	int32_t damage;
};
YLT_REFL(Weapon, name, damage);

struct Monster {
	Vec3 pos;
	int32_t mana;
	int32_t hp;
	std::string name;
	std::string inventory;
	Color color;
	std::vector<Weapon>weapons;
	Weapon equipped;
	std::vector<Vec3>path;
};
YLT_REFL(Monster, pos, mana, hp, name, inventory, color, weapons, equipped, path);

struct Monsters {
	std::vector<Monster>monsters;
};
YLT_REFL(Monsters, monsters);

struct person {
	int32_t id;
	std::string name;
	int32_t age;
	double salary;
};
YLT_REFL(person, id, name, age, salary);

struct persons {
	std::vector<person>person_list;
};
YLT_REFL(persons, person_list);

struct bench_int32 {
	int32_t a;
	int32_t b;
	int32_t c;
	int32_t d;
};
YLT_REFL(bench_int32, a, b, c, d);


```

There are two parameters:

## add_optional

Generate C++ files in optional struct pack format.

```shell
protoc --plugin=protoc-gen-custom=./build/proto_to_struct  data.proto --custom_out=add_optional:./protos
```

In the generated file, `std::string` will be converted to `std::optional<std::string>`, and the 'class' type(For example `class Foo`) will be converted to `std::optional<Foo>`.

```cpp
#pragma once
#include <ylt/struct_pb.hpp>

enum class Color {
	Red = 0,
	Green = 1,
	Blue = 2,
};

struct Vec3 {
	float x;
	float y;
	float z;
};
YLT_REFL(Vec3, x, y, z);

struct Weapon {
	std::optional<std::string> name;
	int32_t damage;
};
YLT_REFL(Weapon, name, damage);

struct Monster {
	std::optional<Vec3> pos;
	int32_t mana;
	int32_t hp;
	std::optional<std::string> name;
	std::optional<std::string> inventory;
	Color color;
	std::optional<std::vector<Weapon>> weapons;
	std::optional<Weapon> equipped;
	std::optional<std::vector<Vec3>> path;
};
YLT_REFL(Monster, pos, mana, hp, name, inventory, color, weapons, equipped, path);

struct Monsters {
	std::optional<std::vector<Monster>> monsters;
};
YLT_REFL(Monsters, monsters);

struct person {
	int32_t id;
	std::optional<std::string> name;
	int32_t age;
	double salary;
};
YLT_REFL(person, id, name, age, salary);

struct persons {
	std::optional<std::vector<person>> person_list;
};
YLT_REFL(persons, person_list);

struct bench_int32 {
	int32_t a;
	int32_t b;
	int32_t c;
	int32_t d;
};
YLT_REFL(bench_int32, a, b, c, d);


```

## enable_inherit

Generate C++ files in non std::optional format and the file conforms to the `struct pb` standard.

```shell
protoc --plugin=protoc-gen-custom=./build/proto_to_struct  data.proto --custom_out=enable_inherit:./protos
```

```cpp
#pragma once
#include <ylt/struct_pb.hpp>

enum class Color {
	Red = 0,
	Green = 1,
	Blue = 2,
};

struct Vec3 : public iguana::base_impl<Vec3>  {
	Vec3() = default;
	Vec3(float a, float b, float c) : x(a), y(b), z(c) {}
	float x;
	float y;
	float z;
};
YLT_REFL(Vec3, x, y, z);

struct Weapon : public iguana::base_impl<Weapon>  {
	Weapon() = default;
	Weapon(std::string a, int32_t b) : name(std::move(a)), damage(b) {}
	std::string name;
	int32_t damage;
};
YLT_REFL(Weapon, name, damage);

struct Monster : public iguana::base_impl<Monster>  {
	Monster() = default;
	Monster(Vec3 a, int32_t b, int32_t c, std::string d, std::string e, Color f, std::vector<Weapon> g, Weapon h, std::vector<Vec3> i) : pos(a), mana(b), hp(c), name(std::move(d)), inventory(std::move(e)), color(f), weapons(std::move(g)), equipped(h), path(std::move(i)) {}
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
YLT_REFL(Monster, pos, mana, hp, name, inventory, color, weapons, equipped, path);

struct Monsters : public iguana::base_impl<Monsters>  {
	Monsters() = default;
	Monsters(std::vector<Monster> a) : monsters(std::move(a)) {}
	std::vector<Monster> monsters;
};
YLT_REFL(Monsters, monsters);

struct person : public iguana::base_impl<person>  {
	person() = default;
	person(int32_t a, std::string b, int32_t c, double d) : id(a), name(std::move(b)), age(c), salary(d) {}
	int32_t id;
	std::string name;
	int32_t age;
	double salary;
};
YLT_REFL(person, id, name, age, salary);

struct persons : public iguana::base_impl<persons>  {
	persons() = default;
	persons(std::vector<person> a) : person_list(std::move(a)) {}
	std::vector<person> person_list;
};
YLT_REFL(persons, person_list);

struct bench_int32 : public iguana::base_impl<bench_int32>  {
	bench_int32() = default;
	bench_int32(int32_t a, int32_t b, int32_t c, int32_t d) : a(a), b(b), c(c), d(d) {}
	int32_t a;
	int32_t b;
	int32_t c;
	int32_t d;
};
YLT_REFL(bench_int32, a, b, c, d);


```

## add_optional and enable_inherit

The presence of these two parameters indicates that these two functions take effect on the generated file at the same time.

```shell
protoc --plugin=protoc-gen-custom=./build/proto_to_struct  data.proto --custom_out=add_optional+enable_inherit:./protos
```

```cpp
#pragma once
#include <ylt/struct_pb.hpp>

enum class Color {
	Red = 0,
	Green = 1,
	Blue = 2,
};

struct Vec3 : public iguana::base_impl<Vec3>  {
	Vec3() = default;
	Vec3(float a, float b, float c) : x(a), y(b), z(c) {}
	float x;
	float y;
	float z;
};
YLT_REFL(Vec3, x, y, z);

struct Weapon : public iguana::base_impl<Weapon>  {
	Weapon() = default;
	Weapon(std::optional<std::string> a, int32_t b) : name(std::move(a)), damage(b) {}
	std::optional<std::string> name;
	int32_t damage;
};
YLT_REFL(Weapon, name, damage);

struct Monster : public iguana::base_impl<Monster>  {
	Monster() = default;
	Monster(std::optional<Vec3> a, int32_t b, int32_t c, std::optional<std::string> d, std::optional<std::string> e, Color f, std::optional<std::vector<Weapon>> g, std::optional<Weapon> h, std::optional<std::vector<Vec3>> i) : pos(a), mana(b), hp(c), name(std::move(d)), inventory(std::move(e)), color(f), weapons(std::move(g)), equipped(h), path(std::move(i)) {}
	std::optional<Vec3> pos;
	int32_t mana;
	int32_t hp;
	std::optional<std::string> name;
	std::optional<std::string> inventory;
	Color color;
	std::optional<std::vector<Weapon>> weapons;
	std::optional<Weapon> equipped;
	std::optional<std::vector<Vec3>> path;
};
YLT_REFL(Monster, pos, mana, hp, name, inventory, color, weapons, equipped, path);

struct Monsters : public iguana::base_impl<Monsters>  {
	Monsters() = default;
	Monsters(std::optional<std::vector<Monster>> a) : monsters(std::move(a)) {}
	std::optional<std::vector<Monster>> monsters;
};
YLT_REFL(Monsters, monsters);

struct person : public iguana::base_impl<person>  {
	person() = default;
	person(int32_t a, std::optional<std::string> b, int32_t c, double d) : id(a), name(std::move(b)), age(c), salary(d) {}
	int32_t id;
	std::optional<std::string> name;
	int32_t age;
	double salary;
};
YLT_REFL(person, id, name, age, salary);

struct persons : public iguana::base_impl<persons>  {
	persons() = default;
	persons(std::optional<std::vector<person>> a) : person_list(std::move(a)) {}
	std::optional<std::vector<person>> person_list;
};
YLT_REFL(persons, person_list);

struct bench_int32 : public iguana::base_impl<bench_int32>  {
	bench_int32() = default;
	bench_int32(int32_t a, int32_t b, int32_t c, int32_t d) : a(a), b(b), c(c), d(d) {}
	int32_t a;
	int32_t b;
	int32_t c;
	int32_t d;
};
YLT_REFL(bench_int32, a, b, c, d);


```