# struct_pb introduction

struct_pb is an easy to use, C++17, header only, high performance protobuf format serialization library.

## motiviation

Don't depend on proto files, don't depend on protoc to generate code.

Utilize inline, zero copy, compile-time compute to optimize performance.

## example

### define struct
```cpp
#include <ylt/struct_pb.hpp>

struct my_struct {
  int x;
  bool y;
  struct_pb::fixed64_t z;
};
YLT_REFL(my_struct, x, y, z);

struct nest {
  std::string name;
  my_struct value;
  int var;
};
YLT_REFL(nest, name, value, var);
```

### serialization and deserialization
```cpp
int main() {
  nest v{"Hi", {1, false, {3}}, 5}, v2{};
  std::string s;
  struct_pb::to_pb(v, s);
  struct_pb::from_pb(v2, s);
  assert(v.var == v2.var);
  assert(v.value.y == v2.value.y);
  assert(v.value.z == v2.value.z);
}
```
the above struct mapping to a proto file, like this:
```
message my_struct {
  int32 optional_int32 = 1;
  bool optional_int64 = 2;
  sfixed64 z = 3;
}

message nest {
  string name = 1;
  my_struct value = 2;
  int32 var = 3;
}
```

## dynamic reflection
features：
- create instance by object's name;
- get all fields name from an object;
- get/set field value by filed name and instance;

### create instance by object's name
```cpp
struct my_struct {
  int x;
  bool y;
  iguana::fixed64_t z;
};
YLT_REFL(my_struct, x, y, z);

struct nest1 : public iguana::base_imple<nest1> {
  nest1() = default;
  nest1(std::string s, my_struct t, int d)
      : name(std::move(s)), value(t), var(d) {}
  std::string name;
  my_struct value;
  int var;
};
YLT_REFL(nest1, name, value, var);
```

```cpp
std::shared_ptr<base> t = struct_pb::create_instance("nest1");
```
"create instance by object's name" require the struct inherited from struct_pb::base_impl. If not inherited,create_instance will throw exception.

### get/set field value by filed name and instance
```cpp
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
```
“set field value by filed name and instance” require the setting value type is same with filed type, don't allow explicit transform, otherwise throw exception.

also support set field value with its type:
```cpp
t->set_field_value<std::string>("name", "test");
```
implicit set field type can support implicit assign field value. If the field type is not the real field type, will throw exception.  

## benchmark 

monster case: 

struct_pb is 2.4 faster than protobuf when serializing a monster;

struct_pb is 3.4 faster than protobuf when deserializing a monster;

the code in struct_pack benchmark.

## struct_pb and protobuf type mapping
Scalar Value Types with no modifier (a.k.a **singular**) -> T

Scalar Value Types with **optional** -> `std::optional <T>`

any type with **repeat** -> `std::vector<T>`

types with **map** -> `std::map<K, V>`

any message type -> `std::optional <T>`

enum -> enum class

oneof -> `std::variant <...>`

### type mapping table
| .proto Type | struct_pb Type                    | pb native C++ type | Notes                              |
|-------------|-----------------------------------|--------------------|------------------------------------|
| double      | double                            | double             | 8 bytes                            |
| float       | float                             | float              | 4 bytes                            |
| int32       | int32                             | int32              | Uses variable-length encoding.     |
| int64       | int64                             | int64              |                                    |
| uint32      | uint32                            | uint32             |                                    |
| uint64      | uint64                            | uint64             |                                    |
| sint32      | sint32_t                             | int32              | ZigZag + variable-length encoding. |
| sint64      | sint6_t                             | int64              |                                    |
| fixed32     | fixed32_t                            | uint32             | 4 bytes                            |
| fixed64     | fixed64_t                            | uint64             | 8 bytes                            |
| sfixed32    | sfixed32_t                             | int32              | 4 bytes,$2^{28}$                   |
| sfixed64    | sfixed64_t                             | int64              | 8 bytes,$2^{56}$                   |
| bool        | bool                              | bool               |                                    |
| string      | std::string                       | string             | $len < 2^{32}$                     |
| bytes       | std::string                       | string             |                                    |
| enum        | enum class: int {}/enum                | enum: int {}       |                                    |
| oneof       | std::variant<...> |                    |                                    |

## limitation
- only support proto3, not support proto2 now;
- don't support reflection now;
- don't support  unkonwn fields;
- struct must inherited from base_impl;

## roadmap
- support proto2;
- support reflection;
- support unkonwn fields;
- no need inheriting from base_impl;
