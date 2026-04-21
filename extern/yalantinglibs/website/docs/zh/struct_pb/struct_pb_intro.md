# struct_pb 简介

struct_pb 是基于C++17/C++20 开发的高性能、易用、header only的protobuf格式序列化/反序列化库。

## 动机
不再依赖proto文件去定义dsl message，而是通过C++ 结构体去定义需要序列化/反序列化的对象；因为没有protoc文件所以也不再依赖protoc去生成代码。通过C++17/C++20去实现可以做很多性能优化，从而获得更好的性能，比如可以支持反序列化时对字符串的零拷贝、尽可能内联和编译期计算以及字符串非memset的resize等。

## 例子

### 定义结构体
```cpp
#include <ylt/struct_pb.hpp>

struct person {
  int id;
  std::string name;
  int age;
};
#if __cplusplus < 202002L
YLT_REFL(person, id, name, age);
#endif
```

如果使用C++20标准，结构体为aggregate类型，且编译器版本为gcc11+, clang13+ 则不需要定义额外的宏YLT_REFL。

### 序列化
```cpp
int main() {
  person p{1, "tom", 22};
  std::string str;
  struct_pb::to_pb(p, str);

  person p1;
  struct_pb::from_pb(p1, str);
  assert(p.age == p1.age);
  assert(p.name == p1.name);
  assert(p.id == p1.id);
}
```
上面的这个结构体如果对应到protobuf的proto文件则是:
```
message my_struct {
  int32 id = 1;
  string name = 2;
  int32 age = 3;
}
```

## 动态反射
特性：
- 根据对象名称创建实例；
- 获取对象的所有字段名；
- 根据对象实例和字段名获取或设置字段的值

### 根据名称创建对象
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
std::shared_ptr<base> t = iguana::create_instance("nest1");
```
根据对象nest1创建了实例，返回的是基类指针。

“根据对象名称创建实例” 要求对象必须从iguana::base_impl 派生，如果没有派生则创建实例会抛异常。

### 根据名称设置字段的值
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
“根据对象实例和字段名获取或设置字段的值” 如果字段名不存在则会抛异常；如果设置的值类型和结构体字段类型不相同则会抛异常；需要类型完全一样，不允许隐式转换。比如字段类型是double，但是设置字段的值类型是int也会抛异常，必须显式传double；如果字段类型是std::string, 设置值类型是const char * 同样会报错；如果字段类型是int32_t, 设置值类型是uint_8也会抛异常，因为类型不相同。

设置字段值时也可以显式指定字段类型：
```cpp
t->set_field_value<std::string>("name", "test");
```
这种方式则不要求设置值的类型和字段类型完全一样，只要能赋值成功即可。如果显式指定的字段类型不是实际的字段类型时也会抛异常。

## benchmark 
在benchmark monster场景下，struct_pb 性能比protobuf 更好，序列化速度是protobuf的2.4倍，反序列化是protobuf的3.4倍。详情可以自行运行struct_pack 中的benchmark复现结果。

## struct_pb 和 protobuf 类型映射
Scalar Value Types with no modifier (a.k.a **singular**) -> T

Scalar Value Types with **optional** -> `std::optional <T>`

any type with **repeat** -> `std::vector<T>`

types with **map** -> `std::map<K, V>`

any message type -> `std::optional <T>`

enum -> enum class

oneof -> `std::variant <...>`

### 映射表
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

## 约束
- 目前还只支持proto3，不支持proto2；
- 还没支持unkonwn字段；

## roadmap
- 支持proto2；
- 支持unkonwn字段；
