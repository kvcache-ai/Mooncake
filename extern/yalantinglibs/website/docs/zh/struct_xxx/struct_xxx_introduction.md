# 简介

struct_json、struct_xml和struct_yaml 是C++17 实现的header only、跨平台、高性能易用的序列化库，包括xml/json/yaml 的序列化和反序列化。

设计目标是通过编译期间反射将对象序列化到任意数据格式并大幅提升对象的序列化和反序列化的易用性。

# 如何引入

库是header only的，从github 将yalantinglibs clone 下来之后，在cmake 里包含其目录即可。
```
include_directories(include)
include_directories(include/ylt/thirdparty)
```

写代码的时候包含对应的头文件即可：
```cpp
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"
```

# 编译器要求

gcc9+、clang11+、msvc2019+

# json 序列化/反序列化
序列化需要先定义一个可反射的对象，通过YLT_REFL 可以轻松定义一个可反射对象。

```cpp
struct person
{
    std::string_view name;
    int age;
};
YLT_REFL(person, name, age); // 通过这个宏定义元数据让person 成为一个可反射对象
```
通过YLT_REFL 宏定义元数据之后就可以一行代码实现json 的序列化与反序列化了。

```cpp
person p = { "tom", 28 };

std::string ss;
struct_json::to_json(p, ss); // 序列化

person p1;
struct_json::from_json(p1, ss); // 反序列化
assert(p1.name == "tom");
```

## 异常处理
如果反序列化的时候字符串中的字段类型和结构体定义的字段类型不一致的时候会出现什么情况，比如json 字符串中为string，但是定义结构体时把它定义为int了，这时候会抛异常，需要外面捕捉异常。

如果存在一些结构体中没有的字段，即unknown 字段时，默认情况下不会抛异常，会忽略不认识的字段，如果希望这时候跑异常，那么需要开启预定义宏 THROW_UNKNOWN_KEY 。

## json 的dom 解析
json 解析也提供了dom 解析接口，使用parse 接口时，不需要定义json 对应的结构体。

```cpp
std::string_view str = R"(false)";
struct_json::jvalue val;
struct_json::parse(val, str.begin(), str.end());

// 错误码接口
std::error_code ec;
auto b = val.get<bool>(ec);
CHECK(!ec);
CHECK(!b);

// 抛异常的接口
b = val.get<bool>(); // this interface maybe throw exception
CHECK(!b);
```

# 最佳实践
## 零拷贝的反序列化
通过零拷贝的反序列化，可以完全消除内存分配的开销。
```cpp
struct some_obj {
    std::string_view name;
    struct_json::numeric_str age;
};
YLT_REFL(some_obj, name, age);

void test_view(){
    std::string_view str = "{\"name\":\"tom\", \"age\":20}";
    some_obj obj;
    
    try{
      struct_json::from_json(obj, str);
    }catch(std::exception &e){
    }
    
    assert(obj.name == "tom");

    assert(obj.age.value() == "20");

    try{
      int age = obj.age.convert<int>();
      assert(age == 20);
    }catch(std::exception &e){
    }
}
```
定义结构体时，使用std::string_view 而不是std::string 可以避免内存分配和拷贝。如果希望延迟解析数字类型，可以将数字类型定义为struct_json::numeric_str，它也是一个字符串视图，在后面需要数字的时候通过
它的convert方法转换得到数字，如果转换失败则会抛异常。

## 更高效的反序列化
反序列化不要求结构体字段顺序和json 的字段顺序一致，但是如果能保证结构体顺序和json 字段顺序一致则可以提高反序列化效率。

# xml 序列化/反序列化
和json 类似，先定义xml 数据对应的结构体，再通过to_xml/from_xml 实现序列化和反序列化。

```cpp
struct some_obj {
    std::string_view name;
    int age;
};
YLT_REFL(some_obj, name, age);

void test() {
    person p = {"admin", 20};
    std::string_view ss;  // here use std::string is also ok
    struct_xml::to_xml(p, ss);
    std::cout << ss << std::endl;
    
    // deserialization the structure from the string
    std::string_view xml = R"(
    <?xml version=\"1.0\" encoding=\"UTF-8\">  
    <root> 
      <name>buke</name> 
      <age>30</age> 
    </root>)";
    struct_xml::from_xml(p, xml);
}
```

## pretty 格式化xml
```cpp
struct person {
    std::string_view name;
    int age;
};
YLT_REFL(person, name, age);

void test_pretty() {
    person p{"tom", 20};
    std::string str;
    struct_xml::to_xml(p, str);
    std::cout<<str<<"\n";// <person><name>tom</name><age>20</age></person>

    str.clear();
    struct_xml::to_xml<true>(p, str);
    std::cout<<str<<"\n";
    /*
     <person>
	   <name>tom</name>
	   <age>20</age>
     </person>
     */
}
```
to_xml 模式输出的xml 字符串在一行，如果希望pretty 输出则传true 参数给to_xml。

## xml 属性解析

```cpp
struct book_t {
  std::string title;
  std::string author;
};
YLT_REFL(book_t, title, author);
struct library {
  struct_xml::xml_attr_t<book_t> book;
};
YLT_REFL(library, book);
TEST_CASE("test library with attr") {
  auto validator = [](library lib) {
    CHECK(lib.book.attr()["id"] == "1234");
    CHECK(lib.book.attr()["language"] == "en");
    CHECK(lib.book.attr()["edition"] == "1");
    CHECK(lib.book.value().title == "Harry Potter and the Philosopher's Stone");
    CHECK(lib.book.value().author == "J.K. Rowling");
  };
  std::string str = R"(
  <?xml version="1.0" encoding="UTF-8"?>
  <library name="UESTC library">
    <book id="1234" language="en" edition="1">
      <title>Harry Potter and the Philosopher's Stone</title>
      <author>J.K. Rowling</author>
    </book>
  </library>
)";
  {
    library lib;
    struct_xml::from_xml(lib, str);
    validator(lib);
  }
  {
    struct_xml::xml_attr_t<library> lib;
    struct_xml::from_xml(lib, str);
    CHECK(lib.attr()["name"] == "UESTC library");
    validator(lib.value());
  }
}
```

# 字段别名
一般情况下序列化/反序列化要求定义的结构体字段和被解析字符串如xml 字符串中的标签名称一一对应，比如下面的例子：
```cpp
struct some_obj {
    std::string_view name;
    int age;
};
YLT_REFL(some_obj, name, age);

void test() {
    std::string_view xml = R"(
    <?xml version=\"1.0\" encoding=\"UTF-8\">  
    <root> 
      <name>buke</name> 
      <age>30</age> 
    </root>)";
    
    some_obj p;
    struct_xml::from_xml(p, xml);
}
```
xml 标签name 和 age 对应的就是结构体some_obj::name, some_obj::age，正确解析要求结构体的名称必须和xml 标签name，age 名称完全相同。

有时候这个约束对于一些已经存在的结构体可能存在一些不便之处，已有的结构体字段名可能和xml 标签名字不相同，这时候可以通过字段别名来保证正确解析。

```cpp
std::string xml_str = R"(
<?xml version="1.0" encoding="utf-8"?>
<rootnode version="1.0" type="example">
  <id entry="1">20</id>
  <name entry="2">tom</name>
</rootnode>
)";

class person {
    std::shared_ptr<std::string> id_;
    std::unique_ptr<std::string> name_;

public:
    person() = default;
    person(std::shared_ptr<std::string> id, std::unique_ptr<std::string> name) : id_(id), name_(std::move(name)) {}

    static constexpr auto get_alias_field_names(person*) {
      return std::array{field_alias_t{"id", 0}, field_alias_t{"name", 1}};
    }
    static constexpr std::string_view get_alias_struct_name(person*) {
      return "rootnode";
    }

    YLT_REFL(person, id_, name_);

    void parse_xml() {
        struct_xml::from_xml(*this, xml_str);

        assert(*id_ == "20");
        assert(*name_ == "tom");
    }
}
```
get_alias_field_names 函数中需要填写结构体的别名，get_alias_struct_name 函数中填写字段的别名，别名字段的索引需要和YLT_REFL宏定义的字段顺序一致，通过别名将标签和结构体字段关联起来就可以保证正确的解析了。

这个例子同时也展示了序列化和反序列化智能指针。

# 如何处理私有字段
如果类里面有私有字段，在外面定义YLT_REFL 宏会出错，因为无法访问私有字段，这时候把宏定义到类里面即可，但要保证宏是public的。
```cpp
class person {
    std::string name;
    int age;
public:
    YLT_REFL(person, name, age);
};
```

# yaml 序列化/反序列化
和json，xml 类似：
```cpp
enum class enum_status {
  start,
  stop,
};
struct plain_type_t {
  bool isok;
  enum_status status;
  char c;
  std::optional<bool> hasprice;
  std::optional<float> num;
  std::optional<int> price;
};
YLT_REFL(plain_type_t, isok, status, c, hasprice, num, price);
```

```cpp
// deserialization the structure from the string
std::string str = R"(
isok: false
status: 1
c: a
hasprice: true
num:
price: 20
)";

plain_type_t p;
struct_yaml::from_yaml(p, str);
// serialization the structure to the string
std::string ss;
struct_yaml::to_yaml(ss, p);
std::cout << ss << "\n";
```

# 如何将enum 作为字符串处理
一般情况下enum 将按照int 去处理，如果希望将enum 按照字符串名称去处理，则需要定义enum_value来做适配。

```cpp
enum class Status { STOP = 10, START };
struct enum_t {
    Status a;
    Status b;
};
YLT_REFL(enum_t, a, b);

namespace iguana {
    template <> struct enum_value<Status> {
        constexpr static std::array<int, 2> value = {10, 11};
    };
} // namespace iguana

void test() {
    std::string str1 = R"(
{
  "a": "START",
  "b": "STOP"
}
  )";
    // deserialization
    enum_t e;
    struct_json::from_json(e, str1);
    
    // serialization
    enum_t e1;
    e1.a = Status::START;
    e1.b = Status::STOP;
    std::string ss;
    struct_json::to_json(e1, ss);
    std::cout<<ss<<"\n"; // {"a":"START","b":"STOP"}
}
```
通过特化iguana 命名空间里的模版类enum_value，将枚举的值填充到其内部的array 中即可实现按字符串处理enum 了。
