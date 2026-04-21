#include <cassert>
#include <iostream>

#include "ylt/struct_xml/xml_reader.h"
#include "ylt/struct_xml/xml_writer.h"

struct Owner_t {
  std::string_view ID;
  std::string_view DisplayName;
};
YLT_REFL(Owner_t, ID, DisplayName);

struct Contents_t {
  std::string_view Key;
  std::string_view LastModified;
  std::string_view ETag;
  std::string_view Type;
  uint32_t Size;
  std::string_view StorageClass;
  Owner_t Owner;
};
YLT_REFL(Contents_t, Key, LastModified, ETag, Type, Size, StorageClass, Owner);

struct ListBucketResult {
  std::string_view Name;
  std::string_view Prefix;
  std::string_view Marker;
  int MaxKeys;
  std::string_view Delimiter;
  bool IsTruncated;
  std::vector<Contents_t> Contents;
  //  std::unordered_map<std::string, std::string> __attr;
};
YLT_REFL(ListBucketResult, Name, Prefix, Marker, MaxKeys, Delimiter,
         IsTruncated, Contents);

std::string xml_str = R"(
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://doc.oss-cn-hangzhou.aliyuncs.com">
<Name>examplebucket</Name>
<Prefix></Prefix>
<Marker></Marker>
<MaxKeys>100</MaxKeys>
<Delimiter></Delimiter>
<IsTruncated>false</IsTruncated>
<Contents>
      <Key>fun/movie/001.avi</Key>
      <LastModified>2012-02-24T08:43:07.000Z</LastModified>
      <ETag>"5B3C1A2E053D763E1B002CC607C5A0FE1****"</ETag>
      <Type>Normal</Type>
      <Size>344606</Size>
      <StorageClass>Standard</StorageClass>
      <Owner>
          <ID>0022012****</ID>
          <DisplayName>user-example</DisplayName>
      </Owner>
</Contents>
<Contents>
      <Key>fun/movie/007.avi</Key>
      <LastModified>2012-02-24T08:43:27.000Z</LastModified>
      <ETag>"5B3C1A2E053D763E1B002CC607C5A0FE1****"</ETag>
      <Type>Normal</Type>
      <Size>344606</Size>
      <StorageClass>Standard</StorageClass>
      <Owner>
          <ID>0022012****</ID>
          <DisplayName>user-example</DisplayName>
      </Owner>
</Contents>
<Contents>
      <Key>fun/test.jpg</Key>
      <LastModified>2012-02-24T08:42:32.000Z</LastModified>
      <ETag>"5B3C1A2E053D763E1B002CC607C5A0FE1****"</ETag>
      <Type>Normal</Type>
      <Size>344606</Size>
      <StorageClass>Standard</StorageClass>
      <Owner>
          <ID>0022012****</ID>
          <DisplayName>user-example</DisplayName>
      </Owner>
</Contents>
<Contents>
      <Key>oss.jpg</Key>
      <LastModified>2012-02-24T06:07:48.000Z</LastModified>
      <ETag>"5B3C1A2E053D763E1B002CC607C5A0FE1****"</ETag>
      <Type>Normal</Type>
      <Size>344606</Size>
      <StorageClass>Standard</StorageClass>
      <Owner>
          <ID>0022012****</ID>
          <DisplayName>user-example</DisplayName>
      </Owner>
</Contents>
</ListBucketResult>
)";

void nested_xml() {
  ListBucketResult list_bucket;
  struct_xml::from_xml(list_bucket, xml_str);

  std::string out;
  struct_xml::to_xml(list_bucket, out);

  std::cout << out << "\n";
}

struct book_t {
  std::string title;
  std::string author;
};
YLT_REFL(book_t, title, author);
struct library {
  iguana::xml_attr_t<book_t> book;
};
YLT_REFL(library, book);

void attribute() {
  std::string str = R"(
  <library name="UESTC library">
    <book id="1234" language="en" edition="1">
      <title>Harry Potter and the Philosopher's Stone</title>
      <author>J.K. Rowling</author>
      </book>
  </library>
)";
  {
    std::cout << "========= serialize book_t with attr ========\n";
    library lib;
    iguana::from_xml(lib, str);
    std::string ss;
    iguana::to_xml(lib, ss);
    std::cout << ss << "\n\n";
  }
  {
    std::cout << "========= serialize library with attr ========\n";
    iguana::xml_attr_t<library> lib;
    iguana::from_xml(lib, str);

    std::string ss;
    iguana::to_xml(lib, ss);
    std::cout << ss << "\n\n";
  }
}

struct person {
  std::string name;
  int age;
};
YLT_REFL(person, name, age);

void basic_usage() {
  std::string_view xml = R"(
<person>
    <name>tom</name>
    <age>20</age>
</person>
)";

  person p;
  struct_xml::from_xml(p, xml);

  assert(p.name == "tom" && p.age == 20);

  std::string str;
  struct_xml::to_xml(p, str);

  std::cout << str << "\n";

  std::string pretty_xml_str;
  struct_xml::to_xml</*pretty=*/true>(p, pretty_xml_str);

  std::cout << pretty_xml_str;
}

enum class Color { red, black };

enum Size { small, large };

class my_message {};

void type_to_string() {
  static_assert(struct_xml::type_string<my_message>() == "my_message");
  static_assert(struct_xml::enum_string<Color::red>() == "Color::red");
  static_assert(struct_xml::enum_string<Size::small>() == "small");
}

void get_error() {
  std::string xml = "invalid xml";
  person p;
  try {
    struct_xml::from_xml(p, xml);
  } catch (std::exception &e) {
    std::cout << e.what() << "\n";
  }
}

struct person2 {
  std::string name;
  int age;
};
YLT_REFL(person2, name, age);
REQUIRED(person2, name, age);

void required_field() {
  std::string xml = R"(
<person>
    <name>tom</name>
</person>
)";

  std::string xml1 = R"(
<person>
    <age>20</age>
</person>
)";

  person2 p;
  try {
    struct_xml::from_xml(p, xml);
  } catch (std::exception &e) {
    std::cout << e.what()
              << "\n";  // lack of required field, will throw in this case
  }

  try {
    struct_xml::from_xml(p, xml1);
  } catch (std::exception &e) {
    std::cout << e.what()
              << "\n";  // lack of required field, will throw in this case
  }
}

class some_object {
  int id;
  std::string name;

 public:
  some_object() = default;
  some_object(int i, std::string str) : id(i), name(str) {}
  int get_id() const { return id; }
  std::string get_name() const { return name; }
  YLT_REFL(some_object, id, name);
};

void test_inner_object() {
  some_object obj{20, "tom"};
  std::string str;
  iguana::to_xml(obj, str);
  std::cout << str << "\n";

  some_object obj1;
  iguana::from_xml(obj1, str);
  assert(obj1.get_id() == 20);
  assert(obj1.get_name() == "tom");

  std::string xml_str =
      R"(<some_object><id>20</id><name>小强</name></some_object>)";
  some_object obj2;
  iguana::from_xml(obj2, xml_str);
  assert(obj2.get_id() == 20);
  assert(obj2.get_name() == "小强");
}

struct shared_object {
  std::shared_ptr<std::vector<std::shared_ptr<int>>> vec;
  std::string b;
  std::shared_ptr<int> c;
  std::vector<std::shared_ptr<int>> d;
};
YLT_REFL(shared_object, vec, b, c, d);

void test_sp() {
  auto vec = std::make_shared<std::vector<std::shared_ptr<int>>>();
  vec->push_back(std::make_unique<int>(42));
  vec->push_back(std::make_unique<int>(21));
  shared_object contents{std::move(vec),
                         "test",
                         std::make_shared<int>(24),
                         {std::make_shared<int>(1), std::make_shared<int>(4)}};
  std::string str;
  iguana::to_xml(contents, str);

  shared_object cont;
  iguana::from_xml(cont, str);  // throw exception.
  std::cout << cont.b << "\n";
}

struct next_obj_t {
  int x;
  int y;
};
YLT_REFL(next_obj_t, x, y);

constexpr std::string_view get_alias_struct_name(next_obj_t *) {
  return "next";
}
constexpr auto get_alias_field_names(next_obj_t *) {
  using namespace ylt::reflection;
  return std::array{field_alias_t{"w", 0}, field_alias_t{"h", 1}};
}

struct out_object {
  std::unique_ptr<int> id;
  std::string_view name;
  next_obj_t obj;
};
YLT_REFL(out_object, id, name, obj);

constexpr std::string_view get_alias_struct_name(out_object *) { return "qi"; }
constexpr auto get_alias_field_names(out_object *) {
  using namespace ylt::reflection;
  return std::array{field_alias_t{"i", 0}, field_alias_t{"na", 1}};
}

void test_alias() {
  out_object m{std::make_unique<int>(20), "tom", {21, 42}};
  std::string xml_str;
  iguana::to_xml(m, xml_str);

  std::cout << xml_str << "\n";

  out_object m1;
  iguana::from_xml(m1, xml_str);
  assert(m1.name == "tom");
  assert(m1.obj.x == 21);
  assert(m1.obj.y == 42);
}

struct test_optstr_reader_null {
  std::optional<std::string> name;
};
YLT_REFL(test_optstr_reader_null, name);
void test_optional() {
  test_optstr_reader_null v;
  v.name = "name";  // optional<string> begin with 'n'
  std::string xml;
  iguana::to_xml(v, xml);

  test_optstr_reader_null v1;
  iguana::from_xml(v1, xml);
  assert(v.name == v1.name);
}

int main() {
  test_optional();
  test_alias();
  test_sp();
  basic_usage();
  type_to_string();
  nested_xml();
  attribute();
  get_error();
  required_field();
  test_inner_object();
}