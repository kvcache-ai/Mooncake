#include <cassert>
#include <iguana/json_reader.hpp>
#include <iguana/json_writer.hpp>
#include <iostream>

#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

void test_user_defined_struct();

struct person {
  std::string name;
  int age;
};

bool operator==(const person& a, const person& b) {
  return a.name == b.name && a.age == b.age;
}

YLT_REFL(person, name, age);

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
  iguana::to_json(obj, str);
  std::cout << str << "\n";

  some_object obj1;
  iguana::from_json(obj1, str);
  assert(obj1.get_id() == 20);
  assert(obj1.get_name() == "tom");

#if __cplusplus > 201703L
#if __has_include(<span>)
  {
    std::vector<int> v{1, 2};
    std::span<int> span(v.data(), v.data() + 2);
    std::string str;
    iguana::to_json(span, str);

    std::vector<int> v1;
    v1.resize(2);
    std::span<int> span1(v1.data(), v1.data() + 2);

    iguana::from_json(span1, str);
    assert(v == v1);
  }
#endif
#endif
}

struct person1 {
  std::shared_ptr<std::string> name;
  std::unique_ptr<int64_t> age;
};
YLT_REFL(person1, name, age);

void use_smart_pointer() {
  person1 p{std::make_shared<std::string>("tom"),
            std::make_unique<int64_t>(42)};
  std::string str;
  iguana::to_json(p, str);

  person1 p1;
  iguana::from_json(p1, str);

  assert(*p1.name == "tom");
  assert(*p1.age == 42);
}

void test_escape_serialize() {
#ifdef __linux__
  person p{"老\t人", 20};
  std::string ss;
  struct_json::to_json(p, ss);
  std::cout << ss << std::endl;
  person p1;
  struct_json::from_json(p1, ss);
  assert(p1.name == p.name);
#endif
}

struct test_optstr_reader_null {
  std::optional<std::string> name;
};
YLT_REFL(test_optstr_reader_null, name);

void test_optional() {
  test_optstr_reader_null v;
  v.name = "name";  // optional<string> begin with 'n'
  std::string json;
  iguana::to_json(v, json);

  test_optstr_reader_null v1;
  iguana::from_json(v1, json);
  assert(v.name == v1.name);
}

struct Foo {
  std::vector<std::tuple<int, std::string>> i;
};
YLT_REFL(Foo, i);

int main() {
  Foo f;
  f.i.emplace_back(1, "2");
  f.i.emplace_back(3, "4");
  std::string json_str;
  struct_json::to_json(f, json_str);
  std::cout << "Serialized: " << json_str << " \n";
  assert(!json_str.empty());

  Foo loaded_config;
  struct_json::from_json(loaded_config, json_str);

  assert(loaded_config.i == f.i);

  person p{"tom", 20};
  std::string str;

  // struct to json
  struct_json::to_json(p, str);
  std::cout << str << "\n";

  person p1;
  // json to struct
  struct_json::from_json(p1, str);
  assert(p == p1);

  // dom parse, json to dom
  struct_json::jvalue val;
  struct_json::parse(val, str);
  assert(val.at<std::string>("name") == "tom");
  assert(val.at<int>("age") == 20);

  test_inner_object();
  use_smart_pointer();
  test_escape_serialize();
  test_user_defined_struct();
  test_optional();
}