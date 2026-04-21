#include <deque>
#include <iterator>
#include <list>
#include <vector>
#if __cplusplus > 201703L
#if __has_include(<span>)
#include <span>
#endif
#endif

#ifndef THROW_UNKNOWN_KEY
#define THROW_UNKNOWN_KEY
#endif

// #define SEQUENTIAL_PARSE
#include <iostream>
#include <optional>

#include "doctest.h"
#include "test_headers.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

TEST_CASE("test parse item num_t") {
  {
    std::string str{"1.4806532964699196e-22"};
    double p{};
    iguana::from_json(p, str.begin(), str.end());
    CHECK(p == 1.4806532964699196e-22);

    std::error_code ec;
    iguana::from_json(p, str.begin(), str.end(), ec);
    CHECK(p == 1.4806532964699196e-22);
    CHECK(!ec);
  }
  {
    std::string str{""};
    double p{};

    CHECK_THROWS(iguana::from_json(p, str.begin(), str.end()));
    std::error_code ec;
    iguana::from_json(p, str.begin(), str.end(), ec);
    CHECK(ec);
  }
  {
    std::string str{"1"};
    int p{};
    iguana::from_json(p, str.begin(), str.end());
    CHECK(p == 1);
  }
  {
    std::string str{"3000000"};
    long long p{};
    iguana::from_json(p, str.begin(), str.end());
    CHECK(p == 3000000);

    iguana::from_json(p, str.data(), str.size());
    CHECK(p == 3000000);

    std::error_code ec;
    iguana::from_json(p, str.data(), str.size(), ec);
    CHECK(!ec);
    CHECK(p == 3000000);
  }
  {
    std::string str;
    str.append(300, '1');
    int p{};
    CHECK_THROWS(iguana::from_json(p, str.begin(), str.end()));
  }
  {
    std::list<char> arr{'[', '0', '.', '9', ']'};
    std::vector<double> test;
    iguana::from_json(test, arr);
    CHECK(test[0] == 0.9);

    std::error_code ec;
    iguana::from_json(test, arr, ec);
    CHECK(!ec);
    CHECK(test[0] == 0.9);

    std::deque<char> arr1{'[', '0', '.', '9', ']'};
    iguana::from_json(test, arr1);
    CHECK(test[0] == 0.9);
  }
  {
    std::list<char> arr{'0', '.', '9'};
    for (int i = 0; i < 999; i++) {
      arr.push_back('1');
    }

    double test = 0;
    CHECK_THROWS(iguana::from_json(test, arr.begin(), arr.end()));
  }
}

TEST_CASE("test parse item array_t") {
  {
    std::string str{"[1, -222]"};
    std::array<int, 2> test;
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test[0] == 1);
    CHECK(test[1] == -222);
  }
  // #if __cplusplus > 201703L
  // #if __has_include(<span>)
  //   {
  //     std::vector<int> v{1, 2};
  //     std::span<int> span(v.data(), v.data() + 2);
  //     std::string str;
  //     iguana::to_json(span, str);

  //     std::vector<int> v1;
  //     v1.resize(2);
  //     std::span<int> span1(v1.data(), v1.data() + 2);

  //     iguana::from_json(span1, str);
  //     assert(v == v1);
  //   }
  // #endif
  // #endif
  {
    std::string str{"[1, -222,"};
    std::array<int, 2> test;
    CHECK_NOTHROW(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"[   "};
    std::array<int, 2> test;
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"[ ]  "};
    std::array<int, 2> test;
    CHECK_NOTHROW(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"[ 1.2345]  "};
    std::array<int, 2> test;
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
}

TEST_CASE("test parse item str_t") {
  {
    std::string str{"[\"aaaaaaaaaa1\"]"};
    std::vector<std::string> test{};
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test[0] == "aaaaaaaaaa1");
  }
  {
    // this case throw at json_util@line 132
    std::string str{"\"aaa1\""};
    std::string test{};
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test == "aaa1");
  }
  {
    std::list<char> str;
    str.push_back('[');
    str.push_back('\"');
    str.push_back('\\');
    str.push_back('a');
    str.push_back('\"');
    str.push_back(']');
    str.push_back('a');
    str.push_back('a');
    str.push_back('1');
    std::vector<std::string> test{};
    iguana::from_json(test, str.begin(), str.end());

    CHECK(test[0] == "a");
  }

  {
    std::list<char> str;
    str.push_back('\"');
    str.push_back('\\');
    str.push_back('a');
    str.push_back('\"');
    std::string test{};
    test.resize(1);
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test == "a");
  }

  {
    std::list<char> list{'"', 'a', '"'};
    std::string test{};
    test.resize(2);
    iguana::from_json(test, list);
    CHECK(test == "a");
  }

  {
    std::list<char> list{'"', '\\', 'u', '8', '0', '0', '1', '"'};
    std::string test{};
    test.resize(20);
    iguana::from_json(test, list);

#ifdef __GNUC__
    CHECK(test == "老");
#endif  // __GNUC__
  }
}

TEST_CASE("test parse item seq container") {
  {
    std::string str{"[0,1,2,3]"};
    std::vector<double> test{};
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test.size() == 4);
    CHECK(test[0] == 0);
    CHECK(test[1] == 1);
    CHECK(test[2] == 2);
    CHECK(test[3] == 3);
  }
  {
    std::string str{"[0,1,2,3,]"};
    std::vector<double> test{};
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"[0,1,2,3,"};
    std::vector<double> test{};
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"[0,1,2"};
    std::array<int, 3> test{};
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"[0,1,2"};
    std::list<int> test{};
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
}

enum class Fruit {
  APPLE = 9999,
  BANANA = -4,
  ORANGE = 10,
  MANGO = 99,
  CHERRY = 7,
  GRAPE = 100000
};
enum class Color {
  BULE = 10,
  RED = 15,
};
enum class Status { stop = 10, start };
namespace iguana {
template <>
struct enum_value<Fruit> {
  constexpr static std::array<int, 6> value = {9999, -4, 10, 99, 7, 100000};
};

}  // namespace iguana
struct test_enum_t {
  Fruit a;
  Fruit b;
  Fruit c;
  Fruit d;
  Fruit e;
  Fruit f;
  Color g;
  Color h;
};
YLT_REFL(test_enum_t, a, b, c, d, e, f, g, h);

#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)

TEST_CASE("test enum") {
  auto validator = [](test_enum_t e) {
    CHECK(e.a == Fruit::APPLE);
    CHECK(e.b == Fruit::BANANA);
    CHECK(e.c == Fruit::ORANGE);
    CHECK(e.d == Fruit::MANGO);
    CHECK(e.e == Fruit::CHERRY);
    CHECK(e.f == Fruit::GRAPE);
    CHECK(e.g == Color::BULE);
    CHECK(e.h == Color::RED);
  };
  std::string str = R"(
{
  "a": "APPLE",
  "b": "BANANA",
  "c": "ORANGE",
  "d": "MANGO",
  "e": "CHERRY",
  "f": "GRAPE",
  "g": 10,
  "h": 15
}
  )";
  test_enum_t e;
  iguana::from_json(e, str);
  validator(e);

  std::string ss;
  iguana::to_json(e, ss);
  std::cout << ss << std::endl;
  test_enum_t e1;
  iguana::from_json(e1, ss);
  validator(e1);
}
#endif

TEST_CASE("test parse item map container") {
  {
    std::string str{"{\"key1\":\"value1\", \"key2\":\"value2\"}"};
    std::map<std::string, std::string> test{};
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test.size() == 2);
    CHECK(test.at("key1") == "value1");
    CHECK(test.at("key2") == "value2");
  }
}

TEST_CASE("test parse item char") {
  {
    std::string str{"\"c\""};
    char test{};
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test == 'c');
  }
  {
    std::string str{"\""};
    char test{};
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{R"("\)"};
    char test{};
    CHECK_THROWS_WITH(iguana::from_json(test, str.begin(), str.end()),
                      "Unxpected end of buffer");
  }
  {
    std::string str{""};
    char test{};
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"\"\\a\""};
    char test{};
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test == 'a');
  }
}

TEST_CASE("test parse item tuple") {
  {
    std::string str{"[1],\"a\",1.5]"};

    std::tuple<int, std::string, double> tp;

    iguana::from_json(tp, str.begin(), str.end());
    CHECK(std::get<0>(tp) == 1);
  }
  {
    std::string str{"[1,\"a\",1.5,[1,1.5]]"};

    std::tuple<int, std::string, double, std::tuple<int, double>> tp;

    iguana::from_json(tp, str.begin(), str.end());
    CHECK(std::get<0>(tp) == 1);
  }
}

TEST_CASE("test parse item bool") {
  {
    std::string str{"true"};
    bool test = false;
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test == true);
  }
  {
    std::string str{"false"};
    bool test = true;
    iguana::from_json(test, str.begin(), str.end());
    CHECK(test == false);
  }
  {
    std::string str{"True"};
    bool test = false;

    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"False"};
    bool test = true;

    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"\"false\""};
    bool test = false;

    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{""};
    bool test = false;
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
}

TEST_CASE("test parse item optional") {
  {
    std::string str{"null"};
    std::optional<int> test{};
    iguana::from_json(test, str.begin(), str.end());
    CHECK(!test.has_value());
  }
  {
    std::string str{""};
    std::optional<int> test{};
    CHECK_THROWS(iguana::from_json(test, str.begin(), str.end()));
  }
  {
    std::string str{"1"};
    std::optional<int> test{};
    iguana::from_json(test, str.begin(), str.end());
    CHECK(*test == 1);
  }
}

struct optional_t {
  std::optional<bool> p;
};
YLT_REFL(optional_t, p);

struct struct_test_t {
  int32_t value;
};
YLT_REFL(struct_test_t, value);

struct struct_container_t {
  std::vector<struct_test_t> values;
};
YLT_REFL(struct_container_t, values);

struct struct_container_1_t {
  std::optional<struct_container_t> val;
};  // entities_t
YLT_REFL(struct_container_1_t, val);

TEST_CASE("test optional") {
  {
    struct_container_t container{};
    container.values = {{1}, {2}, {3}};

    struct_container_1_t t1{};
    t1.val = container;

    std::string str;
    iguana::to_json(t1, str);
    std::cout << str << "\n";

    struct_container_1_t t2{};
    iguana::from_json(t2, str);
    std::cout << t2.val.has_value() << "\n";

    CHECK(t1.val->values[0].value == t2.val->values[0].value);
    CHECK(t1.val->values[1].value == t2.val->values[1].value);
    CHECK(t1.val->values[2].value == t2.val->values[2].value);
  }

  {
    optional_t p;
    std::string str;
    iguana::to_json(p, str);

    optional_t p1;
    iguana::from_json(p1, str);
    CHECK(!p1.p.has_value());

    p.p = false;

    str.clear();
    iguana::to_json(p, str);
    std::cout << str << "\n";

    iguana::from_json(p1, str);
    CHECK(*p1.p == false);

    p.p = true;
    str.clear();
    iguana::to_json(p, str);
    std::cout << str << "\n";

    iguana::from_json(p1, str);
    CHECK(*p1.p == true);
  }
}

struct empty_t {};
YLT_REFL(empty_t);

TEST_CASE("test empty struct") {
  empty_t t;
  std::string str;
  iguana::to_json(t, str);
  std::cout << str << "\n";

  iguana::from_json(t, str);
}

struct keyword_t {
  std::string ___private;
  std::string ___public;
  std::string ___protected;
  std::string ___class;
};
YLT_REFL(keyword_t, ___private, ___protected, ___public, ___class);

TEST_CASE("test keyword") {
  std::string ss =
      R"({"private":"private","protected":"protected","public":"public","class":"class"})";
  keyword_t t2;
  iguana::from_json(t2, ss);
  CHECK(t2.___private == "private");
  CHECK(t2.___protected == "protected");
  CHECK(t2.___public == "public");
  CHECK(t2.___class == "class");
}

struct config_actor_type {
  long id = 0;
  std::string make;
  std::string config;
};
YLT_REFL(config_actor_type, id, make, config);

struct config_app_json_type {
  long id = 0;
  int threads;
  std::string loglevel;
  std::vector<config_actor_type> actors;
};
YLT_REFL(config_app_json_type, id, threads, loglevel, actors);

TEST_CASE("test long") {
  config_app_json_type app{1234};
  std::string str;
  iguana::to_json(app, str);

  config_app_json_type app1;
  iguana::from_json(app1, str);
  CHECK(app1.id == 1234);
}

TEST_CASE("test unknown fields") {
  std::string str = R"({"dummy":0, "name":"tom", "age":20})";
  person p;
  CHECK_THROWS_WITH(iguana::from_json(p, str), "Unknown key: dummy");

  std::string str1 = R"({"name":"tom", "age":20})";
  person p1;
  iguana::from_json(p1, str1);

  std::string str2 = R"({"name":"tom", "age":20})";
  person p2;
  iguana::from_json(p2, str2);
  std::cout << p2.name << "\n";
  CHECK(p2.name == "tom");
}

TEST_CASE("test unicode") {
  {
    std::string str2 = R"({"name":"\u8001", "age":20})";
    person p2;
    iguana::from_json(p2, str2);
#ifdef __GNUC__
    CHECK(p2.name == "老");
#endif
  }
  {
    std::string str = R"("\u8001")";

    std::string t;
    iguana::from_json(t, str);
#ifdef __GNUC__
    CHECK(t == "老");
#endif
  }
  {
    std::list<char> str = {'[', '"', '\\', 'u', '8', '0', '0', '1', '"', ']'};
    std::list<std::string> list;
    iguana::from_json(list, str);
#ifdef __GNUC__
    CHECK(*list.begin() == "老");
#endif
  }
}

TEST_CASE("test escape in string") {
  std::string str = R"({"name":"A\nB\tC\rD\bEF\n\f\n", "age": 20})";
  {
    person p;
    iguana::from_json(p, str);
    CHECK(p.name == "A\nB\tC\rD\bEF\n\f\n");
    CHECK(p.age == 20);
    {
      std::string ss;
      iguana::to_json(p, ss);
      person p1;
      iguana::from_json(p1, ss);
      CHECK(p1.name == "A\nB\tC\rD\bEF\n\f\n");
      CHECK(p1.age == 20);
    }
    {
      person p0;
      p0.name.push_back(0x1E);
      std::string ss;
      iguana::to_json<false>(p0, ss);
      person p1;
      CHECK(ss != R"({"name":"\u001E","age":0})");
    }
  }
  {
    std::string str1 = R"({"name":"\u001E", "age": 20})";
    person p;
    iguana::from_json(p, str1);
    CHECK(static_cast<unsigned>((p.name)[0]) == 0x1E);
    std::string ss;
    iguana::to_json(p, ss);
    person p1;
    iguana::from_json(p1, ss);
    CHECK(static_cast<unsigned>((p1.name)[0]) == 0x1E);
  }
  {
    std::string slist = R"({"name":"\u8001", "age":20})";
    std::list<char> strlist(slist.begin(), slist.end());
    person p;
    iguana::from_json(p, strlist);
    CHECK(p.name == "老");
    CHECK(p.age == 20);
    std::string ss;
    iguana::to_json(p, ss);
    std::cout << ss << std::endl;
    person p1;
    iguana::from_json(p1, ss);
    CHECK(p1.name == "老");
  }
  {
    std::list<char> strlist(str.begin(), str.end());
    person p;
    iguana::from_json(p, strlist);
    CHECK(p.name == "A\nB\tC\rD\bEF\n\f\n");
    CHECK(p.age == 20);
  }
}

TEST_CASE("test pmr") {
#ifdef IGUANA_ENABLE_PMR
#if __has_include(<memory_resource>)
  iguana::string_stream str{&iguana::iguana_resource};
#endif
#else
  iguana::string_stream str;
#endif
  person obj{"tom", 20};
  iguana::to_json(obj, str);
}

TEST_CASE("test from_json_file") {
  std::string str = R"({"name":"tom", "age":20})";
  std::string filename = "test.json";
  std::ofstream out(filename, std::ios::binary);
  out.write(str.data(), str.size());
  out.close();

  person obj;
  iguana::from_json_file(obj, filename);
  CHECK(obj.name == "tom");
  CHECK(obj.age == 20);

  std::error_code ec;
  iguana::from_json_file(obj, filename, ec);
  CHECK(!ec);
  CHECK(obj.name == "tom");
  CHECK(obj.age == 20);

  std::filesystem::remove(filename);

  person p;
  CHECK_THROWS_AS(iguana::from_json_file(p, "not_exist.json"),
                  std::runtime_error);

  iguana::from_json_file(p, "not_exist.json", ec);
  CHECK(ec);

  std::string cur_path = std::filesystem::current_path().string();
  std::filesystem::create_directories("dummy_test_dir");
  CHECK_THROWS_AS(iguana::from_json_file(p, "dummy_test_dir"),
                  std::runtime_error);
  std::filesystem::remove("dummy_test_dir");

  std::ofstream out1("dummy_test_dir.json", std::ios::binary);
  CHECK_THROWS_AS(iguana::from_json_file(p, "dummy_test_dir.json"),
                  std::runtime_error);
  out1.close();
  std::filesystem::remove("dummy_test_dir.json");
}

struct book_t {
  std::string_view title;
  std::optional<std::string_view> edition;
  std::vector<std::string_view> author;
};
YLT_REFL(book_t, title, edition, author);
TEST_CASE("test the string_view") {
  {
    std::string str = R"("C++ \ntemplates")";
    std::string_view t;
    iguana::from_json(t, str);
    CHECK(t == R"(C++ \ntemplates)");
  }
  {
    std::string str = R"({
      "title": "C++ templates",
      "edition": "invalid number"})";
    book_t b;
    iguana::from_json(b, str);
    CHECK(b.title == "C++ templates");
    CHECK(*(b.edition) == "invalid number");
  }
  {
    auto validator = [](book_t b) {
      CHECK(b.title == "C++ templates");
      CHECK(*(b.edition) == "invalid number");
      CHECK(b.author[0] == "David Vandevoorde");
      CHECK(b.author[1] == "Nicolai M. Josuttis");
    };
    std::string str = R"({
    "title": "C++ templates",
    "edition": "invalid number",
    "author": [
      "David Vandevoorde",
      "Nicolai M. Josuttis"
    ]})";
    book_t b;
    iguana::from_json(b, str);
    validator(b);
    {
      std::string ss;
      iguana::to_json(b, ss);
      book_t b1;
      iguana::from_json(b1, ss);
      validator(b1);
    }
    {
      // test variant
      std::variant<int, book_t> vb = b;
      std::string ss;
      iguana::to_json(vb, ss);
      book_t b1;
      iguana::from_json(b1, ss);
      validator(b1);
    }
  }
  {
    std::string str = R"(["tom", 30, 25.8])";
    std::tuple<std::string_view, int, float> t;
    iguana::from_json(t, str);
    CHECK(std::get<0>(t) == "tom");
    CHECK(std::get<1>(t) == 30);
    CHECK(std::get<2>(t) == 25.8f);
  }
  {
    std::string str = R"(["tom", "jone"])";
    std::string_view arr[2];
    iguana::from_json(arr, str);
    CHECK(arr[0] == "tom");
    CHECK(arr[1] == "jone");
  }
  {
    std::unordered_map<std::string_view, std::string_view> mp;
    std::string str = R"({"tom" : "C++", "jone" : "Go"})";
    iguana::from_json(mp, str);
    CHECK(mp["tom"] == "C++");
    CHECK(mp["jone"] == "Go");

    std::string ss;
    iguana::to_json(mp, ss);
    std::unordered_map<std::string_view, std::string_view> mp2;
    iguana::from_json(mp2, ss);
    CHECK(mp2["tom"] == "C++");
    CHECK(mp2["jone"] == "Go");
  }
}

struct st_char_t {
  std::vector<char> a;
  char b[5];
  char c[5];
};
YLT_REFL(st_char_t, a, b, c);
TEST_CASE("test char") {
  std::string str = R"(
  {
    "a": ["\n", "\t", "\f", "\r", "\b"],
    "b": ["1", "2", "3", "4", "5"],
    "c": "1234\n"
  }
  )";
  auto validator = [](st_char_t c) {
    CHECK(c.a[0] == '\n');
    CHECK(c.a[1] == '\t');
    CHECK(c.a[2] == '\f');
    CHECK(c.a[3] == '\r');
    CHECK(c.a[4] == '\b');
    CHECK(c.b[0] == '1');
    CHECK(c.b[1] == '2');
    CHECK(c.b[2] == '3');
    CHECK(c.b[3] == '4');
    CHECK(c.b[4] == '5');
    CHECK(c.c[0] == '1');
    CHECK(c.c[1] == '2');
    CHECK(c.c[2] == '3');
    CHECK(c.c[3] == '4');
    CHECK(c.c[4] == '\n');
  };
  st_char_t c;
  iguana::from_json(c, str);
  validator(c);
  std::string ss;
  iguana::to_json(c, ss);
  std::cout << ss << std::endl;

  std::string ss1;
  const char a[3] = {'1', '2', '\0'};
  iguana::to_json(a, ss1);
  std::cout << ss1 << std::endl;
  CHECK(ss1 == "\"12\"");
}

struct fixed_vector_arr_t {
  std::vector<int> a[4];
};
YLT_REFL(fixed_vector_arr_t, a);
TEST_CASE("test fixed array") {
  std::string str = R"(
{
  "a": [
    [1, 2, 3],
    [4, 5, 6],
    [],
    [7, 8]
    ]
}
  )";
  auto validator = [](fixed_vector_arr_t f) {
    CHECK(f.a[0][0] == 1);
    CHECK(f.a[0][1] == 2);
    CHECK(f.a[0][2] == 3);
    CHECK(f.a[1][0] == 4);
    CHECK(f.a[1][1] == 5);
    CHECK(f.a[1][2] == 6);
    CHECK(f.a[2].empty());
    CHECK(f.a[3][0] == 7);
    CHECK(f.a[3][1] == 8);
  };
  fixed_vector_arr_t f;
  iguana::from_json(f, str);
  validator(f);

  std::string ss;
  iguana::to_json(f, ss);
  fixed_vector_arr_t f1;
  iguana::from_json(f1, ss);
  validator(f1);
}

struct vector_bool_t {
  std::vector<bool> a;
  bool b;
};
YLT_REFL(vector_bool_t, a, b);
TEST_CASE("test vector<bool>") {
  std::string str = R"({
  "a": [true, false],
  "b": true
  })";
  auto validator = [](vector_bool_t v) {
    CHECK(v.a[0] == true);
    CHECK(v.a[1] == false);
    CHECK(v.b == true);
  };
  vector_bool_t v;
  iguana::from_json(v, str);
  validator(v);

  std::string ss;
  iguana::to_json(v, ss);
  vector_bool_t v1;
  iguana::from_json(v1, ss);
  validator(v1);
}

struct test_numeric_t {
  std::vector<iguana::numeric_str> arr;
  iguana::numeric_str num;
};
YLT_REFL(test_numeric_t, arr, num);
TEST_CASE("test numeric string") {
  std::string str = R"(
{ "arr": [1.5 , 2.4 , 3.7],
  "num": 420}
  )";
  auto validator = [](test_numeric_t &t) {
    CHECK(t.arr[0].convert<float>() == 1.5f);
    CHECK(t.arr[1].convert<float>() == 2.4f);
    CHECK(t.arr[2].convert<float>() == 3.7f);
    CHECK(t.num.convert<int>() == 420);
  };
  test_numeric_t t;
  iguana::from_json(t, str);
  validator(t);

  std::string ss;
  iguana::to_json(t, ss);
  std::cout << ss << std::endl;
  test_numeric_t t1;
  iguana::from_json(t1, ss);
  validator(t1);
}

struct test_uint8_t {
  uint8_t a;
  int8_t b;
  char c;
};
YLT_REFL(test_uint8_t, a, b, c);
TEST_CASE("test uint8 and int8") {
  std::string str = R"(
{ 
"a" : 50,
"b" : -10,
"c" : "c"
}
  )";
  auto validator = [](test_uint8_t &t) {
    CHECK(t.a == 50);
    CHECK(t.b == -10);
    CHECK(t.c == 'c');
  };

  test_uint8_t t;
  iguana::from_json(t, str);
  validator(t);

  std::string ss;
  iguana::to_json(t, ss);
  test_uint8_t t1;
  iguana::from_json(t1, ss);
  validator(t1);
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

TEST_CASE("test inner YLT_REFL") {
  some_object obj{20, "tom"};
  std::string str;
  iguana::to_json(obj, str);
  std::cout << str << "\n";

  some_object obj1;
  iguana::from_json(obj1, str);
  CHECK(obj1.get_id() == 20);
  CHECK(obj1.get_name() == "tom");
}

struct Contents_t {
  std::unique_ptr<std::vector<std::unique_ptr<int>>> vec;
  std::shared_ptr<std::vector<std::unique_ptr<int>>> vec_s;
  std::string b;
};
YLT_REFL(Contents_t, vec, vec_s, b);

TEST_CASE("test smart_ptr") {
  std::string str = R"(
{"vec":[42,21],"vec_s":[15,16],"b":"test"}
  )";
  auto validator = [](Contents_t &cont) {
    CHECK(cont.b == "test");
    CHECK(*(*cont.vec)[0] == 42);
    CHECK(*(*cont.vec)[1] == 21);
    CHECK(*(*cont.vec_s)[0] == 15);
    CHECK(*(*cont.vec_s)[1] == 16);
  };

  Contents_t cont;
  iguana::from_json(cont, str);
  validator(cont);

  std::string ss;
  iguana::to_json(cont, ss);
  Contents_t cont1;
  iguana::from_json(cont1, ss);
  validator(cont1);
}

struct person1 {
  std::shared_ptr<std::string> name;
  std::shared_ptr<int64_t> age;
};
YLT_REFL(person1, name, age);

TEST_CASE("test smart point issue 223") {
  person1 p{std::make_shared<std::string>("tom"),
            std::make_shared<int64_t>(42)};
  std::string str;
  iguana::to_json(p, str);

  person1 p1;
  iguana::from_json(p1, str);  // here throw exception

  CHECK(*p1.name == "tom");
  CHECK(*p1.age == 42);
}
