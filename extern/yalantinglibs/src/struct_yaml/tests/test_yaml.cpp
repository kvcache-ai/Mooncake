#include <deque>
#include <iterator>
#include <list>
#include <vector>
#define DOCTEST_CONFIG_IMPLEMENT
#include <iostream>
#include <optional>

#include "doctest.h"
#include "ylt/struct_yaml/yaml_reader.h"
#include "ylt/struct_yaml/yaml_writer.h"

TEST_CASE("test without struct") {
  using MapType = std::unordered_map<int, std::vector<int>>;
  {
    MapType mp;
    mp[0] = {1, 2};
    mp[1] = {3, 4};
    std::string ss;
    iguana::to_yaml(mp, ss);

    MapType mp1;
    iguana::from_yaml(mp1, ss);
    CHECK(mp[0] == mp1[0]);
    CHECK(mp1[1] == mp1[1]);

    std::string ss1 = R"({0 : [1, 2], 1 : [3, 4]})";
    MapType mp2;
    iguana::from_yaml(mp2, ss1);
    CHECK(mp[0] == mp2[0]);
    CHECK(mp1[1] == mp2[1]);
  }
  {
    using TupleType = std::tuple<int, std::string, double>;
    TupleType t_v(10, "Hello", 3.14);
    std::string ss;
    iguana::to_yaml(t_v, ss);
    TupleType t;
    iguana::from_yaml(t, ss);
    CHECK(t == t_v);

    std::string ss1 = R"([10, Hello, 3.14])";
    TupleType t1;
    iguana::from_yaml(t1, ss1);
    CHECK(t1 == t_v);

    std::unique_ptr<TupleType> tuple_ptr;
    iguana::from_yaml(tuple_ptr, ss1);
    CHECK(*tuple_ptr == t_v);
  }
  {
    using TupleType = std::tuple<std::vector<int>, std::string, double>;
    TupleType t_v({1, 2, 3}, "Hello", 3.14);
    std::string ss;
    iguana::to_yaml(t_v, ss);
    std::cout << ss << std::endl;
    TupleType t;
    iguana::from_yaml(t, ss);
    CHECK(t == t_v);

    std::string ss1 = R"([ [1, 2 ,3], Hello, 3.14])";
    TupleType t1;
    iguana::from_yaml(t1, ss1);
    CHECK(t1 == t_v);

    std::optional<TupleType> op_t;
    iguana::from_yaml(op_t, ss1);
    CHECK(*op_t == t_v);

    // test tuple exception
    TupleType t2;
    std::string ss2 = R"(1, 2 ,3)";
    std::error_code ec;
    iguana::from_yaml(t2, ss2.begin(), ss2.end(), ec);
    CHECK(ec);
  }
  {
    std::unique_ptr<int> p_tr;
    std::string ss;
    iguana::to_yaml(p_tr, ss);
    CHECK(ss == "\n");
  }
}

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
TEST_CASE("test plain_type") {
  plain_type_t p{false, enum_status::stop, 'a', true};
  p.price = 20;
  std::string ss;
  iguana::to_yaml(p, ss);
  std::cout << ss << std::endl;
  plain_type_t p1;
  iguana::from_yaml(p1, ss);
  auto validator = [&p](plain_type_t p1) {
    CHECK(p1.isok == p.isok);
    CHECK(p1.status == p.status);
    CHECK(p1.c == p.c);
    CHECK(*p1.hasprice == *p.hasprice);
    CHECK(!p1.num);
    CHECK(*p1.price == 20);
  };
  validator(p1);

  std::string ss1 = R"(
isok: false # this is a bool type
status: 1
--- 
c: a
hasprice: true
num: 
price: 20 # this is price
  )";
  std::vector<char> arr_char(ss1.begin(), ss1.end());
  plain_type_t p2;
  iguana::from_yaml(p2, arr_char.begin(), arr_char.end());
  validator(p2);
  // Unknown key: pri
  std::string str = R"(pri: 10)";
  CHECK_THROWS(iguana::from_yaml(p, str));

  std::optional<plain_type_t> op_p;
  iguana::from_yaml(op_p, ss1);
  validator(*op_p);

  std::unique_ptr<plain_type_t> op_ptr;
  iguana::from_yaml(op_ptr, ss1);
  validator(*op_ptr);

  std::string ss2;
  iguana::to_yaml(op_ptr, ss2);

  std::optional<plain_type_t> op_p2;
  iguana::from_yaml(op_p2, ss2);
  validator(*op_p2);
}

TEST_CASE("test optional") {
  std::string str = R"()";
  std::optional<int> opt;
  iguana::from_yaml(opt, str.begin(), str.end());
  CHECK(!opt);

  std::string ss;
  iguana::to_yaml(opt, ss);
  CHECK(ss == "null\n");

  std::string ss1 = R"(null)";
  iguana::from_yaml(opt, ss1);
  CHECK(!opt);
}

TEST_CASE("test exception") {
  {
    // Expected 4 hexadecimal digits
    std::string str = R"("\u28")";
    std::string str_v;
    std::error_code ec;
    iguana::from_yaml(str_v, str.begin(), str.end(), ec);
    CHECK(ec);
  }
  {
    // Failed to parse number
    std::string str = R"(c1wd)";
    std::string str1 = R"()";
    float f = 1.0f;
    std::error_code ec;
    iguana::from_yaml(f, str1.begin(), str1.end());
    CHECK(f == 1.0f);
    iguana::from_yaml(f, str.begin(), str.end(), ec);
    CHECK(ec);
  }
  {
    // Failed to parse number
    std::string str = R"(c1wd)";
    int d;
    std::error_code ec;
    iguana::from_yaml(d, str.begin(), str.end(), ec);
    CHECK(ec);
  }
  {
    // Expected "
    std::string str = R"("Hello)";
    std::string str_v;
    std::error_code ec;
    iguana::from_yaml(str_v, str.begin(), str.end(), ec);
    CHECK(ec);
    // Expected '
    std::string str1 = R"('Hello)";
    std::error_code ec1;
    iguana::from_yaml(str_v, str1.begin(), str1.end(), ec1);
    CHECK(ec1);
  }
  {
    // Expected one character
    std::string str = R"(He)";
    char c;
    std::error_code ec;
    iguana::from_yaml(c, str.begin(), str.end(), ec);
    CHECK(ec);
  }
  {
    // Expected true or false
    std::string str = R"(Flsae)";
    bool isok;
    std::error_code ec;
    iguana::from_yaml(isok, str.begin(), str.end(), ec);
    CHECK(ec);
  }
  {
    // Expected ] or '-'
    std::string str = R"(1, 3)";
    std::vector<int> arr;
    std::error_code ec;
    iguana::from_yaml(arr, str.begin(), str.end(), ec);
    CHECK(ec);
  }
}

struct test_string_t {
  std::vector<std::string> txt;
};
YLT_REFL(test_string_t, txt);
TEST_CASE("test block string") {
  std::string str = R"(
txt:
 - |
  Hello
  World
 - >
  Hello
  World
 - >-
  Hello
  World
 - "Hello\nWorld\n"
 - "\u8001A\nB\tC\rD\bEF\n\f\n123"
 - >
 -)";
  test_string_t s;
  std::error_code ec;
  iguana::from_yaml(s, str.begin(), str.end(), ec);
  CHECK(s.txt[0] == "Hello\nWorld\n");
  CHECK(s.txt[1] == "Hello World\n");
  CHECK(s.txt[2] == "Hello World");
  CHECK(s.txt[3] == "Hello\nWorld\n");  // escape
  CHECK(s.txt[4] == "老A\nB\tC\rD\bEF\n\f\n123");
  CHECK(s.txt[5] == "\n");
  CHECK(s.txt[6].empty());
}

struct arr_t {
  std::vector<std::string_view> arr;
};
YLT_REFL(arr_t, arr);
TEST_CASE("test array") {
  std::string str = R"(
  arr : [
      a,
      
      b] 
  )";
  auto validator = [](const arr_t &a) {
    CHECK(a.arr[0] == std::string_view("a"));
    CHECK(a.arr[1] == std::string_view("b"));
  };
  arr_t a;
  iguana::from_yaml(a, str);
  validator(a);

  std::string str1 = R"(
  arr: 
    - a
    - b
  )";
  arr_t a1;
  iguana::from_yaml(a1, str1);
  validator(a1);

  std::string ss;
  iguana::to_yaml(a1, ss);
  arr_t a2;
  iguana::from_yaml(a2, ss);
  validator(a2);

  std::string ss1;
  iguana::to_yaml(a1.arr, ss1);
  std::vector<std::string_view> arr;
  iguana::from_yaml(arr, ss1);
  validator(arr_t{arr});
}

struct nest_arr_t {
  std::vector<std::vector<std::string_view>> arr;
};
YLT_REFL(nest_arr_t, arr);
TEST_CASE("test nest arr ") {
  std::string str = R"(
  arr : [[
      a,
      b],
   [c, d]]
  )";
  auto validator = [](const nest_arr_t &a) {
    CHECK(a.arr[0][0] == std::string_view("a"));
    CHECK(a.arr[0][1] == std::string_view("b"));
    CHECK(a.arr[1][0] == std::string_view("c"));
    CHECK(a.arr[1][1] == std::string_view("d"));
  };
  nest_arr_t a;
  iguana::from_yaml(a, str);
  validator(a);
  std::string str0 = R"(
  arr : [
    [a, b],
    [c, d]]
  )";
  nest_arr_t a0;
  iguana::from_yaml(a0, str0);
  validator(a0);
  std::string str1 = R"(
  arr :
    - - a
       - b
    - - c
      - d
  )";
  nest_arr_t a1;
  iguana::from_yaml(a1, str1);
  validator(a1);
  std::string str2 = R"(
  arr :
    - [a, b]
    - [c, d]
  )";
  nest_arr_t a2;
  iguana::from_yaml(a2, str2);
  validator(a2);
  std::string ss;
  iguana::to_yaml(a2, ss);
  nest_arr_t a3;
  iguana::from_yaml(a3, ss);
  validator(a3);
}

struct nest_float_arr_t {
  std::vector<std::vector<float>> arr;
};
YLT_REFL(nest_float_arr_t, arr);
TEST_CASE("test arr with float") {
  std::string str = R"(
  arr :
    - - 28.5
       - 56.7
    - - 123.4
      - -324.9)";
  nest_float_arr_t a;
  iguana::from_yaml(a, str);
  CHECK(a.arr[0][0] == 28.5f);
  CHECK(a.arr[0][1] == 56.7f);
  CHECK(a.arr[1][0] == 123.4f);
  CHECK(a.arr[1][1] == -324.9f);
  std::cout << a.arr[1][1] << std::endl;
}

struct map_t {
  std::unordered_map<std::string_view, std::string_view> map;
};
YLT_REFL(map_t, map);
TEST_CASE("test map") {
  std::string str = R"(
  map : {
      k1 : v1,
   k2 : v2}
  )";
  map_t m;
  iguana::from_yaml(m, str);
  CHECK(m.map["k1"] == "v1");
  CHECK(m.map["k2"] == "v2");
  std::string str1 = R"(
  map:
    k1: v1
    k2: v2
)";
  map_t m1;
  iguana::from_yaml(m1, str1);
  CHECK(m1.map["k1"] == "v1");
  CHECK(m1.map["k2"] == "v2");
}

struct map_arr_t {
  std::unordered_map<std::string_view, std::vector<std::string_view>> map;
};
YLT_REFL(map_arr_t, map);
TEST_CASE("test map") {
  std::string str = R"(
  map : {
      k1 : [a , b],
   k2 : [c, d], 
   }
  )";
  auto validator = [](map_arr_t &m) {
    CHECK(m.map["k1"][0] == "a");
    CHECK(m.map["k1"][1] == "b");
    CHECK(m.map["k2"][0] == "c");
    CHECK(m.map["k2"][1] == "d");
  };
  map_arr_t m;
  iguana::from_yaml(m, str);
  validator(m);
  std::string str1 = R"(
  map:
    k1:
      - a
      - b
    k2:
      - c
      - d)";
  map_arr_t m1;
  iguana::from_yaml(m1, str1);
  validator(m1);
  std::string ss;
  iguana::to_yaml(m1, ss);
  map_arr_t m2;
  iguana::from_yaml(m2, ss);
  validator(m2);
}

struct test_str_t {
  std::string_view str;
};
YLT_REFL(test_str_t, str);
TEST_CASE("test str type") {
  std::string str = R"(
  str :
      hello world
  )";
  test_str_t s;
  iguana::from_yaml(s, str);
  CHECK(s.str == "hello world");
  std::string str1 = R"(
  str :
      'hello world')";
  test_str_t s1;
  iguana::from_yaml(s1, str1);
  CHECK(s1.str == "hello world");
  std::string ss;
  iguana::to_yaml(s1, ss);
}

struct some_type_t {
  std::vector<float> price;
  std::optional<std::string> description;
  std::map<std::string, int> child;
  bool hasdescription;
  char c;
  std::optional<double> d_v;
  std::string name;
  std::string_view addr;
  enum_status status;
};
YLT_REFL(some_type_t, price, description, child, hasdescription, c, d_v, name,
         addr, status);
TEST_CASE("test some_type") {
  auto validator_some_type = [](some_type_t s) {
    CHECK(s.price[0] == 1.23f);
    CHECK(s.price[1] == 3.25f);
    CHECK(s.price[2] == 9.57f);
    CHECK(*s.description == "Some description");
    CHECK(s.child["key1"] == 10);
    CHECK(s.child["key2"] == 20);
    CHECK(s.hasdescription == true);
    CHECK(s.c == 'X');
    CHECK(*s.d_v == 3.14159);
    CHECK(s.name == "John Doe");
    CHECK(s.addr == "123 Main St");
    CHECK(s.status == enum_status::stop);
  };
  std::string str = R"(
price: [1.23, 3.25, 9.57]
description: >- #test comment
    Some 
    description
child:
  key1: 10
  key2: 20
hasdescription: true
c: X
d_v: 3.14159
name: John Doe
addr: '123 Main St'
status : 1
)";
  some_type_t s;
  iguana::from_yaml(s, str);
  validator_some_type(s);
  std::string ss;
  iguana::to_yaml(s, ss);
  some_type_t s1;
  iguana::from_yaml(s1, ss);
  validator_some_type(s1);
}

struct address_t {
  std::string_view street;
  std::string_view city;
  std::string_view state;
  std::string_view country;
};
YLT_REFL(address_t, street, city, state, country);
struct contact_t {
  std::string_view type;
  std::string_view value;
};
YLT_REFL(contact_t, type, value);
struct person_t {
  std::string_view name;
  int age;
  address_t address;
  std::vector<contact_t> contacts;
};
YLT_REFL(person_t, name, age, address, contacts);
TEST_CASE("test person_t") {
  auto validator_person = [](const person_t &p) {
    CHECK(p.name == "John Doe");
    CHECK(p.age == 30);
    CHECK(p.address.street == "123 Main St");
    CHECK(p.address.city == "Anytown");
    CHECK(p.address.state == "Example State");
    CHECK(p.address.country == "Example Country");
    CHECK(p.contacts[0].type == "email");
    CHECK(p.contacts[0].value == "john@example.com");
    CHECK(p.contacts[1].type == "phone");
    CHECK(p.contacts[1].value == "123456789");
    CHECK(p.contacts[2].type == "social");
    CHECK(p.contacts[2].value == "johndoe");
  };
  std::string str = R"(
name: John Doe
age: 30
address:
  street: 123 Main St
  city: Anytown
  state: Example State
  country: Example Country
contacts:
  - type: email
    value: john@example.com
  - type: phone
    value: 123456789
  - type: social
    value: "johndoe"
  )";
  person_t p;
  iguana::from_yaml(p, str);
  validator_person(p);
  std::string ss;
  iguana::to_yaml(p, ss);
  person_t p2;
  iguana::from_yaml(p2, ss);
  validator_person(p2);
}

struct product_t {
  std::string_view name;
  float price;
  std::optional<std::string> description;
};
YLT_REFL(product_t, name, price, description);
struct store_t {
  std::string name;
  std::string_view location;
  std::vector<product_t> products;
};
YLT_REFL(store_t, name, location, products);
struct store_example_t {
  store_t store;
};
YLT_REFL(store_example_t, store);
TEST_CASE("test example store") {
  std::string str = R"(
store:
  name: "\u6c38\u8f89\u8d85\u5e02\t"
  location: Chengdu
  products:
    - name: iPad
      price: 
        899.4
      description: >
        nice
        ipad
    - name: watch
      price: 488.8
      description: |
        cheap watch
    - name: iPhone
      price: 999.99
      description: >-   
        expensive
        iphone
  )";
  store_example_t store_1;
  iguana::from_yaml(store_1, str);
  auto store = store_1.store;
  CHECK(store.name == "永辉超市\t");
  CHECK(store.location == "Chengdu");
  CHECK(store.products[0].name == "iPad");
  CHECK(store.products[0].price == 899.4f);
  CHECK(store.products[0].description == "nice ipad\n");
  CHECK(store.products[1].name == "watch");
  CHECK(store.products[1].price == 488.8f);
  CHECK(store.products[1].description == "cheap watch\n");
  CHECK(store.products[2].name == "iPhone");
  CHECK(store.products[2].price == 999.99f);
  CHECK(store.products[2].description == "expensive iphone");
}

struct book_t {
  std::optional<std::string_view> title;
  std::vector<std::string_view> categories;
};
YLT_REFL(book_t, title, categories);
struct library_t {
  std::unique_ptr<std::string_view> name;
  std::string location;
  std::vector<std::unique_ptr<book_t>> books;
};
YLT_REFL(library_t, name, location, books);
struct library_example_t {
  std::vector<library_t> libraries;
};
YLT_REFL(library_example_t, libraries);

TEST_CASE("test example books") {
  std::vector<book_t> books;
  std::string str = R"(
    - title:
      categories: 
        - computer science
        - programming
    - title: The Great Gatsby
      categories:
        - classic literature
        - fiction
  )";
  iguana::from_yaml(books, str);
  CHECK(!books[0].title);
  CHECK(books[0].categories[0] == "computer science");
  CHECK(books[0].categories[1] == "programming");
  CHECK(*books[1].title == "The Great Gatsby");
  CHECK(books[1].categories[0] == "classic literature");
  CHECK(books[1].categories[1] == "fiction");
}

TEST_CASE("test example libraries") {
  std::string str = R"(
libraries:
  - name: 
      Central Library
    location: "Main\tStreet"#this is a comment
    books:
      - title:
        categories: 
          - computer science
          - programming
      - title: The Great Gatsby
        categories:
          - classic literature
          - fiction
  - name: North Library
    location: "Elm Avenue"
    books:
      - title: 
        categories:
          - computer science
          - algorithms
      - title: 
        categories:
          - classic literature
          - romance
  )";
  auto validator = [](const library_example_t &libs) {
    auto &lib0 = libs.libraries[0];
    CHECK(*lib0.name == "Central Library");
    CHECK(lib0.location == "Main\tStreet");
    CHECK(!lib0.books[0]->title);
    CHECK(lib0.books[0]->categories[0] == "computer science");
    CHECK(lib0.books[0]->categories[1] == "programming");
    CHECK(*lib0.books[1]->title == "The Great Gatsby");
    CHECK(lib0.books[1]->categories[0] == "classic literature");
    CHECK(lib0.books[1]->categories[1] == "fiction");
    auto &lib1 = libs.libraries[1];
    CHECK(*lib1.name == "North Library");
    CHECK(lib1.location == "Elm Avenue");
    CHECK(!lib1.books[0]->title);
    CHECK(lib1.books[0]->categories[0] == "computer science");
    CHECK(lib1.books[0]->categories[1] == "algorithms");
    CHECK(!lib1.books[1]->title);
    CHECK(lib1.books[1]->categories[0] == "classic literature");
    CHECK(lib1.books[1]->categories[1] == "romance");
  };
  library_example_t libs;
  iguana::from_yaml(libs, str);
  validator(libs);

  std::string ss;
  iguana::to_yaml(libs, ss);
  library_example_t libs1;
  iguana::from_yaml(libs1, ss);
  validator(libs1);
}

struct movie_t {
  std::string title;
  std::optional<int> year;
  std::vector<std::string> actors;
};
YLT_REFL(movie_t, title, year, actors);
TEST_CASE("test_tuple_example") {
  std::string str = R"(
# this is a movie
  - title: The Shawshank Redemption
    year: 
    actors:
      - Tim Robbins
      - Freeman
   
  - # this is a number array
    - 1998
    - 2005
    - 3007
  - Pulp Fiction
  )";
  using TupleType =
      std::tuple<std::unique_ptr<movie_t>, std::vector<int>, std::string>;
  auto validator = [](TupleType &t) {
    auto &movie = std::get<0>(t);
    CHECK(movie->title == "The Shawshank Redemption");
    CHECK(!movie->year);
    CHECK(movie->actors[0] == "Tim Robbins");
    CHECK(movie->actors[1] == "Freeman");
    auto arr = std::get<1>(t);
    CHECK(arr[0] == 1998);
    CHECK(arr[1] == 2005);
    CHECK(arr[2] == 3007);
    CHECK(std::get<2>(t) == "Pulp Fiction");
  };
  TupleType tuple1;
  iguana::from_yaml(tuple1, str);
  validator(tuple1);

  std::string ss;
  iguana::to_yaml(tuple1, ss);
  TupleType tuple2;
  iguana::from_yaml(tuple2, ss);
  validator(tuple2);
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
---
a: APPLE
b: BANANA
c: ORANGE
d: MANGO
e: CHERRY
f: GRAPE
g: 10
h: 15
  )";
  test_enum_t e;
  iguana::from_yaml(e, str);
  validator(e);

  std::string ss;
  iguana::to_yaml(e, ss);
  std::cout << ss << std::endl;
  test_enum_t e1;
  iguana::from_yaml(e1, ss);
  validator(e1);
}
#endif

enum class State { STOP = 10, START };
namespace iguana {
template <>
struct enum_value<State> {
  constexpr static std::array<int, 1> value = {10};
};
}  // namespace iguana

struct enum_exception_t {
  State a;
  State b;
};
YLT_REFL(enum_exception_t, a, b);

#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)

TEST_CASE("enum exception") {
  std::string str = R"(
a: START
b: STOP
  )";
  {
    enum_exception_t e;
    CHECK_THROWS(iguana::from_yaml(e, str));
  }
  {
    enum_exception_t e;
    std::string ss;
    e.a = State::START;
    e.b = State::STOP;
    CHECK_THROWS(iguana::to_yaml(e, ss));
  }
}

#endif

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
  iguana::to_yaml(obj, str);
  std::cout << str << "\n";

  some_object obj1;
  iguana::from_yaml(obj1, str);
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
vec:
  - 42
  - 21
vec_s:
  - 15
  - 16
b: test
  )";
  auto validator = [](Contents_t &cont) {
    CHECK(cont.b == "test");
    CHECK(*(*cont.vec)[0] == 42);
    CHECK(*(*cont.vec)[1] == 21);
    CHECK(*(*cont.vec_s)[0] == 15);
    CHECK(*(*cont.vec_s)[1] == 16);
  };

  Contents_t cont;
  iguana::from_yaml(cont, str);
  validator(cont);

  {
    std::string ss;
    iguana::to_yaml(cont, ss);
    Contents_t cont1;
    iguana::from_yaml(cont1, ss);
    validator(cont1);
  }
  {
    cont.b = "\n老";
    std::string ss;
    iguana::to_yaml<true>(cont, ss);
    std::cout << ss << std::endl;
    Contents_t cont1;
    iguana::from_yaml(cont1, ss);
    CHECK(cont1.b == cont.b);
  }
}

struct Empties_t {
  std::string empty_empty;
  std::string empty_tilde;
  std::string empty_null;
  std::string tilde_word;
  std::string null_word;
};
YLT_REFL(Empties_t, empty_empty, empty_tilde, empty_null, tilde_word,
         null_word);

TEST_CASE("test empty string") {
  std::string str = R"(
    empty_empty:
    empty_tilde: ~
    empty_null: null
    tilde_word: "~"
    null_word: "null"
  )";
  auto validator = [](Empties_t &cont) {
    CHECK(cont.empty_empty == "");
    CHECK(cont.empty_tilde == "");
    CHECK(cont.empty_null == "");
  };

  Empties_t cont;
  iguana::from_yaml(cont, str);
  validator(cont);
  CHECK(cont.tilde_word == "~");
  CHECK(cont.null_word == "null");

  {
    std::string ss;
    iguana::to_yaml(cont, ss);
    Empties_t cont1;
    iguana::from_yaml(cont1, ss);
    validator(cont1);
  }
  {
    cont.empty_empty = "";
    cont.empty_tilde = "";
    cont.empty_null = "";
    std::string ss;
    iguana::to_yaml<true>(cont, ss);
    std::cout << ss << std::endl;
    Empties_t cont1;
    iguana::from_yaml(cont1, ss);
    CHECK(cont1.empty_empty == cont.empty_empty);
    CHECK(cont1.empty_tilde == cont.empty_tilde);
    CHECK(cont1.empty_null == cont.empty_null);
    CHECK(cont1.tilde_word == cont.tilde_word);
    CHECK(cont1.null_word == cont.null_word);
  }
}

struct EmptyList_t {
  std::vector<int> empty_vector;
  std::vector<int> second_value;
};
YLT_REFL(EmptyList_t, empty_vector, second_value);

TEST_CASE("test empty vector") {
  std::string str = R"(
    empty_vector: []
    second_value: []
  )";
  auto validator = [](EmptyList_t &cont) {
    CHECK(cont.empty_vector.empty());
  };

  EmptyList_t cont;
  iguana::from_yaml(cont, str);
  validator(cont);

  {
    std::string ss;
    iguana::to_yaml(cont, ss);
    EmptyList_t cont1;
    iguana::from_yaml(cont1, ss);
    validator(cont1);
  }
  {
    cont.empty_vector = {};
    cont.second_value = {};
    std::string ss;
    iguana::to_yaml<true>(cont, ss);
    EmptyList_t cont1;
    iguana::from_yaml(cont1, ss);
    CHECK(cont1.empty_vector == cont.empty_vector);
  }
}

struct SSLConfig {
  std::string cert_file = "certs/server.crt??";
  std::string key_file = "certs/server.key??";
  std::string ca_file = "certs/ca.crt??";
};
YLT_REFL(SSLConfig, cert_file, key_file, ca_file);

TEST_CASE("test string with default value") {
  SSLConfig cfg;
  std::string buf = R"(
    cert_file: "certs/server.crt"
    key_file: "certs/server.key"
    ca_file: "certs/ca.crt"
  )";

  iguana::from_yaml(cfg, buf);
  CHECK(cfg.cert_file == "certs/server.crt");
  CHECK(cfg.key_file == "certs/server.key");
  CHECK(cfg.ca_file == "certs/ca.crt");
  std::cout << cfg.cert_file << std::endl;
}

// doctest comments
// 'function' : must be 'attribute' - see issue #182
DOCTEST_MSVC_SUPPRESS_WARNING_WITH_PUSH(4007)
int main(int argc, char **argv) { return doctest::Context(argc, argv).run(); }
DOCTEST_MSVC_SUPPRESS_WARNING_POP
