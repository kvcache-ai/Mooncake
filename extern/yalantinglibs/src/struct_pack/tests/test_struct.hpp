#include <array>
#include <bitset>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <stack>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <ylt/struct_pack.hpp>
#include <ylt/util/expected.hpp>

struct rect {
  int a, b, c, d;
  auto operator==(const rect &rhs) const {
    return a == rhs.a && b == rhs.b && c == rhs.c && d == rhs.d;
  }
};

struct person {
  int age;
  std::string name;
  auto operator==(const person &rhs) const {
    return age == rhs.age && name == rhs.name;
  }
  bool operator<(person const &p) const {
    return age < p.age || (age == p.age && name < p.name);
  }
};

struct person1 {
  int age;
  std::string name;
  struct_pack::compatible<int32_t> id;
  struct_pack::compatible<bool> maybe;
  auto operator==(const person1 &rhs) const {
    return age == rhs.age && name == rhs.name && id == rhs.id &&
           maybe == rhs.maybe;
  }
};

struct empty {};

enum class Color { red, black, white };

struct trivial_one {
  int a;
  double b;
  float c;
  auto operator==(const trivial_one &rhs) const {
    return a == rhs.a && b == rhs.b && c == rhs.c;
  }
};

struct complicated_object {
  Color color;
  int a;
  std::string b;
  std::vector<person> c;
  std::list<std::string> d;
  std::deque<int> e;
  std::map<int, person> f;
  std::multimap<int, person> g;
  std::set<std::string> h;
  std::multiset<int> i;
  std::unordered_map<int, person> j;
  std::unordered_multimap<int, int> k;
  std::array<person, 2> m;
  person n[2];
  std::pair<std::string, person> o;
  std::vector<std::array<trivial_one, 2>> p;
  bool operator==(const complicated_object &o) const {
    return color == o.color && a == o.a && b == o.b && c == o.c && d == o.d &&
           e == o.e && f == o.f && g == o.g && h == o.h && i == o.i &&
           j == o.j && k == o.k && m == o.m && n[0] == o.n[0] &&
           n[1] == o.n[1] && this->o == o.o && p == o.p;
  }
};

inline complicated_object create_complicated_object() {
  return complicated_object{
      Color::red,
      42,
      "hello",
      {{20, "tom"}, {22, "jerry"}},
      {"hello", "world"},
      {1, 2},
      {{1, {20, "tom"}}},
      {{1, {20, "tom"}}, {1, {22, "jerry"}}},
      {"aa", "bb"},
      {1, 2},
      {{1, {20, "tom"}}},
      {{1, 2}},
      {person{20, "tom"}, {22, "jerry"}},
      {person{15, "tom"}, {31, "jerry"}},
      std::make_pair("aa", person{20, "tom"}),
      {{trivial_one{1232114, 1.7, 2.4}, trivial_one{12315, 1.4, 2.6}},
       {trivial_one{4, 0.7, 1.4}, trivial_one{1123115, 11111.4, 2213321.6}}}};
}

struct nested_object {
  int id;
  std::string name;
  person p;
  complicated_object o;
  bool operator==(const nested_object &O) const {
    return id == O.id && name == O.name && p == O.p && o == O.o;
  }
};

struct my_list {
  std::unique_ptr<my_list> next;
  std::string name;
  int value;
  friend bool operator==(const my_list &a, const my_list &b) {
    return a.value == b.value && a.name == b.name &&
           ((a.next == nullptr && b.next == nullptr) ||
            ((a.next != nullptr && b.next != nullptr) && *a.next == *b.next));
  }
};
struct my_list2 {
  struct my_list {
    std::unique_ptr<my_list2> next;
    std::string name;
    int value;
  };
  my_list list;
  friend bool operator==(const my_list2 &a, const my_list2 &b) {
    return a.list.value == b.list.value && a.list.name == b.list.name &&
           ((a.list.next == nullptr && b.list.next == nullptr) ||
            ((a.list.next != nullptr && b.list.next != nullptr) &&
             *a.list.next == *b.list.next));
  }
};

struct my_tree {
  std::unique_ptr<my_tree> left_child;
  std::unique_ptr<my_tree> right_child;
  std::string name;
  int value;
  friend bool operator==(const my_tree &a, const my_tree &b) {
    return a.value == b.value && a.name == b.name &&
           ((a.left_child == nullptr && b.left_child == nullptr) ||
            ((a.left_child != nullptr && b.left_child != nullptr) &&
             *a.left_child == *b.left_child)) &&
           ((a.right_child == nullptr && b.right_child == nullptr) ||
            ((a.right_child != nullptr && b.right_child != nullptr) &&
             *a.right_child == *b.right_child));
  }
};

struct person_with_refl {
  std::string name;
  int age;
};
YLT_REFL(person_with_refl, name, age);