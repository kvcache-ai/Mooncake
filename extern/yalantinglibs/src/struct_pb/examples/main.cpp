#include <cassert>
#include <iostream>
#include <ylt/struct_pb.hpp>

struct my_struct : struct_pb::base_impl<my_struct> {
  my_struct() = default;
  my_struct(int a, bool b, struct_pb::fixed64_t c) : x(a), y(b), z(c) {}
  int x;
  bool y;
  struct_pb::fixed64_t z;
};
YLT_REFL(my_struct, x, y, z);

struct nest : struct_pb::base_impl<nest> {
  nest() = default;
  nest(std::string s, my_struct t, int v)
      : name(std::move(s)), value(t), var(v) {}
  std::string name;
  my_struct value;
  int var;
  std::variant<int, double> mv;
};
YLT_REFL(nest, name, value, var, mv);

struct person {
  int id;
  std::string name;
  int age;
};
YLT_REFL(person, id, name, age);

int main() {
  nest v{"Hi", my_struct{1, false, {3}}, 5};
  std::string s;
  struct_pb::to_pb(v, s);

  nest v2;
  struct_pb::from_pb(v2, s);
  assert(v.var == v2.var);
  assert(v.value.y == v2.value.y);
  assert(v.value.z == v2.value.z);

  person p{1, "tom", 22};
  std::string str;
  struct_pb::to_pb(p, str);

  person p1;
  struct_pb::from_pb(p1, str);
  assert(p.age == p1.age);
  assert(p.name == p1.name);
  assert(p.id == p1.id);

  // dynamic reflection
  auto t = struct_pb::create_instance("nest");
  auto names = t->get_fields_name();
  bool r =
      (names == std::vector<std::string_view>{"name", "value", "var", "mv"});
  assert(r);

  t->set_field_value("mv", std::variant<int, double>{1});
  auto mv = t->get_field_value<std::variant<int, double>>("mv");
  auto const temp = std::variant<int, double>{1};
  assert(mv == temp);

  t->set_field_value("name", std::string("tom"));
  auto name = t->get_field_value<std::string>("name");
  assert(name == "tom");

  auto d = dynamic_cast<nest*>(t.get());
  assert(d->name == "tom");

  t->set_field_value<std::string>("name", "hello");
  auto& field_name = t->get_field_value<std::string>("name");
  assert(field_name == "hello");

  // dynamic any
  auto const& any_name = t->get_field_any("name");
  assert(std::any_cast<std::string>(any_name) == "hello");

  auto const& mvar_any = t->get_field_any("mv");
  auto const& mvar = std::any_cast<std::variant<int, double>>(mvar_any);
  assert(mvar == temp);
}