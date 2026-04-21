#include <cassert>
#include <iostream>

#include "ylt/struct_yaml/yaml_reader.h"
#include "ylt/struct_yaml/yaml_writer.h"

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

std::ostream &operator<<(std::ostream &os, person_t p) {
  os << "name: " << p.name << "\tage: " << p.age << std::endl;
  os << p.address.street << "\n";
  os << p.address.city << "\n";
  os << p.address.state << "\n";
  os << p.address.country << "\n";
  os << p.contacts[0].type << " : " << p.contacts[0].value << "\n";
  os << p.contacts[1].type << " : " << p.contacts[1].value << "\n";
  os << p.contacts[2].type << " : " << p.contacts[2].value << "\n";
  return os;
}

void person_example() {
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
  std::cout << "========= deserialize person_t ========\n";
  std::cout << p;
  std::string ss;
  iguana::to_yaml(p, ss);
  std::cout << "========== serialize person_t =========\n";
  std::cout << ss;
  person_t p2;
  iguana::from_yaml(p2, ss);
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
  iguana::to_yaml(obj, str);
  std::cout << str << "\n";

  some_object obj1;
  iguana::from_yaml(obj1, str);
  assert(obj1.get_id() == 20);
  assert(obj1.get_name() == "tom");
}

int main() {
  person_example();
  test_inner_object();
}