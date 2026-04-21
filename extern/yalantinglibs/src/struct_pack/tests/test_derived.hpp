#pragma once
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "doctest.h"
#include "ylt/struct_pack.hpp"
namespace test2 {

struct base {
  virtual uint32_t get_struct_pack_id() const;
  virtual std::string get_name() const { return std::to_string(a); }
  int a = 1;
  virtual ~base(){};
};
YLT_REFL(base, a);

struct derived1 : public base {
  virtual uint32_t get_struct_pack_id() const;
  virtual std::string get_name() const {
    return base::get_name() + std::to_string(b);
  }
  int b = 2;
};

YLT_REFL(derived1, a, b);
struct derived2 : public base {
  virtual uint32_t get_struct_pack_id() const;
  virtual std::string get_name() const { return base::get_name() + c; }
  std::string c = "Hello";
};

YLT_REFL(derived2, a, c);
struct derived3 : public base {
  virtual uint32_t get_struct_pack_id() const;
  virtual std::string get_name() const { return base::get_name() + d; }
  std::string d = "Oh no";
  static constexpr std::size_t struct_pack_id = 0;
};
YLT_REFL(derived3, a, d);
struct derived4 : public derived3 {
  virtual uint32_t get_struct_pack_id() const;
  virtual std::string get_name() const { return derived3::get_name() + e; }
  std::string e = "!";
};

YLT_REFL(derived4, a, d, e);

STRUCT_PACK_DERIVED_IMPL(base, derived1, derived2, derived3, derived4);
}  // namespace test2
namespace test3 {

struct base {
  virtual uint32_t get_struct_pack_id() const = 0;
  virtual std::string get_name() const = 0;
  int a = 1;
  virtual ~base(){};
};

inline std::string base::get_name() const { return std::to_string(a); }

struct derived1 : public base {
  virtual uint32_t get_struct_pack_id() const;
  virtual std::string get_name() const {
    return base::get_name() + std::to_string(b);
  }
  int b = 2;
};

YLT_REFL(derived1, a, b);
struct derived2 : public base {
  virtual uint32_t get_struct_pack_id() const;
  virtual std::string get_name() const { return base::get_name() + c; }
  std::string c = "Hello";
};

YLT_REFL(derived2, a, c);
struct derived3 : public base {
  virtual uint32_t get_struct_pack_id() const;
  virtual std::string get_name() const { return base::get_name() + d; }
  std::string d = "Oh no";
  static constexpr std::size_t struct_pack_id = 0;
};

YLT_REFL(derived3, a, d);

struct derived4 : public derived3 {
  virtual uint32_t get_struct_pack_id() const;
  virtual std::string get_name() const { return derived3::get_name() + e; }
  std::string e = "!";
};

YLT_REFL(derived4, a, d, e);

STRUCT_PACK_DERIVED_IMPL(base, derived1, derived2, derived3, derived4);

}  // namespace test3