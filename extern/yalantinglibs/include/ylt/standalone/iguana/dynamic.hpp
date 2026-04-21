#pragma once
#include "common.hpp"

namespace iguana {
using base = detail::base;

constexpr inline uint8_t ENABLE_JSON = 0x01;
constexpr inline uint8_t ENABLE_YAML = 0x02;
constexpr inline uint8_t ENABLE_XML = 0x04;
constexpr inline uint8_t ENABLE_PB = 0x08;
constexpr inline uint8_t ENABLE_ALL = 0x0F;

template <typename T, uint8_t ENABLE_FLAG = ENABLE_PB>
struct base_impl : public base {
  base_impl() { [[maybe_unused]] static bool r = register_type<T>(); }
  void to_pb(std::string& str) const override {
    if constexpr ((ENABLE_FLAG & ENABLE_PB) != 0) {
      to_pb_adl((iguana_adl_t*)nullptr, *(static_cast<T const*>(this)), str);
    }
    else {
      throw std::runtime_error("Protobuf Disabled");
    }
  }

  void from_pb(std::string_view str) override {
    if constexpr ((ENABLE_FLAG & ENABLE_PB) != 0) {
      from_pb_adl((iguana_adl_t*)nullptr, *(static_cast<T*>(this)), str);
    }
    else {
      throw std::runtime_error("Protobuf Disabled");
    }
  }

  void to_json(std::string& str) const override {
    if constexpr ((ENABLE_FLAG & ENABLE_JSON) != 0) {
      to_json_adl((iguana_adl_t*)nullptr, *(static_cast<T const*>(this)), str);
    }
    else {
      throw std::runtime_error("Json Disabled");
    }
  }

  void from_json(std::string_view str) override {
    if constexpr ((ENABLE_FLAG & ENABLE_JSON) != 0) {
      from_json_adl((iguana_adl_t*)nullptr, *(static_cast<T*>(this)), str);
    }
    else {
      throw std::runtime_error("Json Disabled");
    }
  }

  void to_xml(std::string& str) const override {
    if constexpr ((ENABLE_FLAG & ENABLE_XML) != 0) {
      to_xml_adl((iguana_adl_t*)nullptr, *(static_cast<T const*>(this)), str);
    }
    else {
      throw std::runtime_error("Xml Disabled");
    }
  }

  void from_xml(std::string_view str) override {
    if constexpr ((ENABLE_FLAG & ENABLE_XML) != 0) {
      from_xml_adl((iguana_adl_t*)nullptr, *(static_cast<T*>(this)), str);
    }
    else {
      throw std::runtime_error("Xml Disabled");
    }
  }

  void to_yaml(std::string& str) const override {
    if constexpr ((ENABLE_FLAG & ENABLE_YAML) != 0) {
      to_yaml_adl((iguana_adl_t*)nullptr, *(static_cast<T const*>(this)), str);
    }
    else {
      throw std::runtime_error("Yaml Disabled");
    }
  }

  void from_yaml(std::string_view str) override {
    if constexpr ((ENABLE_FLAG & ENABLE_YAML) != 0) {
      from_yaml_adl((iguana_adl_t*)nullptr, *(static_cast<T*>(this)), str);
    }
    else {
      throw std::runtime_error("Yaml Disabled");
    }
  }

  iguana::detail::field_info get_field_info(
      std::string_view name) const override {
    static auto map =
        detail::get_members(ylt::reflection::internal::wrapper<T>::value);
    iguana::detail::field_info info{};
    for (auto const& [no, field] : map) {
      if (info.offset > 0) {
        break;
      }
      std::visit(
          [&](auto const& val) {
            if (val.field_name == name) {
              info.offset = val.offset;
              using value_type =
                  typename std::remove_reference_t<decltype(val)>::value_type;
#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ > 8)
              info.type_name = type_string<value_type>();
#endif
            }
          },
          field);
    }

    return info;
  }

  std::vector<std::string_view> get_fields_name() const override {
    static auto map =
        detail::get_members(ylt::reflection::internal::wrapper<T>::value);

    std::vector<std::string_view> vec;

    for (auto const& [no, val] : map) {
      std::visit(
          [&](auto const& field) {
            std::string_view const current_name{field.field_name.data(),
                                                field.field_name.size()};
            if (vec.empty() || (vec.back() != current_name)) {
              vec.push_back(current_name);
            }
          },
          val);
    }
    return vec;
  }

  std::any get_field_any(std::string_view name) const override {
    static auto map =
        detail::get_members(ylt::reflection::internal::wrapper<T>::value);
    std::any result;

    for (auto const& [no, field] : map) {
      if (result.has_value()) {
        break;
      }
      std::visit(
          [&](auto const& val) {
            if (val.field_name == name) {
              using value_type =
                  typename std::remove_reference_t<decltype(val)>::value_type;
              auto ptr = (char*)this + val.offset;
              result = *((value_type*)ptr);
            }
          },
          field);
    }

    return result;
  }

  virtual ~base_impl() {}

  mutable size_t cache_size = 0;

 private:
  virtual void dummy() {
    // make sure init T before main, and can register_type before main.
    [[maybe_unused]] static auto t =
        ylt::reflection::internal::wrapper<T>::value;
  }
};

IGUANA_INLINE std::shared_ptr<base> create_instance(std::string_view name) {
  auto it = iguana::detail::g_pb_map.find(name);
  if (it == iguana::detail::g_pb_map.end()) {
    throw std::invalid_argument(std::string(name) +
                                "not inheried from iguana::base_impl");
  }
  return it->second();
}
}  // namespace iguana