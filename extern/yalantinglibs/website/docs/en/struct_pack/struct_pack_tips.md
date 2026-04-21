# struct_pack Tips
This document introcude some tips about struct_pack
## requirements

struct_pack need compiler at least support C++17. We have test it on GCC/Clang/MSVC.

| compiler      | tested min version    | 
| ----------- | ------------------   |
| GCC         | 9.1                               |
| Clang/Apple Clang   | 6.0                        |
| MSVC        | Visual Studio 2019 (MSVC19.20)    |

struct_pack is cross-platform. struct_pack can work in both big-endian and little-endian. struct_pack support 32bit arch or 64bit arch. 

When C++20 standard was enabled, struct_pack has better performance by support memcpy user-defined trivial continuous container. In C++17, struct_pack only support memcpy container `std::array<T>`, `std::vector<T>`, `std::basic_string<T>` and `std::basic_string_view<T>`.

## endian

struct_pack will save the data as little-endian even if the arch is big-endian. The trivial-copy optimize and zero-copy optimize will be disable when the object is width than 1 byte. so struct_pack performance in little-endian is better than big-endian.

## macro
| macro      | description |
| ----------- | ------------------ |
| STRUCT_PACK_OPTIMIZE               | Allow more extremed loop unrolling to get better performance, but it cost more compile time.    |
| STRUCT_PACK_ENABLE_UNPORTABLE_TYPE | Enable serialize unportable type, such as wchar_t/std::wstring/std::bitset. Deserialize them in other platform maybe an undefined bevaior. |
| STRUCT_PACK_ENABLE_INT128 | Enable serialize __int128 and __uint128. Not all compiler support it. |
| STRUCT_PACK_DISABLE_OUTER_BRACE | Disable outer brace in type info format, It means that `std::tuple<int>`,`std::tuple<int,std::string>` & `int` could be same type. It could be useful to update a non-structure type to a structure type with compatible field. |
## How to speed up serialization/deserialization
1. use string_view instead of string, use span instead of vector/array.
2. move trivial field to a standlone struct, so that struct_pack could copy it faster. 
3. use std::vector instead of std::list.
4. define macro `STRUCT_PACK_OPTIMIZE`.
## Type Check
struct_pack will generate a name for the serialized type during compilation, and get a 32-bit MD5 based on the string, then take its upper 31 bits for type check. When deserializing, it will check whether the hash code stored is the same as the type to be deserialized. 
In order to alleviate possible hash collisions, in debug mode, struct_pack will store the complete type name instead of hash code. Therefore, the binary size in debug mode is slightly larger than release. 
## serialization config
struct_pack allows user to configure the content of the metadata via `struct_pack::sp_config`, which currently has the following settings:
| enum value      | description |
| ----------- | ------------------ |
| DEFAULT               |default value          |
| DISABLE_TYPE_INFO  | Prohibit storing full type information in serialized data, even in DEBUG mode|
| ENABLE_TYPE_INFO | Prohibit storing full type information in serialized data, even in DEBUG mode|
| DISABLE_META_INFO| Force full type information to be stored in serialized data, even if not in DEBUG mode|
| ENCODING_WITH_VARINT| encode integer(int32_t,int64_t,uint32_t,uint64_t) as variable length coding|
| USE_FAST_VARINT| encode integer(int32_t,int64_t,uint32_t,uint64_t) as fast variable length coding|

Note that when serialization is configured with the DISABLE_META_INFO option, it must be ensured that deserialization also uses this option, otherwise the behavior is undefined and the probability is that deserialization will fail.

Also, note that if structure A nests structure B, the configuration for A will not take effect for B.

For example：
```cpp
struct rect {
  int a, b, c, d;
};
```

## global config

`sp_config::default` is default global config, you can also use other config value by following codes:

```cpp
namespace struct_pack {
  //default global config
  constexpr sp_config set_default(sp_config*){ return sp_config::DISABLE_ALL_META_INFO; }
}
static_assert(struct_pack::get_needed_size(rect{}).size()==16);
```

You can have different config value in different code unit(*.cpp file).

### config by class static member

This config takes precedence over global config.

Just add a constexpr static member named `struct_pack_config` to the class

```cpp
struct rect {
  int a, b, c, d;
  constexpr static auto struct_pack_config = struct_pack::DISABLE_ALL_META_INFO;
};
```

### config by ADL

ADL configuration takes precedence over class static member.

we can declare a function `set_sp_config` in the same namespace of type `rect`: 

```cpp
inline constexpr struct_pack::sp_config set_sp_config(rect*) {
  return struct_pack::DISABLE_ALL_META_INFO;
}
```

### config by template parameters

This method has the highest priority and requires the configuration to be passed in as a template parameter each time it is serialised.

Note that the `USE_FAST_VARINT` and `ENCODING_WITH_VARINT` configurations are ineffective here.

```cpp
auto buffer = struct_pack::serialize<struct_pack::DISABLE_ALL_META_INFO>(rect{});
auto result = struct_pack::deserialize<struct_pack::DISABLE_ALL_META_INFO,rect>(buffer);
```

Currently we do not allow the `DISABLE_ALL_META_INFO` configuration to be enabled with a compatible field.

## Type requires

1. The type to serialize should be legal struct_pack type.。See document：[struct_pack type system](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_type_system.html)。
2. struct_pack support update protocol by add struct_pack::compatible field, which is forward backward compatibility. User should make sure the version number is increment for each update. It's not allow to delete/modify exist field. See : [document](https://alibaba.github.io/yalantinglibs/en/struct_pack/struct_pack_type_system.html#%E5%85%BC%E5%AE%B9%E7%B1%BB%E5%9E%8B)
3. Using the `YLT_REFL` macro, a maximum of 256(in msvc 124) fields are supported by default. Without using macros, the number of struct members should not exceed 256.

## How to Extend the Limit on Struct Fields

Some users might need to handle structs with hundreds of fields, which may exceed the supported range of yalantinglibs. Due to compile-time and compiler constraints, the number of fields supported by default is limited. Below, we introduce how to modify the source code to increase the field limit.

### Without Using Macros

The default limit is 256 fields. If you need to extend this limit, please modify the file `member_macro.hpp`. At the top of this header file, there is a python script; you can change the number 256 in the code to the number of fields you need. Then run the script, and overwrite the `member_macro.hpp` with the generated C++ code.

### Using the `YLT_REFL` Macro

The default limit is 256 fields (124 fields for MSVC by default). The following issues need to be noted:

#### Compiler Constraints

If you are using the MSVC compiler, you need to use the parameter `/Zc:preprocessor` instead of `/Zc:preprocessor-`. Otherwise, we can only support up to 124 fields for reflection under MSVC.

#### Modifying the `arg_list_macro_gen.hpp` File

At the top of this header file, there is a python script; you can change the field limit in the script to the number of fields you need. Then run the script, and overwrite the `arg_list_macro_gen.hpp` with the generated code.
