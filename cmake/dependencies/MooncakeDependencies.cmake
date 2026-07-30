# Copyright 2026 KVCache.AI
#
# Central provider for portable Mooncake dependencies. Business targets consume
# only Mooncake::* aliases backed by fixed in-repository sources.

include_guard(GLOBAL)

set(_MOONCAKE_DEPS_ROOT "${CMAKE_CURRENT_LIST_DIR}/../..")
get_filename_component(_MOONCAKE_DEPS_ROOT "${_MOONCAKE_DEPS_ROOT}" ABSOLUTE)

set(MOONCAKE_XXHASH_VERSION "0.8.3")
set(MOONCAKE_ZSTD_VERSION "1.5.7")
set(MOONCAKE_JSONCPP_VERSION "1.9.6")
set(MOONCAKE_YAML_CPP_VERSION "0.8.0")
set(MOONCAKE_ASIO_VERSION "1.30.2")

function(_mooncake_set_bundled_properties target)
  set_target_properties(
    ${target}
    PROPERTIES POSITION_INDEPENDENT_CODE ON
               C_VISIBILITY_PRESET hidden
               CXX_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN ON)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W0)
  else()
    target_compile_options(${target} PRIVATE -w)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
      # Header-only/template code is instantiated by consumers rather than by
      # the archive itself.  Keep those Asio/JsonCpp/yaml-cpp inline symbols out
      # of libmooncake_store.so's dynamic symbol table as well.
      target_compile_options(
        ${target}
        INTERFACE "$<$<COMPILE_LANGUAGE:CXX>:-fvisibility-inlines-hidden>")
    endif()
  endif()
endfunction()

function(_mooncake_require_vendored path dependency version)
  if(NOT EXISTS "${path}")
    message(
      FATAL_ERROR
        "Mooncake requires the vendored ${dependency} ${version} sources at ${path}"
    )
  endif()
endfunction()

find_package(Threads REQUIRED)

set(_xxhash_dir "${_MOONCAKE_DEPS_ROOT}/extern/xxhash")
set(_zstd_dir "${_MOONCAKE_DEPS_ROOT}/extern/zstd")
set(_jsoncpp_dir "${_MOONCAKE_DEPS_ROOT}/extern/jsoncpp")
set(_yaml_cpp_dir "${_MOONCAKE_DEPS_ROOT}/extern/yaml-cpp")
set(_asio_dir "${_MOONCAKE_DEPS_ROOT}/extern/asio")
set(MOONCAKE_JSONCPP_INCLUDE_DIRS "${_jsoncpp_dir}/include")
set(MOONCAKE_ASIO_INCLUDE_DIRS "${_asio_dir}/include")

_mooncake_require_vendored("${_xxhash_dir}/xxhash.c" xxHash
                           "${MOONCAKE_XXHASH_VERSION}")
_mooncake_require_vendored("${_zstd_dir}/lib/zstd.h" zstd
                           "${MOONCAKE_ZSTD_VERSION}")
_mooncake_require_vendored("${_jsoncpp_dir}/src/lib_json/json_value.cpp"
                           JsonCpp "${MOONCAKE_JSONCPP_VERSION}")
_mooncake_require_vendored("${_yaml_cpp_dir}/src/node.cpp" yaml-cpp
                           "${MOONCAKE_YAML_CPP_VERSION}")
_mooncake_require_vendored("${_asio_dir}/include/asio.hpp" "standalone Asio"
                           "${MOONCAKE_ASIO_VERSION}")

add_library(mooncake_xxhash STATIC EXCLUDE_FROM_ALL "${_xxhash_dir}/xxhash.c")
target_include_directories(mooncake_xxhash SYSTEM PUBLIC "${_xxhash_dir}")
_mooncake_set_bundled_properties(mooncake_xxhash)

file(
  GLOB
  _zstd_sources
  CONFIGURE_DEPENDS
  "${_zstd_dir}/lib/common/*.c"
  "${_zstd_dir}/lib/compress/*.c"
  "${_zstd_dir}/lib/decompress/*.c"
  "${_zstd_dir}/lib/dictBuilder/*.c")
add_library(mooncake_zstd STATIC EXCLUDE_FROM_ALL ${_zstd_sources})
# Keep the bundled target C-only for toolchains without CMake ASM support. zstd
# otherwise assumes its optional x86-64 assembly file is compiled too.
target_compile_definitions(mooncake_zstd PRIVATE ZSTD_DISABLE_ASM)
target_include_directories(mooncake_zstd SYSTEM PUBLIC "${_zstd_dir}/lib")
_mooncake_set_bundled_properties(mooncake_zstd)

add_library(
  mooncake_jsoncpp STATIC EXCLUDE_FROM_ALL
  "${_jsoncpp_dir}/src/lib_json/json_reader.cpp"
  "${_jsoncpp_dir}/src/lib_json/json_value.cpp"
  "${_jsoncpp_dir}/src/lib_json/json_writer.cpp")
target_include_directories(mooncake_jsoncpp SYSTEM
                           PUBLIC "${_jsoncpp_dir}/include")
_mooncake_set_bundled_properties(mooncake_jsoncpp)

file(GLOB _yaml_cpp_sources CONFIGURE_DEPENDS "${_yaml_cpp_dir}/src/*.cpp")
add_library(mooncake_yaml_cpp STATIC EXCLUDE_FROM_ALL ${_yaml_cpp_sources})
target_compile_definitions(mooncake_yaml_cpp PUBLIC YAML_CPP_STATIC_DEFINE)
target_include_directories(mooncake_yaml_cpp SYSTEM
                           PUBLIC "${_yaml_cpp_dir}/include")
_mooncake_set_bundled_properties(mooncake_yaml_cpp)

add_library(mooncake_asio STATIC EXCLUDE_FROM_ALL
            "${_MOONCAKE_DEPS_ROOT}/mooncake-common/src/asio_impl.cpp")
target_compile_definitions(mooncake_asio PUBLIC ASIO_SEPARATE_COMPILATION)
target_include_directories(mooncake_asio SYSTEM PUBLIC "${_asio_dir}/include")
target_link_libraries(mooncake_asio PUBLIC Threads::Threads)
_mooncake_set_bundled_properties(mooncake_asio)

add_library(Mooncake::xxhash ALIAS mooncake_xxhash)
add_library(Mooncake::zstd ALIAS mooncake_zstd)
add_library(Mooncake::jsoncpp ALIAS mooncake_jsoncpp)
add_library(Mooncake::yaml_cpp ALIAS mooncake_yaml_cpp)
add_library(Mooncake::asio ALIAS mooncake_asio)

# Compatibility targets keep existing standalone/in-tree CMake consumers
# source-compatible while the Mooncake::* names are the provider contract.
add_library(xxHash::xxhash ALIAS mooncake_xxhash)
add_library(zstd::libzstd_static ALIAS mooncake_zstd)
add_library(JsonCpp::JsonCpp ALIAS mooncake_jsoncpp)
add_library(yaml-cpp ALIAS mooncake_yaml_cpp)
# The implementation is intentionally static even though this historical target
# name said "shared".
add_library(asio_shared ALIAS mooncake_asio)

include("${CMAKE_CURRENT_LIST_DIR}/BundledGflagsGlog.cmake")
include("${CMAKE_CURRENT_LIST_DIR}/BundledOptionalDependencies.cmake")

message(
  STATUS
    "Mooncake bundled dependencies: xxHash ${MOONCAKE_XXHASH_VERSION}, zstd ${MOONCAKE_ZSTD_VERSION}, JsonCpp ${MOONCAKE_JSONCPP_VERSION}, yaml-cpp ${MOONCAKE_YAML_CPP_VERSION}, Asio ${MOONCAKE_ASIO_VERSION}, gflags ${MOONCAKE_GFLAGS_VERSION}, glog ${MOONCAKE_GLOG_VERSION}"
)
