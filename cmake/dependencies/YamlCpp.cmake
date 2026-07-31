include_guard(GLOBAL)

set(MOONCAKE_YAMLCPP_VERSION "0.8.0")
set(MOONCAKE_YAMLCPP_REVISION "0.8.0")
set(MOONCAKE_YAMLCPP_URL
    "https://codeload.github.com/jbeder/yaml-cpp/tar.gz/refs/tags/0.8.0")
set(MOONCAKE_YAMLCPP_SHA256
    "fbe74bbdcee21d656715688706da3c8becfd946d92cd44705cc6098bb23b3a16")

FetchContent_Declare(
  mooncake_yamlcpp
  URL "${MOONCAKE_YAMLCPP_URL}"
  URL_HASH "SHA256=${MOONCAKE_YAMLCPP_SHA256}")
FetchContent_GetProperties(mooncake_yamlcpp)
if(NOT mooncake_yamlcpp_POPULATED)
  FetchContent_Populate(mooncake_yamlcpp)
endif()

function(_mooncake_add_yamlcpp)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(BUILD_SHARED_LIBS OFF)
  set(YAML_BUILD_SHARED_LIBS
      OFF
      CACHE BOOL "" FORCE)
  set(YAML_CPP_BUILD_CONTRIB
      OFF
      CACHE BOOL "" FORCE)
  set(YAML_CPP_BUILD_TOOLS
      OFF
      CACHE BOOL "" FORCE)
  set(YAML_CPP_BUILD_TESTS
      OFF
      CACHE BOOL "" FORCE)
  set(YAML_CPP_INSTALL
      OFF
      CACHE BOOL "" FORCE)
  add_subdirectory("${mooncake_yamlcpp_SOURCE_DIR}"
                   "${mooncake_yamlcpp_BINARY_DIR}" EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_yamlcpp()

add_library(Mooncake::YamlCpp ALIAS yaml-cpp)
mooncake_configure_bundled_target(yaml-cpp)

mooncake_register_dependency(
  NAME
  yaml-cpp
  VERSION
  "${MOONCAKE_YAMLCPP_VERSION}"
  REVISION
  "${MOONCAKE_YAMLCPP_REVISION}"
  URL
  "${MOONCAKE_YAMLCPP_URL}"
  SHA256
  "${MOONCAKE_YAMLCPP_SHA256}"
  SOURCE_DIR
  "${mooncake_yamlcpp_SOURCE_DIR}"
  LICENSE_FILES
  LICENSE)
