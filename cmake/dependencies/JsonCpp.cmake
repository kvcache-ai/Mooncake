include_guard(GLOBAL)

set(MOONCAKE_JSONCPP_VERSION "1.9.6")
set(MOONCAKE_JSONCPP_REVISION "1.9.6")
set(MOONCAKE_JSONCPP_URL
    "https://codeload.github.com/open-source-parsers/jsoncpp/tar.gz/refs/tags/1.9.6"
)
set(MOONCAKE_JSONCPP_SHA256
    "f93b6dd7ce796b13d02c108bc9f79812245a82e577581c4c9aabe57075c90ea2")

FetchContent_Declare(
  mooncake_jsoncpp
  URL "${MOONCAKE_JSONCPP_URL}"
  URL_HASH "SHA256=${MOONCAKE_JSONCPP_SHA256}")
FetchContent_GetProperties(mooncake_jsoncpp)
if(NOT mooncake_jsoncpp_POPULATED)
  FetchContent_Populate(mooncake_jsoncpp)
endif()

function(_mooncake_add_jsoncpp)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(BUILD_SHARED_LIBS OFF)
  set(BUILD_STATIC_LIBS ON)
  set(BUILD_OBJECT_LIBS OFF)
  set(JSONCPP_WITH_TESTS OFF)
  set(JSONCPP_WITH_POST_BUILD_UNITTEST OFF)
  set(JSONCPP_WITH_PKGCONFIG_SUPPORT OFF)
  set(JSONCPP_WITH_CMAKE_PACKAGE OFF)
  set(JSONCPP_WITH_EXAMPLE OFF)
  add_subdirectory("${mooncake_jsoncpp_SOURCE_DIR}"
                   "${mooncake_jsoncpp_BINARY_DIR}" EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_jsoncpp()

add_library(Mooncake::JsonCpp ALIAS jsoncpp_static)
add_library(JsonCpp::JsonCpp ALIAS jsoncpp_static)
add_library(jsoncpp ALIAS jsoncpp_static)
mooncake_configure_bundled_target(jsoncpp_static)

mooncake_register_dependency(
  NAME
  JsonCpp
  VERSION
  "${MOONCAKE_JSONCPP_VERSION}"
  REVISION
  "${MOONCAKE_JSONCPP_REVISION}"
  URL
  "${MOONCAKE_JSONCPP_URL}"
  SHA256
  "${MOONCAKE_JSONCPP_SHA256}"
  SOURCE_DIR
  "${mooncake_jsoncpp_SOURCE_DIR}"
  LICENSE_FILES
  LICENSE)
