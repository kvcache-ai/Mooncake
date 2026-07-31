include_guard(GLOBAL)

set(MOONCAKE_PYBIND11_REVISION "58c382a8e3d7081364d2f5c62e7f429f0412743b")
set(MOONCAKE_PYBIND11_VERSION "revision-${MOONCAKE_PYBIND11_REVISION}")
set(MOONCAKE_PYBIND11_URL
    "https://codeload.github.com/pybind/pybind11/tar.gz/${MOONCAKE_PYBIND11_REVISION}"
)
set(MOONCAKE_PYBIND11_SHA256
    "12cd8028f7ef8f3a58c7c770e4a6c6f82c465e33ff4928aaefc41371ee24bd46")

FetchContent_Declare(
  mooncake_pybind11
  URL "${MOONCAKE_PYBIND11_URL}"
  URL_HASH "SHA256=${MOONCAKE_PYBIND11_SHA256}")
FetchContent_GetProperties(mooncake_pybind11)
if(NOT mooncake_pybind11_POPULATED)
  FetchContent_Populate(mooncake_pybind11)
endif()

function(_mooncake_add_pybind11)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(PYBIND11_INSTALL OFF)
  set(PYBIND11_TEST OFF)
  set(PYBIND11_FINDPYTHON ON)
  add_subdirectory("${mooncake_pybind11_SOURCE_DIR}"
                   "${mooncake_pybind11_BINARY_DIR}" EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_pybind11()

add_library(Mooncake::Pybind11 ALIAS pybind11_headers)
set(MOONCAKE_PYBIND11_INCLUDE_DIR
    "${mooncake_pybind11_SOURCE_DIR}/include"
    CACHE INTERNAL "Bundled pybind11 include directory")

mooncake_register_dependency(
  NAME
  pybind11
  VERSION
  "${MOONCAKE_PYBIND11_VERSION}"
  REVISION
  "${MOONCAKE_PYBIND11_REVISION}"
  URL
  "${MOONCAKE_PYBIND11_URL}"
  SHA256
  "${MOONCAKE_PYBIND11_SHA256}"
  SOURCE_DIR
  "${mooncake_pybind11_SOURCE_DIR}"
  LICENSE_FILES
  LICENSE)
