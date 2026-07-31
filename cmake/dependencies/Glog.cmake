include_guard(GLOBAL)

set(MOONCAKE_GLOG_VERSION "0.6.0")
set(MOONCAKE_GLOG_REVISION "v0.6.0")
set(MOONCAKE_GLOG_URL
    "https://codeload.github.com/google/glog/tar.gz/refs/tags/v0.6.0")
set(MOONCAKE_GLOG_SHA256
    "8a83bf982f37bb70825df71a9709fa90ea9f4447fb3c099e1d720a439d88bad6")

FetchContent_Declare(
  mooncake_glog
  URL "${MOONCAKE_GLOG_URL}"
  URL_HASH "SHA256=${MOONCAKE_GLOG_SHA256}")
FetchContent_GetProperties(mooncake_glog)
if(NOT mooncake_glog_POPULATED)
  FetchContent_Populate(mooncake_glog)
endif()

function(_mooncake_add_glog)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(BUILD_SHARED_LIBS OFF)
  set(BUILD_TESTING OFF)
  set(WITH_GFLAGS ON)
  set(gflags_proxy_dir "${mooncake_glog_BINARY_DIR}/gflags-package")
  file(MAKE_DIRECTORY "${gflags_proxy_dir}")
  file(
    WRITE "${gflags_proxy_dir}/gflags-config.cmake"
    "if(NOT TARGET gflags::gflags)\n  set(gflags_FOUND FALSE)\n  return()\nendif()\nset(gflags_FOUND TRUE)\nset(gflags_VERSION \"${MOONCAKE_GFLAGS_VERSION}\")\nset(gflags_NAMESPACE gflags)\n"
  )
  file(
    WRITE "${gflags_proxy_dir}/gflags-config-version.cmake"
    "set(PACKAGE_VERSION \"${MOONCAKE_GFLAGS_VERSION}\")\nif(PACKAGE_FIND_VERSION VERSION_GREATER PACKAGE_VERSION)\n  set(PACKAGE_VERSION_COMPATIBLE FALSE)\nelse()\n  set(PACKAGE_VERSION_COMPATIBLE TRUE)\n  if(PACKAGE_FIND_VERSION VERSION_EQUAL PACKAGE_VERSION)\n    set(PACKAGE_VERSION_EXACT TRUE)\n  endif()\nendif()\n"
  )
  set(gflags_DIR "${gflags_proxy_dir}")
  set(WITH_GTEST OFF)
  set(WITH_GMOCK OFF)
  set(WITH_PKGCONFIG OFF)
  set(WITH_UNWIND OFF)
  add_subdirectory("${mooncake_glog_SOURCE_DIR}" "${mooncake_glog_BINARY_DIR}"
                   EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_glog()

add_library(Mooncake::Glog ALIAS glog)
mooncake_configure_bundled_target(glog)
mooncake_configure_bundled_target(glogbase)

mooncake_register_dependency(
  NAME
  glog
  VERSION
  "${MOONCAKE_GLOG_VERSION}"
  REVISION
  "${MOONCAKE_GLOG_REVISION}"
  URL
  "${MOONCAKE_GLOG_URL}"
  SHA256
  "${MOONCAKE_GLOG_SHA256}"
  SOURCE_DIR
  "${mooncake_glog_SOURCE_DIR}"
  LICENSE_FILES
  COPYING)
