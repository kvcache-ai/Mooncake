include_guard(GLOBAL)

set(YLT_COMMIT 54f9c0642386e7860e8e11ada698c484dc562d7d)
set(_ylt_url
    "${GH_MIRROR}https://github.com/alibaba/yalantinglibs/archive/${YLT_COMMIT}.tar.gz"
)
set(_ylt_hash
    SHA256=11040b1d3f859599e5453ef7e9b5f84f8afa2388b057330f4c667973e5b20f01)

# Docker runs this file in script mode to prefetch the same verified archive.
if(CMAKE_SCRIPT_MODE_FILE)
  file(
    DOWNLOAD "${_ylt_url}" "${YLT_ARCHIVE}"
    EXPECTED_HASH "${_ylt_hash}"
    TLS_VERIFY ON
    INACTIVITY_TIMEOUT 60
    TIMEOUT 600)
  return()
endif()

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

if(APPLE)
  # macOS has no usable ibverbs; build yalantinglibs RPC over TCP only.
  set(YLT_ENABLE_IBV
      OFF
      CACHE BOOL "Enable yalantinglibs ibverbs support")
elseif(USE_SHCA)
  # SHCA redefines IB LID as u17, which stock yalanting ib_socket.hpp cannot
  # compile against. Control-plane RPC stays on TCP here; data-plane RDMA goes
  # through Transfer Engine / SHCA, not yalanting ibverbs.
  set(YLT_ENABLE_IBV
      OFF
      CACHE BOOL "Enable yalantinglibs ibverbs support" FORCE)
  message(STATUS "USE_SHCA=ON: forcing YLT_ENABLE_IBV=OFF")
else()
  set(YLT_ENABLE_IBV
      ON
      CACHE BOOL "Enable yalantinglibs ibverbs support")
endif()

if(YLT_ARCHIVE)
  set(_ylt_url "${YLT_ARCHIVE}")
endif()
FetchContent_Declare(
  yalantinglibs
  URL ${_ylt_url}
  URL_HASH "${_ylt_hash}")

# Exclude from install for CMake before 3.28
FetchContent_GetProperties(yalantinglibs)
FetchContent_Populate(yalantinglibs)
add_subdirectory(${yalantinglibs_SOURCE_DIR} ${yalantinglibs_BINARY_DIR}
                 EXCLUDE_FROM_ALL)

# Suppress warnings for CMake before 3.25
get_target_property(YLT_INCL_DIRS yalantinglibs INTERFACE_INCLUDE_DIRECTORIES)
set_target_properties(
  yalantinglibs PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES
                           "${YLT_INCL_DIRS}")
