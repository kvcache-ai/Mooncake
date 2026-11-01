include_guard(GLOBAL)

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

if(APPLE)
  # macOS has no usable ibverbs; build yalantinglibs RPC over TCP only.
  set(YLT_ENABLE_IBV
      OFF
      CACHE BOOL "Enable yalantinglibs ibverbs support")
else()
  set(YLT_ENABLE_IBV
      ON
      CACHE BOOL "Enable yalantinglibs ibverbs support")
endif()

set(YLT_COMMIT 45c6463c0afbcc46262e373effa30635be3846a0)
FetchContent_Declare(
  yalantinglibs
  URL ${GH_MIRROR}https://github.com/alibaba/yalantinglibs/archive/${YLT_COMMIT}.tar.gz
  URL_HASH
    SHA256=baa6b5c9ffcc0af8252742bba8f0fb9c6d37387c13b2ff5b26d29c6aaff6ee37)

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
