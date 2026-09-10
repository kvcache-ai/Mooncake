include_guard(GLOBAL)

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

set(YLT_ENABLE_IBV ON CACHE BOOL "Enable yalantinglibs ibverbs support")

set(YLT_COMMIT 7801bc9ad9021781f15217552214e325a1cf7373)
FetchContent_Declare(
  yalantinglibs
  URL ${GH_MIRROR}https://github.com/alibaba/yalantinglibs/archive/${YLT_COMMIT}.tar.gz
  URL_HASH SHA256=2a4b93c256c09fa84e1507bf4d5b33571ee47a4e2c65316ca255680ad3cdfc1e
)

# Exclude from install for CMake before 3.28
FetchContent_GetProperties(yalantinglibs)
FetchContent_Populate(yalantinglibs)
add_subdirectory(
  ${yalantinglibs_SOURCE_DIR}
  ${yalantinglibs_BINARY_DIR}
  EXCLUDE_FROM_ALL
)

# Suppress warnings for CMake before 3.25
get_target_property(YLT_INCL_DIRS yalantinglibs INTERFACE_INCLUDE_DIRECTORIES)
set_target_properties(yalantinglibs PROPERTIES
  INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${YLT_INCL_DIRS}")
