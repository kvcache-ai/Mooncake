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

set(YLT_COMMIT 7801bc9ad9021781f15217552214e325a1cf7373)
FetchContent_Declare(
  yalantinglibs
  URL ${GH_MIRROR}https://github.com/alibaba/yalantinglibs/archive/${YLT_COMMIT}.tar.gz
  URL_HASH
    SHA256=2a4b93c256c09fa84e1507bf4d5b33571ee47a4e2c65316ca255680ad3cdfc1e)

# Exclude from install for CMake before 3.28
FetchContent_GetProperties(yalantinglibs)
FetchContent_Populate(yalantinglibs)

# Instance-level response limits and connection admission. Defaults preserve
# the stock ylt behavior for every caller that does not opt in.
set(_mooncake_ylt_patch_stamp
    "${yalantinglibs_SOURCE_DIR}/.mooncake_handshake_patch_applied")
if(NOT EXISTS "${_mooncake_ylt_patch_stamp}")
  execute_process(
    COMMAND patch -p1 --forward --batch
            --input=${CMAKE_CURRENT_LIST_DIR}/patches/ylt-handshake-limits.patch
    WORKING_DIRECTORY ${yalantinglibs_SOURCE_DIR}
    RESULT_VARIABLE _mooncake_ylt_patch_rc
    OUTPUT_VARIABLE _mooncake_ylt_patch_out
    ERROR_VARIABLE _mooncake_ylt_patch_err)
  if(NOT _mooncake_ylt_patch_rc EQUAL 0)
    message(
      FATAL_ERROR
        "Failed to apply ylt handshake patch (${_mooncake_ylt_patch_rc}): "
        "${_mooncake_ylt_patch_out}${_mooncake_ylt_patch_err}")
  endif()
  file(WRITE "${_mooncake_ylt_patch_stamp}" "applied\n")
endif()

add_subdirectory(${yalantinglibs_SOURCE_DIR} ${yalantinglibs_BINARY_DIR}
                 EXCLUDE_FROM_ALL)

# Suppress warnings for CMake before 3.25
get_target_property(YLT_INCL_DIRS yalantinglibs INTERFACE_INCLUDE_DIRECTORIES)
set_target_properties(
  yalantinglibs PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES
                           "${YLT_INCL_DIRS}")
