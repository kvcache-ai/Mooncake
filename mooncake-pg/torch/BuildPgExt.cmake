# BuildPgExt.cmake - Build the Mooncake PG Python extension.
#
# Invoked at build time via cmake -P from the root CMakeLists.txt when
# WITH_EP=ON.  Variables are passed with -D from the custom target:
#
#   SOURCE_DIR          - mooncake-pg/torch source directory
#   EP_CUDA_MAJOR       - CUDA major version (integer)
#   EP_CUDA_MINOR       - CUDA minor version (integer)
#   EP_TORCH_VERSIONS   - pipe-separated (|) PyTorch versions to build for
#                         (empty = use the currently-installed torch)
#   STAGING_DIR         - destination directory for the built .so files
#   PG_CORE_SO_PATH     - absolute path to the built libmooncake_pg.so
#   PG_DEVICE_SO_PATH   - absolute path to libmooncake_pg_device.so
#   EP_USE_MUSA         - set to "1" when building for MUSA (MTLink path)
#   EP_USE_MACA         - set to "1" when building for MACA (MTLink path)

cmake_minimum_required(VERSION 3.16)

# Include common build utilities.
include("${SOURCE_DIR}/../../mooncake-common/SetupPyTorchEnv.cmake")

# Restore pipe-separated strings back to CMake semicolon-separated lists.
if(EP_TORCH_VERSIONS)
  string(REPLACE "|" ";" EP_TORCH_VERSIONS "${EP_TORCH_VERSIONS}")
endif()

# ---------------------------------------------------------------------------
# 1. Set up the build environment.
# ---------------------------------------------------------------------------
# Clear jobserver variables so that sub-processes started by setup.py do not
# try to connect to the parent ninja's jobserver pipe FDs, which are not
# inherited and cause: "ninja: error: Could not initialize jobserver: Invalid
# file descriptors".
set(ENV{MAKEFLAGS} "")
set(ENV{MFLAGS} "")
if(NOT PG_CORE_SO_PATH OR NOT EXISTS "${PG_CORE_SO_PATH}")
  message(FATAL_ERROR
    "[PG] PG_CORE_SO_PATH is missing or does not exist: ${PG_CORE_SO_PATH}")
endif()
if(NOT PG_DEVICE_SO_PATH OR NOT EXISTS "${PG_DEVICE_SO_PATH}")
  message(FATAL_ERROR
    "[PG] PG_DEVICE_SO_PATH is missing or does not exist: ${PG_DEVICE_SO_PATH}")
endif()
set(ENV{MOONCAKE_PG_CORE_SO_PATH} "${PG_CORE_SO_PATH}")
if(EP_USE_MUSA)
  set(ENV{MOONCAKE_EP_USE_MUSA} "1")
else()
  unset(ENV{MOONCAKE_EP_USE_MUSA})
endif()
if(EP_USE_MACA)
  set(ENV{MOONCAKE_EP_USE_MACA} "1")
  if(DEFINED ENV{MACA_PATH})
    set(ENV{MACA_HOME} "$ENV{MACA_PATH}")
  elseif(DEFINED ENV{MACA_HOME})
    set(ENV{MACA_PATH} "$ENV{MACA_HOME}")
  endif()
else()
  unset(ENV{MOONCAKE_EP_USE_MACA})
endif()

# ---------------------------------------------------------------------------
# 2. Build the PG Python extension.
# ---------------------------------------------------------------------------
if("${EP_TORCH_VERSIONS}" STREQUAL "")
  message(STATUS "[PG] Building with currently-installed PyTorch")
  execute_process(
    COMMAND ${Python3_EXECUTABLE} setup.py build_ext --build-lib .
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE _ret
  )
  if(NOT _ret EQUAL 0)
    message(FATAL_ERROR "[PG] Extension build failed (exit code: ${_ret})")
  endif()
else()
  message(STATUS "[PG] Building for PyTorch versions: ${EP_TORCH_VERSIONS}")
  foreach(_version IN LISTS EP_TORCH_VERSIONS)
    install_pytorch_wheel("${_version}" "${EP_CUDA_MAJOR}" "${EP_CUDA_MINOR}" "[PG]")

    execute_process(
      COMMAND ${Python3_EXECUTABLE} setup.py build_ext --build-lib . --force
      WORKING_DIRECTORY "${SOURCE_DIR}"
      RESULT_VARIABLE _ret
    )
    if(NOT _ret EQUAL 0)
      message(FATAL_ERROR "[PG] Extension build failed for PyTorch ${_version}")
    endif()
  endforeach()
endif()

# ---------------------------------------------------------------------------
# 3. Stage only fatbin-bearing device and extension .so files. The host-only
#    core is packaged before auditwheel so its dependencies are repaired.
# ---------------------------------------------------------------------------
file(MAKE_DIRECTORY "${STAGING_DIR}")
file(GLOB _so_files "${SOURCE_DIR}/mooncake/*.so")
list(APPEND _so_files "${PG_DEVICE_SO_PATH}")
foreach(_so IN LISTS _so_files)
  get_filename_component(_fname "${_so}" NAME)
  message(STATUS "[PG] Staging ${_fname} -> ${STAGING_DIR}")
  file(COPY "${_so}" DESTINATION "${STAGING_DIR}" NO_SOURCE_PERMISSIONS)
endforeach()

message(STATUS "[PG] Mooncake PG extension build complete")
