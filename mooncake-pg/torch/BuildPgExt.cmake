# BuildPgExt.cmake - Build the Mooncake PG Python extension.
#
# Invoked at build time via cmake -P from the root CMakeLists.txt when
# WITH_EP=ON.  Variables are passed with -D from the custom target:
#
# SOURCE_DIR: mooncake-pg/torch source directory. EP_CUDA_MAJOR: CUDA major
# version. EP_CUDA_MINOR: CUDA minor version. EP_TORCH_VERSIONS: pipe-separated
# PyTorch versions; empty uses installed Torch. STAGING_DIR: destination for the
# built shared libraries. BUILD_DIR: backend-owned directory for setuptools
# build output. PG_CORE_SO_PATH: absolute path to the built libmooncake_pg.so.
# PG_DEVICE_SO_PATH: absolute path to libmooncake_pg_device.so. EP_USE_MUSA: set
# to "1" for the MUSA/MTLink path. EP_USE_MACA: set to "1" for the MACA/MTLink
# path.

cmake_minimum_required(VERSION 3.16)

# Include common build utilities.
include("${SOURCE_DIR}/../../mooncake-common/SetupPyTorchEnv.cmake")

# Restore pipe-separated strings back to CMake semicolon-separated lists.
if(EP_TORCH_VERSIONS)
  string(REPLACE "|" ";" EP_TORCH_VERSIONS "${EP_TORCH_VERSIONS}")
endif()

# ---------------------------------------------------------------------------
# Set up the build environment.
# ---------------------------------------------------------------------------
# Clear jobserver variables so that sub-processes started by setup.py do not try
# to connect to the parent ninja's jobserver pipe FDs, which are not inherited
# and cause: "ninja: error: Could not initialize jobserver: Invalid file
# descriptors".
set(ENV{MAKEFLAGS} "")
set(ENV{MFLAGS} "")
if(NOT PG_CORE_SO_PATH OR NOT EXISTS "${PG_CORE_SO_PATH}")
  message(
    FATAL_ERROR
      "[PG] PG_CORE_SO_PATH is missing or does not exist: ${PG_CORE_SO_PATH}")
endif()
if(NOT PG_DEVICE_SO_PATH OR NOT EXISTS "${PG_DEVICE_SO_PATH}")
  message(
    FATAL_ERROR
      "[PG] PG_DEVICE_SO_PATH is missing or does not exist: ${PG_DEVICE_SO_PATH}"
  )
endif()
if(NOT BUILD_DIR)
  message(FATAL_ERROR "[PG] BUILD_DIR is required")
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
# Build the PG Python extension.
# ---------------------------------------------------------------------------
file(REMOVE_RECURSE "${BUILD_DIR}")
file(MAKE_DIRECTORY "${BUILD_DIR}" "${STAGING_DIR}")

function(build_pg_extension _build_key)
  set(_build_lib "${BUILD_DIR}/${_build_key}/lib")
  set(_build_temp "${BUILD_DIR}/${_build_key}/temp")
  file(MAKE_DIRECTORY "${_build_lib}" "${_build_temp}")

  execute_process(
    COMMAND ${Python3_EXECUTABLE} setup.py build_ext --build-lib "${_build_lib}"
            --build-temp "${_build_temp}" --force
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE _ret)
  if(NOT _ret EQUAL 0)
    message(FATAL_ERROR "[PG] Extension build failed for ${_build_key}")
  endif()

  file(GLOB _extension_files "${_build_lib}/mooncake/*.so")
  if(NOT _extension_files)
    message(
      FATAL_ERROR
        "[PG] Extension build produced no shared library in ${_build_lib}/mooncake"
    )
  endif()
  foreach(_extension IN LISTS _extension_files)
    get_filename_component(_fname "${_extension}" NAME)
    message(STATUS "[PG] Staging ${_fname} -> ${STAGING_DIR}")
    file(
      COPY "${_extension}"
      DESTINATION "${STAGING_DIR}"
      NO_SOURCE_PERMISSIONS)
  endforeach()
endfunction()

if("${EP_TORCH_VERSIONS}" STREQUAL "")
  message(STATUS "[PG] Building with currently-installed PyTorch")
  build_pg_extension("current")
else()
  message(STATUS "[PG] Building for PyTorch versions: ${EP_TORCH_VERSIONS}")
  foreach(_version IN LISTS EP_TORCH_VERSIONS)
    install_pytorch_wheel("${_version}" "${EP_CUDA_MAJOR}" "${EP_CUDA_MINOR}"
                          "[PG]")
    string(REPLACE "." "_" _build_key "torch_${_version}")
    build_pg_extension("${_build_key}")
  endforeach()
endif()

# ---------------------------------------------------------------------------
# Stage the device library. Torch ABI extensions were staged from their
# backend-owned build directories above. The host-only core is installed by
# CMake's python component.
# ---------------------------------------------------------------------------
get_filename_component(_device_name "${PG_DEVICE_SO_PATH}" NAME)
message(STATUS "[PG] Staging ${_device_name} -> ${STAGING_DIR}")
file(
  COPY "${PG_DEVICE_SO_PATH}"
  DESTINATION "${STAGING_DIR}"
  NO_SOURCE_PERMISSIONS)

message(STATUS "[PG] Mooncake PG extension build complete")
