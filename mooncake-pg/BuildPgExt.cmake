# BuildPgExt.cmake - Build the Mooncake PG Python extension.
#
# Invoked at build time via cmake -P from the root CMakeLists.txt when
# WITH_EP=ON.  Variables are passed with -D from the custom target:
#
#   SOURCE_DIR          - mooncake-pg source directory
#   EP_CUDA_MAJOR       - CUDA major version (integer)
#   EP_TORCH_VERSIONS   - pipe-separated (|) PyTorch versions to build for
#                         (empty = use the currently-installed torch)
#   TORCH_CUDA_ARCH_LIST - pipe-separated CUDA arch list forwarded to torch
#   STAGING_DIR         - destination directory for the built .so files

cmake_minimum_required(VERSION 3.16)

# Restore pipe-separated strings back to CMake semicolon-separated lists.
if(EP_TORCH_VERSIONS)
  string(REPLACE "|" ";" EP_TORCH_VERSIONS "${EP_TORCH_VERSIONS}")
endif()
if(TORCH_CUDA_ARCH_LIST)
  string(REPLACE "|" ";" TORCH_CUDA_ARCH_LIST "${TORCH_CUDA_ARCH_LIST}")
endif()

# ---------------------------------------------------------------------------
# 1. Forward TORCH_CUDA_ARCH_LIST to the extension build.
# ---------------------------------------------------------------------------
set(ENV{TORCH_CUDA_ARCH_LIST} "${TORCH_CUDA_ARCH_LIST}")

# ---------------------------------------------------------------------------
# 3. Build the PG Python extension.
# ---------------------------------------------------------------------------
if("${EP_TORCH_VERSIONS}" STREQUAL "")
  message(STATUS "[PG] Building with currently-installed PyTorch")
  execute_process(
    COMMAND python setup.py build_ext --build-lib .
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE _ret
  )
  if(NOT _ret EQUAL 0)
    message(FATAL_ERROR "[PG] Extension build failed (exit code: ${_ret})")
  endif()
else()
  message(STATUS "[PG] Building for PyTorch versions: ${EP_TORCH_VERSIONS}")
  foreach(_version IN LISTS EP_TORCH_VERSIONS)
    message(STATUS "[PG] Installing PyTorch ${_version}")
    if(EP_CUDA_MAJOR GREATER_EQUAL 13)
      # TODO: Fix when we need to support more CUDA 13 versions or when the CI
      #       env is fixed.
      execute_process(
        COMMAND pip install "torch==${_version}" --index-url https://download.pytorch.org/whl/cu130
        RESULT_VARIABLE _ret
      )
    else()
      execute_process(
        COMMAND pip install "torch==${_version}"
        RESULT_VARIABLE _ret
      )
    endif()
    if(NOT _ret EQUAL 0)
      message(FATAL_ERROR "[PG] Failed to install PyTorch ${_version}")
    endif()

    execute_process(
      COMMAND python setup.py build_ext --build-lib . --force
      WORKING_DIRECTORY "${SOURCE_DIR}"
      RESULT_VARIABLE _ret
    )
    if(NOT _ret EQUAL 0)
      message(FATAL_ERROR "[PG] Extension build failed for PyTorch ${_version}")
    endif()
  endforeach()
endif()

# ---------------------------------------------------------------------------
# 4. Copy the built .so files to the staging directory.
# ---------------------------------------------------------------------------
file(MAKE_DIRECTORY "${STAGING_DIR}")
file(GLOB _so_files "${SOURCE_DIR}/mooncake/*.so")
foreach(_so IN LISTS _so_files)
  get_filename_component(_fname "${_so}" NAME)
  message(STATUS "[PG] Staging ${_fname} -> ${STAGING_DIR}")
  file(COPY "${_so}" DESTINATION "${STAGING_DIR}")
endforeach()

message(STATUS "[PG] Mooncake PG extension build complete")
