# BuildEpExt.cmake - Build the Mooncake EP Python extension.
#
# Invoked at build time via cmake -P from the root CMakeLists.txt when
# WITH_EP=ON.  Variables are passed with -D from the custom target:
#
#   SOURCE_DIR          - mooncake-ep source directory
#   BUILD_DIR           - cmake binary / build directory
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
# 1. Stage engine.so so setup.py can link against it.
#    setup.py uses:  -L<SOURCE_DIR>/../mooncake-wheel/mooncake  -l:engine.so
# ---------------------------------------------------------------------------
set(_engine_staging "${SOURCE_DIR}/../mooncake-wheel/mooncake")
file(GLOB _engine_so "${BUILD_DIR}/mooncake-integration/engine.*.so")
if(_engine_so)
  list(GET _engine_so 0 _engine_so_path)
  message(STATUS "[EP] Staging engine.so from ${_engine_so_path}")
  file(COPY "${_engine_so_path}" DESTINATION "${_engine_staging}")
  get_filename_component(_engine_so_name "${_engine_so_path}" NAME)
  if(NOT _engine_so_name STREQUAL "engine.so")
    file(RENAME
      "${_engine_staging}/${_engine_so_name}"
      "${_engine_staging}/engine.so"
    )
  endif()
else()
  message(WARNING "[EP] engine.so not found in ${BUILD_DIR}/mooncake-integration/ — EP build may fail")
endif()

# ---------------------------------------------------------------------------
# 2. Forward TORCH_CUDA_ARCH_LIST to the extension build.
# ---------------------------------------------------------------------------
set(ENV{TORCH_CUDA_ARCH_LIST} "${TORCH_CUDA_ARCH_LIST}")

# ---------------------------------------------------------------------------
# 3. Build the EP Python extension.
# ---------------------------------------------------------------------------
if("${EP_TORCH_VERSIONS}" STREQUAL "")
  message(STATUS "[EP] Building with currently-installed PyTorch")
  execute_process(
    COMMAND python setup.py build_ext --build-lib .
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE _ret
  )
  if(NOT _ret EQUAL 0)
    message(FATAL_ERROR "[EP] Extension build failed (exit code: ${_ret})")
  endif()
else()
  message(STATUS "[EP] Building for PyTorch versions: ${EP_TORCH_VERSIONS}")
  foreach(_version IN LISTS EP_TORCH_VERSIONS)
    message(STATUS "[EP] Installing PyTorch ${_version}")
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
      message(FATAL_ERROR "[EP] Failed to install PyTorch ${_version}")
    endif()

    execute_process(
      COMMAND python setup.py build_ext --build-lib . --force
      WORKING_DIRECTORY "${SOURCE_DIR}"
      RESULT_VARIABLE _ret
    )
    if(NOT _ret EQUAL 0)
      message(FATAL_ERROR "[EP] Extension build failed for PyTorch ${_version}")
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
  message(STATUS "[EP] Staging ${_fname} -> ${STAGING_DIR}")
  file(COPY "${_so}" DESTINATION "${STAGING_DIR}")
endforeach()

message(STATUS "[EP] Mooncake EP extension build complete")
