# Locate NCCL 2.30.4+ with the experimental Device API.
#
# Prefer NCCL's exported package so version metadata, include paths, library
# variants, CUDA, and thread dependencies come from NCCL itself. Keep a manual
# fallback for tarball and pre-package installations.

set(_NCCL_DEVICE_MIN_VERSION "2.30.4")
set(_NCCL_DEVICE_USING_CONFIG FALSE)

find_package(NCCL ${_NCCL_DEVICE_MIN_VERSION} CONFIG QUIET)
if(NCCL_FOUND AND TARGET NCCL::nccl)
  set(_NCCL_DEVICE_USING_CONFIG TRUE)
  set(NCCLDevice_VERSION "${NCCL_VERSION}")
  find_path(NCCL_DEVICE_INCLUDE_DIR
    NAMES nccl_device.h
    HINTS ${NCCL_INCLUDE_DIRS}
    NO_DEFAULT_PATH)
endif()

if(NOT _NCCL_DEVICE_USING_CONFIG)
  find_path(NCCL_DEVICE_INCLUDE_DIR
    NAMES nccl_device.h
    HINTS
      ${NCCL_ROOT}
      $ENV{NCCL_ROOT}
      $ENV{NCCL_HOME}
    PATH_SUFFIXES include build/include
    PATHS
      /usr/local/cuda
      /usr/local
      /usr)

  find_library(NCCL_DEVICE_LIBRARY
    NAMES nccl
    HINTS
      ${NCCL_ROOT}
      $ENV{NCCL_ROOT}
      $ENV{NCCL_HOME}
    PATH_SUFFIXES lib lib64 build/lib build/lib64
    PATHS
      /usr/local/cuda
      /usr/local
      /usr)

  if(NCCL_DEVICE_INCLUDE_DIR AND
     EXISTS "${NCCL_DEVICE_INCLUDE_DIR}/nccl.h")
    foreach(_component MAJOR MINOR PATCH)
      file(STRINGS "${NCCL_DEVICE_INCLUDE_DIR}/nccl.h"
        _nccl_${_component}_line
        REGEX "^#define NCCL_${_component}[ \t]+[0-9]+"
        LIMIT_COUNT 1)
      string(REGEX MATCH "[0-9]+$" _nccl_${_component}
        "${_nccl_${_component}_line}")
    endforeach()
    if(_nccl_MAJOR AND _nccl_MINOR AND _nccl_PATCH)
      set(NCCLDevice_VERSION
        "${_nccl_MAJOR}.${_nccl_MINOR}.${_nccl_PATCH}")
    endif()
  endif()
endif()

include(FindPackageHandleStandardArgs)
if(_NCCL_DEVICE_USING_CONFIG)
  find_package_handle_standard_args(NCCLDevice
    REQUIRED_VARS NCCL_DEVICE_INCLUDE_DIR
    VERSION_VAR NCCLDevice_VERSION)
else()
  find_package_handle_standard_args(NCCLDevice
    REQUIRED_VARS NCCL_DEVICE_INCLUDE_DIR NCCL_DEVICE_LIBRARY
    VERSION_VAR NCCLDevice_VERSION)
endif()

if(NCCLDevice_FOUND)
  find_package(CUDAToolkit REQUIRED)

  if(NOT TARGET NCCL::nccl)
    add_library(NCCL::nccl UNKNOWN IMPORTED)
    set_target_properties(NCCL::nccl PROPERTIES
      IMPORTED_LOCATION "${NCCL_DEVICE_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${NCCL_DEVICE_INCLUDE_DIR}"
      INTERFACE_LINK_LIBRARIES "CUDA::cudart")
  endif()

  set_property(TARGET NCCL::nccl APPEND PROPERTY
    INTERFACE_COMPILE_DEFINITIONS
    NCCL_DEVICE_PERMIT_EXPERIMENTAL_CODE=1)
endif()

mark_as_advanced(NCCL_DEVICE_INCLUDE_DIR NCCL_DEVICE_LIBRARY)
unset(_NCCL_DEVICE_MIN_VERSION)
unset(_NCCL_DEVICE_USING_CONFIG)
