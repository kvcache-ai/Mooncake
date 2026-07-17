# Locate NCCL 2.30.4+. The host backend needs nccl.h; the device backend also
# requires the experimental nccl_device.h API.
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
    NAMES nccl.h
    HINTS ${NCCL_INCLUDE_DIRS}
    NO_DEFAULT_PATH)
endif()

if(NOT _NCCL_DEVICE_USING_CONFIG)
  find_path(NCCL_DEVICE_INCLUDE_DIR
    NAMES nccl.h
    HINTS
      ${NCCL_ROOT}
      $ENV{NCCL_ROOT}
      $ENV{NCCL_HOME}
    PATH_SUFFIXES include build/include
    PATHS
      /usr/local/cuda
      /usr/local
      /usr)

  # PyPI's NCCL wheel ships the versioned SONAME without an unversioned
  # development symlink. Prefer that exact artifact before generic discovery
  # can select an incompatible system NCCL.
  foreach(_nccl_root ${NCCL_ROOT} $ENV{NCCL_ROOT} $ENV{NCCL_HOME})
    if(EXISTS "${_nccl_root}/lib/libnccl.so.2")
      set(NCCL_DEVICE_LIBRARY "${_nccl_root}/lib/libnccl.so.2")
      break()
    endif()
  endforeach()

  if(NOT NCCL_DEVICE_LIBRARY)
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
  endif()

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
    if(NOT "${_nccl_MAJOR}" STREQUAL "" AND
       NOT "${_nccl_MINOR}" STREQUAL "" AND
       NOT "${_nccl_PATCH}" STREQUAL "")
      set(NCCLDevice_VERSION
        "${_nccl_MAJOR}.${_nccl_MINOR}.${_nccl_PATCH}")
    endif()
  endif()
endif()

if(USE_NCCL_DEVICE AND NCCL_DEVICE_INCLUDE_DIR)
  find_path(NCCL_DEVICE_API_INCLUDE_DIR
    NAMES nccl_device.h
    HINTS ${NCCL_DEVICE_INCLUDE_DIR}
    NO_DEFAULT_PATH)
endif()

include(FindPackageHandleStandardArgs)
set(_NCCL_DEVICE_REQUIRED_VARS NCCL_DEVICE_INCLUDE_DIR)
if(NOT _NCCL_DEVICE_USING_CONFIG)
  list(APPEND _NCCL_DEVICE_REQUIRED_VARS NCCL_DEVICE_LIBRARY)
endif()
if(USE_NCCL_DEVICE)
  list(APPEND _NCCL_DEVICE_REQUIRED_VARS NCCL_DEVICE_API_INCLUDE_DIR)
endif()
find_package_handle_standard_args(NCCLDevice
  REQUIRED_VARS ${_NCCL_DEVICE_REQUIRED_VARS}
  VERSION_VAR NCCLDevice_VERSION)

if(NCCLDevice_FOUND)
  find_package(CUDAToolkit REQUIRED)

  if(NOT TARGET NCCL::nccl)
    add_library(NCCL::nccl UNKNOWN IMPORTED)
    set_target_properties(NCCL::nccl PROPERTIES
      IMPORTED_LOCATION "${NCCL_DEVICE_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${NCCL_DEVICE_INCLUDE_DIR}"
      INTERFACE_LINK_LIBRARIES "CUDA::cudart")
  endif()

  if(USE_NCCL_DEVICE)
    set_property(TARGET NCCL::nccl APPEND PROPERTY
      INTERFACE_COMPILE_DEFINITIONS
      NCCL_DEVICE_PERMIT_EXPERIMENTAL_CODE=1)
  endif()
endif()

mark_as_advanced(NCCL_DEVICE_INCLUDE_DIR NCCL_DEVICE_API_INCLUDE_DIR
                 NCCL_DEVICE_LIBRARY)
unset(_NCCL_DEVICE_REQUIRED_VARS)
unset(_NCCL_DEVICE_MIN_VERSION)
unset(_NCCL_DEVICE_USING_CONFIG)
