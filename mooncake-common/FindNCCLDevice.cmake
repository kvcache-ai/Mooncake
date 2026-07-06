# Locate an NCCL 2.30+ installation that exposes the strong-signal GIN API.

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

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NCCLDevice
  REQUIRED_VARS NCCL_DEVICE_INCLUDE_DIR NCCL_DEVICE_LIBRARY)

if(NCCLDevice_FOUND)
  find_package(CUDAToolkit REQUIRED)
endif()

if(NCCLDevice_FOUND AND NOT TARGET NCCL::Device)
  add_library(NCCL::Device UNKNOWN IMPORTED)
  set_target_properties(NCCL::Device PROPERTIES
    IMPORTED_LOCATION "${NCCL_DEVICE_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${NCCL_DEVICE_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS "NCCL_DEVICE_PERMIT_EXPERIMENTAL_CODE=1"
    INTERFACE_LINK_LIBRARIES "CUDA::cudart")
endif()

mark_as_advanced(NCCL_DEVICE_INCLUDE_DIR NCCL_DEVICE_LIBRARY)
