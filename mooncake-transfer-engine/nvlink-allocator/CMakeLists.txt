include(${CMAKE_CURRENT_SOURCE_DIR}/../fabric_allocator.cmake)

set(_extra_build_opts "")
set(_extra_build_depends "")
if(USE_HIP)
  list(APPEND _extra_build_opts --use-hipcc)
  list(APPEND _extra_build_depends
       ${CMAKE_CURRENT_SOURCE_DIR}/../include/gpu_vendor/hip.h)
elseif(USE_MUSA)
  list(APPEND _extra_build_opts --use-mcc)
  list(APPEND _extra_build_depends
       ${CMAKE_CURRENT_SOURCE_DIR}/../include/gpu_vendor/musa.h)
elseif(USE_MACA)
  list(APPEND _extra_build_opts --use-maca)
  list(APPEND _extra_build_depends
       ${CMAKE_CURRENT_SOURCE_DIR}/../include/gpu_vendor/maca.h)
endif()

set(_enable_nvlink_allocator_build FALSE)
if(USE_CUDA
   OR USE_HIP
   OR USE_MUSA
   OR USE_MACA)
  set(_enable_nvlink_allocator_build TRUE)
endif()

add_fabric_allocator_build_target(
  TARGET_NAME
  build_nvlink_allocator
  BUILD_SCRIPT
  ${CMAKE_CURRENT_SOURCE_DIR}/build.sh
  OUTPUT_NAME
  nvlink_allocator.so
  BUILD_ARGS
  ${_extra_build_opts}
  BUILD_DEPENDS
  ${CMAKE_CURRENT_SOURCE_DIR}/nvlink_allocator.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/../include/cuda_alike.h
  ${CMAKE_CURRENT_SOURCE_DIR}/../scripts/allocator_build_common.sh
  ${_extra_build_depends}
  COMMENT
  "Building nvlink allocator to ${CMAKE_CURRENT_BINARY_DIR}"
  ENABLE_BUILD
  ${_enable_nvlink_allocator_build})
