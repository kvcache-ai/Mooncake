include(${CMAKE_CURRENT_SOURCE_DIR}/../fabric_allocator.cmake)

set(_extra_build_opts "")
if(USE_HIP)
  list(APPEND _extra_build_opts --use-hipcc)
elseif(USE_MUSA)
  list(APPEND _extra_build_opts --use-mcc)
endif()

set(_enable_nvlink_allocator_build FALSE)
if(USE_CUDA
   OR USE_HIP
   OR USE_MUSA)
  set(_enable_nvlink_allocator_build TRUE)
endif()

add_fabric_allocator_build_target(
  TARGET_NAME
  build_nvlink_allocator
  BUILD_SCRIPT
  ${CMAKE_CURRENT_SOURCE_DIR}/build.sh
  BUILD_ARGS
  ${_extra_build_opts}
  COMMENT
  "Building nvlink allocator to ${CMAKE_CURRENT_BINARY_DIR}"
  ENABLE_BUILD
  ${_enable_nvlink_allocator_build})
