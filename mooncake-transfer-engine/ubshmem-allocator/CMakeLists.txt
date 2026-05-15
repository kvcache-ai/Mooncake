include(${CMAKE_CURRENT_SOURCE_DIR}/../fabric_allocator.cmake)

set(_enable_ubshmem_allocator_build FALSE)
if(USE_UBSHMEM)
  set(_enable_ubshmem_allocator_build TRUE)
endif()

add_fabric_allocator_build_target(
  TARGET_NAME
  build_ubshmem_allocator
  BUILD_SCRIPT
  ${CMAKE_CURRENT_SOURCE_DIR}/build.sh
  COMMENT
  "Building ubshmem allocator to ${CMAKE_CURRENT_BINARY_DIR}"
  ENABLE_BUILD
  ${_enable_ubshmem_allocator_build})
