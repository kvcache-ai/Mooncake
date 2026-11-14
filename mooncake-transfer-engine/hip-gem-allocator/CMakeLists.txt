include(${CMAKE_CURRENT_SOURCE_DIR}/../fabric_allocator.cmake)

add_fabric_allocator_build_target(
  TARGET_NAME
  build_hip_gem_allocator
  BUILD_SCRIPT
  ${CMAKE_CURRENT_SOURCE_DIR}/build.sh
  OUTPUT_NAME
  hip_gem_allocator.so
  BUILD_DEPENDS
  ${CMAKE_CURRENT_SOURCE_DIR}/hip_gem_allocator.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/../scripts/allocator_build_common.sh
  COMMENT
  "Building hip gem allocator to ${CMAKE_CURRENT_BINARY_DIR}"
  ENABLE_BUILD
  TRUE)
