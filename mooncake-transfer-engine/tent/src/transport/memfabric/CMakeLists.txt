if(USE_MEMFABRIC)
  # Standalone TENT builds do not visit the classic engine's allocator entry.
  if(NOT TARGET memfabric_allocator)
    add_subdirectory(
      ${TENT_REPOSITORY_ROOT}/mooncake-transfer-engine/allocator/memfabric_allocator
      ${CMAKE_CURRENT_BINARY_DIR}/memfabric_allocator)
  endif()
  add_library(tent_xport_memfabric STATIC memfabric_transport.cpp
                                          memfabric_under_api.cpp)
  target_link_libraries(tent_xport_memfabric PRIVATE memfabric_allocator)
  if(BUILD_UNIT_TESTS AND USE_MEMFABRIC)
    target_include_directories(
      tent_xport_memfabric
      PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../tests/mock_acl)
    target_link_libraries(tent_xport_memfabric PUBLIC tent_interface dl)
    message(STATUS "Using stub ACL headers for tent_xport_memfabric unit tests")
  else()
    target_link_libraries(tent_xport_memfabric PUBLIC tent_interface dl
                                                      ascendcl)
  endif()
  target_include_directories(
    tent_xport_memfabric PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../../include)
endif()
