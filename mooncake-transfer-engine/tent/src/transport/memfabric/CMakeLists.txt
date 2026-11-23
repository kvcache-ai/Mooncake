if(USE_MEMFABRIC)
  add_library(
    tent_xport_memfabric STATIC
    memfabric_transport.cpp memfabric_under_api.cpp memfabric_allocator.cpp)
  find_path(
    MEMFABRIC_HYBM_INCLUDE_DIR hybm_big_mem.h
    HINTS ENV MEMFABRIC_HOME
    PATH_SUFFIXES include include/host)
  if(MEMFABRIC_HYBM_INCLUDE_DIR)
    target_include_directories(tent_xport_memfabric
                               PRIVATE ${MEMFABRIC_HYBM_INCLUDE_DIR})
    target_compile_definitions(tent_xport_memfabric
                               PRIVATE TENT_WITH_HYBM_GVA=1)
  else()
    message(
      STATUS
        "HYBM headers unavailable; MemFabric trans remains enabled without GVA")
  endif()
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
