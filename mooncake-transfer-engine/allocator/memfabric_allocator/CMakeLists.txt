# The transport embeds these objects so allocator calls and transport calls use
# the same MemFabricUnderApi state without a circular library dependency.
add_library(memfabric_allocator OBJECT memfabric_allocator.cpp)
set_target_properties(memfabric_allocator PROPERTIES POSITION_INDEPENDENT_CODE
                                                     ON)
target_compile_features(memfabric_allocator PRIVATE cxx_std_20)
target_include_directories(
  memfabric_allocator
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../tent/include)
target_link_libraries(memfabric_allocator PRIVATE glog::glog)

if(BUILD_UNIT_TESTS)
  target_include_directories(
    memfabric_allocator
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../tent/tests/mock_acl)
else()
  target_include_directories(memfabric_allocator PRIVATE ${ASCEND_INCLUDE_DIR})
  target_link_libraries(memfabric_allocator PRIVATE ascendcl)
endif()
