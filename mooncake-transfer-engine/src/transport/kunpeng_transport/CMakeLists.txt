set(UB_ALLOCATOR_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/ub_allocator.cpp")
file(GLOB UB_SOURCES "*.cpp" "urma/urma_endpoint.cpp")
list(REMOVE_ITEM UB_SOURCES "${UB_ALLOCATOR_SOURCE}")

add_library(ub_allocator OBJECT "${UB_ALLOCATOR_SOURCE}")
target_include_directories(
  ub_allocator PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                      $<INSTALL_INTERFACE:include>)
target_link_libraries(ub_allocator PRIVATE glog::glog numa)

# Check if liburma.so exists
find_library(URMA_LIBRARY urma PATHS /usr/lib64)

# Build ub_transport with the real URMA library when available. The production
# target never includes the mock API; tests include it directly.
if(URMA_LIBRARY)
  add_library(ub_transport OBJECT ${UB_SOURCES})
  target_link_libraries(ub_transport PUBLIC ${URMA_LIBRARY})
  message(STATUS "Using real URMA library: ${URMA_LIBRARY}")
else()
  # Fall back to the bundled mock when liburma.so is unavailable.
  list(APPEND UB_SOURCES "urma/mock_urma.cpp")
  add_library(ub_transport OBJECT ${UB_SOURCES})
  message(
    WARNING "Not Found liburma.so building ub_transport with Mock URMA library")
endif()
target_include_directories(ub_transport PUBLIC ${urma_INCLUDE_DIR})
target_link_libraries(ub_transport PRIVATE JsonCpp::JsonCpp glog::glog pthread)
