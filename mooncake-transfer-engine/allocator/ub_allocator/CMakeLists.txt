add_library(ub_allocator OBJECT ub_allocator.cpp)
target_include_directories(
  ub_allocator
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(ub_allocator PRIVATE glog::glog numa)
