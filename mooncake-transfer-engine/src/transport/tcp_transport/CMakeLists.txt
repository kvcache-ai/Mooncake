file(GLOB TCP_SOURCES "*.cpp")

if (USE_HIP)
  hipify_files(TCP_SOURCES)
endif()

add_library(tcp_transport OBJECT ${TCP_SOURCES})
target_link_libraries(
  tcp_transport
  PUBLIC asio_shared JsonCpp::JsonCpp yalantinglibs::yalantinglibs
  PRIVATE glog::glog)
