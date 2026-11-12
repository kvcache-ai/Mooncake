file(GLOB EFA_SOURCES "*.cpp")
add_library(efa_transport OBJECT ${EFA_SOURCES})

# libfabric is dlopen'ed at runtime (libfabric_loader.cpp); only its headers are
# needed at build time.
target_link_libraries(
  efa_transport
  PUBLIC glog::glog gflags::gflags
  PRIVATE ${CMAKE_DL_LIBS})

target_include_directories(
  efa_transport
  PRIVATE
    ${CMAKE_SOURCE_DIR}/mooncake-transfer-engine/include
    ${CMAKE_SOURCE_DIR}/mooncake-transfer-engine/include/transport/efa_transport
    ${CMAKE_SOURCE_DIR}/mooncake-common/include ${LIBFABRIC_INCLUDE_DIR})
