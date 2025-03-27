file(GLOB ENGINE_SOURCES "*.cpp")
add_subdirectory(common)
add_subdirectory(transport)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_library(ngxl ${ENGINE_SOURCES} $<TARGET_OBJECTS:ngxl_common> $<TARGET_OBJECTS:ngxl_transport>)
if (BUILD_SHARED_LIBS)
  install(TARGETS ngxl DESTINATION lib)
endif()

find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPCPP REQUIRED grpc++)
pkg_check_modules(GRPC REQUIRED grpc)

add_compile_definitions(ngxl PUBLIC MOONCAKE_USE_ETCD)
if (USE_ETCD)
  if (USE_STATIC_ETCD_CPP_API)
    target_link_libraries(ngxl PUBLIC etcd-cpp-api-core protobuf ${GRPC_LDFLAGS} ${GRPCPP_LDFLAGS})
  else()
    target_link_libraries(ngxl PUBLIC etcd-cpp-api)
  endif()
endif()
if (USE_REDIS)
  target_link_libraries(ngxl PUBLIC hiredis)
endif()
if (USE_HTTP)
  find_package(CURL REQUIRED)
  target_link_libraries(ngxl PUBLIC ${CURL_LIBRARIES})
endif()
target_link_libraries(
  ngxl PUBLIC
  ngxl_common ngxl_transport rdma_ngxl_transport ibverbs glog gflags pthread jsoncpp numa boost_thread
)

if (USE_CUDA)
  target_include_directories(ngxl PRIVATE /usr/local/cuda/include)
  target_link_libraries(ngxl PUBLIC cuda cudart rt)
  if (USE_NVMEOF)
    target_link_libraries(ngxl PUBLIC nvmeof_transport cufile)
  endif()
endif()
