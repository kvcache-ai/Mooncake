file(GLOB ENGINE_SOURCES "*.cpp")
add_subdirectory(common)
add_subdirectory(transport)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# Find Abseil's CMake targets
find_package(absl REQUIRED)
include_directories(${absl_INCLUDE_DIR})
# Add all libs from absl into third party libs.
list(APPEND ABSL_LIBS absl::algorithm)
list(APPEND ABSL_LIBS absl::base)
list(APPEND ABSL_LIBS absl::debugging)
list(APPEND ABSL_LIBS absl::flat_hash_map)
list(APPEND ABSL_LIBS absl::flags)
list(APPEND ABSL_LIBS absl::memory)
list(APPEND ABSL_LIBS absl::meta)
list(APPEND ABSL_LIBS absl::numeric)
list(APPEND ABSL_LIBS absl::random_random)
list(APPEND ABSL_LIBS absl::strings)
list(APPEND ABSL_LIBS absl::synchronization)
list(APPEND ABSL_LIBS absl::time)
list(APPEND ABSL_LIBS absl::utility)

add_library(transfer_engine ${ENGINE_SOURCES} $<TARGET_OBJECTS:transport>)
if (BUILD_SHARED_LIBS)
  install(TARGETS transfer_engine DESTINATION lib)
endif()

add_compile_definitions(transfer_engine PUBLIC MOONCAKE_USE_ETCD)
if (USE_ETCD)
  if (USE_STATIC_ETCD_CPP_API)
    target_link_libraries(transfer_engine PUBLIC etcd-cpp-api-core protobuf grpc++ grpc)
  else()
    target_link_libraries(transfer_engine PUBLIC etcd-cpp-api)
  endif()
endif()
if (USE_REDIS)
  target_link_libraries(transfer_engine PUBLIC hiredis)
endif()
if (USE_HTTP)
  find_package(CURL REQUIRED)
  target_link_libraries(transfer_engine PUBLIC ${CURL_LIBRARIES})
endif()
target_link_libraries(
    transfer_engine
    PUBLIC
    base transport rdma_transport ibverbs glog gflags pthread jsoncpp numa
    ${ABSL_LIBS}
    )

if (USE_CUDA)
  target_include_directories(transfer_engine PRIVATE /usr/local/cuda/include)
  target_link_libraries(transfer_engine PUBLIC cuda cudart rt)
  if (USE_NVMEOF)
    target_link_libraries(transfer_engine PUBLIC nvmeof_transport cufile)
  endif()
endif()
