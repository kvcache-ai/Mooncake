file(GLOB TEV1_ENGINE_SOURCES "*.cpp")

add_subdirectory(common)
add_subdirectory(memory)
add_subdirectory(metadata)
add_subdirectory(transport)
add_subdirectory(utility)
add_subdirectory(metastore)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

if (USE_GDS)
  add_compile_definitions(USE_GDS)
endif()

add_library(transfer_engine_v1 ${TEV1_ENGINE_SOURCES} $<TARGET_OBJECTS:transport_v1> $<TARGET_OBJECTS:metadata_v1>)
if (BUILD_SHARED_LIBS)
  install(TARGETS transfer_engine_v1 DESTINATION lib)
endif()

target_link_libraries(
    transfer_engine_v1
    PUBLIC
    metadata_v1
    common_v1 memory_v1 utility_v1 transport_v1 transport_rdma_v1 ibverbs glog gflags pthread jsoncpp numa boost_thread
    )

if (USE_CUDA)
  target_include_directories(transfer_engine_v1 PRIVATE /usr/local/cuda/include)
  target_link_libraries(transfer_engine_v1 PUBLIC cuda cudart rt cufile)
endif()

target_link_libraries(transfer_engine_v1 PUBLIC uring)
