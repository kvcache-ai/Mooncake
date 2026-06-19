# Device transport sources — conditional on GPU vendor. mlx5gda.cpp (IBGDA QP
# lifecycle) is compiled directly into the device_transport OBJECT library, like
# every other transport module, so its mlx5gda_* symbols flow into
# transfer_engine without a separate static lib. This keeps them visible to
# consumers that link transfer_engine via hand- written ldflags (Go p2p-store /
# mooncake-store), which bypass CMake's target_link_libraries propagation.
set(DEVICE_TRANSPORT_SOURCES p2p_device_transport.cpp)
if((USE_CUDA OR USE_MUSA) AND NOT (USE_CXI))
  list(APPEND DEVICE_TRANSPORT_SOURCES ibgda_device_transport.cpp mlx5gda.cpp)
endif()

add_library(device_transport OBJECT ${DEVICE_TRANSPORT_SOURCES})
target_include_directories(device_transport PRIVATE ${CMAKE_SOURCE_DIR}/include)

if(USE_CUDA)
  target_include_directories(device_transport PRIVATE /usr/local/cuda/include)
endif()
if(USE_MUSA)
  target_include_directories(device_transport PRIVATE /usr/local/musa/include)
  target_compile_definitions(device_transport PRIVATE USE_MUSA)
endif()
