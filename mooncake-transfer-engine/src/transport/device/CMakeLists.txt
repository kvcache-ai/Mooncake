# Device transport sources — conditional on GPU vendor. mlx5gda.cpp (IBGDA QP
# lifecycle) is compiled directly into the device_transport OBJECT library, like
# every other transport module, so its mlx5gda_* symbols flow into
# transfer_engine without a separate static lib. This keeps them visible to
# consumers that link transfer_engine via hand- written ldflags (Go p2p-store /
# mooncake-store), which bypass CMake's target_link_libraries propagation.
set(DEVICE_TRANSPORT_SOURCES p2p_device_transport.cpp)
if((USE_CUDA OR USE_MUSA) AND NOT USE_CXI)
  list(APPEND DEVICE_TRANSPORT_SOURCES ibgda_device_transport.cpp mlx5gda.cpp)
elseif(USE_MACA)
  list(APPEND DEVICE_TRANSPORT_SOURCES ibgda_device_transport_maca_stub.cpp)
endif()
if(USE_NCCL_DEVICE)
  list(APPEND DEVICE_TRANSPORT_SOURCES nccl_device_transport.cpp)
endif()

add_library(device_transport OBJECT ${DEVICE_TRANSPORT_SOURCES})
target_include_directories(device_transport PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(device_transport PRIVATE glog::glog)

if(USE_CUDA)
  target_include_directories(device_transport
                             PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
  # Compile backend ops once. Consumers device-link this archive with their
  # kernels, then bind the table belonging to that linked CUDA image.
  enable_language(CUDA)
  add_library(mooncake_device_comm STATIC device_comm.cu)
  set_target_properties(
    mooncake_device_comm
    PROPERTIES CUDA_SEPARABLE_COMPILATION ON
               CUDA_RESOLVE_DEVICE_SYMBOLS OFF
               POSITION_INDEPENDENT_CODE ON
               INSTALL_RPATH "$ORIGIN")
  if(CUDAToolkit_VERSION VERSION_GREATER_EQUAL "13.0")
    set_target_properties(mooncake_device_comm PROPERTIES CUDA_ARCHITECTURES
                                                          "80;90;103")
  else()
    set_target_properties(mooncake_device_comm PROPERTIES CUDA_ARCHITECTURES
                                                          "80;90")
  endif()
  target_include_directories(
    mooncake_device_comm
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../../include>
           $<INSTALL_INTERFACE:include>)
  target_link_libraries(mooncake_device_comm PRIVATE CUDA::cudart)
  # Legacy EP uses up to 1024 threads/CTA (64 registers/thread on sm80+).
  # Indirect callees must obey that budget even across CUDA modules.
  target_compile_options(
    mooncake_device_comm
    PRIVATE $<$<COMPILE_LANGUAGE:CUDA>:--maxrregcount=64>
            $<$<COMPILE_LANGUAGE:CUDA>:--expt-relaxed-constexpr>)
  if(USE_NCCL_DEVICE)
    target_link_libraries(mooncake_device_comm PRIVATE NCCL::nccl)
  endif()
  install(TARGETS mooncake_device_comm ARCHIVE DESTINATION lib)
endif()
if(USE_NCCL_DEVICE)
  target_link_libraries(device_transport PRIVATE NCCL::nccl)
endif()
if(USE_MUSA)
  target_include_directories(device_transport PRIVATE /usr/local/musa/include)
  target_compile_definitions(device_transport PRIVATE USE_MUSA)
endif()
if(USE_MACA)
  target_compile_definitions(device_transport PRIVATE USE_MACA)
endif()
