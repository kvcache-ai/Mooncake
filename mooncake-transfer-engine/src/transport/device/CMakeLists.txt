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

if(USE_CUDA)
  target_include_directories(device_transport PRIVATE /usr/local/cuda/include)

  include(CheckCXXSourceCompiles)
  set(_saved_cmake_required_libraries "${CMAKE_REQUIRED_LIBRARIES}")
  set(CMAKE_REQUIRED_LIBRARIES mlx5)
  check_cxx_source_compiles(
    "#include <infiniband/mlx5dv.h>
     int main() {
       struct mlx5dv_devx_umem_in input{};
       input.comp_mask = MLX5DV_UMEM_MASK_DMABUF;
       input.dmabuf_fd = -1;
       auto *umem = mlx5dv_devx_umem_reg_ex(nullptr, &input);
       return umem != nullptr;
     }"
    MOONCAKE_HAVE_MLX5_DMABUF_UMEM)
  set(CMAKE_REQUIRED_LIBRARIES "${_saved_cmake_required_libraries}")
  unset(_saved_cmake_required_libraries)

  if(MOONCAKE_HAVE_MLX5_DMABUF_UMEM)
    message(STATUS "IBGDA DMA-BUF control UMEM support enabled")
    target_compile_definitions(device_transport
                               PRIVATE MOONCAKE_HAVE_MLX5_DMABUF_UMEM=1)
  else()
    message(STATUS "IBGDA DMA-BUF control UMEM support unavailable")
  endif()
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
