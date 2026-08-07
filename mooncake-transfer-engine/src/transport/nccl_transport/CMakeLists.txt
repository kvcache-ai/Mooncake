add_library(nccl_host_transport OBJECT nccl_transport.cpp)
target_include_directories(
  nccl_host_transport
  PRIVATE ${CMAKE_CURRENT_LIST_DIR}/../../../include ${NCCL_DEVICE_INCLUDE_DIR})
target_link_libraries(
  nccl_host_transport
  PRIVATE CUDA::cudart JsonCpp::JsonCpp glog::glog pthread)
target_compile_features(nccl_host_transport PRIVATE cxx_std_17)
