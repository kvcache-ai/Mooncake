file(GLOB INTRANODE_NVLINK_SOURCES "*.cpp")

add_library(intranode_nvlink_transport OBJECT ${INTRANODE_NVLINK_SOURCES})
target_link_libraries(intranode_nvlink_transport PRIVATE Mooncake::glog)

if(USE_CUDA)
  target_include_directories(intranode_nvlink_transport
                             PUBLIC CUDA::cudart "/usr/local/cuda/include")
endif()
