file(GLOB NVLINK_SOURCES "*.cpp")

add_library(nvlink_transport OBJECT ${NVLINK_SOURCES})
target_link_libraries(nvlink_transport PRIVATE glog::glog)

if (USE_CUDA)
target_include_directories(nvlink_transport PUBLIC ${CUDAToolkit_INCLUDE_DIRS})
endif()
