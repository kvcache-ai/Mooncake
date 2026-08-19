file(GLOB INTRANODE_NVLINK_SOURCES "*.cpp")

add_library(intranode_nvlink_transport OBJECT ${INTRANODE_NVLINK_SOURCES})

if (USE_CUDA)
target_include_directories(intranode_nvlink_transport PUBLIC ${CUDAToolkit_INCLUDE_DIRS})
endif()
