file(GLOB TENT_BUFIO_SOURCES "*.cpp")
add_library(tent_xport_bufio STATIC ${TENT_BUFIO_SOURCES})
target_link_libraries(tent_xport_bufio PUBLIC tent_common)
if (CUDAToolkit_FOUND)
    target_link_libraries(tent_xport_bufio PUBLIC 
        CUDA::cudart CUDA::cuda_driver)
endif()
