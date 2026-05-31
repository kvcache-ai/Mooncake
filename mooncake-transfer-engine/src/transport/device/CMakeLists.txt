add_library(device_transport OBJECT
    p2p_device_transport.cpp
    ibgda_device_transport.cpp
)

target_include_directories(device_transport PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${EP_INCLUDE_DIR}
)

if(USE_CUDA)
    target_include_directories(device_transport PRIVATE /usr/local/cuda/include)
endif()
if(USE_MUSA)
    target_include_directories(device_transport PRIVATE /usr/local/musa/include)
endif()
