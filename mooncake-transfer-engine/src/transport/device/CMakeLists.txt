# IBGDA library (mlx5gda depends on GPU runtime + IB DevX)
if(USE_CUDA)
    add_library(ibgda STATIC mlx5gda.cpp)
    target_include_directories(ibgda PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        /usr/local/cuda/include
    )
    target_link_libraries(ibgda PUBLIC ibverbs mlx5)
elseif(USE_MUSA)
    add_library(ibgda STATIC mlx5gda.cpp)
    target_include_directories(ibgda PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        /usr/local/musa/include
    )
    target_compile_definitions(ibgda PRIVATE USE_MUSA)
    target_link_libraries(ibgda PUBLIC ibverbs mlx5 musa musart)
endif()

# Device transport sources — conditional on GPU vendor
set(DEVICE_TRANSPORT_SOURCES p2p_device_transport.cpp)
if(USE_CUDA)
    list(APPEND DEVICE_TRANSPORT_SOURCES ibgda_device_transport.cpp)
elseif(USE_MUSA)
    list(APPEND DEVICE_TRANSPORT_SOURCES ibgda_device_transport.cpp)
elseif(USE_MACA)
    list(APPEND DEVICE_TRANSPORT_SOURCES ibgda_device_transport_maca_stub.cpp)
endif()

add_library(device_transport OBJECT ${DEVICE_TRANSPORT_SOURCES})
target_include_directories(device_transport PRIVATE ${CMAKE_SOURCE_DIR}/include)

if(USE_CUDA)
    target_include_directories(device_transport PRIVATE /usr/local/cuda/include)
    add_dependencies(device_transport ibgda)
endif()
if(USE_MUSA)
    target_include_directories(device_transport PRIVATE /usr/local/musa/include)
    target_compile_definitions(device_transport PRIVATE USE_MUSA)
    add_dependencies(device_transport ibgda)
endif()
if(USE_MACA)
    target_include_directories(device_transport PRIVATE ${MACA_INCLUDE_DIR})
endif()
