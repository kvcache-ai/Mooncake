if (NOT USE_CUDA)
    return()
endif()

find_package(CUDAToolkit REQUIRED)

set(CUDA_LOADER_SOURCES
    src/cuda_loader.cpp
    src/cudart_wrappers.cpp
    src/cuda_driver_wrappers.cpp
)

# Check for cuFile (GDS) support
find_path(CUDA_LOADER_CUFILE_INCLUDE cufile.h
    PATHS ${CUDAToolkit_INCLUDE_DIRS} /usr/local/cuda/include
    NO_DEFAULT_PATH)

if (CUDA_LOADER_CUFILE_INCLUDE)
    list(APPEND CUDA_LOADER_SOURCES src/cufile_wrappers.cpp)
    message(STATUS "cuda_loader: cuFile (GDS) support enabled")
else()
    message(STATUS "cuda_loader: cuFile (GDS) support disabled (cufile.h not found)")
endif()

add_library(cuda_loader SHARED ${CUDA_LOADER_SOURCES})

target_include_directories(cuda_loader
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CUDAToolkit_INCLUDE_DIRS}
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

if (CUDA_LOADER_CUFILE_INCLUDE)
    target_include_directories(cuda_loader PRIVATE ${CUDA_LOADER_CUFILE_INCLUDE})
    target_compile_definitions(cuda_loader PRIVATE USE_GDS)
endif()

target_link_libraries(cuda_loader
    PRIVATE
        ${CMAKE_DL_LIBS}
        glog::glog
)

set_target_properties(cuda_loader PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    SOVERSION 1
)
