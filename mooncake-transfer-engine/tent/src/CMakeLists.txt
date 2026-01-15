SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_library(tent_interface INTERFACE)
target_include_directories(tent_interface
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
        $<INSTALL_INTERFACE:include>
)
target_compile_features(tent_interface INTERFACE cxx_std_20)

# CUDA
find_package(CUDAToolkit)
if (CUDAToolkit_FOUND)
    message(STATUS "CUDA: Enabled")
    target_compile_definitions(tent_interface INTERFACE USE_CUDA)
    target_include_directories(tent_interface INTERFACE ${CUDAToolkit_INCLUDE_DIRS})
    target_link_libraries(tent_interface INTERFACE CUDA::cudart)
else()
    message(STATUS "CUDA: Disabled")
endif()

# GDS
find_library(CUFILE_LIB cufile PATHS /usr/local/cuda/lib64)
find_path(CUFILE_INCLUDE cufile.h PATHS /usr/local/cuda/include)
if (CUDAToolkit_FOUND AND CUFILE_LIB AND CUFILE_INCLUDE)
    message(STATUS "GDS: Enabled")
    target_compile_definitions(tent_interface INTERFACE USE_GDS)
    target_include_directories(tent_interface INTERFACE ${CUFILE_INCLUDE})
    target_link_libraries(tent_interface INTERFACE ${CUFILE_LIB})
else()
    message(STATUS "GDS: Disabled")
endif()

# RDMA (ibverbs)
find_library(IBVERBS_LIB ibverbs PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)
find_path(IBVERBS_INCLUDE infiniband/verbs.h PATHS /usr/include /usr/local/include)
if (IBVERBS_LIB AND IBVERBS_INCLUDE)
    message(STATUS "RDMA: Enabled")
    target_compile_definitions(tent_interface INTERFACE USE_RDMA)
    target_include_directories(tent_interface INTERFACE ${IBVERBS_INCLUDE})
else()
    message(STATUS "RDMA: Disabled")
endif()

# io_uring
find_library(URING_LIB uring PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)
find_path(URING_INCLUDE liburing.h PATHS /usr/include /usr/local/include)
if (URING_LIB AND URING_INCLUDE)
    message(STATUS "Uring: Enabled")
    target_compile_definitions(tent_interface INTERFACE USE_URING)
    target_include_directories(tent_interface INTERFACE ${URING_INCLUDE})
    target_link_libraries(tent_interface INTERFACE ${URING_LIB})
else()
    message(STATUS "Uring: Disabled")
endif()

set(YALANTING_TARGET yalantinglibs::yalantinglibs)
if (NOT TARGET ${YALANTING_TARGET})
    if (CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
        find_package(yalantinglibs CONFIG REQUIRED)
    else()
        message(FATAL_ERROR
            "yalantinglibs::yalantinglibs target not found")
    endif()
endif()

add_subdirectory(common)
add_subdirectory(rpc)
add_subdirectory(metastore)
add_subdirectory(runtime)
add_subdirectory(platform)
add_subdirectory(transport)
add_subdirectory(metrics)
add_subdirectory(python)

file(GLOB TENT_ENGINE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_library(tent STATIC ${TENT_ENGINE_SOURCES})
target_link_libraries(
    tent
    PUBLIC
        tent_runtime
        tent_interface
)

add_library(tent_shared SHARED ${TENT_ENGINE_SOURCES})
target_link_libraries(
    tent_shared
    PUBLIC
        tent_runtime
        tent_interface
)

install(TARGETS tent_shared
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin)
