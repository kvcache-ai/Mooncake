SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

find_package(CUDAToolkit)
if (CUDAToolkit_FOUND)
  add_definitions(-DUSE_CUDA)
  include_directories(${CUDAToolkit_INCLUDE_DIRS})
  message(STATUS "CUDA: Enabled")
else()
  message(STATUS "CUDA: Disabled")
endif()

find_package(CUDAToolkit)
find_library(CUFILE_LIB cufile PATHS /usr/local/cuda/lib64)
find_path(CUFILE_INCLUDE cufile.h PATHS /usr/local/cuda/include)
if (CUDAToolkit_FOUND AND CUFILE_LIB AND CUFILE_INCLUDE)
  message(STATUS "GDS: Enabled")
  add_definitions(-DUSE_GDS)
else()
  message(STATUS "GDS: Disabled")
endif()

find_library(IBVERBS_LIB ibverbs PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)
find_path(IBVERBS_INCLUDE infiniband/verbs.h PATHS /usr/include /usr/local/include)
if (IBVERBS_LIB AND IBVERBS_INCLUDE)
  message(STATUS "RDMA: Enabled")
  add_definitions(-DUSE_RDMA)
else()
  message(STATUS "RDMA: Disabled")
endif()

find_library(URING_LIB uring PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)
find_path(URING_INCLUDE liburing.h PATHS /usr/include /usr/local/include)
if (URING_LIB AND URING_INCLUDE)
  message(STATUS "Uring: Enabled")
  add_definitions(-DUSE_URING)
else()
  message(STATUS "Uring: Disabled")
endif()

add_subdirectory(common)
add_subdirectory(rpc)
add_subdirectory(metastore)
add_subdirectory(runtime)
add_subdirectory(platform)
add_subdirectory(transport)

file(GLOB TEV1_ENGINE_SOURCES "*.cpp")
add_library(transfer_engine_v1 STATIC ${TEV1_ENGINE_SOURCES})
target_link_libraries(
  transfer_engine_v1
  PUBLIC
  tent_runtime
)

add_library(transfer_engine_shared SHARED ${TEV1_ENGINE_SOURCES})
target_link_libraries(
  transfer_engine_shared
  PUBLIC
  tent_runtime
)

install(TARGETS transfer_engine_shared
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin)
