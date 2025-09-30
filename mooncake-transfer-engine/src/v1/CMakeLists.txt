SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

find_package(CUDAToolkit)
if (CUDAToolkit_FOUND)
  add_definitions(-DUSE_CUDA)
  include_directories(${CUDAToolkit_INCLUDE_DIRS})
  message(STATUS "CUDA enabled -- Build with CPU/CUDA GPU support")
else()
  message(STATUS "CUDA not enabled -- Build with CPU support only")
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
