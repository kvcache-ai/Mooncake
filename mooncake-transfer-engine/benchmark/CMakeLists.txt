if(USE_CUDA)
  find_package(CUDAToolkit REQUIRED)
  add_definitions(-DUSE_CUDA)
  include_directories(${CUDAToolkit_INCLUDE_DIRS})
  message(STATUS "CUDA: Enabled")
else()
  message(STATUS "CUDA: Disabled")
endif()

if(USE_HIP)
  list(APPEND CMAKE_PREFIX_PATH "/opt/rocm/lib/cmake")
  find_package(HIP REQUIRED)
  add_definitions(-DUSE_HIP -D__HIP_PLATFORM_AMD__)
  message(STATUS "ROCm/HIP: Enabled")
else()
  message(STATUS "ROCm/HIP: Disabled")
endif()

file(GLOB TEBENCH_SOURCES "*.cpp")
add_executable(tebench ${TEBENCH_SOURCES})
target_link_libraries(tebench PUBLIC transfer_engine tent_link_group)
if(USE_CUDA)
  target_link_libraries(tebench PUBLIC CUDA::cudart)
endif()
if(USE_HIP)
  target_link_libraries(tebench PUBLIC hip::host)
endif()

# Set RPATH for finding libasio.so at runtime
set_target_properties(
  tebench PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE
                     INSTALL_RPATH "$ORIGIN/../lib:$ORIGIN/../../mooncake-common")
