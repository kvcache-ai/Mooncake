if(USE_CUDA)
  find_package(CUDAToolkit REQUIRED)
  add_definitions(-DUSE_CUDA)
  include_directories(${CUDAToolkit_INCLUDE_DIRS})
  message(STATUS "CUDA: Enabled")
else()
  message(STATUS "CUDA: Disabled")
endif()

file(GLOB TEBENCH_SOURCES "*.cpp")
add_executable(tebench ${TEBENCH_SOURCES})
target_link_libraries(tebench PUBLIC transfer_engine tent)
if(USE_CUDA)
  target_link_libraries(tebench PUBLIC CUDA::cudart)
endif()

# Set RPATH for finding libasio.so at runtime
set_target_properties(tebench PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE
                                         INSTALL_RPATH "$ORIGIN/../lib:$ORIGIN/../../mooncake-asio")
