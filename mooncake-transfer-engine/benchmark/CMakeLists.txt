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
# The TENT backend is only available when USE_TENT is enabled; drop its
# translation unit (which pulls in tent/ headers) from non-TENT builds.
if(NOT USE_TENT)
  list(REMOVE_ITEM TEBENCH_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/tent_backend.cpp")
endif()
add_executable(tebench ${TEBENCH_SOURCES})
target_link_libraries(tebench PUBLIC transfer_engine)
if(USE_TENT)
  target_compile_definitions(tebench PRIVATE USE_TENT)
  # tent_link_group's interface brings in the tent/ header search path.
  target_link_libraries(tebench PUBLIC tent_link_group)
else()
  # The classic backend still uses a couple of header-only helpers that live
  # under tent/include (SimpleRandom in utils.h, bindToSocket in te_backend.cpp).
  # Expose just the header path so tebench builds without the TENT library.
  target_include_directories(tebench PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../tent/include")
endif()
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
