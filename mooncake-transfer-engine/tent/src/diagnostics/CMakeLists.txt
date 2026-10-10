add_library(tent_diagnostics STATIC diagnostic.cpp)
target_compile_features(tent_diagnostics PUBLIC cxx_std_20)
target_include_directories(
  tent_diagnostics
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
         $<INSTALL_INTERFACE:include>)

target_compile_definitions(
  tent_diagnostics
  PRIVATE TENT_BUILD_VERSION="${TENT_BUILD_VERSION}"
          TENT_GIT_COMMIT="${TENT_GIT_COMMIT}"
          TENT_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
          TENT_COMPILER_ID="${CMAKE_CXX_COMPILER_ID}"
          TENT_COMPILER_VERSION="${CMAKE_CXX_COMPILER_VERSION}")

foreach(
  feature
  CUDA
  HIP
  ASCEND
  ASCEND_DIRECT
  RDMA
  URING
  SUNRISE
  TPU)
  if(TENT_FEATURE_${feature})
    target_compile_definitions(tent_diagnostics
                               PRIVATE TENT_FEATURE_${feature}=1)
  endif()
endforeach()

install(
  TARGETS tent_diagnostics
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib)
