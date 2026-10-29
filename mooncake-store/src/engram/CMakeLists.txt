find_package(CUDAToolkit REQUIRED)
# Set defaults locally: CUDA may already be enabled in a sibling directory.
if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES AND "$ENV{CUDAARCHS}" STREQUAL "")
  if(CUDAToolkit_VERSION VERSION_GREATER_EQUAL "13.0")
    set(CMAKE_CUDA_ARCHITECTURES 80 90 103)
  else()
    set(CMAKE_CUDA_ARCHITECTURES 80 90)
  endif()
endif()
enable_language(CUDA)

pybind11_add_module(_engram_cuda engram_local_cuda.cpp engram_local_cuda.cu)
set_target_properties(_engram_cuda PROPERTIES CUDA_STANDARD 17 INSTALL_RPATH
                                                               "$ORIGIN")
target_include_directories(
  _engram_cuda PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include/engram)
target_link_libraries(_engram_cuda PRIVATE CUDA::cudart)
install(TARGETS _engram_cuda LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
                                     COMPONENT python)
