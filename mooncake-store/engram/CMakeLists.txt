find_package(CUDAToolkit REQUIRED)
enable_language(CUDA)

pybind11_add_module(_engram_cuda engram_local_cuda.cpp engram_local_cuda.cu)
set_target_properties(_engram_cuda PROPERTIES CUDA_STANDARD 17 INSTALL_RPATH
                                                               "$ORIGIN")
target_link_libraries(_engram_cuda PRIVATE CUDA::cudart)
install(TARGETS _engram_cuda LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
                                     COMPONENT python)
