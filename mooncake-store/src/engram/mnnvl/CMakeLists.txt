pybind11_add_module(_engram_mnnvl engram_mnnvl.cpp ../engram_local_cuda.cu)
target_include_directories(_engram_mnnvl PRIVATE ../../../include/engram)
target_link_libraries(_engram_mnnvl PRIVATE tent_link_group CUDA::cudart
                                            CUDA::cuda_driver)
set_target_properties(_engram_mnnvl PROPERTIES CUDA_STANDARD 17 INSTALL_RPATH
                                                                "$ORIGIN")
install(TARGETS _engram_mnnvl LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
                                      COMPONENT python)
