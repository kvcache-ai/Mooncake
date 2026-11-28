install(
  FILES __init__.py
  DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}/_engram
  COMPONENT python)

install(
  DIRECTORY remote
  DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}/_engram
  COMPONENT python
  FILES_MATCHING
  PATTERN "*.py"
  PATTERN "*.cpp"
  PATTERN "DEEP_EP_LICENSE"
  PATTERN "__pycache__" EXCLUDE)

if(USE_CUDA)
  add_subdirectory(local)
endif()
