find_package(
  Python3
  COMPONENTS Interpreter Development.Module NumPy
  REQUIRED)
find_package(Threads REQUIRED)

# This extension remains in its legacy source location until its subsystem is
# migrated. CMake installs it directly into the backend-owned wheel tree.
python3_add_library(
  _fast_copy MODULE WITH_SOABI
  "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/_fast_copy.c")
target_link_libraries(_fast_copy PRIVATE Python3::NumPy Threads::Threads)
target_compile_options(_fast_copy PRIVATE -O2)
install(TARGETS _fast_copy LIBRARY DESTINATION mooncake COMPONENT python)

# Phase 1 consumes unmigrated modules from their authoritative source locations.
# Nothing is copied into python/mooncake or another tracked source directory.
# These rules disappear subsystem by subsystem during Phase 2.
install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/"
  DESTINATION mooncake
  COMPONENT python
  FILES_MATCHING
  PATTERN "*.py"
  PATTERN "__init__.py" EXCLUDE)
if(WITH_EP)
  # The JIT adapter sources remain authoritative under mooncake-pg/torch.
  # Install them into the wheel only; do not mirror them under
  # mooncake-wheel/mooncake in the source tree.
  install(
    FILES
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-pg/torch/src/pg_py.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-pg/torch/src/mooncake_backend.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-pg/torch/src/work_handles.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-pg/torch/include/mooncake_backend.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-pg/torch/include/work_handles.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-pg/torch/include/torch_utils.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-pg/include/pg_utils.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-pg/include/mooncake_pg.h"
    DESTINATION mooncake/_pg_jit
    COMPONENT python)
endif()
install(
  DIRECTORY
    "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-reshard/python/mooncake/reshard/"
  DESTINATION mooncake/reshard
  COMPONENT python
  FILES_MATCHING
  PATTERN "*.py")
