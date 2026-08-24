find_package(
  Python3
  COMPONENTS Interpreter Development.Module NumPy
  REQUIRED)
find_package(Threads REQUIRED)

# Compile the structured-object copy helper from the canonical Python package.
# CMake installs the native artifact into the backend-owned wheel tree.
python3_add_library(_fast_copy MODULE WITH_SOABI
                    "${CMAKE_CURRENT_SOURCE_DIR}/mooncake/_fast_copy.c")
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
install(
  DIRECTORY
    "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-reshard/python/mooncake/reshard/"
  DESTINATION mooncake/reshard
  COMPONENT python
  FILES_MATCHING
  PATTERN "*.py")
