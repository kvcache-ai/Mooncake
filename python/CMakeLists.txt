find_package(
  Python3
  COMPONENTS Interpreter Development.Module
  REQUIRED)

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
