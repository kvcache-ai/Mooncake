cmake_minimum_required(VERSION 3.16)

find_program(PYTHON3_EXECUTABLE python3)
if(PYTHON3_EXECUTABLE)
  execute_process(
    COMMAND ${PYTHON3_EXECUTABLE} -c "import sys; print(sys.prefix if hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix else '')"
    OUTPUT_VARIABLE AUTO_DETECTED_VENV
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
else()
  set(AUTO_DETECTED_VENV "")
endif()

if(AUTO_DETECTED_VENV)
  message(STATUS "Automatically detected virtual environment: ${AUTO_DETECTED_VENV}")
  set(CMAKE_INSTALL_PREFIX "${AUTO_DETECTED_VENV}" CACHE PATH "Install path prefix" FORCE)
elseif(DEFINED ENV{VIRTUAL_ENV})
  message(STATUS "Using VIRTUAL_ENV environment variable: $ENV{VIRTUAL_ENV}")
  set(CMAKE_INSTALL_PREFIX "$ENV{VIRTUAL_ENV}" CACHE PATH "Install path prefix" FORCE)
else()
  message(WARNING "No virtual environment detected! Using default install prefix.")
  set(DEFAULT_USER_PREFIX "$ENV{HOME}/.local")
  set(CMAKE_INSTALL_PREFIX "${DEFAULT_USER_PREFIX}" CACHE PATH "Install path prefix" FORCE)
endif()

project(mooncake-transfer-engine)

if (NOT GLOBAL_CONFIG)
  if (USE_ETCD)
    message(FATAL_ERROR "Cannot enable USE_ETCD while building transfer engine independently")
  endif()
  include(../mooncake-common/common.cmake)
endif() # GLOBAL_CONFIG

include_directories(include)
add_subdirectory(include)
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()

if (BUILD_EXAMPLES)
  add_subdirectory(example)
endif()

if (WITH_RUST_EXAMPLE)
  add_subdirectory(rust)
endif()
