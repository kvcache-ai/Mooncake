# Opt-in LLVM lld linker for C/C++ targets in CI builds.
# Passed via -DCMAKE_PROJECT_INCLUDE=... so shared CMake modules stay untouched
# while CUDA device link steps are left unaffected.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  find_program(_MOONCAKE_CI_LLD NAMES ld.lld lld)
  if(NOT _MOONCAKE_CI_LLD)
    message(
      FATAL_ERROR "scripts/ci/cmake_lld.cmake requires ld.lld to be installed")
  endif()
  message(STATUS "[ci] Using lld linker: ${_MOONCAKE_CI_LLD}")
  if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.18")
    add_link_options($<$<LINK_LANGUAGE:C,CXX>:-fuse-ld=lld>)
  else()
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=lld")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fuse-ld=lld")
  endif()
endif()
