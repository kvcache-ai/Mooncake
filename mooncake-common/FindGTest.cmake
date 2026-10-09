include_guard(GLOBAL)

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

# Keep test builds independent of the host distribution and Python environment.
# In particular, manylinux images may expose headers from /opt/conda while
# dependencies.sh installs an incompatible system gtest library.
set(BUILD_GMOCK
    OFF
    CACHE BOOL "Build GoogleMock" FORCE)
set(INSTALL_GTEST
    OFF
    CACHE BOOL "Install GoogleTest" FORCE)

set(GOOGLETEST_VERSION 1.17.0)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v${GOOGLETEST_VERSION}.tar.gz
  URL_HASH
    SHA256=65fab701d9829d38cb77c14acdc431d2108bfdbf8979e40eb8ae567edf10b27c)

FetchContent_MakeAvailable(googletest)
