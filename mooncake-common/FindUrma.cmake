include(FetchContent)

# Prefer a system UMDK installation so configure works without downloading a
# second copy and the headers match the liburma ABI used at runtime.
find_path(
  URMA_SYSTEM_INCLUDE_DIR
  NAMES urma_api.h
  PATHS /usr/include /usr/local/include
  PATH_SUFFIXES urma umdk src/urma/lib/urma/core/include)
find_library(
  URMA_LIBRARY
  NAMES urma
  PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)

if(URMA_SYSTEM_INCLUDE_DIR)
  set(urma_INCLUDE_DIR "${URMA_SYSTEM_INCLUDE_DIR}")
else()
  # The source fallback supplies headers only. Production TENT UB remains
  # disabled at runtime when no real liburma is present; tests inject their own
  # adapter instead of defining a second set of global urma_* mock symbols.
  FetchContent_Declare(
    urma
    GIT_REPOSITORY https://atomgit.com/openeuler/umdk.git
    GIT_TAG v25.12.0.B081)
  FetchContent_MakeAvailable(urma)
  set(urma_INCLUDE_DIR "${urma_SOURCE_DIR}/src/urma/lib/urma/core/include")
endif()

if(NOT TARGET Urma::urma)
  add_library(Urma::urma INTERFACE IMPORTED GLOBAL)
  set_property(TARGET Urma::urma PROPERTY INTERFACE_INCLUDE_DIRECTORIES
                                          "${urma_INCLUDE_DIR}")
  if(URMA_LIBRARY)
    set_property(TARGET Urma::urma PROPERTY INTERFACE_LINK_LIBRARIES
                                            "${URMA_LIBRARY}")
  endif()
endif()

set(URMA_INCLUDE_DIR "${urma_INCLUDE_DIR}")
set(URMA_FOUND TRUE)
message(STATUS "URMA include directory: ${URMA_INCLUDE_DIR}")
if(URMA_LIBRARY)
  message(STATUS "URMA library: ${URMA_LIBRARY}")
else()
  message(STATUS "URMA library not found; real UB backends will be unavailable")
endif()
