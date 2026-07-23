include(FetchContent)

# Allow callers to supply headers without downloading UMDK, e.g.: cmake
# -DURMA_INCLUDE_DIR=/usr/include ... cmake
# -DFETCHCONTENT_SOURCE_DIR_URMA=/path/to/umdk ...
if(DEFINED URMA_INCLUDE_DIR AND URMA_INCLUDE_DIR)
  set(urma_INCLUDE_DIR ${URMA_INCLUDE_DIR})
  message(STATUS "Using provided URMA_INCLUDE_DIR: ${urma_INCLUDE_DIR}")
elseif(DEFINED FETCHCONTENT_SOURCE_DIR_URMA AND FETCHCONTENT_SOURCE_DIR_URMA)
  set(urma_SOURCE_DIR ${FETCHCONTENT_SOURCE_DIR_URMA})
  set(urma_INCLUDE_DIR ${urma_SOURCE_DIR}/src/urma/lib/urma/core/include)
  message(STATUS "Using FETCHCONTENT_SOURCE_DIR_URMA: ${urma_SOURCE_DIR}")
else()
  FetchContent_Declare(
    urma
    GIT_REPOSITORY https://atomgit.com/openeuler/umdk.git
    GIT_TAG v25.12.0.B081
    GIT_SHALLOW TRUE)

  FetchContent_GetProperties(urma)
  if(NOT urma_POPULATED)
    FetchContent_Populate(urma)
  endif()

  set(urma_INCLUDE_DIR ${urma_SOURCE_DIR}/src/urma/lib/urma/core/include)
  message(STATUS "URMA source dir: ${urma_SOURCE_DIR}")
  message(STATUS "URMA binary dir: ${urma_BINARY_DIR}")
endif()

message(STATUS "urma_INCLUDE_DIR: ${urma_INCLUDE_DIR}")
