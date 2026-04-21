# * Find liburing Find the liburing library and includes
#
# URING_INCLUDE_DIR - where to find liburing.h, etc. URING_LIBRARIES - List of
# libraries when using uring. URING_FOUND - True if uring found.

find_path(URING_INCLUDE_DIR liburing.h)
find_library(URING_LIBRARIES uring)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uring DEFAULT_MSG URING_LIBRARIES
                                  URING_INCLUDE_DIR)

if(URING_FOUND)
    if(NOT TARGET uring)
        add_library(uring UNKNOWN IMPORTED)
    endif()
    set_target_properties(
        uring
        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${URING_INCLUDE_DIR}"
                   IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                   IMPORTED_LOCATION "${URING_LIBRARIES}")
    mark_as_advanced(URING_LIBRARIES)
endif()