message(STATUS "Testing version_numbers() function")

version_numbers("2" major minor patch)
if (NOT (major STREQUAL "2" AND minor STREQUAL "0" AND patch STREQUAL "0"))
  message(
    FATAL_ERROR
    "version_numbers() failed to extract correct version numbers."
    "\n\tExpected 2.0.0, got: ${major}.${minor}.${patch}"
  )
endif ()

version_numbers("2.2" major minor patch)
if (NOT (major STREQUAL "2" AND minor STREQUAL "2" AND patch STREQUAL "0"))
  message(
    FATAL_ERROR
    "version_numbers() failed to extract correct version numbers."
    "\n\tExpected 2.2.0, got: ${major}.${minor}.${patch}"
  )
endif ()

version_numbers("2.2.3" major minor patch)
if (NOT (major STREQUAL "2" AND minor STREQUAL "2" AND patch STREQUAL "3"))
  message(
    FATAL_ERROR
    "version_numbers() failed to extract correct version numbers."
    "\n\tExpected 2.2.3, got: ${major}.${minor}.${patch}"
  )
endif ()

version_numbers("2.2.3rc1" major minor patch)
if (NOT (major STREQUAL "2" AND minor STREQUAL "2" AND patch STREQUAL "3"))
  message(
    FATAL_ERROR
    "version_numbers() failed to extract correct version numbers."
    "\n\tExpected 2.2.3, got: ${major}.${minor}.${patch}"
  )
endif ()

version_numbers("2.2rc1a" major minor patch)
if (NOT (major STREQUAL "2" AND minor STREQUAL "2" AND patch STREQUAL "0"))
  message(
    FATAL_ERROR
    "version_numbers() failed to extract correct version numbers."
    "\n\tExpected 2.2.0, got: ${major}.${minor}.${patch}"
  )
endif ()

message(STATUS "All version_numbers() checks passed!")
