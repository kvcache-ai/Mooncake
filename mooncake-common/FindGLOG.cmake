find_package(glog QUIET CONFIG)

# Track the detected glog version so we can decide whether newer public APIs
# (e.g. google::IsGoogleLoggingInitialized(), added in glog 0.6.0) are
# available. Different distros ship different glog versions, so relying on that
# symbol unconditionally breaks builds against glog < 0.6.0.
set(GLOG_DETECTED_VERSION "")

if(TARGET glog::glog)
  set(GLOG_FOUND TRUE)
  set(GLOG_TARGET glog::glog)
  if(DEFINED glog_VERSION)
    set(GLOG_DETECTED_VERSION "${glog_VERSION}")
  endif()
else()
  find_package(PkgConfig QUIET)
  if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_GLOG QUIET libglog)
  endif()

  find_path(
    GLOG_INCLUDE_DIR glog/logging.h
    HINTS ${PC_GLOG_INCLUDEDIR} ${PC_GLOG_INCLUDE_DIRS}
    PATHS /usr/include /usr/local/include)

  find_library(
    GLOG_LIBRARY glog
    HINTS ${PC_GLOG_LIBDIR} ${PC_GLOG_LIBRARY_DIRS}
    PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)

  if(GLOG_INCLUDE_DIR AND GLOG_LIBRARY)
    set(GLOG_FOUND TRUE)
    add_library(glog::glog INTERFACE IMPORTED)
    target_include_directories(glog::glog INTERFACE ${GLOG_INCLUDE_DIR})
    target_link_libraries(glog::glog INTERFACE ${GLOG_LIBRARY})
    set(GLOG_TARGET glog::glog)
  endif()

  if(PC_GLOG_VERSION)
    set(GLOG_DETECTED_VERSION "${PC_GLOG_VERSION}")
  endif()
endif()

# The version comes from CONFIG (glog_VERSION) or pkg-config (PC_GLOG_VERSION)
# above. A missing version is treated as "old".

# Only enable the newer API when we can positively confirm glog >= 0.6.0. When
# the version cannot be determined we conservatively assume it is too old and
# use the internal-symbol fallback declared in config.cpp. Note: on every glog
# version, an *unconditional* duplicate InitGoogleLogging() trips a CHECK and
# abort()s at runtime ("You called InitGoogleLogging() twice!"). Both code paths
# therefore guard with IsGoogleLoggingInitialized() before initializing; the
# only difference is where that symbol lives (top-level in >= 0.6.0, the
# internal namespace otherwise).
if(TARGET glog::glog)
  if(GLOG_DETECTED_VERSION AND NOT GLOG_DETECTED_VERSION VERSION_LESS "0.6.0")
    target_compile_definitions(glog::glog
                               INTERFACE MOONCAKE_GLOG_HAS_IS_INITIALIZED=1)
    message(STATUS "glog ${GLOG_DETECTED_VERSION}: "
                   "IsGoogleLoggingInitialized() available")
  else()
    target_compile_definitions(glog::glog
                               INTERFACE MOONCAKE_GLOG_HAS_IS_INITIALIZED=0)
    message(STATUS "glog version "
                   "'${GLOG_DETECTED_VERSION}' (< 0.6.0 or unknown): "
                   "IsGoogleLoggingInitialized() unavailable")
  endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GLOG DEFAULT_MSG GLOG_TARGET)
