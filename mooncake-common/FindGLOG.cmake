find_package(glog QUIET CONFIG)

if(TARGET glog::glog)
    set(GLOG_FOUND TRUE)
    set(GLOG_TARGET glog::glog)
else()
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(PC_GLOG QUIET libglog)
    endif()

    find_path(GLOG_INCLUDE_DIR glog/logging.h
        HINTS ${PC_GLOG_INCLUDEDIR} ${PC_GLOG_INCLUDE_DIRS}
        PATHS /usr/include /usr/local/include)
    
    find_library(GLOG_LIBRARY glog
        HINTS ${PC_GLOG_LIBDIR} ${PC_GLOG_LIBRARY_DIRS}
        PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)

    if(GLOG_INCLUDE_DIR AND GLOG_LIBRARY)
        set(GLOG_FOUND TRUE)
        add_library(glog::glog INTERFACE IMPORTED)
        target_include_directories(glog::glog INTERFACE ${GLOG_INCLUDE_DIR})
        target_link_libraries(glog::glog INTERFACE ${GLOG_LIBRARY})
        set(GLOG_TARGET glog::glog)
    endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GLOG DEFAULT_MSG GLOG_TARGET)