find_package(JsonCpp QUIET CONFIG)

if(TARGET JsonCpp::JsonCpp)
    set(JSONCPP_FOUND TRUE)
    set(JSONCPP_TARGET JsonCpp::JsonCpp)
else()
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(PC_JSONCPP QUIET jsoncpp)
    endif()

    find_path(JSONCPP_INCLUDE_DIR json/json.h
        HINTS ${PC_JSONCPP_INCLUDEDIR} ${PC_JSONCPP_INCLUDE_DIRS}
        PATHS /usr/include /usr/include/jsoncpp /usr/local/include)

    find_library(JSONCPP_LIBRARY jsoncpp
        HINTS ${PC_JSONCPP_LIBDIR} ${PC_JSONCPP_LIBRARY_DIRS}
        PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)

    if(JSONCPP_INCLUDE_DIR AND JSONCPP_LIBRARY)
        set(JSONCPP_FOUND TRUE)
        add_library(JsonCpp::JsonCpp INTERFACE IMPORTED)
        target_include_directories(JsonCpp::JsonCpp INTERFACE ${JSONCPP_INCLUDE_DIR})
        target_link_libraries(JsonCpp::JsonCpp INTERFACE ${JSONCPP_LIBRARY})
        set(JSONCPP_TARGET JsonCpp::JsonCpp)
    endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JsonCpp DEFAULT_MSG JSONCPP_TARGET)