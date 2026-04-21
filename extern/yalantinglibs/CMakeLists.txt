cmake_minimum_required(VERSION 3.15)
project(yaLanTingLibs
        VERSION 0.5.6
        DESCRIPTION "yaLanTingLibs"
        HOMEPAGE_URL "https://github.com/alibaba/yalantinglibs"
        LANGUAGES CXX
        )

# load pack finder
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Find/)

find_package(Threads REQUIRED)
link_libraries(Threads::Threads)

include(cmake/install.cmake)

if(CMAKE_PROJECT_NAME STREQUAL "yaLanTingLibs") # if ylt is top-level project
        # add include path
        include_directories(include)
        include_directories(include/ylt/thirdparty)
        include_directories(include/ylt/standalone)
        include_directories(src/include)

        include(cmake/Find/find_openssl.cmake)
        include(cmake/Find/find_ntls.cmake)
        include(cmake/Find/find_ibverbs.cmake)
        include(cmake/utils.cmake)
        include(cmake/build.cmake)
        include(cmake/develop.cmake)
        # add project config, such as enable_ssl.
        include(cmake/config.cmake)
        # add project's source such as unit test, example & benchmark
        include(cmake/subdir.cmake)
else ()
        # add project config, such as enable_ssl.
        include(cmake/config.cmake)
endif()

