find_package(MPI QUIET CONFIG)

if(TARGET MPI::MPI)
    set(MPI_FOUND TRUE)
    set(MPI_TARGET MPI::MPI)
else()
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(PC_MPI QUIET mpi)
    endif()

    file(GLOB MPI_INCLUDE_DIRS "/usr/include/mpich*" "/usr/local/include/mpich*")

    set(MPI_INCLUDE_SEARCH_PATHS "")
    foreach(dir IN LISTS MPI_INCLUDE_DIRS)
        list(APPEND MPI_INCLUDE_SEARCH_PATHS "${dir}")
    endforeach()

    find_path(MPI_INCLUDE_DIR mpi.h
        HINTS ${PC_MPI_INCLUDEDIR} ${PC_MPI_INCLUDE_DIRS}
        PATHS /usr/include /usr/local/include ${MPI_INCLUDE_SEARCH_PATHS})

    file(GLOB MPI_LIB_DIRS "/usr/lib/mpich*" "/usr/local/lib/mpich*" "/usr/local/mpich*/lib" "/usr/lib/*-linux-gnu" "/usr/lib64/mpich/lib")

    set(MPI_LIB_SEARCH_PATHS "")
    foreach(dir IN LISTS MPI_LIB_DIRS)
        list(APPEND MPI_LIB_SEARCH_PATHS "${dir}")
    endforeach()

    find_library(MPI_LIBRARY mpi
        HINTS ${PC_MPI_LIBDIR} ${PC_MPI_LIBRARY_DIRS}
        PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64 ${MPI_LIB_SEARCH_PATHS})

    if(MPI_INCLUDE_DIR AND MPI_LIBRARY)
        set(MPI_FOUND TRUE)
        add_library(MPI::MPI INTERFACE IMPORTED)
        target_include_directories(MPI::MPI INTERFACE ${MPI_INCLUDE_DIR})
        target_link_libraries(MPI::MPI INTERFACE ${MPI_LIBRARY})
        set(MPI_TARGET MPI::MPI)
    endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MPI DEFAULT_MSG MPI_TARGET)