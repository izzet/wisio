cmake_minimum_required(VERSION 3.0)
project(wisio)
set(WISIO_VERSION_MAJOR "0")
set(WISIO_VERSION_MINOR "0")
set(WISIO_VERSION_PATCH "1")
set(WISIO_PACKAGE ${PROJECT_NAME})
set(WISIO_PACKAGE_NAME ${PROJECT_NAME})
set(WISIO_PACKAGE_VERSION "${WISIO_VERSION_MAJOR}.${WISIO_VERSION_MINOR}.${WISIO_VERSION_PATCH}")
set(WISIO_PACKAGE_VERSION_MAJOR "${WISIO_VERSION_MAJOR}.${WISIO_VERSION_MINOR}")
set(WISIO_PACKAGE_VERSION_MINOR "${WISIO_VERSION_PATCH}")
set(WISIO_PACKAGE_STRING "${WISIO_PACKAGE_NAME} ${WISIO_PACKAGE_VERSION}")
set(WISIO_PACKAGE_TARNAME "${WISIO_PACKAGE}")


#TODO change this to options
set(ARROW_INSTALL_DIR /usr/workspace/iopp/software/arrow/cpp/install)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${CMAKE_SOURCE_DIR}/dependency/.spack-env/view ${CMAKE_BINARY_DIR}/external/Recorder ${CMAKE_BINARY_DIR}/lib/cmake ${ARROW_INSTALL_DIR} ${ARROW_INSTALL_DIR}/lib64/cmake/arrow)
#message(${CMAKE_PREFIX_PATH})
set(CMAKE_CXX_STANDARD 17)
set(WISIO_CMAKE_DIR ${CMAKE_SOURCE_DIR}/CMake)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${WISIO_CMAKE_DIR})
set(DEPENDENCY_LIB -lstdc++fs)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -rdynamic")
include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/include)


add_executable(${PROJECT_NAME} main.cpp)
#------------------------------------------------------------------------------
# Setup install and output Directories
#------------------------------------------------------------------------------
if (NOT WISIO_INSTALL_BIN_DIR)
    set(WISIO_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
    message(STATUS ${WISIO_INSTALL_BIN_DIR})
endif ()
if (NOT WISIO_INSTALL_LIB_DIR)
    set(WISIO_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif ()
if (NOT WISIO_INSTALL_INCLUDE_DIR)
    set(WISIO_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
endif ()
if (NOT WISIO_INSTALL_DATA_DIR)
    set(WISIO_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif ()

if (NOT CMAKE_INSTALL_RPATH)
    set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif ()

#------------------------------------------------------------------------------

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
        ${CMAKE_BINARY_DIR}/bin CACHE PATH "Single Directory for all Executables."
        )

set(CMAKE_INCLUDE_OUTPUT_DIRECTORY
        ${CMAKE_BINARY_DIR}/include CACHE PATH "Store the headers."
        )
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
        ${CMAKE_BINARY_DIR}/lib CACHE PATH "Single Directory for all Libraries"
        )
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY
        ${CMAKE_BINARY_DIR}/lib CACHE PATH "Single Directory for all static libraries."
        )

set_target_properties(${PROJECT_NAME} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})
#------------------------------------------------------------------------------
# Disallow in-source build
#------------------------------------------------------------------------------
if ("${CMAKE_CURRENT_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
    message(FATAL_ERROR
            "${PROJECT_NAME} requires an out of source Build. "
            "Please create a separate binary directory and run CMake there.")
endif ()

#------------------------------------------------------------------------------
# Set a default build type if none was specified
#------------------------------------------------------------------------------
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
            "MinSizeRel" "RelWithDebInfo")
endif ()

if (NOT "${CMAKE_CXX_STANDARD}")
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif ()

#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects.
#-----------------------------------------------------------------------------
if (NOT WISIO_EXPORTED_TARGETS)
    set(WISIO_EXPORTED_TARGETS "${PROJECT_NAME}-targets")
endif ()

set(WISIO_LIBTYPE SHARED)
set_target_properties(${libtarget}
        PROPERTIES
        DEBUG_OUTPUT_NAME ${LIB_DEBUG_NAME}
        RELEASE_OUTPUT_NAME ${LIB_RELEASE_NAME}
        MINSIZEREL_OUTPUT_NAME ${LIB_RELEASE_NAME}
        RELWITHDEBINFO_OUTPUT_NAME ${LIB_RELEASE_NAME}
        VERSION ${LIB_VERSION}
        SOVERSION ${LIB_VERSION}
        )

include(GNUInstallDirs)
# the variant with PUBLIC_HEADER property unfortunately does not preserve the folder structure
#set_target_properties(${PROJECT_NAME} PROPERTIES PUBLIC_HEADER "${public_headers}")
# so instead we iterate through public headers and install them "manually"
foreach (header ${WISIO_PUBLIC_INCLUDE})
    file(RELATIVE_PATH header_file_path "${CMAKE_CURRENT_SOURCE_DIR}/${PROJECT_NAME}" "${header}")
    get_filename_component(header_directory_path "${header_file_path}" DIRECTORY)
    install(
            FILES ${header}
            DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${header_directory_path}"
    )
    file(COPY ${header}
            DESTINATION "${CMAKE_INCLUDE_OUTPUT_DIRECTORY}/${header_directory_path}")
endforeach ()
#-----------------------------------------------------------------------------
# Configure the config.cmake file for the build directory
#-----------------------------------------------------------------------------
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/CMake/${PROJECT_NAME}-config.cmake.build.in
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}-config.cmake @ONLY
)
install(
        FILES
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}-config.cmake
        DESTINATION
        ${WISIO_INSTALL_LIB_DIR}/cmake/${PROJECT_NAME}
)

install(TARGETS ${PROJECT_NAME}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
#-----------------------------------------------------------------------------
# Configure the ${PROJECT_NAME}-config-version .cmake file for the install directory
#-----------------------------------------------------------------------------
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/CMake/${PROJECT_NAME}-config-version.cmake.in
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}-config-version.cmake @ONLY
)

install(
        FILES
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}-config-version.cmake
        DESTINATION
        ${WISIO_INSTALL_LIB_DIR}/cmake/${PROJECT_NAME}
)
install(
        TARGETS
        ${PROJECT_NAME}
        EXPORT
        ${WISIO_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${WISIO_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${WISIO_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${WISIO_INSTALL_BIN_DIR}
)
export(EXPORT ${WISIO_EXPORTED_TARGETS}
        FILE "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}Targets.cmake")
install(EXPORT
        ${WISIO_EXPORTED_TARGETS}
        DESTINATION
        ${WISIO_INSTALL_LIB_DIR}/cmake/${PROJECT_NAME}
        FILE
        ${WISIO_EXPORTED_TARGETS}.cmake
        )

add_subdirectory(external/Recorder/tools)
add_subdirectory(external/cpp-logger)
find_package(cpp-logger REQUIRED)
if (${CPP_LOGGER_FOUND})
    include_directories(${CPP_LOGGER_INCLUDE_DIRS})
    set(DEPENDENCY_LIB ${DEPENDENCY_LIB} ${CPP_LOGGER_LIBRARIES})
    message(STATUS "[CPP_LOGGER] found cpp-logger at ${CPP_LOGGER_INCLUDE_DIRS}")
else ()
    message(FATAL_ERROR "-- [CPP_LOGGER] is needed for ${PROJECT_NAME} build")
endif ()
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external/Recorder/tools)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external/Recorder/include)
find_package(MPI COMPONENTS CXX REQUIRED)
if (MPI_FOUND)
    message(STATUS "[MPI] found mpi.h at ${MPI_CXX_INCLUDE_DIRS}")
    include_directories(${MPI_CXX_INCLUDE_DIRS})
    set(DEPENDENCY_LIB ${DEPENDENCY_LIB} ${MPI_CXX_LIBRARIES})
endif ()

add_subdirectory(converter)
