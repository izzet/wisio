cmake_minimum_required(VERSION 3.0)
project(wisio)

#message(${CMAKE_PREFIX_PATH})
set(CMAKE_CXX_STANDARD 17)
set(WISIO_CMAKE_DIR ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${WISIO_CMAKE_DIR} ${WISIO_CMAKE_DIR}/modules)
set(DEPENDENCY_LIB -lstdc++fs)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -rdynamic")

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
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif ()

if (NOT "${CMAKE_CXX_STANDARD}")
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif ()

add_subdirectory(lib/backward-cpp)
add_subdirectory(lib/cpp-logger)
add_subdirectory(lib/recorder/tools)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib/recorder/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib/recorder/tools)
include_directories(${RECORDER_INCLUDE_DIR})

find_package(MPI COMPONENTS CXX REQUIRED)
if (MPI_FOUND)
    message(STATUS "[MPI] found mpi.h at ${MPI_CXX_INCLUDE_DIRS}")
    include_directories(${MPI_CXX_INCLUDE_DIRS})
    set(DEPENDENCY_LIB ${DEPENDENCY_LIB} ${MPI_CXX_LIBRARIES})
endif ()

find_package(Arrow REQUIRED)
if(${Arrow_FOUND})
message("-- " "Found Arrow: TRUE")
include_directories(${ARROW_INCLUDE_DIR})
else()
message("-- " "Found Arrow: FALSE")
endif()
find_package(Parquet REQUIRED)
if(${Parquet_FOUND})
message("-- " "Found Parquet: TRUE")
include_directories(${PARQUET_INCLUDE_DIR})
else()
message("-- " "Found Parquet: FALSE")
endif()

find_package(nlohmann_json 3.11.2 REQUIRED)
find_package(PythonExtensions REQUIRED)

add_executable(wisio_recorder2parquet recorder2parquet.cpp)
target_link_libraries(wisio_recorder2parquet
    ${MPI_CXX_LIBRARIES}
    reader
    arrow_shared
    parquet_shared
    -lstdc++fs
    cpp-logger
    nlohmann_json::nlohmann_json
    Backward::Interface
)
add_dependencies(wisio_recorder2parquet reader)
install(TARGETS wisio_recorder2parquet RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
python_standalone_executable(wisio_recorder2parquet)
