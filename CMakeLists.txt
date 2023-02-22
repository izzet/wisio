cmake_minimum_required(VERSION 3.0)
project(wisio)
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


add_subdirectory(external/Recorder)

find_package(recorder REQUIRED)
if (NOT ${RECORDER_FOUND})
    message(FATAL_ERROR "Recorder is needed for Wisio build")
else ()
    message(STATUS "[Recorder] found headers at ${RECORDER_INCLUDE_DIR}")
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external/Recorder/tools)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external/Recorder/include)
endif ()
find_package(MPI COMPONENTS CXX REQUIRED)
if (MPI_FOUND)
    message(STATUS "[MPI] found mpi.h at ${MPI_CXX_INCLUDE_DIRS}")
    include_directories(${MPI_CXX_INCLUDE_DIRS})
    set(DEPENDENCY_LIB ${DEPENDENCY_LIB} ${MPI_CXX_LIBRARIES})
endif ()


add_subdirectory(converter)
