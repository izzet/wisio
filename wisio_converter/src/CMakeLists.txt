find_package(nlohmann_json 3.10.5 REQUIRED)
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
include_directories(${RECORDER_INCLUDE_DIR})
add_executable(converter recorder2parquet.cpp)
target_link_libraries(converter
        PUBLIC ${MPI_CXX_LIBRARIES}
        reader
        arrow_shared
        parquet_shared
        -lstdc++fs
        cpp-logger
        nlohmann_json::nlohmann_json
        )
add_dependencies(converter reader)