# add_project_arguments('-fPIC', '-rdynamic', language: 'cpp')

# cpp-logger
cpp_logger_sources = [
    'lib/cpp-logger/src/cpp-logger/logger.cpp',
    'lib/cpp-logger/src/cpp-logger/clogger.cpp',
]
cpp_logger_lib = static_library(
    'cpp_logger',
    cpp_logger_sources,
    include_directories: include_directories('lib/cpp-logger/include'),
    install: false,
)
cpp_logger_dep = declare_dependency(
    link_with: cpp_logger_lib,
    include_directories: include_directories('lib/cpp-logger/include'),
)

# backward-cpp
backward_cpp_inc = include_directories('lib/backward-cpp')
backward_cpp_dep = declare_dependency(
    include_directories: backward_cpp_inc,
    # compile_args: ['-DBACKWARD_HAS_DW=1'],
    # link_args: ['-ldw']
)

# recorder
reader_lib = static_library(
    'reader',
    'lib/recorder/tools/reader.c',
    include_directories: ['lib/recorder/include', 'lib/recorder/tools'],
    install: false,
)
recorder_dep = declare_dependency(
    include_directories: [
        include_directories('lib/recorder/include'),
        include_directories('lib/recorder/tools'),
    ],
    link_with: reader_lib,
)

# 3rd party
arrow_dep = dependency('arrow', required: true)
mpi_dep = dependency('mpi', language: 'cpp', required: true)
ncurses_dep = dependency('ncurses', required: true)
nlohmann_json_dep = dependency('nlohmann_json', required: true)
parquet_dep = dependency('parquet', required: true)
zlib_dep = dependency('zlib', required: false)

executable(
    meson.project_name() + '-recorder2parquet',
    'recorder2parquet.cpp',
    install: true,
    dependencies: [
        arrow_dep,
        backward_cpp_dep,
        cpp_logger_dep,
        mpi_dep,
        ncurses_dep,
        nlohmann_json_dep,
        parquet_dep,
        recorder_dep,
        zlib_dep,
    ],
    link_args: ['-liberty', '-lreadline', '-lstdc++fs'],
)