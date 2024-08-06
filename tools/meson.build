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
backward_cpp_dep = declare_dependency(include_directories: backward_cpp_inc)

# recorder
reader_lib = static_library(
    'reader',
    'lib/recorder/tools/reader.c',
    'lib/recorder/tools/reader-cst-cfg.c',
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
libiberty_dep = dependency('libiberty', required: false)
libstdcxxfs_dep = cpp.find_library('stdc++fs', required: true)
mpi_dep = dependency('mpi', language: 'cpp', required: true)
ncurses_dep = dependency('ncurses', required: true)
nlohmann_json_dep = dependency('nlohmann_json', required: true)
parquet_dep = dependency('parquet', required: true)
readline_dep = dependency('readline', required: true)
zlib_dep = dependency('zlib', required: false)

recorder2parquet_exe = executable(
    meson.project_name() + '-recorder2parquet',
    'recorder2parquet.cpp',
    install: true,
    dependencies: [
        arrow_dep,
        backward_cpp_dep,
        cpp_logger_dep,
        libiberty_dep,
        libstdcxxfs_dep,
        mpi_dep,
        ncurses_dep,
        nlohmann_json_dep,
        parquet_dep,
        readline_dep,
        recorder_dep,
        zlib_dep,
    ],
)

test_data_dir = 'tests/data'
extracted_dir = test_data_dir + '/extracted'

prepare_test_data = custom_target(
    'prepare_test_data',
    output: '0_0.parquet',
    command: [
        'sh',
        '-c', 'if [ ! -d '
        + meson.project_source_root()
        + '/'
        + extracted_dir
        + '/recorder-raw ]; then '
        + 'mkdir -p '
        + meson.project_source_root()
        + '/'
        + extracted_dir
        + '/recorder-raw && '
        + 'tar -xzf '
        + meson.project_source_root()
        + '/'
        + test_data_dir
        + '/recorder-raw.tar.gz -C '
        + meson.project_source_root()
        + '/'
        + extracted_dir
        + '/recorder-raw; '
        + 'fi',
    ],
    build_by_default: false,
)

test(
    'test_recorder2parquet',
    recorder2parquet_exe,
    args: [meson.project_source_root() + '/' + extracted_dir + '/recorder-raw'],
    depends: prepare_test_data,
    timeout: 0,
)

add_test_setup('mpi', exe_wrapper: ['mpirun', '-np', '2'])
