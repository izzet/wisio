cmake = import('cmake')

backward_opts = cmake.subproject_options()
backward_opts.add_cmake_defines({
    'BACKWARD_SHARED': true,
    'CMAKE_INSTALL_LIBDIR': get_option('libdir'),
    'CMAKE_INSTALL_PREFIX': get_option('prefix'),
})
backward_proj = cmake.subproject('backward-cpp', options: backward_opts)
backward_dep = backward_proj.dependency('backward')

cpp_logger_opts = cmake.subproject_options()
cpp_logger_opts.add_cmake_defines({
    'CMAKE_INSTALL_LIBDIR': get_option('libdir'),
    'CMAKE_INSTALL_PREFIX': get_option('prefix'),
})
cpp_logger_proj = cmake.subproject('cpp-logger', options: cpp_logger_opts)
cpp_logger_dep = cpp_logger_proj.dependency('cpp_logger')

recorder_opts = cmake.subproject_options()
recorder_opts.add_cmake_defines({
    'CMAKE_INSTALL_LIBDIR': get_option('libdir'),
    'CMAKE_INSTALL_PREFIX': get_option('prefix'),
    'RECORDER_INSTALL_LIB_DIR': get_option('libdir'), 
})
recorder_proj = cmake.subproject('recorder', options: recorder_opts)
reader_dep = recorder_proj.dependency('reader')

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
        backward_dep,
        cpp_logger_dep,
        libiberty_dep,
        libstdcxxfs_dep,
        mpi_dep,
        ncurses_dep,
        nlohmann_json_dep,
        parquet_dep,
        reader_dep,
        readline_dep,
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
