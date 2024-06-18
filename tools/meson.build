cmake = import('cmake')

backward_cpp_proj = cmake.subproject('backward-cpp')
cpp_logger_proj = cmake.subproject('cpp-logger')
recorder_proj = cmake.subproject('recorder')
recorder_includes = include_directories('lib/recorder/tools')

message('Recorder found: ', recorder_proj.found())

arrow_dep = dependency('arrow')
nlohmann_json_dep = dependency('nlohmann_json')

executable('wisio_recorder2parquet', 
    'recorder2parquet.cpp',
    dependencies: [
        nlohmann_json_dep,
        backward_cpp_proj.dependency('backward'),
        cpp_logger_proj.dependency('cpp-logger'),
        recorder_proj.dependency('recorder'),
        arrow_dep,
    ],
    include_directories: [
        backward_cpp_proj.include_directories('backward'),
        cpp_logger_proj.include_directories('cpp-logger'),
        recorder_proj.include_directories('recorder'),
        recorder_includes,
  ],
)
