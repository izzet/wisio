project(
    'wisio',
    ['c', 'cpp'],
    version: '0.0.6',
    default_options: ['buildtype=debugoptimized', 'cpp_std=c++17', 'warning_level=3'],
)

cc = meson.get_compiler('c')
cpp = meson.get_compiler('cpp')

py = import('python').find_installation(pure: false)
py_dep = py.dependency()

subdir('wisio')

if get_option('enable_tools')
    subdir('tools')
endif