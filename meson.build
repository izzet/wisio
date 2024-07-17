project(
    'wisio',
    ['c', 'cpp'],
    version: '0.0.1',
    default_options: ['buildtype=debugoptimized', 'cpp_std=c++17', 'warning_level=1'],
)

cc = meson.get_compiler('c')
cpp = meson.get_compiler('cpp')

py = import('python').find_installation(pure: false)
py_dep = py.dependency()

subdir('tools')
subdir('wisio')
