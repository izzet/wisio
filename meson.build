project(
    'WisIO', 
    ['c', 'cpp'],
    version: '0.0.1', 
    default_options: ['buildtype=debugoptimized', 'cpp_std=c++17', 'warning_level=1'],
    subproject_dir: 'tools/lib',
)

add_project_arguments(['-lstdc++fs'], language: 'cpp')
# add_project_link_arguments(['-lstdc++fs'], language: 'cpp')

cpp = meson.get_compiler('cpp')

py = import('python').find_installation(pure: false)
py_dep = py.dependency()

subdir('tools')
subdir('wisio')
