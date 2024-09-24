config_files = run_command(
    'find',
    meson.current_source_dir(),
    '-name', '*.yaml',
    '-type', 'f',
    check: false,
).stdout().strip().split('\n')

config_files += ['__init__.py']

py.install_sources(
    config_files,
    subdir: 'wisio/configs',
    preserve_path: true,
)