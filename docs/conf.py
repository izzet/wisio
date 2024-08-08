# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'WisIO'
copyright = '2024, Izzet Yildirim'
author = 'Izzet Yildirim'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['myst_parser']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_favicon = '_static/icon.png'
html_logo = '_static/logo.png'
html_static_path = ['_static']
html_theme = 'furo'
html_theme_options = {
    'sidebar_hide_name': True,
    'source_repository': 'https://github.com/izzet/wisio/',
    'source_branch': 'main',
    'source_directory': 'docs/',
}

# myst_parser configuration
myst_enable_extensions = ['substitution']
myst_substitutions = {'wisio': 'WisIO'}
