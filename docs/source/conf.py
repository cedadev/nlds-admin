# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

sys.path.insert(0, os.path.abspath('../../'))

from nlds_admin import __version__

project = 'Near-Line Data Store Admin'
copyright = '2025, Centre for Environmental Data Analysis, Science and Technologies Facilities Council, UK Research and Innovation'
author = 'William Cross and Neil Massey'
version = __version__
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx_click']

templates_path = ['_templates']
exclude_patterns = []
html_favicon = '_images/icon-black.png'



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

html_logo = "_images/nlds.png"
html_theme_options = {
    'logo_only': False,
    'display_version': True,
    'collapse_navigation': False,
    'navigation_depth': 4,
}