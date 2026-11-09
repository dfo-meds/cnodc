import pathlib
import sys
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'CNODC Data Management Tools'
copyright = '2026, Erin Turnbull'
author = 'Erin Turnbull'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# my source files
sys.path.append(str(pathlib.Path(__file__).resolve().absolute().parent.parent / "src"))


extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.apidoc",
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
apidoc_modules = [
    {"path": f"../src/{x}", "destination": f"src/api/{x}"}
    for x in [
        "dmd",
        "gcapp",
        "gcclick",
        "gcflask",
        "medsutil",
        "medweb",
        "nodb",
        "pipeman",
        "pipeman_cli",
        "pipeman_desktop",
        "pipeman_service",
        "tkintermapview",
    ]
]



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
