# encoding: utf-8
"""
setup.py
"""
__author__ = "William Cross and Neil Massey"
__date__ = "06 Mar 2025"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import os
from setuptools import setup, find_packages
from nlds_admin import __version__

with open(os.path.join(os.path.dirname(__file__), "README.md")) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name="nlds-admin",
    version=__version__,
    packages=find_packages(),
    install_requires=[
        'decorator',
        'pika',
        'py',
        'retry',
        'click'
    ],
    include_package_data=True,
    package_data={
    },
    license="LICENSE.txt",  # example license
    description=("Admin tool for CEDA Near-Line Data Store"),
    long_description=README,
    url="http://www.ceda.ac.uk/",
    author="William Cross & Neil Massey",
    author_email="neil.massey@stfc.ac.uk",
    classifiers=[
        "Environment :: Web Environment",
        "Framework :: RestAPI",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
    ],
    entry_points = {
        'console_scripts': ['nlds-admin=nlds_admin.nlds_admin:main',
                            'nlds-qm=nlds_admin.nlds_qm:main'],
    }
)
