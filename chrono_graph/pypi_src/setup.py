# Copyright (c) 2025 Kuaishou Technology
# SPDX-License-Identifier: MIT

from setuptools import setup, find_packages

setup(
    name='chrono_graph',
    version='1.0.0',
    packages=find_packages(exclude=["tests*"]),
    include_package_data=True,
    package_data={
        '': ['libgnn_pybind.so'],
    },
    classifiers=[
        'Programming Language :: Python :: 3.10',
        'Development Status :: 4 - Beta',
    ],
)
