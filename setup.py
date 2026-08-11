#!/usr/bin/env python3
"""
Setup script for fluxon
"""

from setuptools import setup, find_packages
import os

# Read version from __init__.py
def get_version():
    init_file = os.path.join(os.path.dirname(__file__), 'fluxon_py', '__init__.py')
    with open(init_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip('"\'')
    return "0.2.2"

# Read README if exists
def get_long_description():
    readme_file = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_file):
        with open(readme_file, 'r', encoding='utf-8') as f:
            return f.read()
    return (
        "Fluxon is a high-performance distributed cache and message queue system for AI workloads. "
        "This package provides the Python client APIs and service/monitoring utilities."
    )

# Dynamic install_requires based on environment
def get_install_requires():
    base_requires = [
        'PyYAML>=6.0', 
        'tenacity==6.1.0',
        'etcd3==0.12.0', 
        'protobuf<=3.20.4', 
        'msgpack',
        'Flask',
        # Required by Mooncake backend (used for RW locks)
        'readerwriterlock',
        ]
    
    return base_requires

_extras = {
    'dev': ['pytest', 'black', 'flake8', 'memray', 'pyinstrument'],
    'mooncake': ['readerwriterlock'],
    'all': ['readerwriterlock'],
}

setup(
    name='fluxon-py',
    version=get_version(),
    description='Fluxon - high-performance distributed cache and message queue for AI workloads',
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    author='TeleAI Infra Team',
    author_email='',
    url='',
    packages=find_packages(include=["fluxon_py", "fluxon_py.*"]),
    python_requires='>=3.10',
    install_requires=get_install_requires(),
    extras_require=_extras,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    entry_points={
        'console_scripts': [
            'kvcache-server=fluxon_py.__main__:main',
        ],
    },
    include_package_data=True,
    zip_safe=False,
) 
