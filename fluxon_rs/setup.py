#!/usr/bin/env python3

import os
import glob
import shutil
from setuptools import setup, find_packages


# Find shared library files
def find_libs():
    libs = []
    lib_dir = "fluxon_pyo3/libs"
    source_lib_dir = "target/cxxpacked/lib"

    # Create the target directory if it does not exist
    if not os.path.exists(lib_dir):
        os.makedirs(lib_dir)

    # If the source directory exists, copy files from it
    if os.path.exists(source_lib_dir):
        print(f"Copying shared libraries from {source_lib_dir} to {lib_dir}")

        # Also copy any other .so files
        for so_file in glob.glob(f"{source_lib_dir}/*.so*"):
            filename = os.path.basename(so_file)
            target_path = os.path.join(lib_dir, filename)
            if not os.path.exists(target_path):
                print(f"  Copy extra shared library {filename}")
                shutil.copy2(so_file, target_path)

    # Return the library file list
    if os.path.exists(lib_dir):
        libs.extend(glob.glob(f"{lib_dir}/*.so*"))
    return [lib.replace("fluxon_pyo3/", "") for lib in libs]


setup(
    name="fluxon_pyo3",
    version="0.2.2",
    description="for export fluxonkv core to python layer",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    author="KV Cache Team",
    python_requires=">=3.10",
    # Package configuration
    packages=find_packages(include=["fluxon_pyo3", "fluxon_pyo3.*"]),
    # Include shared library files
    package_data={
        "fluxon_pyo3": find_libs(),
    },
    include_package_data=True,
    # Classifiers
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Rust",
    ],
    # maturin-related settings
    zip_safe=False,
    # Build requirements
    setup_requires=["maturin>=1.0,<2.0"],
)
