from setuptools import setup, find_packages

import pathlib

root_dir = pathlib.Path(__file__).parent

version = None
with open(root_dir / 'VERSION') as f:
    version = f.read()

setup(
    name='Compact',
    version=version,
    packages=find_packages(),
)
