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
    install_requires=[
        'SQLAlchemy==1.3.17',
        'click==7.1.2'
    ],
    entry_points={
        'console_scripts': [
            'compact = compact.cli:main',
            'compact-utils = compact.cli:utils'
        ]
    }
)
