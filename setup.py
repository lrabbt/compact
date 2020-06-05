from setuptools import setup, find_packages

import pathlib

root_dir = pathlib.Path(__file__).parent

version = None
with open(root_dir / 'VERSION') as f:
    version = f.read()

with open(root_dir / 'README.md') as f:
    long_description = f.read()

setup(
    name='compact-lrabbt',
    version=version,
    author='Breno Brandão',
    author_email='lrabbt@gmail.com',
    description='Merges two csv files for unknown reasons',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/lrabbt/compact',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Posix :: Linux',
    ],
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
    },
    python_requires='>=3.8',
)
