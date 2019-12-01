from pathlib import Path
from setuptools import setup

setup(
    name='pyfiledb',
    version='0.1',
    author='Paulius Šarka',
    author_email='paulius.sarka@gmail.com',
    description='File database',
    long_description=Path('README.md').read_text(),
    long_description_content_type='text/markdown',
    url='https://github.com/psarka/filedb',
    packages=['filedb'],
    classifiers=["Programming Language :: Python :: 3",
                 "License :: OSI Approved :: MIT License",
                 "Operating System :: OS Independent"],
    install_requires=['pymongo',
                      'google-cloud-storage',
                      'pstuil;platform_system=="Windows"'],
    # TODO find out the minimal working versions
    python_requires='>=3.6'
)
