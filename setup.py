from setuptools import setup, find_packages

setup(
    name='filedb',
    version='1.0',
    description='File database',
    packages=['filedb'],
    install_requires=['pymongo'],  # TODO find out the minimal working version
)