from setuptools import setup

setup(
    name="filedb",
    version="1.0",
    description="File database",
    packages=["filedb"],
    install_requires=["pymongo",
                      "google-cloud-storage",
                      "pstuil;platform_system=='Windows'"],
    # TODO find out the minimal working versions
)
