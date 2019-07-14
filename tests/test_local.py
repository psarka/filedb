import tempfile

import pytest

from filedb.db import FileDB
from filedb.local_fs import LocalFS

from pymongo import MongoClient


@pytest.fixture(scope="module")
def mongo_db():
    client = MongoClient()
    yield client.test_db
    client.drop_database(client.test_db)


@pytest.fixture(scope="module")
def local_fs_path():
    with tempfile.TemporaryDirectory() as path:
        yield path


@pytest.fixture(scope="module")
def cache_path():
    with tempfile.TemporaryDirectory() as path:
        yield path


def test_write_read(mongo_db, local_fs_path, cache_path):

    db = FileDB(mongo_db=mongo_db, local_cache_path=cache_path, local_cache_size=None)
    fs = LocalFS(db, name="local", path=local_fs_path)

    fs.file({"a": "1"}).write_text("hi!")
    assert fs.file({"a": "1"}).read_text() == "hi!"
