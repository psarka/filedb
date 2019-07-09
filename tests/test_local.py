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


def test_write_read(mongo_db):

    with tempfile.TemporaryDirectory() as root_path:

        db = FileDB(mongo_db=mongo_db)
        fs = LocalFS(db, root_path)

        fs.file({"a": "1"}).write_text("hi!")
        assert fs.file({"a": "1"}).read_text() == "hi!"
