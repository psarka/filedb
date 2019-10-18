import os
import tempfile

from google.cloud import storage
import pytest

from filedb.db import FileDB
from filedb.gcs_fs import GoogleCloudStorageFS

from pymongo import MongoClient


@pytest.fixture(scope="module")
def mongo_db():
    client = MongoClient()
    yield client.test_db
    client.drop_database(client.test_db)


@pytest.fixture
def bucket_name(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "tests/gcs_key.json")
    client = storage.Client()
    bucket = client.get_bucket("puodas")
    yield "puodas"
    for blob in bucket.list_blobs():
        blob.delete()


@pytest.fixture
def cache_path():
    with tempfile.TemporaryDirectory() as path:
        yield path


def test_write_read(mongo_db, bucket_name, cache_path):

    print(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

    db = FileDB(mongo_db=mongo_db, local_cache_path=cache_path, local_cache_size=None)
    fs = GoogleCloudStorageFS(db, bucket_name=bucket_name, path="/")

    fs.file({"a": "1"}).write_text("hi!")
    assert fs.file({"a": "1"}).read_text() == "hi!"
