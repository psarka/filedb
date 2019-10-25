import tempfile
from contextlib import contextmanager
from pathlib import Path

import boto3
import pytest
import toml
from google.cloud import storage
from pymongo import MongoClient

from filedb.cache import Cache
from filedb.db import FileDB
from filedb.index import Index
from filedb.storage import GoogleCloudStorage
from filedb.storage import LocalStorage
from filedb.storage import S3

env = toml.loads((Path(__file__).resolve().parent / 'env.toml').read_text())


@contextmanager
def temp_mongo_db():
    mongo_client = MongoClient(host=env['mongo']['host'],
                               port=env['mongo']['port'])
    try:
        yield mongo_client.test_db
    finally:
        mongo_client.drop_database(mongo_client.test_db)


@contextmanager
def temp_s3_bucket():
    s3_resource = boto3.resource('s3',
                                 aws_access_key_id=env['s3']['access_key_id'],
                                 aws_secret_access_key=env['s3']['secret_access_key'],
                                 region_name='eu-central-1')
    bucket = s3_resource.Bucket('d5494cbb-6484-4300-8483-82d7d7f550a7')
    bucket.create(CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})
    try:
        yield bucket
    finally:
        bucket.objects.all().delete()
        bucket.delete()


@contextmanager
def temp_gcs_bucket():
    secret_json = env['google_cloud_storage']['GOOGLE_APPLICATION_CREDENTIALS']
    gsc_client = storage.Client.from_service_account_json(secret_json)
    bucket = gsc_client.bucket('d5494cbb-6484-4300-8483-82d7d7f550a7')
    bucket.create(location='europe-west1')
    try:
        yield bucket
    finally:
        bucket.delete()


@contextmanager
def local():
    with temp_mongo_db() as mongo_db:
        with tempfile.TemporaryDirectory() as local_storage_path:
            yield FileDB(index=Index(mongo_db=mongo_db),
                         storage=LocalStorage('test_machine', local_storage_path))


@contextmanager
def s3():
    with temp_mongo_db() as mongo_db:
        with temp_s3_bucket() as bucket:
            with tempfile.TemporaryDirectory() as cache_path:
                yield FileDB(index=Index(mongo_db=mongo_db),
                             storage=S3(bucket,
                                        cache=Cache(cache_path)))


@contextmanager
def gcs():
    with temp_mongo_db() as mongo_db:
        with temp_gcs_bucket() as bucket:
            with tempfile.TemporaryDirectory() as cache_path:
                yield FileDB(index=Index(mongo_db=mongo_db),
                             storage=GoogleCloudStorage(bucket,
                                                        cache=Cache(cache_path)))


@pytest.mark.parametrize("db_factory", [local, s3, gcs])
def test_write_read(db_factory):
    with db_factory() as db:
        db.file({'a': '1'}).write_text('hi!')
        assert db.file({'a': '1'}).read_text() == 'hi!'
        assert db.file({'a': '1'}).exists()
        assert db.find({}) == [db.file({'a': '1'})]

        db.file({'a': '1'}).copy({'b': '2'})
        assert db.file({'b': '2'}).read_text() == 'hi!'
        assert db.file({'b': '2'}).exists()
        assert set(db.find({})) == {db.file({'a': '1'}), db.file({'b': '2'})}
        assert db.find({'a': '1'}) == [db.file({'a': '1'})]

        db.file({'b': '2'}).move({'b': '3'})
        assert db.file({'b': '3'}).read_text() == 'hi!'
        assert db.file({'b': '3'}).exists()
        assert set(db.find({})) == {db.file({'a': '1'}), db.file({'b': '3'})}

        db.file({'a': '1'}).delete()
        assert not db.file({'a': '1'}).exists()
        assert db.find({}) == [db.file({'b': '3'})]

        db.file({'b': '3'}).delete()
        assert not db.file({'b': '3'}).exists()
        assert db.find({}) == []
