import tempfile
from contextlib import contextmanager
from pathlib import Path

import boto3
import toml
from google.cloud import storage
from pymongo import MongoClient

from filedb.cache import Cache
from filedb.db import FileDB
from filedb.index import Index
from filedb.index import MPIndex
from filedb.storage import GoogleCloudStorage
from filedb.storage import LocalStorage
from filedb.storage import MPGoogleCloudStorage
from filedb.storage import MPS3
from filedb.storage import S3

env = toml.loads((Path(__file__).resolve().parent / 'env.toml').read_text())


@contextmanager
def temp_mongo_db_factory():
    def mongo_db_factory():
        mongo_client = MongoClient(host=env['mongo']['host'],
                                   port=env['mongo']['port'])
        return mongo_client.test_db

    try:
        yield mongo_db_factory
    finally:
        db = mongo_db_factory()
        db.client.drop_database(db)


@contextmanager
def temp_s3_bucket_factory():
    def s3_bucket_factory():
        s3_resource = boto3.resource('s3',
                                     aws_access_key_id=env['s3']['access_key_id'],
                                     aws_secret_access_key=env['s3']['secret_access_key'],
                                     region_name='eu-central-1')
        return s3_resource.Bucket('d5494cbb-6484-4300-8483-82d7d7f550a7')

    bucket = s3_bucket_factory()
    bucket.create(CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})
    try:
        yield s3_bucket_factory
    finally:
        bucket = s3_bucket_factory()
        bucket.objects.all().delete()
        bucket.delete()


@contextmanager
def temp_gcs_bucket_factory():
    def gcs_bucket_factory():
        secret_json = env['google_cloud_storage']['GOOGLE_APPLICATION_CREDENTIALS']
        gsc_client = storage.Client.from_service_account_json(secret_json)
        return gsc_client.bucket('d5494cbb-6484-4300-8483-82d7d7f550a7')

    bucket = gcs_bucket_factory()
    bucket.create(location='europe-west1')
    try:
        yield gcs_bucket_factory
    finally:
        bucket.delete(force=True)


@contextmanager
def local():
    with temp_mongo_db_factory() as mongo_db_factory:
        with tempfile.TemporaryDirectory() as local_storage_path:
            yield FileDB(index=Index(mongo_db=mongo_db_factory()),
                         storage=LocalStorage('test_machine', local_storage_path))


@contextmanager
def local_mp():
    with temp_mongo_db_factory() as mongo_db_factory:
        with tempfile.TemporaryDirectory() as local_storage_path:
            yield FileDB(index=MPIndex(mongo_db_factory=mongo_db_factory),
                         storage=LocalStorage('test_machine', local_storage_path))


@contextmanager
def s3():
    with temp_mongo_db_factory() as mongo_db_factory:
        with temp_s3_bucket_factory() as bucket_factory:
            with tempfile.TemporaryDirectory() as cache_path:
                yield FileDB(index=Index(mongo_db=mongo_db_factory()),
                             storage=S3(bucket_factory(),
                                        cache=Cache(cache_path)))


@contextmanager
def s3_mp():
    with temp_mongo_db_factory() as mongo_db_factory:
        with temp_s3_bucket_factory() as bucket_factory:
            with tempfile.TemporaryDirectory() as cache_path:
                yield FileDB(index=MPIndex(mongo_db_factory=mongo_db_factory),
                             storage=MPS3(bucket_factory=bucket_factory,
                                          cache=Cache(cache_path)))


@contextmanager
def gcs():
    with temp_mongo_db_factory() as mongo_db_factory:
        with temp_gcs_bucket_factory() as bucket_factory:
            with tempfile.TemporaryDirectory() as cache_path:
                yield FileDB(index=Index(mongo_db=mongo_db_factory()),
                             storage=GoogleCloudStorage(bucket_factory(),
                                                        cache=Cache(cache_path)))


@contextmanager
def gcs_mp():
    with temp_mongo_db_factory() as mongo_db_factory:
        with temp_gcs_bucket_factory() as bucket_factory:
            with tempfile.TemporaryDirectory() as cache_path:
                yield FileDB(index=MPIndex(mongo_db_factory=mongo_db_factory),
                             storage=MPGoogleCloudStorage(bucket_factory,
                                                          cache=Cache(cache_path)))
