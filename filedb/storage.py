import shutil
from abc import ABC
from abc import abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Union

import boto3
from google.cloud import storage
from google.cloud.storage import Bucket

from filedb.cache import Cache
from filedb.hash import crc32c
from filedb.multiprocessing import MultiprocessingMixin


class Storage(ABC):

    def __init__(self, name):
        self.name = name

    @abstractmethod
    def copy(self, storage_path_1, storage_path_2):
        pass

    @abstractmethod
    def delete(self, storage_path):
        pass

    @abstractmethod
    def crc32c(self, storage_path):
        pass


class DirectTransportStorage(Storage):

    @abstractmethod
    def read_handle(self,
                    storage_path,
                    mode: str,
                    buffering=-1,
                    encoding=None,
                    errors=None,
                    newline=None):
        pass

    @abstractmethod
    def write_handle(self,
                     storage_path,
                     mode: str,
                     buffering=-1,
                     encoding=None,
                     errors=None,
                     newline=None):
        pass


class SyncStorage(Storage):

    def __init__(self, name: str, cache: Cache):
        self.cache = cache
        super().__init__(name=name)

    @abstractmethod
    def download(self, storage_path, cache_path):
        pass

    @abstractmethod
    def upload(self, cache_path, storage_path, file_hash):
        pass


# TODO store keys also
class GoogleCloudStorage(SyncStorage):

    def __init__(self,
                 bucket: Union[str, Bucket],
                 prefix: str = '',
                 delimiter: str = '/',
                 cache: Cache = Cache()):
        self.prefix = prefix
        self.delimiter = delimiter

        if isinstance(bucket, str):
            self.bucket = storage.Client().get_bucket(bucket)
        else:
            self.bucket = bucket

        self.gs_uri = f'gs://{bucket.name}{delimiter}{prefix}{delimiter}'

        super().__init__(name=self.gs_uri,
                         cache=cache)

    def _bucket_path(self, storage_path):
        return f'{self.prefix}{self.delimiter}{storage_path}'
        # TODO maybe split into "folders"

    def copy(self, storage_path_1, storage_path_2):
        source_blob = self.bucket.blob(self._bucket_path(storage_path_1))
        self.bucket.copy_blob(source_blob, self.bucket, self._bucket_path(storage_path_2))

    def delete(self, storage_path):
        self.bucket.blob(self._bucket_path(storage_path)).delete()

    def download(self, storage_path, cache_path):
        self.bucket.blob(self._bucket_path(storage_path)).download_to_filename(cache_path)

    def upload(self, cache_path, storage_path, file_hash):
        blob = self.bucket.blob(self._bucket_path(storage_path))
        blob.crc32c = file_hash
        blob.upload_from_filename(str(cache_path))

    def crc32c(self, storage_path):
        return self.bucket.blob(self._bucket_path(storage_path)).crc32c


class MPGoogleCloudStorage(GoogleCloudStorage, MultiprocessingMixin):

    def __init__(self,
                 bucket_factory: Callable[[], Bucket],
                 prefix: str = '',
                 delimiter: str = '/',
                 cache: Cache = Cache()):
        self.bucket_factory = bucket_factory
        with self.stay_connected():
            super().__init__(bucket=self.bucket,
                             cache=cache,
                             prefix=prefix,
                             delimiter=delimiter)

    def _setup_connection(self):
        self.bucket = self.bucket_factory()

    def _teardown_connection(self):
        self.bucket = None

    def copy(self, storage_path_1, storage_path_2):
        with self.stay_connected():
            super().copy(storage_path_1, storage_path_2)

    def delete(self, storage_path):
        with self.stay_connected():
            super().delete(storage_path)

    def download(self, storage_path, cache_path):
        with self.stay_connected():
            super().download(storage_path, cache_path)

    def upload(self, cache_path, storage_path, file_hash):
        with self.stay_connected():
            super().upload(cache_path, storage_path, file_hash)

    def crc32c(self, storage_path):
        with self.stay_connected():
            return super().crc32c(storage_path)


# boto3 classes are created during runtime, so we can't use them for type annotations
# Thus, this annotation is for humans only
S3Bucket = Any


# TODO store keys also
class S3(SyncStorage):

    def __init__(self,
                 bucket: Union[str, S3Bucket],
                 prefix: str = '',
                 delimiter: str = '/',
                 cache: Cache = Cache()):
        self.prefix = prefix
        self.delimiter = delimiter
        self.bucket = bucket

        if isinstance(bucket, str):
            self.bucket = boto3.resource('s3').Bucket(bucket)
        else:
            self.bucket = bucket

        self.s3_uri = f's3://{self.bucket.name}{delimiter}{prefix}{delimiter}'

        super().__init__(name=self.s3_uri, cache=cache)

    def _bucket_path(self, storage_path):
        return f'{self.prefix}{self.delimiter}{storage_path}'
        # TODO maybe split into "folders"

    def copy(self, storage_path_1, storage_path_2):
        self.bucket.copy(CopySource={'Bucket': self.bucket.name,
                                     'Key': self._bucket_path(storage_path_1)},
                         Key=self._bucket_path(storage_path_2))

    def delete(self, storage_path):
        self.bucket.Object(key=self._bucket_path(storage_path)).delete()

    def download(self, storage_path, cache_path):
        self.bucket.download_file(Key=self._bucket_path(storage_path),
                                  Filename=str(cache_path))

    def upload(self, cache_path, storage_path, file_hash):
        self.bucket.upload_file(Filename=str(cache_path),
                                Key=self._bucket_path(storage_path),
                                ExtraArgs={"Metadata": {"crc32c": file_hash}})

    def crc32c(self, storage_path):
        return self.bucket.Object(key=self._bucket_path(storage_path)).metadata['crc32c']
        # TODO this may fail, maybe raise a more informative error


class MPS3(S3, MultiprocessingMixin):

    def __init__(self,
                 bucket_factory: Callable[[], S3Bucket],
                 prefix: str = '',
                 delimiter: str = '/',
                 cache: Cache = Cache()):
        self.bucket_factory = bucket_factory
        with self.stay_connected():
            super().__init__(bucket=self.bucket,
                             prefix=prefix,
                             delimiter=delimiter,
                             cache=cache)

    def _setup_connection(self):
        self.bucket = self.bucket_factory()

    def _teardown_connection(self):
        self.bucket = None

    def copy(self, storage_path_1, storage_path_2):
        with self.stay_connected():
            super().copy(storage_path_1, storage_path_2)

    def delete(self, storage_path):
        with self.stay_connected():
            super().delete(storage_path)

    def download(self, storage_path, cache_path):
        with self.stay_connected():
            super().download(storage_path, cache_path)

    def upload(self, cache_path, storage_path, file_hash):
        with self.stay_connected():
            super().upload(cache_path, storage_path, file_hash)

    def crc32c(self, storage_path):
        with self.stay_connected():
            return super().crc32c(storage_path)


class LocalStorage(DirectTransportStorage):

    def __init__(self,
                 machine_name: str,
                 path: Path):
        self.machine_name = machine_name
        self.path = Path(path)
        self.uri = f'machine://{machine_name}/{path}'

        super().__init__(self.uri)

    def _file_path(self, storage_path):
        return self.path / storage_path[:2] / storage_path[2:]

    @contextmanager
    def read_handle(self,
                    storage_path,
                    mode: str,
                    buffering=-1,
                    encoding=None,
                    errors=None,
                    newline=None):
        with self._file_path(storage_path).open(mode=mode,
                                                buffering=buffering,
                                                encoding=encoding,
                                                errors=errors,
                                                newline=newline) as f:
            yield f

    # TODO writing should be atomic, if possible
    # TODO raise and catch outside for more informative error
    @contextmanager
    def write_handle(self,
                     storage_path,
                     mode: str,
                     buffering=-1,
                     encoding=None,
                     errors=None,
                     newline=None):
        path = self._file_path(storage_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open(mode=mode,
                       buffering=buffering,
                       encoding=encoding,
                       errors=errors,
                       newline=newline) as f:
            yield f

    # TODO raise and catch outside for more informative error
    def copy(self, storage_path_1, storage_path_2):
        path_1 = self._file_path(storage_path_1)
        path_2 = self._file_path(storage_path_2)
        path_2.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(path_1, path_2)

    # TODO raise and catch outside for more informative error
    def delete(self, storage_path):
        self._file_path(storage_path).unlink()

    # TODO cache crc32c value on disk
    def crc32c(self, storage_path):
        return crc32c(self._file_path(storage_path))
