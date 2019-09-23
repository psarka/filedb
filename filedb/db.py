import logging
import typing
import uuid
from contextlib import contextmanager
from typing import List

from filedb.cache import Cache
from filedb.index import Index
from filedb.key import Key
from filedb.query import Query
from filedb.storage import Storage

logger = logging.getLogger(__name__)


class FileDB:
    def __init__(self,
                 index: Index,
                 storage: Storage,
                 cache: Cache):

        self.index = index
        self.storage = storage
        self.cache = cache

    def find(self, query: Query) -> List[Key]:
        return self.index.find(query)

    def read_text(self,
                  key: Key,
                  buffering=-1,
                  encoding=None,
                  errors=None,
                  newline=None) -> str:

        with self.open(key,
                       mode="r",
                       buffering=buffering,
                       encoding=encoding,
                       errors=errors,
                       newline=newline) as f:
            return f.read()

    def write_text(self,
                   key: Key,
                   data: str,
                   buffering=-1,
                   encoding=None,
                   errors=None,
                   newline=None):

        with self.open(key,
                       mode="w",
                       buffering=buffering,
                       encoding=encoding,
                       errors=errors,
                       newline=newline) as f:
            f.write(data)

    def read_bytes(self, key: Key, buffering=-1) -> bytes:
        with self.open(key, mode="rb", buffering=buffering) as f:
            return f.read()

    def write_bytes(self, key: Key, data: bytes, buffering=-1):
        with self.open(key, mode="wb", buffering=buffering) as f:
            f.write(data)

    @contextmanager
    def open(self,
             key: Key,
             mode: str = "r",
             buffering=-1,
             encoding=None,
             errors=None,
             newline=None) -> typing.IO:

        if mode[0] == "r":

            with self._read_handle(key,
                                   mode,
                                   buffering=buffering,
                                   encoding=encoding,
                                   errors=errors,
                                   newline=newline) as file_object:
                yield file_object

        else:
            with self._write_handle(key,
                                    mode,
                                    buffering=buffering,
                                    encoding=encoding,
                                    errors=errors,
                                    newline=newline) as file_object:
                yield file_object

    def copy(self, key_1: Key, key_2: Key):

        storage_path_1 = self.index.storage_path(key_1)
        if storage_path_1 is None:
            raise FileNotFoundError(f"File({key_1}) does not exist!")
        storage_path_2 = uuid.uuid4()
        self.storage.copy(storage_path_1, storage_path_2)
        self.index.upsert(key_2, storage_path_2)

    def move(self, key_1: Key, key_2: Key):

        storage_path_1 = self.index.storage_path(key_1)
        if storage_path_1 is None:
            raise FileNotFoundError(f"File({key_1}) does not exist!")
        storage_path_2 = uuid.uuid4()
        self.storage.copy(storage_path_1, storage_path_2)
        self.index.upsert(key_2, storage_path_2)
        self.index.delete(key_1)
        self.storage.delete(storage_path_1)

    def delete(self, key: Key):

        storage_path = self.index.storage_path(key)
        self.index.delete(key)
        self.storage.delete(storage_path)

    @contextmanager
    def _read_handle(self,
                     key: Key,
                     mode: str,
                     buffering=-1,
                     encoding=None,
                     errors=None,
                     newline=None):

        storage_path = self.index.storage_path(key)
        if storage_path is None:
            raise FileNotFoundError(f"File({key}) does not exist!")

        cache_path = self.cache.path(storage_path, index_name=self.index.name, storage_name=self.storage.name)
        with self.cache.read_lock(cache_path):
            if not cache_path.exists() or self.cache.crc32(cache_path) != self.storage.crc32(storage_path):
                with self.cache.write_lock(cache_path):
                    self.storage.download(storage_path, cache_path)

            with cache_path.open(mode,
                                 buffering=buffering,
                                 encoding=encoding,
                                 errors=errors,
                                 newline=newline) as f:
                yield f

    @contextmanager
    def _write_handle(self,
                      key: Key,
                      mode: str,
                      buffering=-1,
                      encoding=None,
                      errors=None,
                      newline=None):

        storage_path = uuid.uuid4()
        cache_path = self.cache.path(storage_path, index_name=self.index.name, storage_name=self.storage.name)
        with self.cache.read_lock(cache_path):
            with self.cache.write_lock(cache_path):
                with cache_path.open(mode,
                                     buffering=buffering,
                                     encoding=encoding,
                                     errors=errors,
                                     newline=newline) as f:
                    yield f

            self.storage.upload(cache_path, storage_path)
        self.index.upsert(key, storage_path)
