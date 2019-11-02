from typing import Any

from dataclasses import dataclass
from dataclasses import asdict
import logging
import uuid
from contextlib import contextmanager
from typing import IO
from typing import List
from typing import Union

from filedb import cache
from filedb import lock
from filedb.index import Index
from filedb.key import Key
from filedb.key import key_hash
from filedb.query import Query
from filedb.storage import DirectTransportStorage
from filedb.storage import SyncStorage

logger = logging.getLogger(__name__)


@dataclass
class _HandleParams:
    mode: str
    buffering: Any = -1
    encoding: Any = None
    errors: Any = None
    newline: Any = None


class FileDB:
    def __init__(self,
                 index: Index,
                 storage: Union[DirectTransportStorage, SyncStorage]):
        self.index = index
        self.storage = storage

    def find(self, query: Query) -> List['File']:
        return [self.file(key) for key in self.index.find(query, self.storage.name)]

    def file(self, key):
        return File(key,
                    index=self.index,
                    storage=self.storage)


class File:
    def __init__(self,
                 key: Key,
                 index: Index,
                 storage: Union[DirectTransportStorage, SyncStorage]):

        self.key = key
        self.index = index
        self.storage = storage

    def read_text(self,
                  buffering=-1,
                  encoding=None,
                  errors=None,
                  newline=None) -> str:

        with self.open(mode="r",
                       buffering=buffering,
                       encoding=encoding,
                       errors=errors,
                       newline=newline) as f:
            return f.read()

    def write_text(self,
                   data: str,
                   buffering=-1,
                   encoding=None,
                   errors=None,
                   newline=None):

        with self.open(mode="w",
                       buffering=buffering,
                       encoding=encoding,
                       errors=errors,
                       newline=newline) as f:
            f.write(data)

    def read_bytes(self, buffering=-1) -> bytes:
        with self.open(mode="rb", buffering=buffering) as f:
            return f.read()

    def write_bytes(self, data: bytes, buffering=-1):
        with self.open(mode="wb", buffering=buffering) as f:
            f.write(data)

    @contextmanager
    def open(self,
             mode: str = "r",
             buffering=-1,
             encoding=None,
             errors=None,
             newline=None) -> IO:

        handle_params = _HandleParams(mode=mode,
                                      buffering=buffering,
                                      encoding=encoding,
                                      errors=errors,
                                      newline=newline)

        if mode[0] == "r":

            with self._read_handle(handle_params) as file_object:
                yield file_object

        else:
            with self._write_handle(handle_params) as file_object:
                yield file_object

    def copy(self, to: Union[Key, 'File']):

        if isinstance(to, File) and (self.index != to.index or self.storage != to.storage):
            raise NotImplementedError('Copying to a file in a different storage or index is not'
                                      'yet implemented!')

        to_key = to.key if isinstance(to, File) else to
        storage_path_1 = self.index.storage_path(self.key, self.storage.name)
        if storage_path_1 is None:
            raise FileNotFoundError(f"File({self.key}) does not exist!")
        storage_path_2 = str(uuid.uuid4())
        self.storage.copy(storage_path_1, storage_path_2)
        self.index.upsert(to_key, storage_path_2, self.storage.name)

    def move(self, to: Union[Key, 'File']):

        if isinstance(to, File) and (self.index != to.index or self.storage != to.storage):
            raise NotImplementedError('Moving to a file in a different storage or index is not'
                                      'yet implemented!')

        to_key = to.key if isinstance(to, File) else to
        storage_path_1 = self.index.storage_path(self.key, self.storage.name)
        if storage_path_1 is None:
            raise FileNotFoundError(f"File({self.key}) does not exist!")
        storage_path_2 = str(uuid.uuid4())
        self.storage.copy(storage_path_1, storage_path_2)
        self.index.upsert(to_key, storage_path_2, self.storage.name)
        self.index.delete(self.key, self.storage.name)
        self.storage.delete(storage_path_1)

    def delete(self):

        storage_path = self.index.storage_path(self.key, self.storage.name)
        self.index.delete(self.key, self.storage.name)
        self.storage.delete(storage_path)

    def exists(self):

        return self.index.storage_path(self.key, self.storage.name) is not None

    def __eq__(self, other):
        if not isinstance(other, File):
            return NotImplemented
        return (self.key == other.key and
                self.index == other.index and
                self.storage == other.storage)

    def __hash__(self):
        try:
            return self._hash
        except AttributeError:
            self._hash = hash((key_hash(self.key), hash(self.storage), hash(self.index)))
            return self._hash

    def __repr__(self):
        return f'File({self.key})@{self.storage.name}'

    @contextmanager
    def _read_handle(self, handle_params: _HandleParams):

        storage_path = self.index.storage_path(self.key, self.storage.name)
        if storage_path is None:
            raise FileNotFoundError(f"File({self.key}) does not exist!")

        if isinstance(self.storage, DirectTransportStorage):
            with self.storage.read_handle(storage_path, **asdict(handle_params)) as f:
                yield f
        else:
            with self._syncd_read_handle(storage_path=storage_path,
                                         handle_params=handle_params) as f:
                yield f

    @contextmanager
    def _syncd_read_handle(self, storage_path, handle_params):

        try:
            with self.storage.cache.reading_path(storage_path=storage_path,
                                                 index_name=self.index.name,
                                                 storage_name=self.storage.name,
                                                 timeout=None) as path:

                with path.open(**asdict(handle_params)) as f:
                    yield f

        except cache.FileNotCachedError:

            try:
                with self.storage.cache.writing_path(storage_path=storage_path,
                                                     index_name=self.index.name,
                                                     storage_name=self.storage.name,
                                                     timeout=0) as path:

                    self.storage.download(storage_path, path)

            except lock.FileLocked:
                with self._syncd_read_handle(storage_path, handle_params) as f:
                    yield f

            else:
                with self._syncd_read_handle(storage_path, handle_params) as f:
                    yield f

    @contextmanager
    def _write_handle(self, handle_params):

        storage_path = str(uuid.uuid4())

        if isinstance(self.storage, DirectTransportStorage):
            with self.storage.write_handle(storage_path, **asdict(handle_params)) as f:
                yield f
        else:
            with self._syncd_write_handle(storage_path, handle_params) as f:
                yield f

        self.index.upsert(self.key, storage_path, self.storage.name)

    @contextmanager
    def _syncd_write_handle(self, storage_path, handle_params):

        with self.storage.cache.writing_path(storage_path,
                                             index_name=self.index.name,
                                             storage_name=self.storage.name,
                                             timeout=None) as path:
            with path.open(**asdict(handle_params)) as f:
                yield f

        crc32c = self.storage.cache.crc32c(storage_path=storage_path,
                                           storage_name=self.storage.name,
                                           index_name=self.index.name)
        self.storage.upload(path, storage_path, crc32c)
        self.index.upsert(self.key, storage_path, self.storage.name)
