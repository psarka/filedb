from abc import ABC
from abc import abstractmethod
from contextlib import contextmanager
import json
import logging
import os
from typing import Any
from typing import Dict
from typing import IO
from typing import List
from typing import Optional
from typing import Union
import uuid

from pymongo.collection import Collection
from pymongo.database import Database as MongoDatabase
from pymongo.errors import DuplicateKeyError

from filedb.local_cache import LocalCache

Query = Dict[str, Any]
Key = Dict[str, str]

logger = logging.getLogger(__name__)


class FileDB:
    def __init__(
        self,
        mongo_db: MongoDatabase,
        local_cache_path: os.PathLike,
        local_cache_size: Optional[float],
    ):

        self.mongo_db = mongo_db
        self.local_cache = LocalCache(root_path=local_cache_path, size=local_cache_size)
        self.ids_collection: Collection = mongo_db.ids

    def key_id(self, key: Key):

        query = {"key": json.dumps(key, sort_keys=True)}

        res = self.ids_collection.update_one(
            query, {"$setOnInsert": query}, upsert=True
        )

        if res.upserted_id is not None:
            return res.upserted_id
        else:
            return self.ids_collection.find_one(query)["_id"]


class FileSystem(ABC):
    def __init__(self, file_db: FileDB):
        self.file_db = file_db
        self.data_collection: Collection = file_db.mongo_db[self.collection_name()]
        logger.debug(
            f"Using collection {self.collection_name()}"
        )  # TODO add collection registration

    def find(self, query: Query) -> List["File"]:
        return [self.file(k) for k in self.data_collection.find(query)]

    def file(self, key: Key) -> "File":
        return File(fs=self, key=key)

    @abstractmethod
    def collection_name(self):
        ...

    @abstractmethod
    def delete(self, _id, _local_id):
        ...

    @abstractmethod
    def download(self, _id, _local_id):
        ...

    @abstractmethod
    def upload(self, _id, _local_id):
        ...

    @abstractmethod
    def temp_handle(self, _id, _local_id):
        ...

    @abstractmethod
    def mark_temp_for_gc(self, _id, _local_id):
        ...


class File:
    def __init__(self, fs: FileSystem, key: Key):
        self.fs: FileSystem = fs
        self.file_db: FileDB = fs.file_db
        self.data_collection: Collection = self.fs.data_collection
        self.key: Key = key

        assert "_id" not in key, "_id is a reserved field!"
        assert "_local_id" not in key, "_local_id is a reserved field!"

    def delete(self):
        _id = self.file_db.key_id(self.key)

        full_key = self.data_collection.find_one({**self.key, "_id": _id})
        if full_key is None:
            return  # already deleted
        else:
            _local_id = full_key["_local_id"]
        self.data_collection.delete_one({"_id": _id})
        self.fs.delete(_id, _local_id)

    def move(self, destination: Union[Key, "File", FileSystem]):
        ...

    def copy(self, destination: Union[Key, "File", FileSystem]):
        ...

    @contextmanager
    def open(
        self,
        mode="r",
        overwrite_existing=False,
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
    ) -> IO:

        if mode not in ["r", "w", "rb", "wb"]:
            raise ValueError(
                f'mode has to be one of "r", "w", "rb" or "wb", got {mode}!'
            )

        if mode[0] == "r":
            with self._read(
                mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
            ) as file_object:
                yield file_object

        else:
            with self._write(
                mode,
                overwrite_existing,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
            ) as file_object:
                yield file_object

    @contextmanager
    def _read(self, mode: str, buffering=-1, encoding=None, errors=None, newline=None):

        _id = self.file_db.key_id(self.key)
        full_key = self.data_collection.find_one({**self.key, "_id": _id})
        if full_key is None:
            raise FileNotFoundError(f"File({self.key}) does not exist!")

        _local_id = full_key["_local_id"]

        # download
        self.fs.download(_id, _local_id)

        # yield handle
        with self.fs.temp_handle(_id, _local_id).open(
            mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline
        ) as f:
            yield f

        # mark temp file for gc
        self.fs.mark_temp_for_gc(_id, _local_id)

    @contextmanager
    def _write(
        self,
        mode: str,
        overwrite_existing: bool,
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
    ):

        # check if does not exist
        if not overwrite_existing and self.exists():
            raise FileExistsError(f"File({self.key}) already exists!")

        # get handle to temp
        _id = self.file_db.key_id(self.key)

        # yield handle
        _local_id = str(uuid.uuid4())
        self.fs.temp_handle(_id, _local_id).parent.mkdir(parents=True, exist_ok=True)
        with self.fs.temp_handle(_id, _local_id).open(
            mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline
        ) as f:
            yield f

        # transfer to final location
        self.fs.upload(_id, _local_id)

        # add key to db
        query = {**self.key, "_id": _id, "_local_id": _local_id}

        if overwrite_existing:
            self.data_collection.update_one(query, {"$set": query}, upsert=True)
        else:
            try:
                self.data_collection.insert_one(query)
            except DuplicateKeyError:
                raise FileExistsError(f"File({self.key}) already exists!")

        # mark temp file for gc
        self.fs.mark_temp_for_gc(_id, _local_id)

    def exists(self) -> bool:
        _id = self.file_db.key_id(self.key)
        query = {**self.key, "_id": _id}
        return self.data_collection.find_one(query) is not None

    def read_text(self, buffering=-1, encoding=None, errors=None, newline=None) -> str:
        with self.open(
            mode="r",
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        ) as f:
            return f.read()

    def write_text(
        self, data: str, buffering=-1, encoding=None, errors=None, newline=None
    ):
        with self.open(
            mode="w",
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        ) as f:
            f.write(data)

    def read_bytes(self, buffering=-1) -> bytes:
        with self.open(mode="rb", buffering=buffering) as f:
            return f.read()

    def write_bytes(self, data: bytes, buffering=-1):
        with self.open(mode="wb", buffering=buffering) as f:
            f.write(data)
