import uuid
from typing import Callable
from typing import List
from typing import Optional

from bson import ObjectId
from pymongo.database import Database

from filedb.key import ID
from filedb.key import KEY_BYTES
from filedb.key import Key
from filedb.key import STORAGE_PATH
from filedb.key import key_bytes
from filedb.multiprocessing import MultiprocessingMixin
from filedb.query import expand
from filedb.query import Query


class Index:

    # TODO register key and storage collections for robustness

    def __init__(self, mongo_db: Database):
        self.mongo_db = mongo_db
        self.key_id_collection = self.mongo_db['key_id']
        self.settings_collection = self.mongo_db['settings']
        self.key_id_collection.create_index(KEY_BYTES)

        settings = self.settings_collection.find_one({ID: ObjectId(b'__settings__')})
        if settings and 'index_name' in settings:
            self.name = settings['index_name']
        else:
            self.name = str(uuid.uuid4())
            self.settings_collection.update_one(filter={ID: ObjectId(b'__settings__')},
                                                update={'$set': {'index_name': self.name}},
                                                upsert=True)

    def find(self, query: Query, storage_name: str) -> List[Key]:
        raw_query = expand(query)
        print(raw_query)
        data_collection = self.mongo_db[storage_name]
        return data_collection.find(raw_query, {ID: False, STORAGE_PATH: False})

    def _key_id(self, key: Key) -> Optional[ObjectId]:
        result = self.key_id_collection.find_one({KEY_BYTES: key_bytes(key)})
        return None if result is None else result[ID]

    def storage_path(self, key: Key, storage_name: str) -> Optional[str]:
        key_id = self._key_id(key)
        if key_id is None:
            return None
        data_collection = self.mongo_db[storage_name]
        result = data_collection.find_one({ID: key_id})
        return None if result is None else result[STORAGE_PATH]

    def upsert(self,
               key: Key,
               storage_path: str,
               storage_name: str):

        query = {KEY_BYTES: key_bytes(key)}

        res = self.key_id_collection.update_one(query, {"$setOnInsert": query}, upsert=True)

        if res.upserted_id is not None:
            key_id = res.upserted_id
        else:
            key_id = self.key_id_collection.find_one(query)[ID]

        data_collection = self.mongo_db[storage_name]
        data_collection.update_one({ID: key_id},
                                   {"$set": {STORAGE_PATH: storage_path},
                                    "$setOnInsert": {**key, ID: key_id}},
                                   upsert=True)

    def delete(self, key: Key, storage_name: str):
        key_id = self._key_id(key)

        if key_id is None:
            FileNotFoundError(f'File({key}) does not exist!')

        self.mongo_db[storage_name].delete_one({ID: key_id})


class MPIndex(Index, MultiprocessingMixin):

    def __init__(self, mongo_db_factory: Callable[[], Database]):
        self.mongo_db_factory = mongo_db_factory
        with self.stay_connected():
            super().__init__(self.mongo_db)

    def _setup_connection(self):
        self.mongo_db = self.mongo_db_factory()
        self.key_id_collection = self.mongo_db['key_id']
        self.settings_collection = self.mongo_db['settings']

    def _teardown_connection(self):
        self.mongo_db.client.close()
        self.mongo_db = None
        self.key_id_collection = None
        self.settings_collection = None

    def find(self, query: Query, storage_name: str) -> List[Key]:
        with self.stay_connected():
            return super().find(query, storage_name)

    def _key_id(self, key: Key) -> Optional[ObjectId]:
        with self.stay_connected():
            return super()._key_id(key)

    def storage_path(self, key: Key, storage_name: str) -> Optional[str]:
        with self.stay_connected():
            return super().storage_path(key, storage_name)

    def upsert(self,
               key: Key,
               storage_path: str,
               storage_name: str):
        with self.stay_connected():
            return super().upsert(key, storage_path, storage_name)

    def delete(self, key: Key, storage_name: str):
        with self.stay_connected():
            return super().delete(key, storage_name)
