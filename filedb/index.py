from typing import List
from typing import Optional

from bson import ObjectId
from pymongo.database import Database

from filedb.key import KEY_BYTES
from filedb.key import Key
from filedb.key import ID
from filedb.key import STORAGE_PATH
from filedb.key import key_bytes
from filedb.query import Operator
from filedb.query import Query


class Index:

    # TODO register key and storage collections for robustness
    # TODO late connection for multiprocessing

    def __init__(self, mongo_db: Database):
        self.name = None
        self.mongo_db = mongo_db
        self.key_id_collection = mongo_db['key_id']
        self.key_id_collection.create_index(KEY_BYTES)

    def find(self, query: Query, storage_name: str) -> List[Key]:
        raw_query = {k: op.value if isinstance(op, Operator) else op
                     for k, op in query.items()}

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

        query = {**key, ID: key_id, STORAGE_PATH: storage_path}
        data_collection = self.mongo_db[storage_name]
        data_collection.update_one(query, {"$set": query}, upsert=True)

    def delete(self, key: Key, storage_name: str):
        key_id = self._key_id(key)

        if key_id is None:
            FileNotFoundError(f'File({key}) does not exist!')

        self.mongo_db[storage_name].delete_one({ID: key_id})
