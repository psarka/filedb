from typing import List

from filedb.key import Key
from filedb.query import Query


class Index:
    ...

    def __init__(self):
        self.name = None

    def find(self, query: Query) -> List[Key]:

        return [self.file(k) for k in self.data_collection.find(query)]
        pass

    def storage_path(self, key):
        pass

    def upsert(self, key, storage_path):
        pass

    def delete(self, key):
        pass
