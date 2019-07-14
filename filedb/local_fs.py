import os
from pathlib import Path

from filedb.db import FileSystem
from filedb.db import FileDB


class LocalFS(FileSystem):
    def __init__(self, file_db: FileDB, name: str, path: os.PathLike):

        self.name = name
        self.path = Path(path).resolve()
        super().__init__(file_db=file_db)

    def collection_name(self):
        return f"local:{self.name}/{self.path}"

    def delete(self, _id, _local_id):
        try:
            self._local_path(_id, _local_id).unlink()
        except FileNotFoundError:
            pass

    def download(self, _id, _local_id):
        pass

    def upload(self, _id, _local_id):
        pass

    def temp_handle(self, _id, _local_id):
        return self._local_path(_id, _local_id)

    def _local_path(self, _id, _local_id):
        return self.path / str(_id)[:2] / f"{str(_id)[2:]}_{_local_id}"

    def mark_temp_for_gc(self, _id, _local_id):
        pass
