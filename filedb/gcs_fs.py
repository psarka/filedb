from filedb.db import FileSystem
from filedb.db import FileDB

from google.cloud import storage
from google.cloud import exceptions

from filedb.local_cache import LocalCache


class GoogleCloudStorageFS(FileSystem):
    def __init__(self, file_db: FileDB, path: str, bucket_name: str):

        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)

        self.bucket_name = bucket_name
        self.path = path
        self.file_db = file_db
        self.local_cache: LocalCache = file_db.local_cache

        super().__init__(file_db=file_db)

    def collection_name(self):
        return f"gcs:{self.bucket_name}/{self.path}"

    def delete(self, _id, _local_id):
        try:
            self.bucket.blob(self._gcs_path(_id, _local_id)).delete()
        except exceptions.NotFound:
            pass

    def download(self, _id, _local_id):

        blob = self.bucket.blob(self._gcs_path(_id, _local_id))
        destination = self.temp_handle(_id, _local_id)
        blob.download_to_filename(destination)

    def upload(self, _id, _local_id):
        blob = self.bucket.blob(self._gcs_path(_id, _local_id))
        source = self.temp_handle(_id, _local_id)
        blob.upload_from_filename(str(source))

    def temp_handle(self, _id, _local_id):
        return self.local_cache.path(self.collection_name(), str(_id), str(_local_id))

    def _gcs_path(self, _id, _local_id):
        return f"{self.path}/{_id}_{_local_id}"

    def mark_temp_for_gc(self, _id, _local_id):
        pass
