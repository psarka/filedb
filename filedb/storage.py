class Storage:

    def __init__(self):
        self.name = None

    def copy(self, storage_path_1, storage_path_2):
        pass

    def delete(self, storage_path):
        pass

    def md5(self, storage_path):
        pass

    def download(self, storage_path, cache_path):
        pass

    def upload(self, cache_path, storage_path):
        pass

    def crc32(self, storage_path):
        pass
