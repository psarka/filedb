import json
import shutil
import sqlite3
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import atomicwrites
from dataclasses import dataclass

from filedb import hash
from filedb.lock import FileLocked
from filedb.lock import ReaderWriterLock

READ = object()
WRITE = object()


class FileNotCachedError(Exception):
    pass


@dataclass
class _CachePaths:
    directory: Path
    data: Path
    crc32c: Path  # doubles as a marker that the write was completed


class Cache:

    def __init__(self,
                 root_path: Path = Path(tempfile.gettempdir()) / 'filedb',
                 size: Optional[float] = shutil.disk_usage(tempfile.gettempdir()).free / 5):
        self.root_path = Path(root_path)
        self.size = size
        self.registry = CacheRegistry(self.root_path, size)

    def _paths(self, storage_path, storage_name, index_name):
        directory = self.root_path.joinpath(index_name, storage_name, storage_path)
        directory.mkdir(parents=True, exist_ok=True)
        return _CachePaths(directory=directory,
                           data=directory / 'data',
                           crc32c=directory / 'crc32c')

    @contextmanager
    def writing_path(self, storage_path, storage_name, index_name, timeout):

        try:
            with ReaderWriterLock(self.root_path).write_lock(timeout=timeout):
                self.registry.cleanup()
        except FileLocked:
            pass  # someone else is cleaning up, ok to proceed

        paths = self._paths(storage_path, storage_name, index_name)

        with ReaderWriterLock(paths.directory).write_lock(timeout=timeout):

            # register intent and mark directory as incomplete
            self.registry.register_write_intent(paths)
            try:
                paths.crc32c.unlink()
            except FileNotFoundError:
                pass

            # allow client to write
            yield paths.data

            # mark as complete and register write
            with atomicwrites.atomic_write(paths.crc32c) as f:
                json.dump(hash.crc32c(paths.data), f)
            self.registry.register_write_complete(paths)
            self.registry.register_access(paths)

    @contextmanager
    def reading_path(self, storage_path, storage_name, index_name, timeout):

        paths = self._paths(storage_path, storage_name, index_name)
        with ReaderWriterLock(paths.directory).read_lock(timeout=timeout):
            if paths.crc32c.exists() and paths.data.exists():
                self.registry.register_access(paths)
                yield paths.data
            else:
                raise FileNotCachedError

    def crc32c(self, storage_path, storage_name, index_name):
        paths = self._paths(storage_path, storage_name, index_name)
        try:
            return json.loads(paths.crc32c.read_text())
        except FileNotFoundError:
            raise FileNotCachedError


class CacheRegistry:

    def __init__(self,
                 cache_root_path: Path,
                 size: Optional[float]):

        self.cache_root_path = cache_root_path
        self.size = size
        self.registry_dir = cache_root_path / 'registry'
        self.registry_db_path = self.registry_dir / 'db.sqlite'

        # initialize, if database does not exist yet
        if not self.registry_db_path.exists():
            with ReaderWriterLock(self.registry_dir).write_lock(timeout=None):
                if not self.registry_db_path.exists():
                    temp_path = self.registry_dir / str(uuid.uuid4())
                    conn = sqlite3.connect(str(temp_path))

                    conn.execute('create table cached_files ('
                                 'path text not null unique, '
                                 'size integer not null, '
                                 'last_access_time integer not null);')

                    conn.execute('create index cached_files_atime '
                                 'on cached_files (last_access_time);')

                    conn.execute('create table pending_files ('
                                 'path text not null, '
                                 'write_start_time integer not null);')

                    conn.commit()
                    conn.close()
                    temp_path.replace(self.registry_db_path)

    def cleanup(self):
        pass

    def register_write_intent(self, paths: _CachePaths):

        with ReaderWriterLock(self.registry_dir).write_lock(timeout=None):
            conn = sqlite3.connect(str(self.registry_db_path))
            conn.execute('insert into pending_files values (?, ?)',
                         (str(paths.directory), int(time.time())))
            conn.commit()
            conn.close()

    def register_write_complete(self, paths: _CachePaths):

        with ReaderWriterLock(self.registry_dir).write_lock(timeout=None):
            conn = sqlite3.connect(str(self.registry_db_path))
            size = paths.data.stat().st_size
            # TODO there must be a safer way
            script = f"""
             begin transaction;
             insert into cached_files values ("{paths.directory}", {size}, {int(time.time())});
             delete from pending_files where path = "{paths.directory}";
             commit;
            """
            conn.executescript(script)
            conn.commit()
            conn.close()

    def register_access(self, paths: _CachePaths):
        with ReaderWriterLock(self.registry_dir).write_lock(timeout=None):
            conn = sqlite3.connect(str(self.registry_db_path))
            conn.execute('update cached_files set last_access_time = ? where path = ?;',
                         (int(time.time()), str(paths.directory)))
            conn.commit()
            conn.close()
