import itertools
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from filedb import psutil
from filedb.hash import crc32


class FileLockedException(Exception):
    pass


class Cache:

    def __init__(self,
                 root_path: Path,
                 size: Optional[float] = None):
        self.root_path = Path(root_path)
        self.size = size

    def path(self, storage_path, storage_name, index_name):
        path_ = self.root_path.joinpath(index_name, storage_name, storage_path, 'data')
        path_.parent.mkdir(parents=True, exist_ok=True)
        return path_

    @contextmanager
    def read_lock(self, cache_path: Path):
        my_pid = os.getpid()
        my_create_time = psutil.pid_create_time(my_pid)
        directory = cache_path.parent
        lock_path = directory / f'read_lock_{my_pid}_{my_create_time}_{time.time()}'

        directory.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(json.dumps({'pid': my_pid,
                                         'pid_create_time': my_create_time}))

        for existing_lock in directory.glob('write_lock*'):
            lock_info = json.loads(existing_lock.read_text())
            lock_pid = lock_info['pid']
            lock_create_time = lock_info['pid_create_time']

            if my_pid == lock_pid and my_create_time == lock_create_time:
                continue  # my own lock

            if psutil.pid_exists(lock_pid) and psutil.pid_create_time(lock_pid) == lock_create_time:
                lock_path.unlink()
                raise FileLockedException(f'Cache file {cache_path} is write-locked by a process '
                                          f'{lock_pid}! If you think that the lock is stale, '
                                          f'delete the lock file {existing_lock} manually.')

        try:
            yield
        finally:
            lock_path.unlink()

    @contextmanager
    def write_lock(self, cache_path):
        my_pid = os.getpid()
        my_create_time = psutil.pid_create_time(my_pid)
        directory = cache_path.parent
        lock_path = directory / f'write_lock_{my_pid}_{my_create_time}_{time.time()}'

        directory.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(json.dumps({'pid': my_pid,
                                         'pid_create_time': my_create_time}))

        for existing_lock in itertools.chain(directory.glob('write_lock*'),
                                             directory.glob('read_lock*')):

            lock_info = json.loads(existing_lock.read_text())
            lock_pid = lock_info['pid']
            lock_create_time = lock_info['pid_create_time']

            if my_pid == lock_pid and my_create_time == lock_create_time:
                continue  # my own lock

            if psutil.pid_exists(lock_pid) and psutil.pid_create_time(lock_pid) == lock_create_time:
                lock_path.unlink()
                raise FileLockedException(f'Cache file {cache_path} is locked by a process '
                                          f'{lock_pid}! If you think that the lock is stale, '
                                          f'delete the lock file {existing_lock} manually.')

        try:
            yield
        finally:
            lock_path.unlink()

    # TODO cache this to disk
    def crc32(self, cache_path):
        with self.read_lock(cache_path):
            return crc32(cache_path)
