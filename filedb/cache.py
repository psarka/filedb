import itertools
import json
import os
import time
import zlib
from contextlib import contextmanager
from pathlib import Path
from typing import Optional
from filedb import psutil


class FileLockedException(Exception):
    pass


class Cache:

    def __init__(self,
                 root_path: Path,
                 size: Optional[float]):
        self.root_path = root_path
        self.size = size

    def path(self, storage_path, storage_name, index_name):
        path_ = self.root_path.joinpath(index_name, storage_name, storage_path, 'data')
        path_.parent.mkdir(parents=True, exist_ok=True)
        return path_

    @contextmanager
    def read_lock(self, cache_path: Path):
        pid = os.getpid()
        pid_create_time = psutil.pid_create_time(pid)
        directory = cache_path.parent
        lock_path = directory / f'read_lock_{pid}_{pid_create_time}_{time.time()}'

        directory.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(json.dumps({'pid': pid,
                                         'pid_create_time': pid_create_time}))

        for existing_lock in directory.glob('write_lock*'):
            lock_info = json.loads(existing_lock.read_text())
            pid = lock_info['pid']
            if psutil.pid_exists(pid) and pid_create_time(pid) == lock_info['pid_create_time']:
                lock_path.unlink()
                raise FileLockedException(f'Cache file {cache_path} is write-locked by a process {pid}! If you think'
                                          f'that the lock is stale, delete the lock file {existing_lock} manually.')

        yield

        lock_path.unlink()

    @contextmanager
    def write_lock(self, cache_path):
        pid = os.getpid()
        pid_create_time = psutil.pid_create_time(pid)
        directory = cache_path.parent
        lock_path = directory / f'write_lock_{pid}_{pid_create_time}_{time.time()}'

        directory.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(json.dumps({'pid': pid,
                                         'pid_create_time': pid_create_time}))

        for existing_lock in itertools.chain(directory.glob('write_lock*'),
                                             directory.glob('read_lock*')):

            if existing_lock == lock_path:
                continue

            lock_info = json.loads(existing_lock.read_text())
            pid = lock_info['pid']
            if psutil.pid_exists(pid) and pid_create_time(pid) == lock_info['pid_create_time']:
                lock_path.unlink()
                raise FileLockedException(f'Cache file {cache_path} is locked by a process {pid}! If you think'
                                          f'that the lock is stale, delete the lock file {existing_lock} manually.')

        yield

        lock_path.unlink()

    def crc32(self, cache_path):
        with self.read_lock(cache_path):
            prev = 0
            for line in open(cache_path, 'rb'):
                prev = zlib.crc32(line, prev)
            return "%X" % (prev & 0xFFFFFFFF)
