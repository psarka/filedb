"""
Document false positives for read_lock: with timeout = None, it can raise even though it
was just another writer holding entrance lock
"""
import json
import os
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import fasteners

from filedb import psutil


class FileLocked(Exception):
    def __init__(self, directory):
        super().__init__(f'Cache file {directory} is locked!')


class ReaderWriterLock:

    def __init__(self, directory: Path):
        directory = Path(directory)
        self.directory = directory
        self._entrance_lock = fasteners.InterProcessLock(directory / 'entrance_lock')
        self._write_lock = fasteners.InterProcessLock(directory / 'write_lock')
        self.my_pid = os.getpid()
        self.my_pid_create_time = psutil.pid_create_time(self.my_pid)

    @contextmanager
    def read_lock(self, timeout=None, max_delay=0.1, delay=0.01):

        got = self._entrance_lock.acquire(blocking=timeout != 0,
                                          delay=delay,
                                          max_delay=max_delay,
                                          timeout=timeout)
        if not got:
            raise FileLocked(self.directory)

        got = self._write_lock.acquire(blocking=timeout != 0,
                                       delay=delay,
                                       max_delay=max_delay,
                                       timeout=timeout)
        if not got:
            raise FileLocked(self.directory)

        self._write_lock.release()

        flag = Flag(directory=self.directory,
                    pid=self.my_pid,
                    pid_create_time=self.my_pid_create_time)
        flag.plant()
        self._entrance_lock.release()
        try:
            yield
        finally:
            flag.remove()

    @contextmanager
    def write_lock(self, timeout=None, max_delay=0.1, delay=0.01):
        got = self._entrance_lock.acquire(blocking=timeout != 0,
                                          delay=delay,
                                          max_delay=max_delay,
                                          timeout=timeout)
        if not got:
            raise FileLocked(self.directory)

        got = self._write_lock.acquire(blocking=timeout != 0,
                                       delay=delay,
                                       max_delay=max_delay,
                                       timeout=timeout)
        if not got:
            raise FileLocked(self.directory)

        for existing_flag in Flag.planted_flags(self.directory):

            # my own flag
            if existing_flag.pid == self.my_pid and \
                    existing_flag.pid_create_time == self.my_pid_create_time:
                continue

            # stale flag
            if existing_flag.is_stale():
                existing_flag.remove()
                continue

            # otherwise
            if timeout == 0:
                raise FileLocked(self.directory)
            else:
                waited = 0
                sleep_time = delay
                while (existing_flag.is_planted() and
                       not existing_flag.is_stale() and
                       (timeout is None or waited < timeout)):
                    time.sleep(sleep_time)
                    waited += sleep_time
                    sleep_time = min(max_delay, sleep_time + delay)

        self._entrance_lock.release()
        try:
            yield
        finally:
            self._write_lock.release()


class Flag:

    def __init__(self,
                 directory: Path,
                 pid: int,
                 pid_create_time: int):

        self.directory = Path(directory)
        self.pid = pid
        self.pid_create_time = pid_create_time

    @property
    def path(self):
        return self.directory / f'read_lock_{self.pid}_{self.pid_create_time}'

    def plant(self):
        temp = self.directory / str(uuid.uuid4())
        temp.write_text(json.dumps({'pid': self.pid,
                                    'pid_create_time': self.pid_create_time}))
        temp.replace(self.path)

    @classmethod
    def planted_flags(cls, directory):
        directory = Path(directory)

        flags = []
        for flag_file in directory.glob(f'read_lock*'):
            try:
                info = json.loads(flag_file.read_text())
                flags.append(cls(directory=directory,
                                 pid=info['pid'],
                                 pid_create_time=info['pid_create_time']))
            except FileNotFoundError:
                pass

        return flags

    def is_stale(self):
        return (not psutil.pid_exists(self.pid) or
                psutil.pid_create_time(self.pid) != self.pid_create_time)

    def is_planted(self):
        return self.path.exists()

    def remove(self):
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
