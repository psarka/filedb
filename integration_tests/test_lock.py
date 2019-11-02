"""
Adapted from https://github.com/harlowja/fasteners/
"""
import itertools
import os
import random
import tempfile
import time
from multiprocessing import Process
from pathlib import Path

import more_itertools
import pytest
from diskcache import Cache
from diskcache import Deque
# noinspection PyProtectedMember
from pathos.pools import _ProcessPool as ProcessPool

from filedb.lock import FileLocked
from filedb.lock import ReaderWriterLock

PROCESS_COUNT = 20


class StopWatch(object):
    """A really basic stop watch."""

    def __init__(self, duration=None):
        self.duration = duration
        self.started_at = None
        self.stopped_at = None

    def leftover(self):
        if self.duration is None:
            return None
        return max(0.0, self.duration - self.elapsed())

    def elapsed(self):
        if self.stopped_at is not None:
            end_time = self.stopped_at
        else:
            end_time = time.monotonic()
        return max(0.0, end_time - self.started_at)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.stopped_at = time.monotonic()

    def start(self):
        self.started_at = time.monotonic()
        self.stopped_at = None

    def expired(self):
        if self.duration is None:
            return False
        else:
            return self.elapsed() > self.duration


@pytest.fixture()
def disk_cache_dir():
    with tempfile.TemporaryDirectory() as disk_cache_dir:
        yield disk_cache_dir


@pytest.fixture()
def lock_dir():
    with tempfile.TemporaryDirectory() as disk_cache_dir:
        yield disk_cache_dir


def test_doesnt_hang(lock_dir, disk_cache_dir):
    def chaotic_locker(type_):
        lock = (ReaderWriterLock(lock_dir).write_lock if type_ == 'w' else
                ReaderWriterLock(lock_dir).read_lock)
        with lock():
            with Cache(disk_cache_dir) as dc_:
                dc_.incr(type_)

    pool = ProcessPool(PROCESS_COUNT)
    pool.map(chaotic_locker, ['r'] * 10 + ['w'] * 10)

    with Cache(disk_cache_dir) as dc:
        assert dc.get('w') == 10
        assert dc.get('r') == 10


def test_no_double_writers(disk_cache_dir, lock_dir):
    watch = StopWatch(duration=5)
    watch.start()

    def acquire_check(dc_):
        with ReaderWriterLock(lock_dir).write_lock(timeout=None):
            if dc_.get('active_count', 0) >= 1:
                dc_.incr('dups_count')
            dc_.incr('active_count')
            time.sleep(random.random() / 100)
            dc_.decr('active_count')
            dc_.incr('visited_count')

    def run(_):
        with Cache(disk_cache_dir) as dc_:
            while not watch.expired():
                acquire_check(dc_)

    pool = ProcessPool(PROCESS_COUNT)
    pool.map(run, [() for _ in range(PROCESS_COUNT)], chunksize=1)

    with Cache(disk_cache_dir) as dc:

        assert dc.get('active_count') == 0
        assert dc.get('dups_count', default=0) == 0
        assert dc.get('visited_count') > 100


def test_no_concurrent_readers_writers(disk_cache_dir, lock_dir):
    watch = StopWatch(duration=5)
    watch.start()

    def acquire_check(dc_, reader):
        if reader:
            lock_func = ReaderWriterLock(lock_dir).read_lock
        else:
            lock_func = ReaderWriterLock(lock_dir).write_lock
        with lock_func(timeout=None):
            if not reader:
                if dc_.get('active_count', 0) >= 1:
                    dc_.incr('dups_count')
            dc_.incr('active_count')
            time.sleep(random.random() / 100)
            dc_.decr('active_count')
            dc_.incr('visited_count')

    def run(_):
        with Cache(disk_cache_dir) as dc_:
            while not watch.expired():
                acquire_check(dc_, random.choice([True, False]))

    pool = ProcessPool(PROCESS_COUNT)
    pool.map(run, [() for _ in range(PROCESS_COUNT)], chunksize=1)

    with Cache(disk_cache_dir) as dc:

        assert dc.get('active_count') == 0
        assert dc.get('dups_count', default=0) == 0
        assert dc.get('visited_count') > 10


def test_writer_releases_lock_upon_crash(lock_dir, disk_cache_dir):
    def lock_(i, crash):
        with ReaderWriterLock(lock_dir).write_lock(timeout=5):
            with Cache(disk_cache_dir) as dc_:
                dc_.set(f'pid{i}', os.getpid())
            if crash:
                raise RuntimeError('')

    p1 = Process(target=lock_, args=(1, True))
    p2 = Process(target=lock_, args=(2, False))

    p1.start()
    p1.join()

    p2.start()
    p2.join()

    with Cache(disk_cache_dir) as dc:
        assert dc.get('pid1') != dc.get('pid2')

    assert p1.exitcode != 0
    assert p2.exitcode == 0


def test_reader_releases_lock_upon_crash(lock_dir, disk_cache_dir):
    def read_lock_and_crash(i):
        with ReaderWriterLock(lock_dir).read_lock():
            with Cache(disk_cache_dir) as dc_:
                dc_.set(f'pid{i}', os.getpid())
            raise RuntimeError('')

    def write_lock(i):
        with ReaderWriterLock(lock_dir).write_lock(timeout=5):
            with Cache(disk_cache_dir) as dc_:
                dc_.set(f'pid{i}', os.getpid())

    p1 = Process(target=read_lock_and_crash, args=(1,))
    p2 = Process(target=write_lock, args=(2,))

    p1.start()
    p1.join()

    p2.start()
    p2.join()

    with Cache(disk_cache_dir) as dc:
        assert dc.get('pid1') != dc.get('pid2')

    assert p1.exitcode != 0
    assert p2.exitcode == 0


def test_reader_writer_chaotic(lock_dir, disk_cache_dir):
    def chaotic_locker(type_, blow_up):
        lock = (ReaderWriterLock(lock_dir).write_lock if type_ == 'w' else
                ReaderWriterLock(lock_dir).read_lock)
        with lock():
            with Cache(disk_cache_dir) as dc_:
                dc_.incr(type_)
            if blow_up:
                raise RuntimeError()

    pool = ProcessPool(PROCESS_COUNT)
    users = list(more_itertools.ncycles(itertools.product(['r', 'w'], [True, False]), 10))
    random.shuffle(users)

    with pytest.raises(RuntimeError):
        pool.starmap(chaotic_locker, users)

    with Cache(disk_cache_dir) as dc:
        assert dc.get('w') == 20
        assert dc.get('r') == 20


def test_reader_to_writer(lock_dir):
    lock = ReaderWriterLock(lock_dir)

    with lock.read_lock(timeout=1):
        with lock.write_lock(timeout=1):
            pass


@pytest.mark.skip('Not supported!')
def test_reader_to_reader(lock_dir):
    lock = ReaderWriterLock(lock_dir)

    with lock.read_lock(timeout=1):
        with lock.read_lock(timeout=1):
            pass

        # fails, as read lock is dropped after first exit
        with pytest.raises(FileLocked):
            with lock.read_lock(timeout=1):
                pass


@pytest.mark.skip('Not supported!')
def test_writer_to_reader(lock_dir):
    lock = ReaderWriterLock(lock_dir)

    # fails to release second time, as is already released
    with lock.write_lock(timeout=1):
        with lock.read_lock(timeout=1):
            pass


@pytest.mark.skip('Not supported!')
def test_writer_to_writer(lock_dir):
    lock = ReaderWriterLock(lock_dir)

    # fails to release second time, as is already released
    with lock.write_lock(timeout=1):
        with lock.write_lock(timeout=1):
            pass


def _find_overlaps(times, start, end):
    overlaps = 0
    for (s, e) in times:
        if s >= start and e <= end:
            overlaps += 1
    return overlaps


def _spawn_variation(readers, writers, lock_dir, disk_cache_dir):
    times = {'w': Deque(directory=disk_cache_dir / 'w'),
             'r': Deque(directory=disk_cache_dir / 'r')}

    def func(type_):
        lock = (ReaderWriterLock(lock_dir).write_lock if type_ == 'w' else
                ReaderWriterLock(lock_dir).read_lock)
        with lock(timeout=5):
            enter_time = time.monotonic()
            time.sleep(random.random() / 100)
            exit_time = time.monotonic()
            times[type_].append((enter_time, exit_time))
            # time.sleep(0.0001)

    pool = ProcessPool(readers + writers)
    pool.map(func, ['w'] * writers + ['r'] * readers)
    return list(times['w']), list(times['r'])


def test_multi_reader_multi_writer(lock_dir, disk_cache_dir):
    writer_times, reader_times = _spawn_variation(10, 10, Path(lock_dir), Path(disk_cache_dir))
    assert len(writer_times) == 10
    assert len(reader_times) == 10
    for start, stop in writer_times:
        assert _find_overlaps(reader_times, start, stop) == 0
        assert _find_overlaps(writer_times, start, stop) == 1
    for start, stop in reader_times:
        assert _find_overlaps(writer_times, start, stop) == 0


def test_multi_reader_single_writer(lock_dir, disk_cache_dir):
    writer_times, reader_times = _spawn_variation(9, 1, Path(lock_dir), Path(disk_cache_dir))
    assert len(writer_times) == 1
    assert len(reader_times) == 9
    start, stop = writer_times[0]
    assert _find_overlaps(reader_times, start, stop) == 0


def test_multi_writer(lock_dir, disk_cache_dir):
    writer_times, reader_times = _spawn_variation(0, 10, Path(lock_dir), Path(disk_cache_dir))
    assert len(writer_times) == 10
    assert len(reader_times) == 0

    for (start, stop) in writer_times:
        assert _find_overlaps(writer_times, start, stop) == 1
