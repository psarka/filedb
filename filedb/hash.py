import hashlib
import zlib
from pathlib import Path


# TODO chunking as in md5 is better
def crc32(path: Path):
    prev = 0
    for line in path.open('rb'):
        prev = zlib.crc32(line, prev)
    return "%X" % (prev & 0xFFFFFFFF)


def md5(path: Path):
    hash_md5 = hashlib.md5()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
