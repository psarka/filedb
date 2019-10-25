import base64
import hashlib
from pathlib import Path

import crcmod


def md5(path: Path):
    hash_md5 = hashlib.md5()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def crc32c(path: Path):
    hash_crc32c = crcmod.predefined.Crc('crc-32c')
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_crc32c.update(chunk)

    return base64.encodebytes(hash_crc32c.digest()).rstrip(b'\n').decode('utf-8')
