import datetime
from typing import Dict
from typing import List
from typing import Pattern
from typing import Union

import bson

ID = '_id'
STORAGE_PATH = '_storage_path_e5c8b4a5-96b1-4ed3-9a36-d8bb28204240'
KEY_BYTES = 'key_bytes'

Value = Union[None,
              bool,
              int,
              str,
              bytes,
              datetime.datetime,
              Pattern,
              List['Value'],
              Dict[str, 'Value'],
              bson.int64.Int64,
              bson.regex.Regex,
              bson.binary.Binary,
              bson.objectid.ObjectId,
              bson.dbref.DBRef,
              bson.code.Code]

Key = Dict[str, Value]


def key_bytes(key: Key):
    return bson.BSON.encode(key_sorted(key))


def key_sorted(key: Key):
    if isinstance(key, dict):
        return dict(sorted((k, key_sorted(v)) for k, v in key.items()))
    else:
        return key


def key_hash(key: Key):
    return hash(_immutable(key))


def _immutable(nested):

    # separate strings from other iterables
    if isinstance(nested, str):
        return nested

    try:
        return tuple(sorted((k, _immutable(v)) for k, v in nested.items()))
    except (AttributeError, TypeError):
        try:
            return tuple(_immutable(x) for x in nested)
        except TypeError:
            return nested
