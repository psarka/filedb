import datetime
from typing import Dict
from typing import List
from typing import Pattern
from typing import Union

import bson

ID = '_id'
STORAGE_PATH = '_storage_path_e5c8b4a5-96b1-4ed3-9a36-d8bb28204240'
KEY_STRING = 'key_string'

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


def to_string(key: Key):
    return bson.BSON.encode(sort_key(key))


def sort_key(key: Key):
    if isinstance(key, dict):
        return dict(sorted((k, sort_key(v)) for k, v in key.items()))
    else:
        return key
