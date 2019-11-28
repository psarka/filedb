import datetime as dt
import re

import pytest
from bson import Decimal128
from bson import ObjectId

from filedb.index import Index
from filedb.query import BSONType
from filedb.query import q
from integration_tests.fixtures import temp_mongo_db_factory


@pytest.fixture
def db_with_keys():
    with temp_mongo_db_factory() as mongo_db_factory:
        class MongoWithKeys:
            def __init__(self, keys):
                self.index = Index(mongo_db_factory())
                for key in keys:
                    self.index.upsert(key, 'storage_path_1', 'storage_name')

            def f(self, query):
                return list(self.index.find(query, 'storage_name'))

        yield MongoWithKeys


def test_raw_queries(db_with_keys):
    db = db_with_keys([{'a': 1},
                       {'b': 1,
                        'c': 1}])

    assert db.f({'a': 1}) == [{'a': 1}]
    assert db.f({'a': 2}) == []
    assert db.f({'b': 1, 'c': 1}) == [{'b': 1, 'c': 1}]


def test_single_level_queries(db_with_keys):
    db = db_with_keys([{'a': 1}, {'a': 2}])

    assert db.f({'a': q.equal(1)}) == [{'a': 1}]
    assert db.f({'a': q.equal(3)}) == []

    assert db.f({'a': q.not_equal(1)}) == [{'a': 2}]
    assert db.f({'a': q.not_equal(3)}) == [{'a': 1}, {'a': 2}]

    assert db.f({'a': q.greater_than(1)}) == [{'a': 2}]
    assert db.f({'a': q.greater_than(3)}) == []

    assert db.f({'a': q.greater_or_equal(1)}) == [{'a': 1}, {'a': 2}]
    assert db.f({'a': q.greater_or_equal(3)}) == []

    assert db.f({'a': q.less_than(1)}) == []
    assert db.f({'a': q.less_than(3)}) == [{'a': 1}, {'a': 2}]

    assert db.f({'a': q.less_or_equal(1)}) == [{'a': 1}]
    assert db.f({'a': q.less_or_equal(3)}) == [{'a': 1}, {'a': 2}]

    assert db.f({'a': q.is_in([1])}) == [{'a': 1}]
    assert db.f({'a': q.is_in([1, 2])}) == [{'a': 1}, {'a': 2}]
    assert db.f({'a': q.is_in([3, 4])}) == []

    assert db.f({'a': q.not_in([1])}) == [{'a': 2}]
    assert db.f({'a': q.not_in([1, 2])}) == []
    assert db.f({'a': q.not_in([3, 4])}) == [{'a': 1}, {'a': 2}]

    assert db.f({'a': q.exists}) == [{'a': 1}, {'a': 2}]
    assert db.f({'b': q.exists}) == []

    assert db.f({'a': q.not_exists}) == []
    assert db.f({'b': q.not_exists}) == [{'a': 1}, {'a': 2}]

    assert db.f({'a': q.has_type(BSONType.Int32)}) == [{'a': 1}, {'a': 2}]
    assert db.f({'a': q.has_type(BSONType.Double)}) == []


def test_logic_operators(db_with_keys):
    db = db_with_keys([{'a': 1}, {'a': 2}])

    assert db.f({'a': q.equal(1) & q.not_equal(2)}) == [{'a': 1}]
    assert db.f({'a': q.equal(1) & q.not_equal(1)}) == []
    assert db.f({'a': q.equal(1) & q.not_equal(1)}) == []

    assert db.f(q.any({'a': q.equal(1)},
                      {'a': q.equal(2)})) == [{'a': 1}, {'a': 2}]
    assert db.f(q.any({'a': q.equal(1)},
                      {'a': q.equal(3)})) == [{'a': 1}]

    assert db.f(q.all({'a': q.equal(1)},
                      {'a': q.equal(1)})) == [{'a': 1}]
    assert db.f(q.all({'a': q.equal(1)},
                      {'a': q.equal(2)})) == []

    assert db.f({'a': ~q.equal(1)}) == [{'a': 2}]
    assert db.f({'a': ~q.equal(3)}) == [{'a': 1}, {'a': 2}]


def test_value_types(db_with_keys):
    db = db_with_keys([{BSONType.Double.value: 1.},
                       {BSONType.String.value: 'a'},
                       {BSONType.Object.value: {'a': 1, 'b': 2}},
                       {BSONType.Array.value: [1, 2, 3]},
                       {BSONType.BinaryData.value: b'123'},
                       {BSONType.ObjectId.value: ObjectId(b'123412341234')},
                       {BSONType.Boolean.value: True},
                       {BSONType.Date.value: dt.datetime(2000, 1, 1)},
                       {BSONType.Null.value: None},
                       {BSONType.RegularExpression.value: re.compile('a')},
                       {BSONType.Int32.value: 2 ** 31 - 1},
                       {BSONType.Int64.value: 2 ** 63 - 1},
                       {BSONType.Decimal128.value: Decimal128('1.1')},
                       {BSONType.Number.value: 1}])

    assert db.f({BSONType.Double.value: q.has_type(BSONType.Double)})
    assert db.f({BSONType.String.value: q.has_type(BSONType.String)})
    assert db.f({BSONType.Object.value: q.has_type(BSONType.Object)})
    assert db.f({BSONType.Array.value: q.has_type(BSONType.Array)})
    assert db.f({BSONType.BinaryData.value: q.has_type(BSONType.BinaryData)})
    assert db.f({BSONType.ObjectId.value: q.has_type(BSONType.ObjectId)})
    assert db.f({BSONType.Boolean.value: q.has_type(BSONType.Boolean)})
    assert db.f({BSONType.Date.value: q.has_type(BSONType.Date)})
    assert db.f({BSONType.Null.value: q.has_type(BSONType.Null)})
    assert db.f({BSONType.RegularExpression.value: q.has_type(BSONType.RegularExpression)})
    assert db.f({BSONType.Int32.value: q.has_type(BSONType.Int32)})
    assert db.f({BSONType.Int64.value: q.has_type(BSONType.Int64)})
    assert db.f({BSONType.Decimal128.value: q.has_type(BSONType.Decimal128)})
    assert db.f({BSONType.Number.value: q.has_type(BSONType.Number)})
