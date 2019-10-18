import tempfile

import pytest
from pymongo import MongoClient

from filedb.db import FileDB
from filedb.index import Index
from filedb.storage import LocalStorage


@pytest.fixture(scope='module')
def mongo_db():
    client = MongoClient()
    yield client.test_db
    client.drop_database(client.test_db)


@pytest.fixture(scope='module')
def local_storage_path():
    with tempfile.TemporaryDirectory() as path:
        yield path


def test_write_read(mongo_db, local_storage_path):
    db = FileDB(index=Index(mongo_db=mongo_db),
                storage=LocalStorage('test_machine', local_storage_path))

    db.file({'a': '1'}).write_text('hi!')
    assert db.file({'a': '1'}).read_text() == 'hi!'
    assert db.file({'a': '1'}).exists()
    assert db.find({}) == [db.file({'a': '1'})]

    db.file({'a': '1'}).copy({'b': '2'})
    assert db.file({'b': '2'}).read_text() == 'hi!'
    assert db.file({'b': '2'}).exists()
    assert set(db.find({})) == {db.file({'a': '1'}), db.file({'b': '2'})}
    assert db.find({'a': '1'}) == [db.file({'a': '1'})]

    db.file({'b': '2'}).move({'b': '3'})
    assert db.file({'b': '3'}).read_text() == 'hi!'
    assert db.file({'b': '3'}).exists()
    assert set(db.find({})) == {db.file({'a': '1'}), db.file({'b': '3'})}

    db.file({'a': '1'}).delete()
    assert not db.file({'a': '1'}).exists()
    assert db.find({}) == [db.file({'b': '3'})]

    db.file({'b': '3'}).delete()
    assert not db.file({'b': '3'}).exists()
    assert db.find({}) == []
