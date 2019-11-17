import pytest

from integration_tests.fixtures import gcs
from integration_tests.fixtures import local
from integration_tests.fixtures import s3


@pytest.mark.parametrize("db_factory", [local, s3, gcs])
def test_write_read(db_factory):
    with db_factory() as db:
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
