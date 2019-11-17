import pytest
from pathos.pools import _ProcessPool as ProcessPool

from integration_tests.fixtures import gcs_mp
from integration_tests.fixtures import local_mp
from integration_tests.fixtures import s3_mp


@pytest.mark.parametrize("db_factory", [local_mp, gcs_mp, s3_mp])
def test_multiple_readers_of_same_file(db_factory):
    def read_and_check(_):
        return db.file({'a': '1'}).read_text() == 'hi!'

    with db_factory() as db:
        db.file({'a': '1'}).write_text('hi!')
        pool = ProcessPool(2)
        results = pool.map(read_and_check, [() for _ in range(4)], chunksize=1)
        assert all(results)
