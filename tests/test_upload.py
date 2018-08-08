import pytest
from lib import db, parser, pipeline

dbconfig = {
    'host': 'mysql',
    'user': 'root',
    'password': 'password',
    'database': 'external'
}

@pytest.fixture
def cur():
    x = db.Db(dbconfig)
    with x.connection() as cnx:
        with x.cursor(cnx) as cur:
            cur.drop_tables()
            yield cur


source1 = '/usr/app/samples/simple/npi-1.csv'
source2 = '/usr/app/samples/simple/npi-2.csv'
table_name = 'npi'
pk_idx = 0
pk_type = 'int'


def test_simple_csv(cur):
    pipeline.process(cur, source1, table_name, pk_idx, pk_type)
    assert cur.count('npi_dumps') == 1
    print(cur.rows('npi_dumps', ['id', 'source', 'created_at', 'updated_at', 'status']))
    assert cur.count('npi') == 3
    assert cur.count('npi_archives') == 0
    pipeline.process(cur, source2, table_name, pk_idx, pk_type)
    assert cur.count('npi_dumps') == 2
    assert cur.count('npi') == 5
    assert cur.count('npi_archives') == 1
    assert cur.rows('npi', ['provider_first_name'], 'where npi = {}'.format(1003000167))[0][0] == 'PEDRO'
    assert cur.rows('npi_archives', ['provider_first_name'])[0][0] == 'JULIO'
