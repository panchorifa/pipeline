from lib import pipeline

source1 = '/usr/app/samples/simple/npi-1.csv'
source2 = '/usr/app/samples/simple/npi-2.csv'
table_name = 'npi'
pk_idx = 0
pk_type = 'int'

def test_simple_csv(db):
    pipeline.process(db, source1, table_name, pk_idx, pk_type)
    assert db.count('npi_dumps') == 1
    print(db.rows('npi_dumps', ['id', 'source', 'created_at', 'updated_at', 'status']))
    assert db.count('npi') == 3
    assert db.count('npi_archives') == 0
    pipeline.process(db, source2, table_name, pk_idx, pk_type)
    assert db.count('npi_dumps') == 2
    assert db.count('npi') == 5
    assert db.count('npi_archives') == 1
    assert db.rows('npi', ['provider_first_name'], 'where npi = {}'.format(1003000167))[0][0] == 'PEDRO'
    assert db.rows('npi_archives', ['provider_first_name'])[0][0] == 'JULIO'
