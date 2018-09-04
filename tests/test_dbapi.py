from lib.model import Pk
#
def test_create_table(dbapi):
    assert dbapi.tables() == []
    dbapi.execute('create table nip(first text, last text)')
    assert dbapi.tables() == ['nip']
    assert dbapi.fields('nip') == ['first', 'last']

source = 'source-a'
source2 = 'source-b'
table_name = 'npi'
pk = Pk(0, 'int')

def test_insert_new_record(dbapi):
    field_names = ['npi','aaa','bbb']
    dump_id = dbapi.upsert_table(source, table_name, field_names, pk)
    values = [100100, 'a', 'b']
    dbapi.upsert_record(dump_id, 1, table_name, field_names, values, pk)
    assert dbapi.count('npi_dumps') == 1
    assert dbapi.count('npi') == 1
    assert dbapi.count('npi_archives') == 0

def test_update_record(dbapi):
    field_names = ['npi','aaa','bbb']
    dump_id = dbapi.upsert_table(source, table_name, field_names, pk)
    values = [100100, 'a', 'NULL']
    dbapi.upsert_record(dump_id, 1, table_name, field_names, values, pk)
    rows = dbapi.rows('npi', ['npi', 'aaa', 'bbb', 'dump_id', 'line'])
    assert rows[0][0] == 100100
    assert rows[0][1] == 'a'
    assert rows[0][2] == 'NULL'
    assert rows[0][3] == dump_id
    assert rows[0][4] == 1

    field_names2 = ['npi','aaa','bbb', 'ccc']
    dump_id = dbapi.upsert_table(source2, table_name, field_names2, pk)
    values = [100100, 'c', 'd', 'e']
    dbapi.upsert_record(dump_id, 2, table_name, field_names2, values, pk)
    assert dbapi.count('npi_dumps') == 2
    assert dbapi.count('npi') == 1
    assert dbapi.count('npi_archives') == 1
    rows = dbapi.rows('npi', ['npi', 'aaa', 'bbb', 'dump_id', 'line'])
    assert rows[0][0] == 100100
    assert rows[0][1] == 'c'
    assert rows[0][2] == 'd'
    assert rows[0][3] == dump_id
    assert rows[0][4] == 2
