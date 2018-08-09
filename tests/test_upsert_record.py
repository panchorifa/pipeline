source = 'source-a'
source2 = 'source-b'
table_name = 'npi'
pk_idx = 0
pk_type = 'int'

def test_insert_new_record(db):
    field_names = ['npi','aaa','bbb']
    dump_id = db.upsert_table(source, table_name, field_names, pk_idx, pk_type)
    values = [100100, 'a', 'b']
    db.upsert_record(table_name, field_names, values, dump_id, 1, pk_idx)
    assert db.count('npi_dumps') == 1
    assert db.count('npi') == 1
    assert db.count('npi_archives') == 0

def test_update_record(db):
    field_names = ['npi','aaa','bbb']
    dump_id = db.upsert_table(source, table_name, field_names, pk_idx, pk_type)
    values = [100100, 'a', 'b']
    db.upsert_record(table_name, field_names, values, dump_id, 1, pk_idx)

    field_names2 = ['npi','aaa','bbb', 'ccc']
    dump_id = db.upsert_table(source2, table_name, field_names2, pk_idx, pk_type)
    values = [100100, 'c', 'd', 'e']
    db.upsert_record(table_name, field_names2, values, dump_id, 1, pk_idx)
    assert db.count('npi_dumps') == 2
    assert db.count('npi') == 1
    assert db.count('npi_archives') == 1
