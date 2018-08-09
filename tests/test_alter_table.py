source = 'source-a'
table_name = 'npi'
field_names=['npi','aaa']
new_field_names=['npi', 'ccc', 'ddd']

def assert_table(fields):
    assert fields['npi'].name == 'npi'
    assert fields['npi'].type == 'int(11)'
    assert fields['npi'].pk == True
    assert fields['aaa'].name == 'aaa'
    assert fields['aaa'].type == 'varchar(255)'
    assert fields['aaa'].pk == False
    assert fields['ccc'].name == 'ccc'
    assert fields['ccc'].type == 'varchar(255)'
    assert fields['ccc'].pk == False
    assert fields['ddd'].name == 'ddd'
    assert fields['ddd'].type == 'varchar(255)'
    assert fields['ddd'].pk == False

def test_alter_table_with_new_cols(db):
    db.upsert_table(source, table_name, field_names, pk_idx=0, pk_type='int')
    db.upsert_table(source, table_name, new_field_names)
    assert_table(db.table_fields('npi'))
    assert_table(db.table_fields('npi_archives'))
