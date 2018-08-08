import csv
import pytest
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager

from lib import db


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

source = 'source-a'
table_name = 'nip'
field_names=['nip','aaa']
new_field_names=['nip', 'ccc', 'ddd']

def assert_table(fields):
    assert fields['nip'].name == 'nip'
    assert fields['nip'].type == 'int(11)'
    assert fields['nip'].pk == True
    assert fields['aaa'].name == 'aaa'
    assert fields['aaa'].type == 'varchar(255)'
    assert fields['aaa'].pk == False
    assert fields['ccc'].name == 'ccc'
    assert fields['ccc'].type == 'varchar(255)'
    assert fields['ccc'].pk == False
    assert fields['ddd'].name == 'ddd'
    assert fields['ddd'].type == 'varchar(255)'
    assert fields['ddd'].pk == False

def test_alter_table_with_new_cols(cur):
    cur.upsert_table(source, table_name, field_names, pk_idx=0, pk_type='int')
    cur.upsert_table(source, table_name, new_field_names)
    assert_table(cur.table_fields(table_name))
    assert_table(cur.table_fields('{}_archives'.format(table_name)))
