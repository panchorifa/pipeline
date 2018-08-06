import csv
import pytest
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager

from lib import db

# ./pipeline.py
# --source-url
#     https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-02-07.csv
# --sink-user
#     root
# --sink-password
#     password
# --sink-host
#     mysql
# --sink-database
#     external
# --sink-table
#     npi


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

# def csv_entries(source):
#     with open(source, 'rt', encoding='UTF8') as csv_file:
#         datareader = csv.reader(csv_file)
#         yield next(datareader)  # yield the header
#         for row in datareader:
#             yield row

def test_create_table_with_primary_key(cur):
    field_names=['simon','aaa','bbb']
    cur.upsert_table('simon', field_names, pk_idx=0, pk_type='int')
    assert len(cur.table_names()) == 3
    assert cur.table_names()[0] == 'simon'
    fields = cur.table_fields('simon')
    assert fields['aaa'].name == 'aaa'
    assert fields['aaa'].type == 'varchar(255)'
    assert fields['aaa'].pk == False
    assert fields['bbb'].name == 'bbb'
    assert fields['bbb'].type == 'varchar(255)'
    assert fields['bbb'].pk == False
    assert fields['simon'].name == 'simon'
    assert fields['simon'].type == 'int(11)'
    assert fields['simon'].pk == True

def test_alter_table_with_new_cols(cur):
    field_names=['simon','aaa','bbb']
    cur.upsert_table('simon', field_names, pk_idx=0, pk_type='int')
    field_names=['simon', 'ccc', 'ddd']
    schema = cur.upsert_table('simon', field_names)
    fields = cur.table_fields('simon')
    assert fields['ddd'].name == 'ddd'
    assert fields['ddd'].type == 'varchar(255)'
    assert fields['ddd'].pk == False
    assert fields['ccc'].name == 'ccc'
    assert fields['ccc'].type == 'varchar(255)'
    assert fields['ccc'].pk == False
    assert fields['aaa'].name == 'aaa'
    assert fields['aaa'].type == 'varchar(255)'
    assert fields['aaa'].pk == False
    assert fields['bbb'].name == 'bbb'
    assert fields['bbb'].type == 'varchar(255)'
    assert fields['bbb'].pk == False
    assert fields['simon'].name == 'simon'
    assert fields['simon'].type == 'int(11)'
    assert fields['simon'].pk == True
