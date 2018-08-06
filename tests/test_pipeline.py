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
    'database': 'external',
}

@pytest.fixture
def cur():
    x = db.Db(dbconfig)
    with x.connection() as cnx:
        with x.cursor(cnx) as cur:
            cur.drop_tables()
            yield cur

def csv_entries(source):
    with open(source, 'rt', encoding='UTF8') as csv_file:
        datareader = csv.reader(csv_file)
        yield next(datareader)  # yield the header
        for row in datareader:
            yield row

def test_schema(cur):
    # assert cur.table_fields().keys() == {}
    assert len(cur.table_names()) == 0
    field_names=['nip','aaa','bbb']
    schema = cur.upsert_table('simon', field_names, pk_idx=0, pk_type='mediumint')
    assert len(cur.table_names()) == 1
    assert cur.table_names()[0] == 'simon'
    fields = cur.table_fields('simon')
    assert fields['aaa'].name == 'aaa'
    assert fields['aaa'].type == 'varchar(255)'
    assert fields['bbb'].name == 'bbb'
    assert fields['bbb'].type == 'varchar(255)'
    assert fields['nip'].name == 'nip'
    assert fields['nip'].type == 'int11'
    # assert len(schema.tables.keys()) == 1
    # table = schema.tables['simon']
    # assert len(table.fields.keys()) == 3
    # assert table.fields.keys() == ['nip', 'aaa', 'bbb']
    # assert table.fields['nip'].pk == True
    # assert table.fields['aaa'].type == 'varchar(255)'
    # assert table.fields['aaa'].pk == False
    # assert table.fields['bbb'].type == 'varchar(255)'
    # assert table.fields['bbb'].pk == False


    # assert tables[0].name == 'simon'
    # fields = tables[0].fields
    # assert fields[0].name == 'nip'
    # assert fields[0].type == 'int(11)'
    # assert fields[0].pk == True
    # assert fields[1].name == 'aa'
    # assert fields[1].type == 'varchar(255)'
    # assert fields[1].pk == False
    # assert fields[2].name == 'bb'
    # assert fields[2].type == 'text'
    # assert fields[2].pk == False
