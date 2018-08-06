import csv
import pytest
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager

from lib import db

source = 'source-a'
field_names=['simon','aaa']
table_name = 'simon'

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


def assert_table_dumps_created(cur):
    assert cur.table_names()[0] == 'simon'
    fields = cur.table_fields('simon_dumps')
    assert len(fields.keys()) == 6
    assert fields['id'].type == 'int(11)'
    assert fields['source'].type == 'varchar(255)'
    assert fields['table_name'].type == 'varchar(255)'
    assert fields['status'].type == "enum('started','completed','failed')"
    assert fields['created_at'].type == 'timestamp'
    assert fields['updated_at'].type == 'timestamp'

def assert_table_created(cur, table_name):
    fields = cur.table_fields(table_name)
    assert fields['simon'].name == 'simon'
    assert fields['simon'].type == 'int(11)'
    assert fields['simon'].pk == True
    assert fields['aaa'].name == 'aaa'
    assert fields['aaa'].type == 'varchar(255)'
    assert fields['aaa'].pk == False
    assert fields['dump_id'].name == 'dump_id'
    assert fields['dump_id'].type == 'int(11)'
    assert fields['dump_id'].pk == False
    assert fields['line_id'].name == 'line_id'
    assert fields['line_id'].type == 'int(11)'
    assert fields['line_id'].pk == False


def test_create_table_with_primary_key(cur):
    cur.upsert_table(source, table_name, field_names, pk_idx=0, pk_type='int')
    assert cur.table_names() == ['simon', 'simon_dumps', 'simon_history']
    assert_table_dumps_created(cur)
    assert_table_created(cur, table_name)
    assert_table_created(cur, '{}_history'.format(table_name))
    assert cur.rows(table_name, field_names) == []
    assert cur.rows('{}_history'.format(table_name), field_names) == []
    dumps = cur.rows('{}_dumps'.format(table_name), ['id', 'source', 'table_name'])
    assert len(dumps) == 1
    assert dumps == [(1, 'source-a', 'simon')]
