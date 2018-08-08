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
source2 = 'source-b'
table_name = 'simon'



def test_insert_new_record(cur):
    field_names = ['simon','aaa','bbb']
    dump_id = cur.upsert_table(source, table_name, field_names, pk_idx=0, pk_type='int')
    values = [100100, 'a', 'b']
    cur.upsert_record(table_name, field_names, values, dump_id, 1)

def test_update_record(cur):
    field_names = ['simon','aaa','bbb']
    dump_id = cur.upsert_table(source, table_name, field_names, pk_idx=0, pk_type='int')
    values = [100100, 'a', 'b']
    cur.upsert_record(table_name, field_names, values, dump_id, 1)

    field_names2 = ['simon','aaa','bbb', 'ccc']
    dump_id = cur.upsert_table(source2, table_name, field_names2, pk_idx=0, pk_type='int')
    values = [100100, 'c', 'd', 'e']
    cur.upsert_record(table_name, field_names2, values, dump_id, 1)
