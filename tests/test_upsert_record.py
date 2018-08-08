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
table_name = 'nip'
pk_idx = 0
pk_type = 'int'


def test_insert_new_record(cur):
    field_names = ['nip','aaa','bbb']
    dump_id = cur.upsert_table(source, table_name, field_names, pk_idx, pk_type)
    values = [100100, 'a', 'b']
    cur.upsert_record(table_name, field_names, values, dump_id, 1, pk_idx)
    assert len(cur.rows('nip_dumps', ['id', 'source'])) == 1
    # assert len(cur.rows('nip', ['nip'])) == 1
    # assert len(cur.rows('nip_archives', ['nip'])) == 0

# def test_update_record(cur):
#     field_names = ['nip','aaa','bbb']
#     dump_id = cur.upsert_table(source, table_name, field_names, pk_idx, pk_type)
#     values = [100100, 'a', 'b']
#     cur.upsert_record(table_name, field_names, values, dump_id, 1, pk_idx)
#
#     field_names2 = ['nip','aaa','bbb', 'ccc']
#     dump_id = cur.upsert_table(source2, table_name, field_names2, pk_idx, pk_type)
#     values = [100100, 'c', 'd', 'e']
#     cur.upsert_record(table_name, field_names2, values, dump_id, 1, pk_idx)
#     assert len(cur.rows('nip_dumps', ['id'])) == 2
#     assert len(cur.rows('nip', ['nip'])) == 1
#     assert len(cur.rows('nip_archives', ['nip'])) == 1
