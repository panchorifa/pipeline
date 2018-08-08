# import csv
# import pytest
# import mysql.connector
# from mysql.connector import pooling
# from contextlib import contextmanager
#
# from lib import db
#
# source = 'source-a'
# field_names=['nip','aaa']
# table_name = 'nip'
#
# dbconfig = {
#     'host': 'mysql',
#     'user': 'root',
#     'password': 'password',
#     'database': 'external'
# }
#
# @pytest.fixture
# def cur():
#     x = db.Db(dbconfig)
#     with x.connection() as cnx:
#         with x.cursor(cnx) as cur:
#             cur.drop_tables()
#             yield cur
#
# def assert_table_dumps(cur):
#     assert cur.table_names()[0] == 'nip'
#     fields = cur.table_fields('nip_dumps')
#     assert len(fields.keys()) == 6
#     assert fields['id'].type == 'int(11)'
#     assert fields['source'].type == 'varchar(255)'
#     assert fields['table_name'].type == 'varchar(255)'
#     assert fields['status'].type == "enum('started','completed','failed')"
#     assert fields['created_at'].type == 'timestamp'
#     assert fields['updated_at'].type == 'timestamp'
#
# def assert_table(cur, table_name, archives=False):
#     fields = cur.table_fields(table_name)
#     assert fields['nip'].name == 'nip'
#     assert fields['nip'].type == 'int(11)'
#     assert fields['nip'].pk == True
#     assert fields['aaa'].name == 'aaa'
#     assert fields['aaa'].type == 'varchar(255)'
#     assert fields['aaa'].pk == False
#     assert fields['dump_id'].name == 'dump_id'
#     assert fields['dump_id'].type == 'int(11)'
#     assert fields['dump_id'].pk == (True if archives else False)
#     assert fields['line'].name == 'line'
#     assert fields['line'].type == 'int(11)'
#     assert fields['line'].pk == (True if archives else False)
#     # assert fields['created_at'].type == 'timestamp'
#     # assert fields['updated_at'].type == 'timestamp'
#
# def test_create_table_with_primary_key(cur):
#     cur.upsert_table(source, table_name, field_names, pk_idx=0, pk_type='int')
#     assert cur.table_names() == ['nip', 'nip_archives', 'nip_dumps']
#     assert_table_dumps(cur)
#     assert_table(cur, table_name)
#     assert_table(cur, '{}_archives'.format(table_name), True)
#     assert cur.count('nip') == 0
#     assert cur.count('nip_archives') == 0
#     dumps = cur.rows('{}_dumps'.format(table_name), ['id', 'source', 'table_name'])
#     assert len(dumps) == 1
#     assert dumps == [(1, 'source-a', 'nip')]
