# import csv
# import pytest
# import mysql.connector
# from mysql.connector import pooling
# from contextlib import contextmanager
#
# from lib import db
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
# def test_insert_record(cur):
#     field_names=['nip','last_name','first_name']
#     cur.upsert_table('nip', field_names, pk_idx=0, pk_type='int')
#     rows = cur.rows('nip')
#     assert len(rows) == 0
#     cur.upsert_record('nip', ['100100100', 'Bolivar', 'Simon', 1, 1])
#     # rows = cur.rows('nip')
#     # assert len(rows) == 1
#
# # def test_update_existing_record(cur):
# #     field_names=['simon','aaa','bbb']
# #     cur.upsert_table('nip', field_names, pk_idx=0, pk_type='int')
# #     cur.upsert_record()
