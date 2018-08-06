# import csv
# import pytest
# import mysql.connector
# from mysql.connector import pooling
# from contextlib import contextmanager
#
# from lib import db
#
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
# def test_insert_new_record(cur):
#     field_names=['simon','aaa','bbb']
#     # cur.upsert_table('simon', field_names, pk_idx=0, pk_type='int')
