# import csv
# import pytest
# import mysql.connector
# from mysql.connector import pooling
# from contextlib import contextmanager
#
# from lib import db
# from lib import parser
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
# def csv_entries(source):
#     with open(source, 'rt', encoding='UTF8') as csv_file:
#         datareader = csv.reader(csv_file)
#         yield next(datareader)  # yield the header
#         for row in datareader:
#             yield row
#
#
# source1 = '/usr/app/samples/simple/npi-1.csv'
# source2 = '/usr/app/samples/simple/npi-2.csv'
# table_name = 'nip'
# pk_idx = 0
# pk_type = 'int'
#
# def process(source, table_name, cur):
#     entry = csv_entries(source)
#     field_names = parser.parse_header(next(entry))
#     print(field_names)
#     dump_id = cur.upsert_table(source, table_name, field_names, pk_idx, pk_type)
#     line=0
#     for values in entry:
#         print(values)
#         # values = parser.parse_data(line)
#         line = line+1
#         cur.upsert_record(table_name, field_names, values, dump_id, line, pk_idx)
#
# def test_simple_csv(cur):
#     process(source1, table_name, cur)
#     assert cur.count('nip_dumps') == 1
#     print(cur.rows('nip_dumps', ['id', 'source', 'created_at', 'updated_at', 'status']))
#     raise Error()
#     assert cur.count('nip') == 3
#     assert cur.count('nip_archives') == 0
#     process(source2, table_name, cur)
#     assert cur.count('nip_dumps') == 2
#     assert cur.count('nip') == 5
#     assert cur.count('nip_archives') == 1
