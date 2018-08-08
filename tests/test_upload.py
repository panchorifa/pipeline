import csv
import pytest
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager

from lib import db
from lib import parser

source = 'source-a'
field_names=['nip','aaa']
table_name = 'nip'

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

def csv_entries(source):
    with open(source, 'rt', encoding='UTF8') as csv_file:
        datareader = csv.reader(csv_file)
        yield next(datareader)  # yield the header
        for row in datareader:
            yield row


source = '/usr/app/samples/simple/npi.csv'
table_name = 'nip'
pk_idx = 0
pk_type = 'int'

def test_simple_csv():
    line = csv_entries(source)
    field_lines = parser.parse_fields(next(line))
    # print(field_lines)
    # cur.upsert_table(source, table_name, field_names, pk_idx, pk_type)
