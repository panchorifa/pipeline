import csv
import pytest
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager

from lib import db
from lib import parser
from lib import pipeline

source = 'source-a'
field_names=['npi','aaa']
table_name = 'npi'

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


source1 = '/usr/app/samples/simple/npi-1.csv'
source2 = '/usr/app/samples/simple/npi-2.csv'
table_name = 'npi'
pk_idx = 0
pk_type = 'int'


def test_simple_csv(cur):
    pipeline.process(source1, table_name, pk_idx, pk_type, cur)
    assert cur.count('npi_dumps') == 1
    print(cur.rows('npi_dumps', ['id', 'source', 'created_at', 'updated_at', 'status']))
    assert cur.count('npi') == 3
    assert cur.count('npi_archives') == 0
    pipeline.process(source2, table_name, pk_idx, pk_type, cur)
    assert cur.count('npi_dumps') == 2
    assert cur.count('npi') == 5
    assert cur.count('npi_archives') == 1
    assert cur.rows('npi', ['provider_first_name'], 'where npi = {}'.format(1003000167))[0][0] == 'PEDRO'
    assert cur.rows('npi_archives', ['provider_first_name'])[0][0] == 'JULIO'
