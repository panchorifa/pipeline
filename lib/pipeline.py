import csv
import mysql.connector
from mysql.connector import pooling
# from contextlib import contextmanager

from lib import db
from lib import parser

def csv_entries(source):
    with open(source, 'rt', encoding='UTF8') as csv_file:
        datareader = csv.reader(csv_file)
        yield next(datareader)  # yield the header
        for row in datareader:
            yield row


def process(cur, source, table_name, pk_idx, pk_type):
    entry = csv_entries(source)
    field_names = parser.parse_header(next(entry))
    dump_id = cur.upsert_table(source, table_name, field_names, pk_idx, pk_type)
    try:
        line=0
        for values in entry:
            print(values)
            # values = parser.parse_data(line)
            line = line+1
            cur.upsert_record(table_name, field_names, values, dump_id, line, pk_idx)
        cur.dump_completed(table_name, dump_id)
    except:
        cur.dump_failed(table_name, dump_id)
