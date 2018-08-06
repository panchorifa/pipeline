import csv
import pytest
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager
from lib import ddl

class Schema:
    def __init__(self, tables):
        self.tables = tables

class Table:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields

class Field:
    def __init__(self, name, type):
        self.name = name
        self.type = type

class Cursor:
    def __init__(self, cur):
        self.cur = cur
        self.schema = self.latest_schema()

    def latest_schema(self):
        tables = {name: Table(name, self.table_fields(name)) for name in self.table_names()}
        return Schema(tables)

    def table_count(self, table_name):
        return self.cur.execute('select count from {}'.format(table_name))

    def table_names(self):
        self.cur.execute('show tables')
        return [name for (name,) in self.cur.fetchall()]

    def table_fields(self, table_name):
        self.cur.execute('describe {}'.format(table_name))
        return {x[0] : Field(x[0], x[1]) for x in self.cur.fetchall()}

    def drop_tables(self):
        for name in self.table_names():
            self.cur.execute('drop table if exists {}'.format(name))

    def upsert_table(self, table_name, field_names, pk_idx=None, pk_type=None):
        pk = Field(field_names[pk_idx], pk_type) if pk_idx != None else None
        sql = ddl.create_table(table_name, field_names, pk)
        print(sql)
        self.cur.execute(ddl)

    def execute(self, cmd):
        return self.cur.execute(cmd)

class Db:
    def __init__(self, config):
        self.config = config
        self.pool = pooling.MySQLConnectionPool(pool_name='mypool',
                                                pool_size=3,
                                                **config)
    @contextmanager
    def connection(self):
        cnx = None
        try:
            cnx = self.pool.get_connection()
            yield cnx
            # mysql.connector.errors.PoolError: Failed getting connection; pool exhausted
        finally:
            cnx and cnx.close()

    @contextmanager
    def cursor(self, cnx):
        cur = None
        try:
            cur = cnx.cursor()
            yield Cursor(cur)
        finally:
            cur and cur.close()
