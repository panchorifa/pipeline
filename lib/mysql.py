import csv
import pytest
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager
from lib import ddl
from lib import model

debug = True

class Cursor:
    def __init__(self, cur):
        self.cur = cur
        self.schema = self.latest_schema()

    def _new_table(self, name):
        return model.Table(name, self.table_fields(name))

    def _new_field(self, values):
        return model.Field(values[0], values[1], values[3]=='PRI')

    def latest_schema(self):
        tables = {name: self._new_table(name) for name in self.table_names()}
        return model.Schema(tables)

    def count(self, table_name):
        self.execute(ddl.table_count(table_name))
        return self.cur.fetchone()[0]

    def table_names(self):
        self.execute(ddl.show_tables())
        return [name for (name,) in self.cur.fetchall()]

    def table_fields(self, table_name):
        self.execute(ddl.describe_table(table_name))
        return {x[0] : self._new_field(x) for x in self.cur.fetchall()}

    def referenced_table_names(self, table_name):
        self.execute(ddl.reference_tables(table_name))
        return  [x for (x,) in self.cur.fetchall()]

    def drop_tables(self):
        for name in self.table_names():
            for ref_name in self.referenced_table_names(name):
                self.execute(ddl.drop_table(ref_name))
            self.execute(ddl.drop_table(name))
            self.schema = self.latest_schema()

    def dump_completed(self, table_name, dump_id):
        self.execute(ddl.update_dump(table_name, dump_id, 'completed'))

    def dump_failed(self, table_name, dump_id):
        self.execute(ddl.update_dump(table_name, dump_id, 'failed'))

    def upsert_table(self, source, table_name, field_names, pk_idx=None, pk_type=None):
        try:
            existing_table = self.schema.tables.get(table_name)
            if(not existing_table):
                pk = model.Field(field_names[pk_idx], pk_type) if pk_idx != None else None
                self.execute(ddl.create_table_dumps(table_name))
                self.execute(ddl.create_table(table_name, field_names, pk))
                if pk:
                    self.execute(ddl.create_table_archives(table_name, field_names, pk))
                self.execute(ddl.create_dump(source, table_name))
                return self.cur.lastrowid
            else:
                sql = ddl.alter_table(existing_table, field_names)
                if sql:
                    self.execute(sql)
                    self.execute(ddl.alter_table(self.schema.tables.get('{}_archives'.format(table_name)), field_names))
                self.execute(ddl.get_dump(source, table_name))
                res = self.cur.fetchone()
                print('------------------------')
                print(res)
                print('------------------------')
                if not res:
                    self.execute(ddl.create_dump(source, table_name))
                    return self.cur.lastrowid
                return res
        finally:
            self.schema = self.latest_schema()

    def upsert_record(self, table_name, field_names, values, dump_id, line, pk_idx):
        table = self.latest_schema().tables.get(table_name)
        if(table):
            try:
                self.execute(ddl.insert_record(table_name, field_names, values, dump_id, line))
                print(self.cur.lastrowid)
            except mysql.connector.errors.IntegrityError as e:
                if(e.msg.find('Duplicate entry')>=0):
                    self.execute(ddl.archive_record(table, field_names, values, pk_idx))
                    self.execute(ddl.update_record(table_name, field_names, values, dump_id, line, pk_idx))

    def rows(self, table_name, field_names, where_statement=None):
        fields = ', '.join(field_names)
        where = ' {}'.format(where_statement) if where_statement else ''
        self.execute('SELECT {} FROM {}{}'.format(fields, table_name, where))
        return [value for value in self.cur.fetchall()]

    def execute(self, cmd):
        if debug:
            print('-------------------------------------------------------------')
            print(cmd)
            print('-------------------------------------------------------------')
        return self.cur.execute(cmd)

class Db:
    def __init__(self, config):
        self.config = config
        self.pool = pooling.MySQLConnectionPool(pool_name='mypool',
                                                pool_size=3,
                                                **config)
    @contextmanager
    def connect(self):
        cnx = None
        try:
            cnx = self.pool.get_connection()
            cur = cnx.cursor()
            yield Cursor(cur)
            # mysql.connector.errors.PoolError: Failed getting connection; pool exhausted
        finally:
            if cur:
                cur.close()
            if cnx:
                cnx.close()

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
