import csv
import pytest
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager
from lib import ddl
from lib import model

debug = False

class Cursor:
    def __init__(self, cur):
        self.cur = cur
        self.schema = self.latest_schema()

    def latest_schema(self):
        tables = {name: model.Table(name, self.table_fields(name)) for name in self.table_names()}
        return model.Schema(tables)

    def count(self, table_name):
        self.execute('SELECT COUNT(*) FROM {}'.format(table_name))
        (rows, ) = self.cur.fetchone()
        return rows

    def table_names(self):
        self.execute('show tables')
        return [name for (name,) in self.cur.fetchall()]

    def table_fields(self, table_name):
        self.execute('describe {}'.format(table_name))
        return {x[0] : model.Field(x[0], x[1], x[3]=='PRI') for x in self.cur.fetchall()}

    def referenced_table_names(self, table_name):
        self.execute("select table_name from information_schema.KEY_COLUMN_USAGE where referenced_table_name = '{}'".format(table_name))
        return  [x for (x,) in self.cur.fetchall()]

    def drop_tables(self):
        for name in self.table_names():
            for ref_name in self.referenced_table_names(name):
                self.execute('drop table if exists {}'.format(ref_name))
            self.execute('drop table if exists {}'.format(name))

    def dump_completed(self, table_name, dump_id):
        self.execute(ddl.complete_dump(table_name, dump_id))

    def dump_failed(self, table_name, dump_id):
        self.execute(ddl.fail_dump(table_name, dump_id))

    def upsert_table(self, source, table_name, field_names, pk_idx=None, pk_type=None):
        schema = self.latest_schema()
        existing_table = schema.tables.get(table_name)
        if(not existing_table):
            pk = model.Field(field_names[pk_idx], pk_type) if pk_idx != None else None
            self.execute(ddl.create_table_dumps(table_name))
            self.execute(ddl.create_table(table_name, field_names, pk))
            if pk:
                self.execute(ddl.create_table_archives(table_name, field_names, pk))
            self.execute(ddl.start_dump(source, table_name))
            return self.cur.lastrowid
        else:
            sql = ddl.alter_table(existing_table, field_names)
            if sql:
                self.execute(sql)
                self.execute(ddl.alter_table(schema.tables.get('{}_archives'.format(table_name)), field_names))
            self.execute(ddl.get_dump(source, table_name))
            res = self.cur.fetchone()
            print('------------------------')
            print(res)
            print('------------------------')
            if not res:
                self.execute(ddl.start_dump(source, table_name))
                return self.cur.lastrowid
            return res

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
