import csv
import pytest
import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager
from lib import ddl
from lib import model

class Cursor:
    def __init__(self, cur):
        self.cur = cur
        self.schema = self.latest_schema()

    def latest_schema(self):
        tables = {name: model.Table(name, self.table_fields(name)) for name in self.table_names()}
        return model.Schema(tables)

    def table_count(self, table_name):
        return self.cur.execute('select count from {}'.format(table_name))

    def table_names(self):
        self.cur.execute('show tables')
        return [name for (name,) in self.cur.fetchall()]

    def table_fields(self, table_name):
        self.cur.execute('describe {}'.format(table_name))
        return {x[0] : model.Field(x[0], x[1], x[3]=='PRI') for x in self.cur.fetchall()}

    def referenced_table_names(self, table_name):
        self.cur.execute("select table_name from information_schema.KEY_COLUMN_USAGE where referenced_table_name = '{}'".format(table_name))
        return  [x for (x,) in self.cur.fetchall()]

    def drop_tables(self):
        for name in self.table_names():
            for ref_name in self.referenced_table_names(name):
                self.cur.execute('drop table if exists {}'.format(ref_name))
            self.cur.execute('drop table if exists {}'.format(name))

    def upsert_table(self, source, table_name, field_names, pk_idx=None, pk_type=None):
        print('1simona.......................>>>>>>>>>')
        print(field_names)
        print('1simona.......................>>>>>>>>>')

        existing_table = self.latest_schema().tables.get(table_name)
        if(not existing_table):
            pk = model.Field(field_names[pk_idx], pk_type) if pk_idx != None else None
            self.cur.execute(ddl.create_table_dumps(table_name))
            print('simona.......................>>>>>>>>>')
            print(field_names)
            print('simona.......................>>>>>>>>>')
            self.cur.execute(ddl.create_table(table_name, field_names, pk))
            self.cur.execute(ddl.create_table_archives(table_name, field_names, pk))
            self.cur.execute(ddl.start_dump(source, table_name))
            return self.cur.lastrowid
        else:
            sql = ddl.alter_table(existing_table, field_names)
            print('------------------------')
            print(sql)
            print('------------------------')
            if sql:
                self.cur.execute(sql)
            self.cur.execute(ddl.get_dump(source, table_name))
            res = self.cur.fetchone()
            print('------------------------')
            print(res)
            print('------------------------')
            if not res:
                self.cur.execute(ddl.start_dump(source, table_name))
                return self.cur.lastrowid
            return res

    def upsert_record(self, table_name, field_names, values, dump_id, line):
        print(table_name)
        table = self.latest_schema().tables.get(table_name)
        if(table):
            try:
                sql = ddl.insert_record(table_name, field_names, values, dump_id, line)
                print(sql)
                self.cur.execute(sql)
                print(self.cur.lastrowid)
            except mysql.connector.errors.IntegrityError as e:
                print('ERROR>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
                print('ERROR>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
                print('ERROR>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

                print(dir(e))
                print(e.args)
                print(e.msg)
                print(e.errno)
                print(e.sqlstate)
                if(e.msg.find('Duplicate entry')>=0):
                    print('99999999999')
                    print('99999999999')
                    print('99999999999')
                    print('99999999999')
                #     print('ERROR>>>>>>>>>>>>>>>>>>>>>>>>>>>>>2')
                self.cur.execute(ddl.archive_record(table, id))
                sql = ddl.update_record(table_name, field_names, values, dump_id, line)
                self.cur.execute(sql)
                raise Exception('simona')

        # TODO throw error

    def rows(self, table_name, field_names):
        fields = ', '.join(field_names)
        sql = 'SELECT {} FROM {}'.format(fields, table_name)
        print(sql)
        self.cur.execute(sql)
        x = [x for x in self.cur.fetchall()]
        print(x)
        return x

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
