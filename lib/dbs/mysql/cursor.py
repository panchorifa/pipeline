import mysql.connector
from lib.model import Schema, Table, Field
from lib.dbs.mysql import ddl

class Cursor:
    def __init__(self, cur, debug=False):
        self.cur = cur
        self.debug = debug
        self.schema = self.latest_schema()

    def _field(self, values):
        return Field(values[0], values[1], values[3]=='PRI')

    def count(self, table_name, dump_id=None):
        self.execute(ddl.count(table_name, dump_id))
        return self.cur.fetchone()[0]

    def _table(self, name):
        return Table(name, self.fields(name))

    def latest_schema(self):
        return Schema({name: self._table(name) for name in self.tables()})

    def dump_completed(self, table_name, dump_id):
        self.execute(ddl.update_dump(table_name, dump_id, 'completed'))

    def dump_failed(self, table_name, dump_id):
        self.execute(ddl.update_dump(table_name, dump_id, 'failed'))

    # Creates all tables for a dump
    # x_dumps, x and x_archives for now
    def _dump(self, source, table_name, field_names, pk):
        self.execute(ddl.create_dumps(table_name))
        self.execute(ddl.create_table(table_name, field_names, pk))
        if pk:
            self.execute(ddl.create_archives(table_name, field_names, pk))
        self.execute(ddl.insert_dump(source, table_name))
        return self.cur.lastrowid

    def _update_dump_schema(self, source, table, field_names):
        sql = ddl.alter_table(table, field_names)
        if sql:
            self.execute(sql)
            archives = self.schema.get('{}_archives'.format(table.name))
            self.execute(ddl.alter_table(archives, field_names))
        self.execute(ddl.get_dump(source, table.name))
        res = self.cur.fetchone()
        if not res:
            self.execute(ddl.insert_dump(source, table.name))
            return self.cur.lastrowid
        return res

    def upsert_table(self, source, table_name, field_names, pk):
        try:
            table = self.schema.get(table_name)
            if(not table):
                return self._dump(source, table_name, field_names, pk)
            return self._update_dump_schema(source, table, field_names)
        finally:
            self.schema = self.latest_schema() #update schema

    def upsert_record(self, dump_id, line, table_name, field_names, values, pk):
        table = self.schema.tables.get(table_name)
        if(table):
            try:
                self.execute(ddl.insert_record(table_name, field_names, values, dump_id, line))
            except mysql.connector.errors.IntegrityError as e:
                # Record already exists. Archive current entry and update all fields
                if(e.msg.find('Duplicate entry')>=0):
                    self.execute(ddl.archive_record(table, field_names, values, pk))
                    self.execute(ddl.update_record(dump_id, line, table_name, field_names, values, pk))

    def rows(self, table_name, field_names, where_statement=None):
        fields = ', '.join(field_names)
        where = ' {}'.format(where_statement) if where_statement else ''
        self.execute('SELECT {} FROM {}{}'.format(fields, table_name, where))
        return [value for value in self.cur.fetchall()]

    def tables(self):
        self.execute(ddl.show_tables())
        return [name for (name,) in self.cur.fetchall()]

    def referenced_tables(self, table_name):
        self.execute(ddl.reference_tables(table_name))
        return [name for (name,) in self.cur.fetchall()]

    def fields(self, table_name):
        self.execute(ddl.describe_table(table_name))
        return [f[0] for f in self.cur.fetchall()]

    def drop_tables(self):
        try:
            for name in self.tables():
                for ref_name in self.referenced_tables(name):
                    self.execute(ddl.drop_table(ref_name))
                self.execute(ddl.drop_table(name))
        finally:
            self.schema = self.latest_schema()

    def execute(self, cmd):
        if self.debug:
            print(cmd)
            print('-----------------------------------------------------------')
        return self.cur.execute(cmd)
