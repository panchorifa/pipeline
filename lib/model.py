class Pk:
    def __init__(self, idx, type):
        self.idx = idx
        self.type = type

class Schema:
    def __init__(self, tables):
        self.tables = tables

    def get(self, table_name):
        return self.tables.get(table_name)

class Table:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields

    def get(self, field_name):
        return self.fields.get(field_name)

class Field:
    def __init__(self, name, type, pk=False):
        self.name = name
        self.type = type
        self.pk = pk
