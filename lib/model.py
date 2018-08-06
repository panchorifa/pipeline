class Schema:
    def __init__(self, tables):
        self.tables = tables

class Table:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields

class Field:
    def __init__(self, name, type, pk=False):
        self.name = name
        self.type = type
        self.pk = pk
