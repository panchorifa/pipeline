import re

def parse_header_column(col):
    return re.sub('\([^)]*\)', '', col).strip().lower().replace(' ', '_')

# def parse_header(header):
#     return [parse_header_column(col) for col in header.split(',')]

# def parse_data_value(entry):
#     return None if entry == 'NULL' else entry
#
# def parse_data(data):
#     return [parse_data_value(value) for value in data.split(',')]

def parse_header(values):
    return [parse_header_column(col) for col in values]


# class Table:
#     def __init__(name, field_names):
#         self.name = name
#         self.fields = []
#         for name in fields_names:
#             if(name.endsWith('_text')):
#                 self.fields.append(Field(name[:-5], Field.TYPE_TEXT))
#             else:
#                 self.fields.append(Field(name, Field.TYPE_TEXT))
#
# class Field:
#     TYPE_TEXT = 'text'
#     def __init__(name, type):
#         self.name = name
#         self.type = type
