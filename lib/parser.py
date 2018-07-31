import re

def parse_header_column(col):
    return re.sub('\([^)]*\)', '', col).strip().lower().replace(' ', '_')

def parse_header(header):
    return [parse_header_column(col) for col in header.split(',')]

def parse_data_value(entry):
    return None if entry == 'NULL' else entry

def parse_data(data):
    return [parse_data_value(value) for value in data.split(',')]
