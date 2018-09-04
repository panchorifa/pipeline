import csv
import re
import requests
from contextlib import closing

def read(source, pk=None):
    if(source.startswith('http')):
        return read_stream(source, pk)
    return read_file(source, pk)

def read_file(source, pk=None):
    with open(source, 'rt', encoding='UTF8') as csv_file:
        datareader = csv.reader(csv_file)
        yield _parse_header_values(next(datareader))  # yield the header
        for row in datareader:
            yield _parse_data_values(row, pk)

def read_stream(url, pk=None):
    with closing(requests.get(url, stream=True)) as r:
        iterator = r.iter_lines()
        yield _parse_header_string(next(iterator).decode('utf-8')) # yield the header
        for row in iterator:
            yield _parse_data_string(row.decode('utf-8'), pk)

def _sanitize_header(value):
    return re.sub('\([^)]*\)', '', value).strip().lower().replace(' ', '_')

def _parse_header_values(values):
    return [_sanitize_header(col) for col in values]

def _parse_header_string(str):
    return [_sanitize_header(col) for col in _split_commas(str)]

def _sanitize_value(value, pk, idx):
    value = value.replace('"', '')
    if(pk and pk.idx == idx and pk.type == 'int'):
        return int(value)
    return value

def _split_commas(value):
    return next(csv.reader([value]))

def _parse_data_values(values, pk):
    return [_sanitize_value(value, pk, idx) for idx, value in enumerate(values)]

def _parse_data_string(str, pk=None):
    values = _split_commas(str)
    return [_sanitize_value(value, pk, idx) for idx, value in enumerate(values)]
