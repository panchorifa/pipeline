import requests
from lib import pipeline
from lib.model import Pk

SOURCE_FILE = '/usr/app/tests/samples/simple/file-1.csv'
SOURCE_FILE2 = '/usr/app/tests/samples/simple/file-2.csv'
SOURCE_STREAM = 'https://s3.amazonaws.com/npi-sample-2016-02-07.csv'

def test_pipeline_simple(dbapi, requests_mock):
    assert dbapi.tables() == []
    pk = Pk(0, 'int')

    with open(SOURCE_FILE2, 'r') as source_file:
        requests_mock.get(SOURCE_STREAM, text=source_file.read())

    dump1 = pipeline.process(SOURCE_FILE, 'npi', dbapi, pk)
    assert dbapi.tables() == ['npi', 'npi_archives', 'npi_dumps']
    assert dbapi.count('npi_dumps') == 1
    assert dbapi.count('npi') == 3
    assert dbapi.count('npi_archives') == 0
    # Header is the first line and the first data line should be 2
    assert dbapi.rows('npi', ['line'])[0][0] == 2

    dump2 = pipeline.process(SOURCE_STREAM, 'npi', dbapi, pk)
    assert dbapi.tables() == ['npi', 'npi_archives', 'npi_dumps']
    assert dbapi.count('npi_dumps') == 2
    assert dbapi.count('npi') == 5
    assert dbapi.count('npi_archives') == 1

    assert dbapi.count('npi', dump1) == 2
    assert dbapi.count('npi', dump2) == 3

## Uncomment to test the full file
## TODO Improve performance
# SAMPLE_FILE = '/usr/app/tests/samples/npi-sample-2016-02-07.csv'
#
# def test_sample_with_weirdness(dbconfig, requests_mock):
#     with open(SAMPLE_FILE, 'r') as source_file:
#         requests_mock.get(SOURCE_STREAM, text=source_file.read())
#     pk = Pk(0, 'int')
#     dump1 = pipeline.process_cli(SOURCE_STREAM, 'npi', dbconfig, pk)
#     assert dump1 == 1
#     assert dbapi.count('npi') == 8260
