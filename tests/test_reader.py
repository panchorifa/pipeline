import requests
from lib import reader
from lib.model import Pk

SOURCE_FILE = '/usr/app/tests/samples/simple/file-1.csv'
SOURCE_STREAM = 'https://s3.amazonaws.com/npi-sample-2016-02-07.csv'
HEADER = ['NIP', 'Provider Last Name (Legal Name)', 'Provider First Name']

pk = Pk(0, 'int')

def test_reader(requests_mock):
    file_entry = reader.read_file(SOURCE_FILE, pk)
    with open(SOURCE_FILE, 'r') as source_file:
        requests_mock.get(SOURCE_STREAM, text=source_file.read())
    stream_entry = reader.read_stream(SOURCE_STREAM, pk)
    for entry in [file_entry, stream_entry]:
        header = next(entry)
        assert len(header) == 26
        assert header[0] == 'npi'
        assert header[1] == 'provider_last_name'
        assert header[2] == 'provider_first_name'
        assert header[25] == 'healthcare_provider_taxonomy_code_3'

        data = next(entry)
        assert data[0] == 1003000167
        assert data[1] == 'ESCOBAR'
        assert data[2] == 'JULIO'
        assert data[25] == 'NULL'

def test_full_read(requests_mock):
    file_entry = reader.read_file(SOURCE_FILE)
    with open(SOURCE_FILE, 'r') as source_file:
        requests_mock.get(SOURCE_STREAM, text=source_file.read())
    stream_entry = reader.read_stream(SOURCE_STREAM)

    for entry in [file_entry, stream_entry]:
        next(entry) # Header
        lines = [line for line in entry]
        assert len(lines) == 3

def test_parse_data_string():
    values = reader._parse_data_string('1003000464,WOLF,LAUREL,NULL,NULL,NULL,P.T.,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1837 RIDGE RD,NULL,KLAMATH FALLS,OR,976035361,US,5418840376,NULL,F,225100000X,225100000X,225100000X')
    assert len(values) == 26
    values = reader._parse_data_string('1003001835,ANDERSON,EDITH,LEWIS,MRS.,NULL,"R. N., B. S. N.",NULL,NULL,NULL,NULL,NULL,NULL,NULL,3708 MAIN ST,NULL,BELLE CHASSE,LA,700373002,US,5043935624,5043935633,F,163W00000X,NULL,NULL')
    assert len(values) == 26
    assert values[4] == 'MRS.'
    assert values[5] == 'NULL'
    assert values[6] == 'R. N., B. S. N.'
