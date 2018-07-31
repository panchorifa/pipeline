# https://semaphoreci.com/community/tutorials/testing-python-applications-with-pytest

def test_pipeline():
    assert 'simona' == 'simona'

def test_pipeline2():
    assert 'simona' == 'simona'



# ./pipeline.py
#     --source-url
#         https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-02-07.csv
#
#     --sink-user
#         root
#
#     --sink-password
#         password
#
#     --sink-host
#         mysql
#
#     --sink-database
#         external
#
#     --sink-table
#         npi
