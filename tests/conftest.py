import pytest
from lib.dbs.mysql import api

debug = False

@pytest.fixture
def dbconfig():
    return {
        'host': 'mysqltest',
        'user': 'root',
        'password': 'passwordtest',
        'database': 'externaltest'
    }

@pytest.fixture
def dbapi(dbconfig):
    with api.Db(dbconfig).connect(debug) as cur:
        cur.drop_tables()
        yield cur
