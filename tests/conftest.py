import pytest
from lib import mysql

dbconfig = {
    'host': 'mysql',
    'user': 'root',
    'password': 'password',
    'database': 'external'
}

@pytest.fixture
def cur():
    with mysql.Db(dbconfig).connect() as cur:
        cur.drop_tables()
        yield cur

@pytest.fixture
def db():
    with mysql.Db(dbconfig).connect() as cur:
        cur.drop_tables()
        yield cur
