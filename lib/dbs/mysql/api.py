import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager

from lib.dbs.mysql.cursor import Cursor


class Db:
    def __init__(self, config, pool_name='dftpool', pool_size=5):
        self.config = config
        self.pool = pooling.MySQLConnectionPool(pool_name=pool_name,
                                                pool_size=pool_size,
                                                **config)
    @contextmanager
    def connect(self, debug=False):
        cnx = None
        try:
            cnx = self.pool.get_connection()
            cur = cnx.cursor()
            yield Cursor(cur, debug)
        finally:
            if cur:
                cur.close()
            if cnx:
                cnx.close()
