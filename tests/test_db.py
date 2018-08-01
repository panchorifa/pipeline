from lib import dbs

def test_connection():
    db = dbs.db('mysql')
    databases = db.databases('root', 'password')
    assert len(databases) == 5
    assert databases[0] == ('information_schema',)
    assert databases[1] == ('external',)
    assert databases[2] == ('mysql',)
    assert databases[3] == ('performance_schema',)
    assert databases[4] == ('sys',)
    # , 'sys', 'mysql']
    db.connect('external', 'root', 'password')
    # assert conn == []
    # x = cnx.execute('show databases')
    assert db.tables() == []
    # assert db.connect('root', 'password').tables() == []
    # assert
