from lib import ddl

def test_create_table():
    cols = ['nip', 'provider_last_name', 'provider_first_name']
    cmd = ddl.create_table('nip', cols)
    assert cmd == ('CREATE TABLE IF NOT EXISTS nip ('
                    'nip TEXT, '
                    'provider_last_name TEXT, '
                    'provider_first_name TEXT)')

    # cursor.execute()
    # db.commit()
