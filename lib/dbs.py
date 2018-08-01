import mysql.connector

def db(host='mysql'):
    return Mysql(host) # Only mysql supported for now


# def authenticate(func):
#     def authenticate_and_call(*args, **kwargs):
#         if not Account.is_authentic(request):
#             raise Exception('Authentication Failed.')
#         return func(*args, **kwargs)
#     return authenticate_and_call

# def with_cursor(func):
#     def with_cursor_and_call(*args, **kwargs):
#         cur = None
#         try:
#             cur = self.cnx.cursor()
#             cur.execute(cmd)
#             return cur.fetchall()
#         finally:
#             cur and cur.close()
#         return func(*args, **kwargs)
#     return with_cursor_and_call

class Mysql:
    def __init__(self, host):
        self.host = host
        self.cnx = None
        self.cur = None

    def connect(self, db, user, pswd):
        self.cnx = mysql.connector.connect(
                host = self.host,
                user = user,
                password = pswd,
                database = db)

    def databases(self, user, pswd):
        cnx = cur = None
        try:
            cnx = mysql.connector.connect(
                host = self.host,
                user = user,
                password = pswd)
            cur = cnx.cursor()
            cur.execute('show databases')
            return cur.fetchall()
        finally:
            cur and cur.close()
            cnx and cnx.close()

    def tables(self):
        return self.execute('show tables')

    def close():
        self.cnx and self.cnx.close()

    def execute(self, ddl):
        cur = None
        try:
            cur = self.cnx.cursor()
            cur.execute(ddl)
            return cur.fetchall()
        finally:
            cur and cur.close()
