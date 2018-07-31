import mysql.connector

class Mysql:
    def __init__(self, host, db):
        self.host = host
        self.db = db

    def connect(self, user, pswd):
        mydb = mysql.connector.connect(
                host=self.host,
                user=user,
                passwd=pswd,
                database=self.db)
        return mydb.cursor()

class Mycursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def execute():
        return self.cursor.execute(cmd)

    def show_tables():
        return self.cursor.execute("SHOW TABLES")

# mycursor.execute(sql = "CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))")
# mycursor.execute("ALTER TABLE customers ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY")
