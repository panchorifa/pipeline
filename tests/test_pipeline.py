# https://semaphoreci.com/community/tutorials/testing-python-applications-with-pytest

def load(file_path):
    return []

# def test_pipeline():
#     config = {
#         host: 'mysql',
#         db: 'external',
#         user: 'root',
#         password: 'password'
#     }
#     db = dbs.db(config)
#     table = db.load('../samples/npi-sample-2016-02-07.csv', 'npi')
#     rows = table.all()
#     assert len(rows) == 2
#     assert rows[0]['npi'] == '123'
    

# def test_create_table():
#     load('../samples/npi-sample-2016-02-07.csv')
#     # add_employee = ("INSERT INTO employees "
#     #            "(first_name, last_name, hire_date, gender, birth_date) "
#     #            "VALUES (%s, %s, %s, %s, %s)")
#     # data_employee = ('Geert', 'Vanderkelen', tomorrow, 'M', date(1977, 6, 14))
#     # cursor.execute(add_employee, data_employee)
#     # emp_no = cursor.lastrowid



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









# CMD ["python", "pipeline.py", "--source-url", "https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-02-07.csv",
# "--sink-user", "root",
 # "--sink-password", "password",
 # "--sink-host", "mysql",
 # "--sink-database", "external",
 # "--sink-table", "npi"]
