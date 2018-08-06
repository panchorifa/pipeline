DEFAULT_FIELD_TYPE = 'VARCHAR(255)'

def field(name, pk):
    type = pk.type if pk and pk.name == name else DEFAULT_FIELD_TYPE
    return '{name} {type}'.format(name=name, type=type)

def fields(names, pk):
    return ', '.join([field(name, pk) for name in names])

def dump_table_name(name):
    return '{}_dumps'.format(name)

def create_table_dumps(table_name):
    fields = ('id INT not null auto_increment primary key, '
              'source VARCHAR(255) not null, '
              'table_name VARCHAR(255) not null, '
              "status ENUM('started', 'completed', 'failed'), "
              'created_at TIMESTAMP NOT NULL DEFAULT NOW(), '
              'updated_at TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE now(), '
              'UNIQUE KEY(source, table_name)')
    sql = 'CREATE TABLE {}({})'.format(dump_table_name(table_name), fields)
    print(sql)
    return sql

def start_dump(source, table_name):
    values = ', '.join(["'{}'".format(x) for x in [source, table_name, 'started']])
    sql = 'INSERT INTO {}(source, table_name, status) values({})'.format(dump_table_name(table_name), values)
    print(sql)
    return sql

def default_fields(table_name, dump_table_name):
    return ', '.join([
        'dump_id int not null',
        'line_id int not null',
        # 'created_at TIMESTAMP NOT NULL DEFAULT NOW(), ',
        # 'updated_at TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE now(), ',
        'FOREIGN KEY fk_{}_dump(dump_id) REFERENCES {}(id) ON DELETE CASCADE'.format(table_name, dump_table_name)
    ])

def create_table(table_name, field_names, pk, history=False):
    pk_field = 'PRIMARY KEY({})'.format(pk.name) if pk else ''
    name = '{}_history'.format(table_name) if history else table_name
    dft_fields = default_fields(name, dump_table_name(table_name))
    cols = ', '.join([fields(field_names, pk), dft_fields, pk_field])
    sql = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(name, cols)
    print(sql)
    return sql

def create_table_history(table_name, field_names, pk):
    return create_table(table_name, field_names, pk, history=True)

def add_column(name, type=DEFAULT_FIELD_TYPE):
    return 'ADD COLUMN {} {}'.format(name, type)

def alter_table(table, latest_fields):
    new_names = [k for k in latest_fields if k not in table.fields]
    if(new_names):
        new_cols = ',\n'.join([add_column(name) for name in new_names])
        return 'ALTER TABLE {}\n{};'.format(table.name, new_cols)
    return None

def insert_record(table, values):
    values = ', '.join(["'{}'".format(x) for x in values])
    field_names = ', '.join([col for col in table.fields.keys()])
    return 'INSERT INTO {} ({})\nVALUES({})'.format(table.name, field_names, values)
