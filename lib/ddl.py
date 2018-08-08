DEFAULT_FIELD_TYPE = 'VARCHAR(255)'

def field(name, pk):
    type = pk.type if pk and pk.name == name else DEFAULT_FIELD_TYPE
    return '{name} {type}'.format(name=name, type=type)

def fields(names, pk):
    return ', '.join([field(name, pk) for name in names])

def dump_table_name(name):
    return '{}_dumps'.format(name)

def get_dump(source, table):
    dump = dump_table_name(table)
    sql = "select * from {} where source='{}' and table_name='{}'".format(dump, source, table)
    print(sql)
    return sql

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
        'line int not null',
        # 'created_at TIMESTAMP NOT NULL DEFAULT NOW(), ',
        # 'updated_at TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE now(), ',
        'FOREIGN KEY fk_{}_dump(dump_id) REFERENCES {}(id) ON DELETE CASCADE'.format(table_name, dump_table_name)
    ])

def create_table(table_name, field_names, pk):
    pk_field = 'PRIMARY KEY({})'.format(pk.name) if pk else ''
    dft_fields = default_fields(table_name, dump_table_name(table_name))
    cols = ', '.join([fields(field_names, pk), dft_fields, pk_field])
    sql = 'CREATE TABLE IF NOT EXISTS {} (id INT not null auto_increment primary key, {})'.format(table_name, cols)
    print(sql)
    return sql

def create_table_archives(table_name, field_names, pk):
    return 'CREATE TABLE {}_archives LIKE {}'.format(table_name, table_name)

def add_column(name, type=DEFAULT_FIELD_TYPE):
    return 'ADD COLUMN {} {}'.format(name, type)

def alter_table(table, latest_fields):
    new_names = [k for k in latest_fields if k not in table.fields]
    if(new_names):
        new_cols = ',\n'.join([add_column(name) for name in new_names])
        return 'ALTER TABLE {}\n{};'.format(table.name, new_cols)
    return None

def archive_record(table, id):
    sql = 'insert into {name}_archives select * from {name} where {pk}={id}'
    return sql.format(name=table.name, pk=table.pk.name, id=id)

def insert_record(table_name, field_names, values, dump_id, line):
    field_names.extend(['dump_id', 'line'])
    values.extend([dump_id, line])
    values = ', '.join(["'{}'".format(x) for x in values])
    fields = ', '.join(field_names)
    sql = 'INSERT INTO {} ({})\nVALUES({})'.format(table_name, fields, values)
    print('========================')
    print(sql)
    print('========================')
    return sql
