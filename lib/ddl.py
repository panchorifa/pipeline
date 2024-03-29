DEFAULT_FIELD_TYPE = 'VARCHAR(255)'

def _field(name, pk):
    type = pk.type if pk and pk.name == name else DEFAULT_FIELD_TYPE
    return '{name} {type}'.format(name=name, type=type)

def _fields(names, pk):
    return ', '.join([_field(name, pk) for name in names])

def _dump_table_name(name):
    return '{}_dumps'.format(name)

def get_dump(source, table):
    dump = _dump_table_name(table)
    sql = "select * from {} where source='{}' and table_name='{}'"
    return sql.format(dump, source, table)

def table_count(table_name):
    return 'SELECT COUNT(*) FROM {}'.format(table_name)

def create_table_dumps(table_name):
    fields = ('id INT not null auto_increment primary key, '
              'source VARCHAR(255) not null, '
              'table_name VARCHAR(255) not null, '
              "status ENUM('started', 'completed', 'failed'), "
              'created_at TIMESTAMP NOT NULL DEFAULT NOW(), '
              'updated_at TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE now(), '
              'UNIQUE KEY(source, table_name)')
    return 'CREATE TABLE {} ({})'.format(_dump_table_name(table_name), fields)

def create_dump(source, table_name):
    fields = [source, table_name, 'started']
    values = ', '.join(["'{}'".format(x) for x in fields])
    sql = 'INSERT INTO {}(source, table_name, status) values({})'
    return sql.format(_dump_table_name(table_name), values)

def update_dump(table_name, dump_id, status):
    return "UPDATE {}_dumps set status='{}' where id={}"\
                    .format(table_name, status, dump_id)

def _default_fields(table_name):
    return ', '.join([
        'dump_id int not null',
        'line int not null',
        'created_at TIMESTAMP NOT NULL DEFAULT NOW()',
        'updated_at TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE now()',
        'FOREIGN KEY fk_{}_dump(dump_id) REFERENCES {}(id) ON DELETE CASCADE'\
        .format(table_name, _dump_table_name(table_name))])

def create_table(table_name, field_names, pk, archives=False):
    pk_name = pk and pk.name or 'id'
    pk_field = None
    if archives:
        pk_field = 'PRIMARY KEY({}, dump_id, line)'.format(pk_name)
    else:
        pk_field = 'PRIMARY KEY({})'.format(pk_name)

    dft_fields = _default_fields(table_name)
    cols = ', '.join([_fields(field_names, pk), dft_fields, pk_field])
    name = '{}_archives'.format(table_name) if archives else table_name
    return 'CREATE TABLE {} ({})'.format(name, cols)

def create_table_archives(table_name, field_names, pk):
    return create_table(table_name, field_names, pk, True)

def _add_column(name, type=DEFAULT_FIELD_TYPE):
    return 'ADD COLUMN {} {}'.format(name, type)

def alter_table(table, latest_fields):
    new_names = [k for k in latest_fields if k not in table.fields]
    if(new_names):
        new_cols = ',\n'.join([_add_column(name) for name in new_names])
        return 'ALTER TABLE {}\n{};'.format(table.name, new_cols)
    return None

def archive_record(table, field_names, values, pk_idx):
    sql = 'INSERT INTO {name}_archives SELECT * FROM {name} WHERE {pk}={id}'
    return sql.format(name=table.name, pk=field_names[pk_idx], id=values[pk_idx])

def insert_record(table_name, field_names, values, dump_id, line):
    names = field_names[:]
    names.extend(['dump_id', 'line'])
    values.extend([dump_id, line])
    values = ', '.join(["'{}'".format(x) for x in values])
    fields = ', '.join(names)
    return 'INSERT INTO {} ({})\nVALUES({})'.format(table_name, fields, values)

def update_record(table_name, field_names, values, dump_id, line, pk_idx):
    fields = ', '.join(["{}='{}'".format(name, values[idx]) \
                    for idx, name in enumerate(field_names) if idx!=pk_idx])
    return 'UPDATE {table_name} SET {fields} WHERE {pk}={id}'\
                .format(table_name=table_name,
                        fields=fields,
                        pk=field_names[pk_idx],
                        id=values[pk_idx])

def reference_tables(table_name):
    sql = ('select table_name from information_schema.KEY_COLUMN_USAGE'
           " where referenced_table_name = '{}'")
    return sql.format(table_name)

def describe_table(table_name):
    return 'describe {}'.format(table_name)

def show_tables():
    return 'show tables'

def drop_table(name):
    return 'drop table if exists {}'.format(name)
