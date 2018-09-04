DEFAULT_FIELD_TYPE = 'VARCHAR(255)'

def _field(name, idx, pk):
    type = pk.type if pk and pk.idx == idx else DEFAULT_FIELD_TYPE
    return '{name} {type}'.format(name=name, type=type)

def _fields(names, pk):
    return ', '.join([_field(name, idx, pk) for idx, name in enumerate(names)])

def get_dump(source, table):
    sql = "select * from {name}_dumps where source='{source}' and table_name='{name}'"
    return sql.format(name=table,  source=source)

def count(table_name, dump_id=None):
    where = ' WHERE dump_id = {}'.format(dump_id) if dump_id else ''
    return 'SELECT COUNT(*) FROM {}{}'.format(table_name, where)

def create_dumps(table_name):
    fields = ('id INT not null auto_increment primary key, '
              'source VARCHAR(255) not null, '
              'table_name VARCHAR(255) not null, '
              "status ENUM('started', 'completed', 'failed'), "
              'created_at TIMESTAMP NOT NULL DEFAULT NOW(), '
              'updated_at TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE now(), '
              'UNIQUE KEY(source, table_name)')
    return 'CREATE TABLE {}_dumps ({})'.format(table_name, fields)

def insert_dump(source, table_name):
    fields = [source, table_name, 'started']
    values = ', '.join(["\"{}\"".format(x) for x in fields])
    sql = 'INSERT INTO {}_dumps (source, table_name, status) values({})'
    return sql.format(table_name, values)

def update_dump(table_name, dump_id, status):
    return "UPDATE {}_dumps set status='{}' where id={}"\
                    .format(table_name, status, dump_id)

def _default_fields(table_name):
    return ', '.join([
        'dump_id int not null',
        'line int not null',
        'created_at TIMESTAMP NOT NULL DEFAULT NOW()',
        'updated_at TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE now()',
        'FOREIGN KEY fk_{name}_dump(dump_id) REFERENCES {name}_dumps (id) ON DELETE CASCADE'\
        .format(name=table_name)])

def create_table(table_name, field_names, pk, archives=False):
    pk_name = field_names[pk.idx] if pk else 'id'
    pk_field = None
    if archives:
        pk_field = 'PRIMARY KEY({}, dump_id, line)'.format(pk_name)
    else:
        pk_field = 'PRIMARY KEY({})'.format(pk_name)

    dft_fields = _default_fields(table_name)
    cols = ', '.join([_fields(field_names, pk), dft_fields, pk_field])
    name = '{}_archives'.format(table_name) if archives else table_name
    return 'CREATE TABLE {} ({})'.format(name, cols)

def create_archives(table_name, field_names, pk):
    return create_table(table_name, field_names, pk, True)

def _add_column(name, type=DEFAULT_FIELD_TYPE):
    return 'ADD COLUMN {} {}'.format(name, type)

def alter_table(table, latest_fields):
    new_names = [k for k in latest_fields if k not in table.fields]
    if(new_names):
        new_cols = ',\n'.join([_add_column(name) for name in new_names])
        return 'ALTER TABLE {}\n{};'.format(table.name, new_cols)
    return None

def archive_record(table, field_names, values, pk):
    sql = 'INSERT INTO {name}_archives SELECT * FROM {name} WHERE {pk}={id}'
    return sql.format(name=table.name, pk=field_names[pk.idx], id=values[pk.idx])

def insert_record(table_name, field_names, values, dump_id, line):
    names = field_names[:]
    names.extend(['dump_id', 'line'])
    values.extend([dump_id, line])
    fields = ', '.join(names)
    values = ["\"{}\"".format(x) if isinstance(x, str) else x for x in values]
    formatted_values = (str(len(values)*'{},')[:-1]).format(*values)
    sql = 'INSERT INTO {} ({})\nVALUES({})'
    return sql.format(table_name, fields, formatted_values)

def update_record(dump_id, line, table_name, field_names, values, pk):
    names = field_names[:]
    names.extend(['dump_id', 'line'])
    values.extend([dump_id, line])
    # TODO clean this
    fields = ', '.join(["{}={}".format(name, \
        "'{}'".format(values[idx]) if isinstance(values[idx], str) else values[idx]) \
        for idx, name in enumerate(names) if idx!=pk.idx])
    return 'UPDATE {table_name}\n SET {fields}\n WHERE {pk}={id}'\
                .format(table_name=table_name,
                        fields=fields,
                        pk=names[pk.idx],
                        id=values[pk.idx])

def show_tables():
    return 'show tables'

def describe_table(table_name):
    return 'describe {}'.format(table_name)

def drop_table(name):
    return 'drop table if exists {}'.format(name)

def reference_tables(table_name):
    sql = ('select table_name from information_schema.KEY_COLUMN_USAGE'
           " where referenced_table_name = '{}'")
    return sql.format(table_name)
