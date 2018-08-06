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
              "status ENUM('started', 'completed', 'failed'), "
              'unique key(source)')
    return 'CREATE TABLE {}({})'.format(dump_table_name(table_name), fields)

def create_table(table_name, field_names, pk):
    pk_field = 'PRIMARY KEY({})'.format(pk.name) if pk else ''
    default_fields = ','.join([
        'dump_id int not null',
        'line_id int not null',
        'FOREIGN KEY fk_{}_dump(dump_id) REFERENCES {}(id)'.format(table_name, dump_table_name(table_name))
    ])
    cols = ','.join([fields(field_names, pk), default_fields, pk_field])
    return 'CREATE TABLE IF NOT EXISTS {} ({})'.format(table_name, cols)

def add_column(name, type=DEFAULT_FIELD_TYPE):
    return 'ADD COLUMN {} {}'.format(name, type)

def alter_table(table, latest_fields):
    new_names = [k for k in latest_fields if k not in table.fields]
    if(new_names):
        new_cols = ',\n'.join([add_column(name) for name in new_names])
        return 'ALTER TABLE {}\n{};'.format(table.name, new_cols)
    return None
