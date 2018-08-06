DEFAULT_FIELD_TYPE = 'varchar(255)'

def field(name, pk):
    type = pk and pk.type or DEFAULT_FIELD_TYPE
    return '{name} {type}{pk}'.format(name=name, type=type,
                                      pk=' PRIMARY_KEY' if pk else '')

def fields(names, pk):
    return ', '.join([field(name, pk) for name in names])

def create_table(table_name, field_names, pk):
    cols = fields(field_names, pk)
    return 'CREATE TABLE IF NOT EXISTS {} ({})'.format(table_name, cols)
