def field(name, type='TEXT', pk=None):
    return '{name} {type}{pk}'.format(name=name, type=type,
                                      pk=' PRIMARY_KEY' if pk == name else '')

def fields(names):
    return ', '.join([field(name) for name in names])

def create_table(name, cols, pk=None, fk=[]):
    return 'CREATE TABLE IF NOT EXISTS {} ({})'.format(name, fields(cols))
