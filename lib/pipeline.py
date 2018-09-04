from lib import reader
from lib.dbs.mysql import api
from lib.model import Pk

def process(source, table, dbapi, pk):
    entries = reader.read(source, pk)
    field_names = next(entries) # header
    dump_id = dbapi.upsert_table(source, table, field_names, pk)
    line=1
    try:
        for values in entries:
            line = line+1
            dbapi.upsert_record(dump_id, line, table, field_names, values, pk)
        dbapi.dump_completed(table, dump_id)
        print('Done. Processed {} lines.'.format(line))
        return dump_id
    except Exception as ex:
        print("*********************************************")
        print("*********************************************")
        print("*********************************************")
        print("Error Found.\nsource: {}\n line: {}".format(source, line))
        print(ex)
        print("*********************************************")
        print("*********************************************")
        print("*********************************************")
        dbapi.dump_failed(table, dump_id)

def process_cli(source, table, dbconfig, pk_idx=0, pk_type='int', debug=False):
    pk = Pk(pk_idx, pk_type)
    with api.Db(dbconfig).connect(debug) as dbapi:
        process(source, table, dbapi, pk)
