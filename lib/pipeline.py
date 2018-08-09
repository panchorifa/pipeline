from lib import parser, reader

def process(db, source, table_name, pk_idx, pk_type):
    entry = reader.csv_entries(source)
    field_names = parser.parse_header(next(entry))
    dump_id = db.upsert_table(source, table_name, field_names, pk_idx, pk_type)
    try:
        line=0
        for values in entry:
            line = line+1
            db.upsert_record(table_name, field_names, values, dump_id, line, pk_idx)
        db.dump_completed(table_name, dump_id)
    except:
        db.dump_failed(table_name, dump_id)
