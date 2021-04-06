from datetime import date, datetime
import psycopg2
import json

name_db = 'test'
name_file = 'bd.json'
name_tbl = 'person'

def converter(obj):
    if isinstance(obj, date):
        return obj.__str__()


pg_conn = psycopg2.connect(dbname= name_db, user='postgres',
                      password='34ubitav', host='localhost', port=5432)

if pg_conn is not None:
    cur= pg_conn.cursor()
    sql = 'select * from ' + name_tbl
    cur.execute(sql)
    rows = cur.fetchall()
    db_name_dict = {'NameTable': name_tbl}
    rows.insert(0, db_name_dict)
    rows_json = json.dumps(rows, indent=4, default=converter)
    with open(name_file, 'w') as file_json:
        file_json.write(rows_json)
        file_json.close()
    cur.close()

pg_conn.close()


