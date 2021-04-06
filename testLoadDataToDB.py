from os.path import join
import psycopg2
import json
from datetime import date
import os
from pathlib import Path
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

path_cur = Path(__file__).parent.absolute()
name_file = 'bd.json'
sql_create_tbl = 'create table '


#create table json
tbl_dict = {'name':'person', 'row_name': ['id', 'name', 'family', 'date'], 'row_type': ['serial primary key',
   "varchar(20) default 'Sergey'", "varchar(20) default 'Syrnikov'", 'date default current_timestamp']}
tbl_json = json.dumps(tbl_dict, indent=4)
with open('tbl.json', 'w') as file_tbl:
    file_tbl.write(tbl_json)
    file_tbl.close()

#read table json
with open('tbl.json', 'r') as file_tbl:
    tbl_data = file_tbl.read()
    file_tbl.close()

tbl_dict = json.loads(tbl_data)
tbl_list = list(tbl_dict.values())
# print(tbl_list)
sql_create_tbl += tbl_list[0] + '('
sql_insert = 'insert into ' + tbl_list[0] + ' values'
tbl_list_row_len = len(tbl_list[1])
for i in range(tbl_list_row_len):
    if (i == (tbl_list_row_len - 1)):
        sql_create_tbl += tbl_list[1][i] + ' ' + tbl_list[2][i] + ');'
        break
    sql_create_tbl += tbl_list[1][i] + ' ' + tbl_list[2][i] + ', '
# sql_create_tbl = "create table person(id serial primary key, name varchar(20) default 'Sergey', family varchar(20) default 'Syrnikov', date date default current_timestamp);"

# print(sql_create_tbl)
# print(sql_insert)



nameDB = 'test'



def converter(obj):
    if isinstance(obj, date):
        return obj.__str__()


if os.path.exists(join(path_cur, name_file)):
    #read rows from file_json
    with open(name_file, 'r') as file_json:
        db_json = file_json.read()
    data_table = json.loads(db_json)


    # print(data_table)
    len_table = len(data_table)
    for i in range(1, len_table):
        if (i == (len_table - 1)):
            sql_insert += str(tuple(data_table[i])) + ';'
            break
        sql_insert += str(tuple(data_table[i])) + ', '
    # print(sql_insert)


    # print(type(data_table))

    db_conn = psycopg2.connect(user='postgres', password='34ubitav', host='localhost', port=5432)

    db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    if db_conn is not None:
        cur = db_conn.cursor()
        sql = 'create database ' + nameDB
        cur.execute(sql)
        cur.close()

    db_conn.close()

    db_conn = psycopg2.connect(dbname=nameDB, user='postgres', password='34ubitav', host='localhost', port=5432)
    if db_conn is not None:
        cur = db_conn.cursor()
        cur.execute(sql_create_tbl)
        db_conn.commit()
        print(sql_insert)
        cur.execute(sql_insert)
        db_conn.commit()
        cur.close()

    db_conn.close()

