import argparse
import json
import sys
from pathlib import Path

from db_connection.Connection import Connection

# Connect Database
mydb = Connection().connect()
mycursor = mydb.cursor()

# Initialize ArgumentParser
parser = argparse.ArgumentParser()

# Required Argument
parser.add_argument("d", help="Source Data Location")

# Optional Argument
parser.add_argument("-o", "--option", default='insert', help="Choose Operation [Default insert]",
                    choices=["insert", "update", "update-on-conflict"])

# Retrieve Argument
args = parser.parse_args()

json_path, option = args.d, args.option

try:
    json_file = open(json_path)
except OSError:
    print("Could not open/read the File", json_path)
    sys.exit()

json_data = json.load(json_file)

if not len(json_data):
    print("File is empty", json_path)
    sys.exit()

table_name = Path(json_path).stem

table_col_structure, table_col = '', ''

for col in json_data[1].keys():
    table_col_structure = table_col_structure + col + " varchar(255),"
    table_col = table_col + "`" + col + "`, "

mycursor.execute(
    "CREATE TABLE IF NOT EXISTS " + table_name + " (id int NOT NULL AUTO_INCREMENT, " + table_col + " PRIMARY KEY(id));")

if option == 'insert':

    ins = "INSERT INTO " + table_name + " (" + table_col + ") VALUES "

    for d in json_data:
        ins = ins + "("
        for k in d.keys():
            ins = ins + "'" + str(d[k]) + "',"
        ins = ins + "), "

    ins = ins + ";"

    mycursor.execute(ins)

elif option == 'update':

    for d in json_data:
        upd = "UPDATE " + table_name + " SET "

        for k in d.keys():
            upd = upd + k + " = " + "'" + str(d[k]) + "',"
        upd = upd + " WHERE uuid = " + d['uuid']

        mycursor.execute(upd)


elif option == 'update-on-conflict':

    for d in json_data:
        rpls = "REPLACE INTO " + table_name + " SET "

        for k in d.keys():
            rpls = rpls + k + " = " + "'" + str(d[k]) + "',"
        rpls = rpls + " WHERE uuid = " + d['uuid']

        mycursor.execute(rpls)
