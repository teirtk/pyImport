import os
import json
import re
import csv

from psycopg2 import pool


def read_dict():
    my_dict = {}
    my_dict["caytrong"] = {
        "-": "",
        r"\s*\d{4}\s*": "",
        r"khác \(.*": "Khác"}
    my_dict["dichbenh"] = {}
    my_dict["channuoi"] = {}
    my_dict["dict_hc"] = {}
    for file in os.listdir("cfg"):
        (filename, ext) = os.path.splitext(file)
        if ext == ".dict":
            with open(os.path.join("cfg", file), "r", encoding="utf8") as f:
                for line in f:
                    il = line.rstrip("\n").split("|")
                    for k in il[1:]:
                        my_dict[filename][re.escape(k.lower())] = il[0]
    return my_dict


def read_town_list():
    town_list = {}
    with open("cfg/ds.csv", newline='', encoding="utf-8") as f:
        reader = csv.reader(f)
        for rows in reader:
            if len(rows[5]) > 0:
                town_list[int(rows[5])] = rows[:-1]
    return town_list


version_string = "v1.5.1"
my_dict = read_dict()
town_list = read_town_list()
with open("cfg/config.json", "r") as f:
    data = json.load(f)

db = data['db']
ext = data['ext']
upload_folder = data['upload_folder']
pgPool = pool.ThreadedConnectionPool(
    1, 10, database=db["db"], user=db["user"],
    password=db["passwd"], host=db["host"], port=db["port"])
