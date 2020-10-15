import os
import json
import re
from psycopg2 import pool


def read_dict():
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


def read_town_list():
    with open("cfg/town.csv", "r", encoding="utf-8") as f:
        for line in f:
            il = line.rstrip("\n").split(",")
            if il[0] not in town_list.keys():
                town_list[il[0]] = []
            town_list[il[0]].append(il[1])

version_string="v1.3"
my_dict = {}
town_list = {}
read_dict()
read_town_list()
with open("cfg/config.json", "r") as f:
    data = json.load(f)

db = data['db']
ext = data['ext']

pgPool = pool.ThreadedConnectionPool(
    1, 10, database=db["db"], user=db["user"],
    password=db["passwd"], host=db["host"], port=db["port"])
