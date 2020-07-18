import os
import json
import re
from psycopg2 import pool

caytrong_dict = {}
with open("mod/caytrong.dict", "r", encoding="utf8") as f:
    for line in f:
        il = line.rstrip("\n").split("|")
        for k in il[1:]:
            caytrong_dict[k] = il[0]
caytrong_pat = re.compile("|".join(caytrong_dict.keys()))

with open("conf/config.json") as f:
    data = json.load(f)

if 'PORT' in os.environ:
    db = data['dbhero']
else:
    db = data['db']
ext = data['ext']

pgPool = pool.ThreadedConnectionPool(
    1, 10, database=db["db"], user=db["user"],
    password=db["passwd"], host=db["host"], port=db["port"])
