import os
import json
from psycopg2 import pool

with open("mod/caytrong.dict", "r", encoding="utf8") as f:
    data1 = f.readines()
print(caytrong_dict.get_all_keywords())
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
