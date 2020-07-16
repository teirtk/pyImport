import os
import json
from psycopg2 import pool
from flashtext import KeywordProcessor

with open('conf/config.json') as f:
    data = json.load(f)
with open('mod/caytrong.dict') as f:
    data = eval(f.read())
    caytrong_dict = KeywordProcessor()
    caytrong_dict.add_keywords_from_dict(data)
if 'PORT' in os.environ:
    db = data['dbhero']
else:
    db = data['db']
ext = data['ext']

pgPool = pool.ThreadedConnectionPool(
    1, 10, database=db["db"], user=db["user"],
    password=db["passwd"], host=db["host"], port=db["port"])
