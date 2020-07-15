import os
import json
from psycopg2 import pool

with open('conf/config.json') as config_file:
    data = json.load(config_file)
if 'PORT' in os.environ:
  # elephantsql
    db = data['dbhero']
else:
    db = data['db']
ext = data['ext']

pgPool = pool.ThreadedConnectionPool(
    1, 10, database=db["db"], user=db["user"],
    password=db["passwd"], host=db["host"], port=db["port"])
