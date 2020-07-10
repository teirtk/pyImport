import os

from psycopg2 import pool

if 'PORT' in os.environ:
  # elephantsql
    db = {
        "host": "satao.db.elephantsql.com",
        "db": "ajkkxcga",
        "user": "ajkkxcga",
        "passwd": "J4cZeMrHCUAJd0tEIUarjALSVQ8KBQtC",
        "port": "5432"
    }
else:
    db = {
        "host": "localhost",
        "db": "postgres",
        "user": "postgres",
        "passwd": "b7ec6b08dc1f4f3383663c0ecdf5dda7",
        "port": "5432"
    }
ext = {
    "dichbenh": {
        "table": "dichbenh",
        "sql": """CREATE TABLE IF NOT EXISTS dichbenh (loai character varying(20),
            nhom character varying(20),svgh character varying(50),gdst character varying(100),
            dtnhiemnhe numeric,dtnhiemtb numeric,dtnhiemnang numeric,dttong numeric,dtmattrang numeric,
            dtsokytruoc numeric,dtphongtru numeric,phanbo character varying(200),fdate date NOT NULL,
            tdate date NOT NULL,mdpb1 numeric,mdpb2 numeric,mdcao1 numeric,mdcao2 numeric);"""
    },
    "caytrong": {
        "table": "caytrong",
        "sql": """CREATE TABLE IF NOT EXISTS caytrong (nhom character varying(30),
            chuyenmuc character varying(30), thuoctinh jsonb, fdate date NOT NULL, 
            mota1 character varying(30), mota2 character varying(30));"""
    },
    "channuoi": {
        "table": "channuoi",
        "sql": """CREATE TABLE IF NOT EXISTS channuoi (hoten character varying(100),
            ap character varying(100),xa character varying(100),gade numeric,gathit
            numeric,vitde numeric,vitthit numeric,vitxiem numeric,heonai numeric,
            heonoc numeric,heothit numeric,trau numeric,bo numeric,cho numeric,
            meo numeric,ngong numeric,tho numeric, decuu numeric, cut numeric,bocaucudat numeric,
                huyen character varying(100), fdate date NOT NULL);"""
    }
}

pgPool = pool.ThreadedConnectionPool(
    1, 10, database=db["db"], user=db["user"],
    password=db["passwd"], host=db["host"], port=db["port"])
