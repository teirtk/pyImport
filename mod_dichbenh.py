import re
import os
import numpy as np
import pandas as pd
from pandas import ExcelFile
from datetime import date
import config
import tempfile
import io
import psycopg2

SQL = "CREATE TABLE IF NOT EXISTS dichbenh (loai character varying(20),nhom character varying(20),svgh character varying(50),gdst character varying(100),dtnhiemnhe numeric,dtnhiemtb numeric,dtnhiemnang numeric,dttong numeric,dtmattrang numeric,dtsokytruoc numeric,dtphongtru numeric,phanbo character varying(100),fdate date NOT NULL,tdate date NOT NULL,mdpb1 numeric,mdpb2 numeric,mdcao1 numeric,mdcao2 numeric);"
TABLE_NAME = "dichbenh"


def convert(s, getdate):
    if type(s) == str:
        if getdate:
            r1 = re.findall(r"([0-9]+) năm ([0-9]+)", s)
        else:
            r1 = re.findall(r"([0-9]+) -  ([0-9]+)", str(s))
        if len(r1):
            return r1[0]
    return (s,)


def process(file):
    basename = os.path.basename(file)
    with pd.ExcelFile(file) as xls:
        try:
            df = pd.read_excel(xls)
            (w, y) = convert(df.loc[6, "Unnamed: 5"], True)
            df["fdate"] = date.fromisocalendar(int(y), int(w), 1)
            df["tdate"] = date.fromisocalendar(int(y), int(w), 7)
            new = df["Unnamed: 6"].str.split("- ", expand=True)
            df["mdpb1"] = new[0]
            df["mdpb2"] = new[1]
            new = df["Unnamed: 7"].str.split("- ", expand=True)
            df["mdcao1"] = new[0]
            df["mdcao2"] = new[1]
            df = df.iloc[11:, 1:].reset_index(drop=True).drop(columns=["Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 10", "Unnamed: 14",
                                                                       "Unnamed: 16", "Unnamed: 18", "Unnamed: 20", "Unnamed: 21"])

            df.columns = ["loai", "nhom", "svgh", "gdst", "dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong",
                          "dtmattrang", "dtsokytruoc", "dtphongtru", "phanbo", "fdate", "tdate", "mdpb1", "mdpb2", "mdcao1", "mdcao2"]
            df.loc[:, ["loai", "nhom", "svgh", "gdst"]] = df.loc[:, [
                "loai", "nhom", "svgh", "gdst"]].applymap(lambda x: x.strip() if isinstance(x, str) else x)
            df.loc[:, "nhom"] = df.loc[:, "svgh"].where(
                df.loc[:, "svgh"].str.startswith("Nhóm cây:")).str.replace("Nhóm cây: ", "")
            df.loc[:, ["loai", "nhom"]] = df.loc[:, [
                "loai", "nhom"]].fillna(method="ffill")
            df = df.dropna(subset=["gdst"])
            df.loc[:, ["dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong", "dtmattrang", "dtsokytruoc", "dtphongtru"]] = df.loc[:, ["dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong",
                                                                                                                                    "dtmattrang", "dtsokytruoc", "dtphongtru"]].applymap(lambda x: x.replace(',', '') if isinstance(x, str) else x)
        except:
            return "{f} bị lỗi".format(basename)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        conn = psycopg2.connect(database=config.db["db"], user=config.db["user"],
                                password=config.db["passwd"], host=config.db["host"], port=config.db["port"])
        with conn.cursor() as cur:
            cur.copy_expert(
                "COPY {0} FROM STDIN WITH CSV HEADER".format(TABLE_NAME), buffer)
        conn.commit()
        conn.close()
        buffer.close()
        return "{f}: {n} dòng được thêm \n".format(f=basename, n=len(df.index)+1)
