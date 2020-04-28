import json
import re
import os
import numpy as np
import pandas as pd
from pandas import ExcelFile
from datetime import date
import config


def convert(s, getdate):
    if type(s) == str:
        if getdate:
            r1 = re.findall(r"([0-9]+) năm ([0-9]+)", s)
        else:
            r1 = re.findall(r"([0-9]+) -  ([0-9]+)", str(s))
        if len(r1):
            return r1[0]
    return (s,)


def process(file, conn):
    table_name = "dichbenh"
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
            df = df.iloc[11:, 1:]
            df = df.reset_index(drop=True)
            df = df.drop(columns=["Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 10", "Unnamed: 14",
                                  "Unnamed: 16", "Unnamed: 18", "Unnamed: 20", "Unnamed: 21"])

            df.columns = ["loai", "nhom", "svgh", "gdst", "dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong",
                          "dtmattrang", "dtsokytruoc", "dtphongtru", "phanbo", "fdate", "tdate", "mdpb1", "mdpb2", "mdcao1", "mdcao2"]

            df.loc[:, "nhom"] = df.loc[:, "svgh"].where(
                df.loc[:, "svgh"].str.startswith("Nhóm cây:")).str.replace("Nhóm cây: ", "")
            df.loc[:, ["loai", "nhom"]] = df.loc[:, [
                "loai", "nhom"]].fillna(method="ffill")
            df.dropna(subset=["gdst"], inplace=True)
            df.loc[:, ["dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong", "dtmattrang", "dtsokytruoc", "dtphongtru"]] = df.loc[:, ["dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong",
                                                                                                                                    "dtmattrang", "dtsokytruoc", "dtphongtru"]].applymap(lambda x: x.replace(',', '') if isinstance(x, str) else x)
        except:
            print("File {f} bị lỗi".format(f=file))
            return
        tmp_file = os.path.join(os.path.dirname(file), "tmp")
        data_file = os.path.join(os.path.dirname(file), "data")
        df.to_csv(tmp_file, index=False)
        with open(tmp_file, "r", encoding="utf8") as f:
            if config.withdb:
                cur = conn.cursor()
                cur.copy_expert(
                    "COPY {t} FROM STDIN WITH CSV HEADER".format(t=table_name), f)
                conn.commit()
            else:
                print(f.read())
        with open(tmp_file, "r", encoding="utf8") as infile, open(data_file, "a", encoding="utf8") as outfile:
            next(infile)
            for line in infile:
                outfile.write(line)
        print("{f}: {n} rows added \n".format(f=file, n=len(df.index)+1))
