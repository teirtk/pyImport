
from datetime import date
import re
import os
import io
import pandas as pd
import config


def convert(s, getdate):
    if isinstance(s, str):
        if getdate:
            r1 = re.findall(r"([0-9]+) năm ([0-9]+)", s)
        else:
            r1 = re.findall(r"([0-9]+) -  ([0-9]+)", str(s))
        if len(r1) > 0:
            return r1[0]
    return (s,)


def process(file, postgreSQL_pool,upsert = False):
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
            df = df.iloc[11:, 1:].reset_index(drop=True).drop(columns=[
                "Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 10", "Unnamed: 14",
                "Unnamed: 16", "Unnamed: 18", "Unnamed: 20", "Unnamed: 21"])

            df.columns = ["loai", "nhom", "svgh", "gdst", "dtnhiemnhe", "dtnhiemtb",
                          "dtnhiemnang", "dttong", "dtmattrang", "dtsokytruoc", "dtphongtru",
                          "phanbo", "fdate", "tdate", "mdpb1", "mdpb2", "mdcao1", "mdcao2"]
            df.loc[:, ["loai", "nhom", "svgh", "gdst"]] = df.loc[:, [
                "loai", "nhom", "svgh", "gdst"]].applymap(
                    lambda x: x.strip() if isinstance(x, str) else x)
            df.loc[:, "nhom"] = df.loc[:, "svgh"].where(
                df.loc[:, "svgh"].str.startswith("Nhóm cây:")).str.replace("Nhóm cây: ", "")
            df.loc[:, ["loai", "nhom"]] = df.loc[:, [
                "loai", "nhom"]].fillna(method="ffill")
            df = df.dropna(subset=["gdst"])
            df.loc[:, ["dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong", "dtmattrang",
                       "dtsokytruoc", "dtphongtru"]] = df.loc[:, [
                           "dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong", "dtmattrang",
                           "dtsokytruoc", "dtphongtru"]].applymap(lambda x: x.replace(',', '')
                                                                  if isinstance(x, str) else x)
        except TypeError:
            return "{0} bị lỗi".format(basename)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        nline = buffer.getvalue().count('\n')
        if nline > 1:
            buffer.seek(0)
            conn = postgreSQL_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS \
                (select * from {0} limit 0);".format(config.ext["dichbenh"]["table"]))
                cur.copy_expert(
                    "COPY tmp_table FROM STDIN WITH CSV HEADER", buffer)
                row = cur.execute("INSERT INTO {0} SELECT * FROM tmp_table \
                ON CONFLICT DO NOTHING;".format(config.ext["dichbenh"]["table"]))
                print(row)
            conn.commit()
            postgreSQL_pool.putconn(conn)
            buffer.close()
            return "{f}: {n} dòng được thêm \n".format(f=basename, n=nline)
        buffer.close()
        return "{f}: Sai định dạng \n".format(f=basename)
