
from datetime import date
import re
import os
import io
import pandas as pd
import config


def get_date(s, getdate):
    if isinstance(s, str):
        if getdate:
            r1 = re.findall(r"([0-9]+) năm ([0-9]+)", s)
        else:
            r1 = re.findall(r"([0-9]+) -  ([0-9]+)", str(s))
        if len(r1) > 0:
            return r1[0]
    return (s,)


rep = {"TX ": "Thị xã ",
       "TP ": "Thành phố "}


def process(file, postgreSQL_pool):
    basename = os.path.basename(file)
    with pd.ExcelFile(file) as xls:
        try:
            df = pd.read_excel(xls)
            (w, y) = get_date(df.loc[6, "Unnamed: 5"], True)
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
            df["phanbo"] = df["phanbo"].replace(rep, regex=True)
            df["nhom"] = df["svgh"].where(
                df["svgh"].str.startswith("Nhóm cây:")).str.replace("Nhóm cây: ", "")
            df.loc[:, ["loai", "nhom"]] = df.loc[:, [
                "loai", "nhom"]].fillna(method="ffill")
            df = df.dropna(subset=["gdst"])
            df.loc[:, ["dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong", "dtmattrang",
                       "dtsokytruoc", "dtphongtru"]] = df.loc[:, [
                           "dtnhiemnhe", "dtnhiemtb", "dtnhiemnang", "dttong", "dtmattrang",
                           "dtsokytruoc", "dtphongtru"]].applymap(lambda x: x.replace(',', '')
                                                                  if isinstance(x, str) else x)
        except TypeError:
            return f"{basename} bị lỗi"
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        nline = buffer.getvalue().count('\n')
        if nline > 1:
            buffer.seek(0)
            conn = postgreSQL_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(f"CREATE TEMP TABLE tmp_table ON COMMIT DROP AS "
                            f"TABLE {config.ext['dichbenh']['table']} WITH NO DATA;")
                cur.copy_expert(
                    "COPY tmp_table FROM STDIN WITH CSV HEADER", buffer)
                cur.execute(f"INSERT INTO {config.ext['dichbenh']['table']} "
                            f"SELECT * FROM tmp_table EXCEPT "
                            f"SELECT * FROM {config.ext['dichbenh']['table']};")
                nrow = cur.rowcount
                conn.commit()
                postgreSQL_pool.putconn(conn)
                buffer.close()
                if nrow > 0:
                    return f"{basename}: {nrow:,} dòng được thêm \n"
                return f"{basename}: Dữ liệu đã có (bỏ qua) \n"
        buffer.close()
        return f"{basename}: Sai định dạng \n"
