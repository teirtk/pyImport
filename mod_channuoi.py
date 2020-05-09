
from datetime import date
import io
import os
import locale
import re
import pandas as pd
import config


def get_col(ds):
    rep = {"trâu": "trau",
           "bò": "bo",
           "chó": "cho",
           "mèo": "meo",
           "thỏ": "tho",
           "cừu": "cuu",
           "dê": "cuu",
           "dêcừu": "cuu",
           "dê,cừu": "cuu",
           "cút": "cut",
           "ngỗng": "ngong",
           "bồcâu,cuđất": "bocaucudat"}
    col = [rep[x] for _, x in ds.items() if x in rep.keys()]
    return col


def get_first_row(df):
    for value in df.columns.values:
        if value == 'Họ và tên':
            return -1
    ds = df.head(20).iloc[:, 1]
    for index, value in ds.items():
        if value == 'Họ và tên':
            return index
    return -2


def get_date(s):
    r1 = re.findall(r"([0-9]+)\.([0-9]+)\.([0-9]+)", s)
    if len(r1) > 0:
        (d, m, y) = r1[0]
        if len(d) == 1:
            d = "0"+d
        if len(m) == 1:
            m = "0"+m
        return date.fromisoformat(y+"-"+m+"-"+d)
    r1 = re.findall(r"tháng ([0-9]+) năm ([0-9]+)", s.lower())
    if len(r1) > 0:
        (m, y) = r1[0]
        if len(m) == 1:
            m = "0"+m
        return date.fromisoformat(y+"-"+m+"-01")
    return None


def process(file, postgreSQL_pool):
    basename = os.path.basename(file)
    count = 0
    fdate = None
    with pd.ExcelFile(file) as xls:
        for _, name in enumerate(xls.sheet_names):
            if name.startswith("Sheet"):
                continue
            try:
                df = pd.read_excel(xls, sheet_name=name)
                if df.empty:
                    continue
                if fdate is None:
                    fdate = get_date(df.head(10).to_string())
                    print(fdate)
                first_row = get_first_row(df)
                if first_row < -1:
                    continue
                df1 = df.iloc[first_row+2, :]
                df1 = df.iloc[first_row+1, :].where(df1.isna())
                columns = ["hoten", "ap", "xa", "gade", "gathit", "vitde", "vitthit",
                           "vitxiem", "heonai", "heonoc", "heothit"]
                columns.extend(
                    get_col(df.iloc[first_row+1, :].str.replace(r'\s', '').str.lower()))
                columns.extend(
                    get_col(df.iloc[first_row+2, :].str.replace(r'\s', '').str.lower()))

                df = df.loc[:, ~(df == 'Tổng').any()]
                df = df.iloc[first_row+3:, 0:len(columns)+1]

                df = df.dropna(subset=[df.columns.values[1], df.columns.values[2],
                                       df.columns.values[3]]).reset_index(drop=True)
                df = df.iloc[:, 1:]
                df.columns = columns
                df["hoten"] = df["hoten"].str.strip().str.title()
                df["fdate"] = fdate
            except TypeError:
                return f"{basename} bị lỗi"
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            nline = buffer.getvalue().count('\n')
            with open("data", "w+", encoding="utf8") as f:
                f.write(buffer.getvalue())
            if nline > 1:
                buffer.seek(0)
                conn = postgreSQL_pool.getconn()
                with conn.cursor() as cur:
                    cur.execute(f"CREATE TEMP TABLE tmp_table ON COMMIT DROP AS "
                                f"TABLE {config.ext['channuoi']['table']} WITH NO DATA;")
                    cur.copy_expert(
                        "COPY tmp_table FROM STDIN WITH CSV HEADER", buffer)
                    cur.execute(f"INSERT INTO {config.ext['channuoi']['table']} "
                                f"SELECT * FROM tmp_table EXCEPT "
                                f"SELECT * FROM {config.ext['channuoi']['table']};")
                    nrow = cur.rowcount
                    conn.commit()
                postgreSQL_pool.putconn(conn)
                buffer.close()
                print(nrow)
                count += nrow
    locale.setlocale(locale.LC_ALL, 'vi_VN.utf-8')
    if count > 0:
        return f"{basename}: {count:n} dòng được thêm \n"
    return f"{basename}: Sai định dạng \n"
