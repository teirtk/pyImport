
from datetime import date
import io
import os
import locale
import re
import pandas as pd
import config

rep = {"trâu": "trau",
       "bò": "bo",
       "chó": "cho",
       "mèo": "meo",
       "thỏ": "tho",
       "cừu": "decuu",
       "dê": "decuu",
       "dêcừu": "decuu",
       "dê,cừu": "decuu",
       "cút": "cut",
       "ngỗng": "ngong",
       "bồcâu,cuđất": "bocaucudat"}


def get_col(ds):
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
    r1 = re.findall(r"([0-9]+) năm ([0-9]+)", s.lower())
    if len(r1) > 0:
        (m, y) = r1[0]
        if len(m) == 1:
            m = "0"+m
        return date.fromisoformat(y+"-"+m+"-01")
    r1 = re.findall(r"([0-9]+)\/([0-9]+) năm ([0-9]+)", s.lower())
    if len(r1) > 0:
        (d, m, y) = r1[0]
        if len(d) == 1:
            d = "0"+d
        if len(m) == 1:
            m = "0"+m
        return date.fromisoformat(y+"-"+m+"-01")
    return None


def get_huyen(ds):
    for _, row in ds.items():
        for _, value in row.items():
            if isinstance(value, str):
                s = value.lower().replace(" tx ", " thị xã ").replace(
                    " tp ", " thành phố ")
                arr = s.split(" thời điểm tháng ")
                if len(arr) < 2:
                    arr = s.split(" thời điểm ")
                if len(arr) < 2:
                    arr = s.split(" tháng ")
                if len(arr) > 1:
                    x = get_date(arr[1])
                    r1 = re.findall(
                        r'(huyện|thị xã|thành phố) (.+)', arr[0])
                    if len(r1) > 0:
                        (p1, p2) = r1[0]
                        return (p1.capitalize()+' '+p2.title(), x)
    return (None, None)


def process(file, postgreSQL_pool):
    basename = os.path.basename(file)
    count = 0
    fdate = None
    huyen = None
    dfa = []
    skip = False
    with pd.ExcelFile(file) as xls:
        for _, name in enumerate(xls.sheet_names):
            if name.startswith("Sheet"):
                continue
            df = pd.read_excel(xls, sheet_name=name)
            if df.empty:
                continue
            dfa.append(df)
            if fdate is None:
                (huyen, fdate) = get_huyen(df.head(10))
    if fdate is None:
        return f"{basename}: Sai định dạng \n"
    for df in dfa:
        try:
            first_row = get_first_row(df)
            if first_row < -1:
                continue
            columns = ["hoten", "ap", "xa", "gade", "gathit", "vitde", "vitthit",
                       "vitxiem", "heonai", "heonoc", "heothit"]
            columns.extend(
                get_col(df.iloc[first_row+1, :].str.replace(r'\s', '').str.lower()))
            columns.extend(
                get_col(df.iloc[first_row+2, :].str.replace(r'\s', '').str.lower()))
            df = df.drop(
                [df.columns.values[4], df.columns.values[7], df.columns.values[11]], axis=1)
            df = df.iloc[first_row+3:, 0:len(columns)+1]
            df = df.dropna(subset=[df.columns.values[1], df.columns.values[2],
                                   df.columns.values[3]]).reset_index(drop=True)
            df = df.iloc[:, 1:]
            df.columns = columns
            df.iloc[:, 3:] = df.iloc[:, 3:].fillna(0).applymap(
                lambda x: 0 if isinstance(x, str) else x)
            df["hoten"] = df["hoten"].str.strip().str.title()
            for col in [x for x in rep.values() if not x in columns]:
                df[col] = 0
            df["huyen"] = huyen
            df["fdate"] = fdate
        except TypeError:
            return f"{basename} bị lỗi\n"
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, line_terminator="\n")
        nline = buffer.getvalue().count('\n')
        if nline > 1:
            skip = True
            buffer.seek(0)
            conn = postgreSQL_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(f"CREATE TEMP TABLE tmp_table ON COMMIT DROP AS "
                            f"(SELECT * FROM {config.ext['channuoi']['table']} LIMIT 0);")
                cur.copy_expert(
                    "COPY tmp_table FROM STDIN WITH CSV HEADER", buffer)
                cur.execute(f"INSERT INTO {config.ext['channuoi']['table']} "
                            f"SELECT * FROM tmp_table EXCEPT "
                            f"SELECT * FROM {config.ext['channuoi']['table']};")
                nrow = cur.rowcount
                conn.commit()
            postgreSQL_pool.putconn(conn)
            buffer.close()
            count += nrow
    locale.setlocale(locale.LC_ALL, 'vi_VN.utf-8')
    if count > 0:
        return f"{basename}: {count:n} dòng được thêm \n"
    if skip:
        return f"{basename}: Dữ liệu đã có (bỏ qua) \n"
