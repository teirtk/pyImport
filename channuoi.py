
from datetime import date
import io
import os
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


def get_so_nha(ds):
    for index, item in ds.items():
        if not isinstance(item, str):
            continue
        if item.lower() == "số nhà":
            return index
    return -1


def get_first_row(df):
    ds = df.head(20).iloc[:, 1]
    for index, value in ds.items():
        if not isinstance(value, str):
            continue
        if value.lower() == 'họ và tên':
            return index
    return -1


def get_date(s):
    r1 = re.findall(r"([0-9]+)\.([0-9]+)\.([0-9]+)", s)
    if len(r1) > 0:
        (d, m, y) = r1[0]
        if len(d) == 1:
            d = "0" + d
        if len(m) == 1:
            m = "0" + m
        return date.fromisoformat(f"{y}-{m}-{d}")
    r1 = re.findall(r"([0-9]+) năm ([0-9]+)", s.lower())
    if len(r1) > 0:
        (m, y) = r1[0]
        if len(m) == 1:
            m = "0" + m
        return date.fromisoformat(f"{y}-{m}-01")
    r1 = re.findall(r"([0-9]+)\/([0-9]+) năm ([0-9]+)", s.lower())
    if len(r1) > 0:
        (d, m, y) = r1[0]
        if len(d) == 1:
            d = "0" + d
        if len(m) == 1:
            m = "0" + m
        return date.fromisoformat(f"{y}-{m}-{d}")
    return None


def get_huyen(ds):
    for row in ds.itertuples():
        for (_, value) in enumerate(row):
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
                        r'(quận|huyện|thị xã|thành phố) (.+)', arr[0])
                    if len(r1) > 0:
                        (p1, p2) = r1[0]
                        return (f"{p1.capitalize()} {p2.title()}", x)
    return (None, None)


def do_process(file, conn):
    basename = os.path.basename(file)
    nline = 0
    fdate = None
    huyen = None
    dfa = []
    header = False
    with pd.ExcelFile(file) as xls:
        for _, name in enumerate(xls.sheet_names):
            if name.startswith("Sheet"):
                continue
            df = pd.read_excel(xls, sheet_name=name, header=None)
            if fdate is None:
                (huyen, fdate) = get_huyen(df.head(10))
            if len(df.index) > 300:
                dfa.append(df)

    if fdate is None:
        return f"{basename}: Sai định dạng \n"
    buffer = io.StringIO()
    try:
        for df in dfa:
            first_row = get_first_row(df)
            drop_col = get_so_nha(df.iloc[first_row, :])
            if drop_col >= 0:
                df.drop(df.columns[drop_col], axis='columns', inplace=True)
            if first_row < -1:
                continue
            columns = ["hoten", "ap", "xa", "gade", "gathit", "vitde", "vitthit",
                       "vitxiem", "heonai", "heonoc", "heothit"]
            columns.extend(
                get_col(df.iloc[first_row + 1, :].str.replace(r'\s', '').str.lower()))
            columns.extend(
                get_col(df.iloc[first_row + 2, :].str.replace(r'\s', '').str.lower()))
            df = df.drop(
                [df.columns.values[4], df.columns.values[7], df.columns.values[11]], axis=1)
            df = df.iloc[first_row + 3:, 0:len(columns) + 1]
            df = df.dropna(subset=[df.columns.values[1],
                                   df.columns.values[2]]).reset_index(drop=True)
            df = df.iloc[:, 1:]
            df.columns = columns
            df.iloc[:, 3:] = df.iloc[:, 3:].fillna(0).applymap(
                lambda x: 0 if isinstance(x, str) else x)
            df["hoten"] = df["hoten"].str.strip().str.title()
            for col in rep.values():
                if col not in columns:
                    df[col] = 0
            df["huyen"] = huyen
            df["fdate"] = fdate
            df.drop_duplicates().to_csv(buffer, index=False, header=header, line_terminator="\n")
            if header:
                header = False
        if buffer.getvalue().count('\n') > 1:
            buffer.seek(0)
            with conn.cursor() as cur:
                cur.execute(f"CREATE TEMP TABLE tmp_table ON COMMIT DROP AS "
                            f"TABLE {config.ext['channuoi']['table']} WITH NO DATA;")
                cur.copy_expert(
                    "COPY tmp_table FROM STDIN WITH CSV HEADER", buffer)
                cur.execute(f"INSERT INTO {config.ext['channuoi']['table']} "
                            f"SELECT * FROM tmp_table EXCEPT "
                            f"SELECT * FROM {config.ext['channuoi']['table']};")
                nline = cur.rowcount
                conn.commit()
        if nline:
            return f"{basename}: {nline:,} dòng được thêm \n"
        else:
            return f"{basename}: Dữ liệu đã có (bỏ qua) \n"
    except TypeError:
        return f"{basename} bị lỗi\n"
    finally:
        buffer.close()
