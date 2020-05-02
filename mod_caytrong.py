import pandas as pd
import numpy as np
import json
import re
import io
import os
from pandas import ExcelFile
from datetime import datetime
import config
import psycopg2
# Lấy ngày ra ở sheet đầu tiên

SQL = "CREATE TABLE IF NOT EXISTS caytrong (data jsonb);"
TABLE_NAME = "caytrong"


def extract_date(s):
    r1 = re.findall(r"gày ([0-9]+) tháng ([0-9]+) năm ([0-9]+)", s)
    if len(r1):
        (d, m, y) = r1[0]
        if len(d) == 1:
            d = "0"+d
        if len(m) == 1:
            m = "0"+m
        return y+"-"+m+"-"+d


def fix_addr(s):
    if s.startswith("TX "):
        s = "Thị xã "+s[3:].title()
    elif s.startswith("TT "):
        s = "Thị trấn " + s[3:].title()
    elif s.startswith("TT. "):
        s = "Thị trấn " + s[4:].title()
    elif s.startswith("TP "):
        s = "Thành phố " + s[3:].title()
    elif re.match("huyện ", s, re.I):
        s = s.title()
    elif s.startswith("P "):
        s = "Phường " + s[2:].title()
    else:
        s = "Xã " + s.title()
    return s.strip()


def get_col(df):
    f = False
    for col in df.columns:
        if f:
            return col
        elif col == 'Unnamed: 1':
            f = True


def get_first_row(ds):
    for index, value in ds.items():
        if value == 'Cây Lúa':
            return index


keyword = set(["Lúa", "Mía", "Dừa", "Đậu Xanh", "Khóm",
               "Cây Ăn Quả", "Cây Rau, Màu", "Cây Lâu Năm Khác"])
rep = {"Cây Lúa": "Lúa",
       "Cây Mía": "Mía",
       "Cây Dừa": "Dừa",
       "Cây khóm": "Khóm",
       "Cây ăn quả": "Cây Ăn Quả",
       "Cây rau, màu": "Cây Rau, Màu",
       "Cây lâu năm khác": "Cây Lâu Năm Khác",
       r"Mía \(ép lấy đường\)": "Lấy đường",
       r"Mía \(ép lấy nước giải khát\)": "Lấy nước",
       "Xòai": "Xoài",
       "X.G": "xuống g",
       " tấn/ha": "",
       " vụ/năm": "",
       r" \(ha\)": "",
       " 2015-2016": "",
       " 2015 -2016": "",
       r"^Khác \(.*": "Khác"}


def process(file, postgreSQL_pool):
    basename = os.path.basename(file)
    buffer = io.StringIO()
    count = 0
    with pd.ExcelFile(file) as xls:
        for idx, name in enumerate(xls.sheet_names):
            try:
                if name.startswith("Sheet") or name.startswith("Compa"):
                    continue
                df = pd.read_excel(xls, sheet_name=name, encoding='utf-16le')
                addr = fix_addr(name)
                if idx == 0:
                    mota2 = addr
                    fdate = extract_date(df.to_string())
                    continue
                mota1 = addr
                col2 = get_col(df)
                df = df.reindex(
                    ["Unnamed: 1", col2], axis="columns")
                df = df.loc[get_first_row(df['Unnamed: 1']):, :]

                df = df.dropna(subset=["Unnamed: 1"]
                               ).reset_index(drop=True)
                df['Unnamed: 1'] = df['Unnamed: 1'].replace(
                    rep, regex=True)
                df[col2] = df[col2].astype(str).replace(
                    {r'[A-Za-z]+': '', r'\s+': ''}, regex=True)
                df = df[~df['Unnamed: 1'].str.contains('GHI CHÚ')]
                df['dup'] = df.duplicated(['Unnamed: 1'], keep=False)
                df[col2] = pd.to_numeric(df[col2], errors='coerce')
                df['nhom'] = df['Unnamed: 1'].where(df['Unnamed: 1'].isin(
                    keyword) & ~df['dup'], np.nan).fillna(method='ffill')
                df['chuyenmuc'] = df['Unnamed: 1'].where(
                    ~df['dup']).fillna(method='ffill')
                df = df[~df['chuyenmuc'].astype(
                    str).str.contains('Cây Rau, Màu', na=False)]
                df = df[df['dup'] & df[col2].astype(float).gt(0)]
                df[col2] = df[col2].round(2).apply(str)
                df['thuoctinh'] = '"'+df['chuyenmuc'] + '":'+df[col2]
                dfp = df.groupby(["nhom", "Unnamed: 1"]).agg(
                    {"thuoctinh": lambda x: ",".join(x)})
                for nhom, chuyenmuc, thuoctinh in dfp.to_records():
                    count += 1
                    buffer.write(json.dumps({
                        "nhom": nhom,
                        "chuyenmuc": str(chuyenmuc),
                        "thuoctinh": json.loads("{"+str(thuoctinh)+"}"),
                        "fdate": fdate,
                        "mota1": mota1,
                        "mota2": mota2
                    }, ensure_ascii=False)+"\n")
            except:
                return "{f}: Sai định dạng ở sheet {n}".format(f=basename, n=name)
    if count:
        buffer.seek(0)
        conn = postgreSQL_pool.getconn()
        with conn.cursor() as cur:
            cur.copy_from(buffer, TABLE_NAME)
        conn.commit()
        postgreSQL_pool.putconn(conn)
        buffer.close()
        return "{f}: {n} dòng được thêm \n".format(f=basename, n=count)
    buffer.close()
    return "{f}: Sai định dạng \n".format(f=basename)
