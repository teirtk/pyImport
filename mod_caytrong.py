import pandas as pd
import numpy as np
import json
import re
import os
from pandas import ExcelFile
from datetime import datetime
import config

# Lấy ngày ra ở sheet đầu tiên


def extract_date(s):
    r1 = re.findall(r"gày ([0-9]+) tháng ([0-9]+) năm ([0-9]+)", s)
    if len(r1):
        (d, m, y) = r1[0]
        if len(d) == 1:
            d = "0"+d
        if len(m) == 1:
            m = "0"+m
        return d+"-"+m+"-"+y


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


def process(file, conn):
    table_name = "caytrong"
    tmp_file = os.path.join(os.path.dirname(file), "tmp")
    data_file = os.path.join(os.path.dirname(file), "data")
    with open(tmp_file, "w+", encoding="utf8") as f, pd.ExcelFile(file) as xls:
        count = 0
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
                    f.write(json.dumps({
                        "nhom": nhom,
                        "chuyenmuc": str(chuyenmuc),
                        "thuoctinh": json.loads("{"+str(thuoctinh)+"}"),
                            "fdate": datetime.strptime(fdate, "%d-%m-%Y").isoformat(),
                            "mota1": mota1,
                            "mota2": mota2
                            }, ensure_ascii=False)+"\n")
            except:
                print("File {f} bị lỗi ở sheet {n}".format(f=file, n=name))
                continue

    with open(data_file, "r", encoding="utf8") as f:
        if config.withdb:
            cur = conn.cursor()
            cur.copy_from(f, table_name)
            conn.commit()
        elif config.verbose:
            print(f.read())
    with open(tmp_file, "r", encoding="utf8") as infile, open(data_file, "a", encoding="utf8") as outfile:
        outfile.write(infile.read())
    print("{f}: {n} rows added \n".format(f=file, n=count))
