import re
import io
import os
from datetime import timedelta, date
import pandas as pd
import numpy as np
from xlrd import XLRDError
import config


def get_date(s):
    r1 = re.findall(r"gày ([0-9]+) tháng ([0-9]+) năm ([0-9]+)", s)
    if len(r1) > 0:
        (d, m, y) = r1[0]
        if len(d) == 1:
            d = f"0{d}"
        if len(m) == 1:
            m = f"0{m}"
        if d <= "05":
            d = "05"
        elif d <= "15":
            d = "15"
        elif d <= "25":
            d = "25"
        else:
            d = "05"
            rdate = date.fromisoformat(f"{y}-{m}-{d}")
            return rdate+timedelta(months=1)
        return date.fromisoformat(f"{y}-{m}-{d}")
    return None


def fix_addr(s):
    if s.startswith("TX "):
        s = f"Thị xã {s[3:].title()}"
    elif s.startswith("TT "):
        s = f"Thị trấn {s[3:].title()}"
    elif s.startswith("TT. "):
        s = f"Thị trấn {s[4:].title()}"
    elif s.startswith("TP "):
        s = f"Thành phố {s[3:].title()}"
    elif re.match("huyện ", s, re.I):
        s = s.title()
    elif s.startswith("P "):
        s = f"Phường {s[2:].title()}"
    else:
        s = f"Xã {s.title()}"
    return s.strip()


def get_col(df):
    f = False
    for col in df.columns:
        if f:
            return col
        elif col == 'Unnamed: 1':
            f = True
    return -1


def get_first_row(ds):
    for index, value in ds.items():
        if value == 'Cây Lúa':
            return index
    return -1


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
    basename = os.path.basename(file)
    buffer = io.StringIO()
    header = True
    with pd.ExcelFile(file) as xls:
        for idx, name in enumerate(xls.sheet_names):
            try:
                if name.startswith("Sheet") or name.startswith("Compa"):
                    continue
                df = pd.read_excel(xls, sheet_name=name, encoding='utf-16le')
                addr = fix_addr(name)
                if idx == 0:
                    mota2 = addr
                    fdate = get_date(df.head(20).to_string())
                    if fdate is None:
                        return f"{basename}: Không lấy được ngày tháng \n"
                    continue
                mota1 = addr
                col2 = get_col(df)
                df = df.reindex(
                    ["Unnamed: 1", col2], axis="columns")
                df = df.loc[get_first_row(df['Unnamed: 1']):, :]
                df = df.dropna(subset=["Unnamed: 1"]).reset_index(drop=True)
                df['Unnamed: 1'] = df['Unnamed: 1'].astype(str).replace(
                    rep, regex=True).str.strip()
                df[col2] = df[col2].astype(str).replace(
                    {r'[A-Za-z]+': '', r'\s+': ''}, regex=True)
                df = df[~df['Unnamed: 1'].str.contains('GHI CHÚ', na=False)]
                df['dup'] = df.duplicated(['Unnamed: 1'], keep=False)
                df[col2] = pd.to_numeric(df[col2], errors='coerce')
                df['nhom'] = df['Unnamed: 1'].str.strip().where(
                    df['Unnamed: 1'].isin(keyword) & ~df['dup'], np.nan).fillna(method='ffill')
                df['thuoctinhlb'] = df['Unnamed: 1'].where(
                    ~df['dup']).fillna(method='ffill')
                df = df[~df['thuoctinhlb'].astype(
                    str).str.contains('Cây Rau, Màu', na=False)]
                df = df[df['dup'] & df[col2].astype(float).gt(0)]
                df[col2] = df[col2].round(2).apply(str)
                df['thuoctinh'] = '"' + \
                    df['thuoctinhlb'].apply(str).str.strip() + '":'+df[col2]
                df.rename(columns={"Unnamed: 1": 'chuyenmuc'}, inplace=True)
                dfp = df.groupby(["nhom", "chuyenmuc"]).agg(
                    {"thuoctinh": ",".join})
                dfp['thuoctinh'] = "{"+dfp['thuoctinh']+"}"
                dfp["fdate"] = fdate
                dfp["mota1"] = mota1
                dfp["mota2"] = mota2
                dfp.to_csv(buffer, header=header)
                if header:
                    header = False
            except XLRDError:
                return f"{basename}: bị protect\n"
            #except TypeError:
                #return f"{basename}: Sai định dạng ở sheet {name}\n"
    if buffer.getvalue().count('\n') > 1:
        buffer.seek(0)
        with conn.cursor() as cur:
            cur.execute(f"CREATE TEMP TABLE tmp_table ON COMMIT DROP AS "
                        f"TABLE {config.ext['caytrong']['table']} WITH NO DATA")
            cur.copy_expert(
                "COPY tmp_table FROM STDIN WITH CSV HEADER", buffer)
            cur.execute(f"INSERT INTO {config.ext['caytrong']['table']} "
                        f"SELECT * FROM tmp_table EXCEPT "
                        f"SELECT * FROM {config.ext['caytrong']['table']};")
            nline = cur.rowcount
        conn.commit()
        buffer.close()
        if nline > 0:
            return f"{basename}: {nline:,} dòng được thêm \n"
        return f"{basename}: Dữ liệu đã có (bỏ qua) \n"
    buffer.close()
    return f"{basename}: Sai định dạng \n"
