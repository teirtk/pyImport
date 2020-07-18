import re
import io
import os
from datetime import timedelta, date
import pandas as pd
import numpy as np
from xlrd import biffh
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
            return rdate + timedelta(months=1)
        return date.fromisoformat(f"{y}-{m}-{d}")
    return None


def fix_addr(s):
    mota_dict = {"Tx ": "Thị xã ", "Tp ": "Thành phố ", "Ghâu": "Châu",
                 "Tt ": "Thị trấn ", "H ": "Huyện ", "P ": "Phường "}
    mota_dict = dict((re.escape(k), v) for k, v in mota_dict.items())
    pattern = re.compile("|".join(mota_dict.keys()))
    s = s.title().strip().replace(".", "")
    s = pattern.sub(lambda m: mota_dict[re.escape(m.group(0))], s)
    return s


def fix_dict(s):
    s = config.caytrong_pat.sub(lambda m: config.caytrong_dict[m.group(0)], s, flags=re.IGNORECASE)
    return s
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
rep = {r"\s*\d{4}\s*-\s*\d{4}\s*": "",
       r"Khác \(.*": "Khác"}


def process(file, conn):
    basename = os.path.basename(file)
    buffer = io.StringIO()
    header = True
    try:
        with pd.ExcelFile(file) as xls:
            for idx, name in enumerate(xls.sheet_names):
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
                df['Unnamed: 1'] = df['Unnamed: 1'].apply(str).replace(
                    rep, regex=True).str.strip()
                df['Unnamed: 1'] = df['Unnamed: 1'].apply(str).replace(config.caytrong_dict,regex=False)
                #print(df.to_string())
                df[col2] = df[col2].astype(str).replace(
                    {r'[A-Za-z]+': '', r'\s+': ''}, regex=True)
                df = df[~df['Unnamed: 1'].str.contains('GHI CHÚ', na=False)]
                df['dup'] = df.duplicated(['Unnamed: 1'], keep=False)
                df.loc[df['Unnamed: 1'].str.contains(
                    r'Đậu các loại', na=True), 'dup'] = False
                df[col2] = pd.to_numeric(
                    df[col2], errors='coerce').round(2).apply(str)
                df['nhom'] = df['Unnamed: 1'].str.strip().where(
                    df['Unnamed: 1'].isin(keyword) & ~df['dup'], np.nan).fillna(method='ffill')
                df['thuoctinhlb'] = df['Unnamed: 1'].where(
                    ~df['dup']).fillna(method='ffill')
                df = df[~df['thuoctinhlb'].astype(
                    str).str.contains('Cây Rau, Màu', na=False)]
                df = df[df['dup'] & df[col2].astype(float).gt(0)]
                df['thuoctinh'] = '"' + \
                    df['thuoctinhlb'].apply(str).str.strip() + '":' + df[col2]
                df.rename(columns={"Unnamed: 1": 'chuyenmuc'}, inplace=True)
                df["nhom"] = df["nhom"].apply(str).replace({'Đậu Xanh': 'Đậu'})
                dfp = df.groupby(["nhom", "chuyenmuc"]).agg(
                    {"thuoctinh": ",".join})
                dfp['thuoctinh'] = "{" + dfp['thuoctinh'] + "}"
                dfp["fdate"] = fdate
                dfp["mota1"] = mota1
                dfp["mota2"] = mota2
                dfp.to_csv(buffer, header=header)
                if header:
                    header = False
    except biffh.XLRDError:
        return f"{basename}: bị bảo vệ\n"
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
