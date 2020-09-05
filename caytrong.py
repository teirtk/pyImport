import re
import io
import os
import operator
from datetime import timedelta, date
import pandas as pd
import numpy as np
from xlrd import biffh
from fuzzywuzzy import process
import config


def get_date(df):
    def get_date_str(s):
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
    for s in df.columns:
        if isinstance(s, str):
            result = get_date_str(s)
            if result is not None:
                return result
    for row in df.itertuples():
        for (_, s) in enumerate(row):
            if isinstance(s, str):
                result = get_date_str(s)
                if result is not None:
                    return result
    return None


def fix_addr(s, s1=""):
    if s1:
        result = process.extractOne(
            s, config.town_list[s1].keys(), score_cutoff=30)
        if result is None:
            return ("", "")
        (sa, _) = result
        return ("", f"{config.town_list[s1][sa][1]} {sa}")
    else:
        result = process.extractOne(
            s, config.town_list.keys(), score_cutoff=30)
        if result is None:
            return ("", "")
        (sa, _) = result
        for key in config.town_list[sa]:
            return (config.town_list[sa][key][0], sa)


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


def get_town(mota2, names):
    def get_ave(name, ratio):
        return (name, (ratio+sum(process.extractOne(subname, config.town_list[name])[
            1] for subname in names)) / (names_len+1))
    names_len = len(names)
    choices = process.extract(mota2, config.town_list.keys(), limit=10)
    mota2 = max([get_ave(name, ratio)
                 for name, ratio in choices], key=operator.itemgetter(1))[0]
    names_arr = ["" for i in range(names_len)]
    tmp = [process.extract(name, config.town_list[mota2], limit=20)
           for name in names]
    for i in range(names_len):
        val, _, id = max(
            [item[0]+(id,) for id, item in enumerate(tmp)], key=operator.itemgetter(1))
        names_arr[id] = val
        tmp[id][0] = ("", 0)
        for j in range(names_len):
            if len(names_arr[j]):
                continue
            while tmp[j][0][0] == val:
                tmp[j].pop(0)
    return mota2, names_arr


keyword = {"Lúa", "Mía", "Dừa", "Đậu xanh", "Khóm",
           "Cây ăn quả", "Cây rau, màu", "Cây lâu năm khác"}


def do_process(file, conn):
    basename = os.path.basename(file)
    buffer = io.StringIO()
    header = True
    dfa = []
    names = []
    with pd.ExcelFile(file) as xls:
        for idx, name in enumerate(xls.sheet_names):
            df = pd.read_excel(xls, sheet_name=name, encoding='utf-8')
            if not idx:
                mota2 = name
                fdate = get_date(df.head(20))
                if fdate is None:
                    return f"{basename}: Không lấy được ngày tháng \n"
                continue
            col2 = get_col(df)
            df = df.reindex(
                ["Unnamed: 1", col2], axis="columns")
            first_row = get_first_row(df['Unnamed: 1'])
            if first_row < 0:
                continue
            df = df.loc[first_row:, :]
            df = df.dropna(subset=["Unnamed: 1"]).reset_index(drop=True)
            df['Unnamed: 1'] = df['Unnamed: 1'].astype(str).str.lower().replace(
                config.my_dict['caytrong'], regex=True).str.strip().str.capitalize()
            df[col2] = df[col2].astype(str).replace(
                {r'[A-Za-z]+': '', r'\s+': ''}, regex=True)
            df['dup'] = df.duplicated(['Unnamed: 1'], keep=False)
            df.loc[df['Unnamed: 1'] == 'Đậu các loại', 'dup'] = False
            df.loc[df['Unnamed: 1'] == "Khác", 'dup'] = False
            df[col2] = pd.to_numeric(
                df[col2], errors='coerce').round(2).apply(str)
            df['nhom'] = df['Unnamed: 1'].str.strip().where(
                df['Unnamed: 1'].isin(keyword) & ~df['dup'], np.nan).fillna(method='ffill')
            df['thuoctinhlb'] = df['Unnamed: 1'].where(
                ~df['dup']).fillna(method='ffill')
            df = df[~df['thuoctinhlb'].astype(
                str).str.contains('Cây rau, màu', na=False)]
            df = df[df['dup'] & df[col2].astype(float).gt(0)]
            df['thuoctinh'] = '"' + \
                df['thuoctinhlb'].apply(str).str.strip() + '":' + df[col2]
            dfa.append(df)
            names.append(name)
    mota2, names = get_town(mota2, names)
    try:
        for idx, df in enumerate(dfa):
            df.rename(columns={"Unnamed: 1": 'chuyenmuc'}, inplace=True)
            df["nhom"] = df["nhom"].apply(str).replace({'Đậu xanh': 'Đậu'})
            dfp = df.groupby(["nhom", "chuyenmuc"]).agg(
                {"thuoctinh": ",".join})
            dfp['thuoctinh'] = "{" + dfp['thuoctinh'] + "}"
            dfp["fdate"] = fdate
            dfp["mota1"] = names[idx]
            dfp["mota2"] = mota2
            dfp.to_csv(buffer, header=header)
            if header:
                header = False
        if not buffer.getvalue().count('\n'):
            return f"{basename}: Sai định dạng \n"
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
        if nline:
            return f"{basename}: {nline:,} dòng được thêm \n"
        return f"{basename}: Dữ liệu đã có (bỏ qua) \n"
    except biffh.XLRDError:
        return f"{basename}: bị bảo vệ\n"
    except TypeError:
        return f"{basename}: Sai định dạng ở sheet {name}\n"
    finally:
        buffer.close()
