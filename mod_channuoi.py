
import io
import os
import pandas as pd


def get_col(df):
    f = False
    for col in df.columns:
        if f:
            return col
        elif col == 'Unnamed: 1':
            f = True
    return 0


def get_first_row(ds):
    for index, value in ds.items():
        if value == 'Họ và tên':
            return index
    return -1


def process(file, postgreSQL_pool):
    basename = os.path.basename(file)
    count = 0
    with pd.ExcelFile(file) as xls:
        for _, name in enumerate(xls.sheet_names):
            if name.startswith("Sheet"):
                continue
            # try:
            df = pd.read_excel(xls, sheet_name=name)
            if df.empty:
                continue
            first_row = get_first_row(df.iloc[:, 1])
            if first_row > 0:
                df = df.iloc[first_row+1:, 0:22]
                df = df.loc[:, ~(df == 'Tổng').any()]
                df = df.dropna(subset=[df.columns.values[0], df.columns.values[1]]
                               ).reset_index(drop=True)
                df = df.iloc[:, 1:]
                df.columns = ["hoten", "ap", "xa", "gade", "gathit", "vitde",
                              "vitthit", "vitxiem", "heonai", "heonoc", "heothit",
                              "trau", "bo", "cho", "meo", "tho", "cuu", "cut"]
               # buffer = io.StringIO()
                df.to_csv('data', index=False)
                nline = 0
                with open('data', 'r', encoding='utf8') as f:
                    for nline, _ in enumerate(f):
                        pass
                #nline = buffer.getvalue().count('\n')
                count += nline
    if count > 0:
        return "{f}: {n} dòng được thêm \n".format(f=basename, n=count)
    return "{f}: Sai định dạng \n".format(f=basename)
