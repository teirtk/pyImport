import glob
import json
import re

import numpy as np
import pandas as pd
from pandas import ExcelFile


def convert(s, prefix):
    if type(s) == str:
        r1 = re.findall(r'([0-9]+) -  ([0-9]+)', str(s))
        if len(r1):
            (f, t) = r1[0]
            return json.loads('{"' + prefix + '1":' + f + ',"' + prefix + '2":' + t + '}')
        else:
            return json.loads('{"' + prefix + '1":' + s + '}')
    return np.nan


file_path = glob.glob("./Pandas_in/*.*")
with open('data.json', 'w+', encoding='utf-8') as f:
    for file in file_path:
        with pd.ExcelFile(file) as ExcelFile.xls:
            for idx, name in enumerate(ExcelFile.xls.sheet_names):
                df = pd.read_excel(ExcelFile.xls, sheet_name=name)
                df = df.iloc[11::, 1:]
                df = df.reset_index(drop=True)
                df = df.drop(
                    columns=['Unnamed: 5', 'Unnamed: 10', 'Unnamed: 14', 'Unnamed: 16', 'Unnamed: 18', 'Unnamed: 20',
                             'Unnamed: 21'])
                df.columns = ['loai', 'nhom', 'svgh', 'gdst', 'mdpb', 'mdcao', 'dtnhiemnhe', 'dtnhiemtb', 'dtnhiemnang',
                              'dttong', 'dtmattrang', 'dtsokytruoc', 'dtphongtru', 'phanbo']
                df.loc[:, 'nhom'] = df.loc[:, 'svgh'].where(df.loc[:, 'svgh'].str.startswith('Nhóm cây:')).replace(
                    'Nhóm cây: ', '')
                df.loc[:, ['loai', 'nhom']] = df.loc[:, ['loai', 'nhom']].fillna(method='ffill')
                df = df[df['phanbo'].notna()]
                df.loc[:, 'mdpb'] = df.loc[:, 'mdpb'].apply(convert, prefix='mdpb')
                df.loc[:, 'mdcao'] = df.loc[:, 'mdcao'].apply(convert, prefix='mdcao')
                df.loc[:, 'dtnhiemnhe':'dtphongtru'] = df.loc[:, 'dtnhiemnhe':'dtphongtru'].applymap(lambda x: x if isinstance(x, str) or x > 0 else np.nan)
                for index, row in df.iterrows():
                    f.write(json.dumps(row.dropna().to_dict(), ensure_ascii=False) + "\n")
