# Reading h5 file
import time

import h5py
import pandas as pd
import numpy as np
from h5_parse.mysql_tool import *
from datetime import datetime as dt

h5_store = pd.HDFStore(path='h5_data/rpt_data.h5', mode='r+')  # 创建新的对象、读入已存在的对象
a = h5_store.keys()
print(a)
df = pd.DataFrame(h5_store.get('/data'))
print(df.columns)
df.fillna('', inplace=True)
# df = df.loc[:100, :]

# -----------------------数据清洗-----------------------#
df['current_create_date'] = df[['current_create_date']].apply(
    lambda x: str(dt.fromtimestamp(float(str(x['current_create_date']).replace("000000000", ""))).date()), axis=1)

df['previous_create_date'] = df[['previous_create_date']].apply(lambda x:
                                                                str(x['previous_create_date']).replace('.0', ''), axis=1
                                                                )
# -----------------------数据入库-----------------------#
sqlObj = MysqlDao()
sqlObj.insert_table(table_name='rpt_earnings_adjust', df_values=df,
                    type_dict={"title": "VARCHAR(150)", "report_id": "INT", "report_type": "INT",
                               "stockcode": "VARCHAR(20)", "stock_name": "VARCHAR(50)",
                               "organ_id": "INT", "organ_name": "VARCHAR(20)",
                               "author": "VARCHAR(100)",
                               "current_create_date": "DATE", "previous_create_date": "DATE",
                               "ann_date": "DATE", "entrytime": "DATE",
                               "PK": "report_id"})

# df.to_csv(path_to)
# h5_store.close()
