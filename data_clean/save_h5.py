# Reading h5 file
import time

import h5py
import pandas as pd
import numpy as np
from data_clean.mysql_tool import *
from datetime import datetime as dt


# 'h5_data/rpt_data.h5'

class ReadH5:
    def __init__(self, file_path):
        self.h5_store = pd.HDFStore(path=file_path, mode='r+')  # 创建新的对象、读入已存在的对象
        self.keys = self.h5_store.keys()
        self.df_h5 = None
        self.SqlObj = MysqlDao()

    def get_df(self):
        print(self.keys)
        # self.df_h5 = pd.DataFrame(self.h5_store.get('/data'))

    def insert_db(self, tabke_name):
        # -----------------------数据清洗-----------------------#
        # 时间处理
        self.df_h5['current_create_date'] = self.df_h5[['current_create_date']].apply(
            lambda x: str(dt.fromtimestamp(float(str(x['current_create_date']).replace("000000000", ""))).date()),
            axis=1)
        # 时间处理
        self.df_h5['previous_create_date'] = self.df_h5[['previous_create_date']].apply(
            lambda x: str(str(x['previous_create_date']).replace('.0', '')), axis=1)

        # -----------------------数据入库-----------------------#
        self.SqlObj.insert_table(tabke_name, self.df_h5,
                                 {"title": "VARCHAR(150)", "report_id": "INT", "report_type": "INT",
                                  "stockcode": "VARCHAR(20)", "stock_name": "VARCHAR(50)",
                                  "organ_id": "INT", "organ_name": "VARCHAR(20)",
                                  "author": "VARCHAR(100)",
                                  "current_create_date": "DATE", "previous_create_date": "VARCHAR(100)",
                                  "ann_date": "DATE", "entrytime": "DATE",
                                  "PK": "report_id"})


ReadH5('/Users/mac/Downloads/rpt_data.h5').get_df()
# print(df.columns)
# df = df.loc[:100, :]
# df = df.loc[:100, :]


# print(df['previous_create_date'])
# df.fillna('', inplace=True)  # 数据库不能识别 nan
# self.MergeTable = self.MergeTable.where(self.MergeTable.notnull(), None)
# time.sleep(11111)

# df.to_csv(path_to)
# h5_store.close()
