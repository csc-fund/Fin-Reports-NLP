# Reading h5 file

from tools.mysql_tool import *
from datetime import datetime as dt


# 'h5_data/rpt_data.h5'

# -i https://pypi.tuna.tsinghua.edu.cn/simple
class ReadH5:
    def __init__(self, file_path):
        self.h5_store = pd.HDFStore(path=file_path, mode='r+')  # 创建新的对象、读入已存在的对象
        self.keys = None
        self.df_h5 = None
        self.SqlObj = MysqlDao()

    def get_df(self):
        self.keys = self.h5_store.keys()
        self.df_h5 = pd.DataFrame(self.h5_store.get(self.keys[0]))
        # self.df_h5 = self.df_h5.loc[:10, :]

    def insert_db(self, table_name):
        if not self.df_h5:
            self.get_df()
        # -----------------------数据清洗-----------------------#
        # 时间处理

        self.df_h5['current_create_date'] = self.df_h5[['current_create_date']].apply(
            lambda x: str(dt.fromtimestamp(float(str(x['current_create_date']).replace("000000000", ""))).date()),
            axis=1)
        # 时间处理

        self.df_h5['previous_create_date'] = self.df_h5[['previous_create_date']].apply(
            lambda x: str(x['previous_create_date']).replace('.0', ''), axis=1)

        # -----------------------数据入库-----------------------#
        self.SqlObj.insert_table(table_name, self.df_h5, MYSQL_STRUCT)


ReadH5('D:/BaiduNetdiskDownload/rpt_data.h5').insert_db('rpt_e_a')
