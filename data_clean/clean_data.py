import re

import pandas as pd

from data_clean.mysql_tool import *
import tushare as ts


# 交易日期处理类
class GenTradeData:
    def __init__(self):
        self.SqlObj = MysqlDao()
        self.TuShare = ts.pro_api(TUSHARE_AK)

        self.TradeTable = None
        self.NaturalTable = None
        self.MergeTable = None

    def get_trade_table(self):
        from datetime import datetime as dt
        # ---------------------生成交易日期表------------------------ #
        self.TradeTable = self.TuShare.query(api_name='index_daily', ts_code='399300.SZ',
                                             start_date='20050101', end_date='20221231', fields='trade_date')
        self.TradeTable['trade_date'] = self.TradeTable['trade_date'].apply(
            lambda x: pd.to_datetime(x, format="%Y%m%d").date())
        self.TradeTable['date'] = self.TradeTable['trade_date']

        # ---------------------生成自然日期表------------------------ #
        self.NaturalTable = pd.DataFrame(pd.date_range(start='20050101', end='20221231'))
        self.NaturalTable.rename(columns={0: 'date'}, inplace=True)

        # ---------------------合并------------------------ #
        self.TradeTable['date'] = self.TradeTable['date'].astype('datetime64[ns]')
        self.MergeTable = pd.merge(self.NaturalTable, self.TradeTable,
                                   how='left', on=['date'])
        self.MergeTable['fill_tradedate'] = self.MergeTable['trade_date'].fillna(method='bfill')

        self.MergeTable.to_csv('MergeTable.csv')



# 语言处理类
class GenData:
    def __init__(self):
        self.SqlObj = MysqlDao()
        self.df = self.SqlObj.select_table(table_name=MYSQL_TABLENAME,
                                           select_column=MYSQL_COLUMN,
                                           filter_dict={"LIMIT": 1000})
        # 实例化Tushare对象
        self.TuShare = ts.pro_api(TUSHARE_AK)

    # 获取公告后的价格用于打标签
    def get_report_price(self):
        self.df = self.df[['stockcode', 'ann_date', 'report_id']]
        df = self.TuShare.query(api_name='daily',
                                ts_code='000001.SZ',
                                trade_date='20180702')

        print(df)

    # 筛选有效数据
    def filter_data(self):
        # 排除非个股报告
        self.df = self.df[self.df['report_type'] != 21]
        # 选择有冒号的数据
        # self.df = self.df[self.df['report_type'].str.contains('：')]
        # 选择指定长度的数据
        # self.df = self.df[self.df['title'].str.contains('：')]

    # 去除无效数据
    def clean_noise(self):
        def delete_tag(s):
            r1 = re.compile(r'\{IMG:.?.?.?\}')  # 图片
            s = re.sub(r1, '', s)
            r2 = re.compile(r'[a-zA-Z]+://[^\u4e00-\u9fa5|\?]+')  # 网址
            s = re.sub(r2, '', s)
            r3 = re.compile(r'<.*?>')  # 网页标签
            s = re.sub(r3, '', s)
            r4 = re.compile(r'&[a-zA-Z0-9]{1,4}')  # &nbsp  &gt  &type &rdqu   ....
            s = re.sub(r4, '', s)
            r5 = re.compile(r'[0-9a-zA-Z]+@[0-9a-zA-Z]+')  # 邮箱
            s = re.sub(r5, '', s)
            r6 = re.compile(r'[#]')  # #号
            s = re.sub(r6, '', s)
            return s

        self.df['title'] = self.df[['title']].apply(lambda x: delete_tag(x) if str(x) != 'nan' else x)


# -----------------------数据清洗-----------------------#


# -----------------------数据清洗-----------------------#
# data = GenData()
# data.get_report_price()
# data.filter_data()
GenTradeData().get_trade_table()
