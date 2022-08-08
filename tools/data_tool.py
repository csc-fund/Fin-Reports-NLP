#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :data_tool.py
# @Time      :2022/8/7 20:47
# @Author    :Colin
# @Note      :None
import time

from tools.settings import *
from tools.mysql_tool import MysqlDao
import pandas as pd
import tushare as ts
from datetime import datetime as dt
from tqdm import tqdm, trange


# 基础的数据类
class BaseDataTool:
    def __init__(self, data_base=None):
        # 本地数据库连接工具
        if data_base:
            self.SqlObj = MysqlDao(dataBase=data_base)
        self.df_select = pd.DataFrame()

        # 在线API工具
        self.TuShare = ts.pro_api(TUSHARE_AK)
        self.TS = ts
        self.TS.set_token(TUSHARE_AK)
        self.df_api = pd.DataFrame()

        # 输入和输出数据
        self.INPUT_TABLE = pd.DataFrame()
        self.OUTPUT_TABLE = pd.DataFrame()
        self.OUTPUT_TABLE_STRUCT = {}

    # 存储数据
    def save_to_db(self, table_name: str):
        self.SqlObj.insert_table(table_name, self.OUTPUT_TABLE, self.OUTPUT_TABLE_STRUCT)


# 用于映射交易日期的类
class MapTradeDate(BaseDataTool):
    #
    def __init__(self):
        super().__init__(data_base='zyyx')

        # 自然日期表
        self.natural_table = pd.DataFrame()

        # 用于参考的日期映射表
        self.date_table = self.SqlObj.select_table('natural_trade_date', ['date', 'map_tradedate'])
        self.date_table.rename(columns={'date': 'natural_date'}, inplace=True)

        # 合并后的映射表

    # 创建一个自然日期表
    def get_naturaldate(self, start_date, end_date):
        self.natural_table = pd.DataFrame(pd.date_range(start=start_date, end=end_date))

    # 从df映射交易日期date
    def get_tradedate(self, df_input: pd.DataFrame, targrt_column: str, lag_period: list) -> pd.DataFrame:
        # 初始化列名
        df_input.rename(columns={targrt_column: 'natural_date'}, inplace=True)
        self.INPUT_TABLE = df_input

        # 通过左连接映射traget_column
        merge_table = pd.merge(self.INPUT_TABLE, self.df_select,
                               how='left', on=['natural_date'])

        # 获得滞前滞后的列
        for i in lag_period:
            out_column = 'map_tradedate_l{}'.format(i)
            merge_table[out_column] = merge_table.shift(i)
            self.OUTPUT_TABLE_STRUCT.update({out_column: 'DATE'})

        # 返回
        self.OUTPUT_TABLE = merge_table
        return self.OUTPUT_TABLE

    # 继续获取交易日期对应的股票价格
    def get_price(self):
        pass


# 获取价格信息
class GetPriceData(BaseDataTool):
    def __init__(self, data_base):
        super(GetPriceData, self).__init__(data_base=data_base)

    def down_kline(self, code: str):
        self.OUTPUT_TABLE = None
        # 循环获取时间序列
        date_list = [x.strftime('%Y%m%d') for x in pd.date_range('20000101', '20221231', freq='5000D')]

        # 从API获取数据(每次返回5000行)
        for date in date_list:
            # 前复权
            self.df_api = self.TS.pro_bar(ts_code=code, start_date=date, adj='qfq')
            # 重构索引
            self.df_api.set_index(['trade_date'], inplace=True)
            # 两个dataframe合并
            self.OUTPUT_TABLE = pd.concat([self.OUTPUT_TABLE, self.df_api])

        # 检查去重
        self.OUTPUT_TABLE = self.OUTPUT_TABLE.drop_duplicates()
        # 存储
        self.OUTPUT_TABLE_STRUCT = {'ts_code': 'VARCHAR(20)', 'open': 'FLOAT', 'high': 'FLOAT',
                                    'low': 'FLOAT', 'close': 'FLOAT', 'pre_close': 'FLOAT', 'change': 'FLOAT',
                                    'pct_chg': 'FLOAT', 'vol': 'FLOAT', 'amount': 'FLOAT'}

        self.save_to_db('{}'.format(code))

        # 下载所有的股票代码Kline

    def down_all_kline(self):
        # 获取股票列表
        data_tool = BaseDataTool(data_base='zyyx')
        data_tool.df_select = data_tool.SqlObj.select_table('rpt_earnings_adjust', ['stockcode'])

        # 从df的某一列获取
        code_list = data_tool.df_select['stockcode'].unique().tolist()
        # print(len(code_list))
        for i in tqdm(code_list):
            self.down_kline(i)

    # 把日期映射到价格


class MapDatePrice(BaseDataTool):
    def __init__(self):
        super(MapDatePrice, self).__init__()


GetPriceData(data_base='tushare_daily').down_all_kline()
