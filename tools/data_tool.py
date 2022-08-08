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

            # 返回值不为空
            if self.df_api is not None:
                # 重构索引
                self.df_api.set_index(['trade_date'], inplace=True)

            # 两个dataframe合并
            self.OUTPUT_TABLE = pd.concat([self.OUTPUT_TABLE, self.df_api])

        # 检查去重
        self.OUTPUT_TABLE = self.OUTPUT_TABLE.drop_duplicates()
        self.OUTPUT_TABLE.reset_index(inplace=True)

        # 存储
        self.OUTPUT_TABLE_STRUCT = {'trade_date': 'DATE', 'ts_code': 'VARCHAR(20)', 'open': 'FLOAT', 'high': 'FLOAT',
                                    'low': 'FLOAT', 'close': 'FLOAT', 'pre_close': 'FLOAT', 'change': 'FLOAT',
                                    'pct_chg': 'FLOAT', 'vol': 'FLOAT', 'amount': 'FLOAT',
                                    'PK': 'trade_date'}

        self.save_to_db('{}'.format(code))

    # 下载所有的股票代码Kline,参照的表
    def down_all_kline(self, database, table, column):
        # 获取股票列表
        data_tool = BaseDataTool(data_base=database)
        data_tool.df_select = data_tool.SqlObj.select_table(table, [column])

        # 从df的某一列获取
        code_list = data_tool.df_select[column].unique().tolist()
        code_list = [str(i).lower() for i in code_list]

        # 去除已经下载的表
        saved_table = BaseDataTool(data_base='tushare_daily').SqlObj.show_tables()
        down_table = [i for i in code_list if i not in saved_table]

        # 循环
        for i in tqdm(down_table):
            # print(i)
            self.down_kline(i)


# 用于映射交易日期的类
# 用于映射指定公告的价格
class MapTradeDate(BaseDataTool):
    #
    def __init__(self, input_table: str, input_column: list, natural_table='natural_trade_date', ):
        super().__init__(data_base='zyyx')

        # 自然日期表
        self.natural_table = pd.DataFrame()

        # 用于参考的日期映射表
        self.date_table = self.SqlObj.select_table(natural_table, ['date', 'map_tradedate'])
        self.date_table.rename(columns={'date': 'natural_date'}, inplace=True)

        # 要获得价格的表
        self.INPUT_TABLE = self.SqlObj.select_table(input_table, input_column)

        # 命名规范
        self.CODE_COLUMN = 'CODE'
        self.DATE_COLUM = 'DATE'

        # 关键列的位置
        self.INPUT_TABLE.rename(columns={input_column[0]: self.CODE_COLUMN,
                                         input_column[1]: self.DATE_COLUM},
                                inplace=True)

    # 创建一个自然日期表
    def get_naturaldate(self, start_date, end_date):
        self.natural_table = pd.DataFrame(pd.date_range(start=start_date, end=end_date))

    # 从df映射交易日期date
    def get_tradedate(self, date_column: str):

        # 初始化列名
        self.INPUT_TABLE.rename(columns={date_column: 'natural_date'}, inplace=True)
        # 通过左连接映射traget_column
        self.OUTPUT_TABLE = pd.merge(self.INPUT_TABLE, self.date_table,
                                     how='left', on=['natural_date'])

        self.OUTPUT_TABLE_STRUCT.update({'map_tradedate': 'DATE'})

    # 从指定日期获得价格
    # 输入为一个df,包含了自然日期的列,期望增加滞前滞后的价格列
    def get_price(self, code_column: str, date_column: str, lag_period: list):
        # ------------------ 初始化 ------------------------#
        self.OUTPUT_TABLE = None

        # ------------------ 增加交易日期列 ------------------------#
        self.get_tradedate(date_column)

        # ------------------ 在股票代码中循环 ------------------------#
        for code in self.INPUT_TABLE[code_column].unique().tolist():
            df_code = self.INPUT_TABLE[self.INPUT_TABLE[code_column] == code].copy()

            # ------------------ 根据交易日期查找价格 ------------------------#
            # 获取历史记录
            df_code_history = BaseDataTool(data_base='tushare_daily').SqlObj.select_table('code',
                                                                                          ['trade_date', 'close'])
            # 循环滞后
            for i in lag_period:
                lag_column = 'close_l{}'.format(i)
                df_code_history[lag_column] = df_code_history.shift(i)
                self.OUTPUT_TABLE_STRUCT.update({lag_column: 'FLOAT'})

            # 连接
            df_code = pd.merge(df_code, df_code_history,
                               how='left', on=['trade_date'])

            # ------------------ 拼接所有的code子表 ------------------------#
            self.OUTPUT_TABLE = pd.concat([self.OUTPUT_TABLE, df_code])
