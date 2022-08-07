#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :data_tool.py
# @Time      :2022/8/7 20:47
# @Author    :Colin
# @Note      :None
from tools.settings import *
from tools.mysql_tool import MysqlDao
import pandas as pd
import tushare as ts


# 基础的数据类
class BaseDataTool:
    def __init__(self, data_base=None):
        # 本地数据库连接工具
        if data_base:
            self.SqlObj = MysqlDao(dataBase=data_base)
        self.df_select = pd.DataFrame()

        # 在线API工具
        self.TuShare = ts.pro_api(TUSHARE_AK)
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

        # 用于参考的日期映射表
        self.date_table = self.SqlObj.select_table('natural_trade_date', ['date', 'map_tradedate'])
        self.date_table.rename(columns={'date': 'natural_date'}, inplace=True)

        # 合并后的映射表

    # 从df映射交易日期date
    def get_from_df(self, df_input: pd.DataFrame, targrt_column: str, lag_period: list) -> pd.DataFrame:
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
