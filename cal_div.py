#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :cal_div.py
# @Time      :2022/8/10 13:22
# @Author    :Colin
# @Note      :None
import time

from tools.data_tool import GetPriceData
import pandas as pd
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from tqdm import tqdm


class DivData(GetPriceData):

    def __init__(self, data_base):
        super(DivData, self).__init__(data_base=data_base)

        # ----------------要使用的表----------------#
        self.DIV_DATA = pd.DataFrame()  # 股利数据
        self.PRICE_DATA = pd.DataFrame()  # 市值数据

    # 计算股息率
    def get_div_data(self):
        # ----------------获取股利数据----------------#
        self.DIV_DATA = self.SqlObj.select_table('茅台股息', ['税前每股派息', '基准股本', '除权除息日', '分红年度'])
        self.DIV_DATA['税前现金股利'] = self.DIV_DATA['税前每股派息'] * self.DIV_DATA['基准股本'] * 10000

        # ----------------获取股价数据----------------#
        self.PRICE_DATA = self.SqlObj.select_table('600519.SH_daily_basic', ['trade_date', 'circ_mv', ])
        self.PRICE_DATA['circ_mv'] = self.PRICE_DATA['circ_mv'].apply(lambda x: x * 10000)

        # --------------------------------LYR计算-------------------------------#
        # 计算的函数
        def find_div_lyr(df_x):
            # 股票每日基本信息表
            end_date = df_x['trade_date']
            mv = df_x['circ_mv']

            # 股票股利表
            # 找出end_date前发生的最近除息除权日
            df_div = self.DIV_DATA[
                (self.DIV_DATA['除权除息日'] < end_date)]
            df_div = df_div.iloc[-1:]
            # 指标计算
            div_count = df_div['税前现金股利'].count()
            divrate_ttm = df_div['税前现金股利'].sum() / mv

            return divrate_ttm, div_count

        # --------------------------------TTM计算-------------------------------#
        # 过去1年的日期作为计算基准
        self.PRICE_DATA['trade_date_y-1'] = self.PRICE_DATA['trade_date'].apply(lambda x: x - relativedelta(years=1))

        # 计算的函数
        def find_div_ttm(df_x):
            # 股票每日基本信息表
            start_date = df_x['trade_date_y-1']
            end_date = df_x['trade_date']
            mv = df_x['circ_mv']

            # 股票股利表
            # 找出trade_date_y-1到现在发生的除息除权日
            df_div = self.DIV_DATA[
                (start_date < self.DIV_DATA['除权除息日'])
                & (self.DIV_DATA['除权除息日'] < end_date)]

            # 指标计算
            div_count = df_div['税前现金股利'].count()
            divrate_ttm = df_div['税前现金股利'].sum() / mv

            return divrate_ttm, div_count

        # ----------------股息率计算---------------#
        # LYR
        self.PRICE_DATA[['divrate_lyr', 'lyr_count']] = self.PRICE_DATA.apply(
            lambda x: find_div_lyr(x), axis=1, result_type='expand')
        # TTM
        self.PRICE_DATA[['divrate_ttm', 'ttm_count']] = self.PRICE_DATA.apply(
            lambda x: find_div_ttm(x), axis=1, result_type='expand')

        # ----------------保存---------------#
        self.PRICE_DATA.to_csv('茅台股息.csv')


DivData('zyyx').get_div_data()
