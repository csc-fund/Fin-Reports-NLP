#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :data_tool.py
# @Time      :2022/8/7 20:47
# @Author    :Colin
# @Note      :None
import hashlib
import math
import os

import pandas as pd
import numpy as np
import tushare as ts
from tqdm import tqdm

from tools.mysql_tool import MysqlDao
from tools.settings import *


# 基础的数据类
class BaseDataTool:
    def __init__(self, data_base=None):
        # 本地数据库连接工具
        if data_base:
            self.SqlObj = MysqlDao(data_base=data_base)
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

        # 数据清洗
        self.LEFT_TAIL = 0.05
        self.RIGHT_TAIL = 0.95

    # 存储数据
    def save_to_db(self, table_name: str):
        self.SqlObj.insert_table(table_name, self.OUTPUT_TABLE, self.OUTPUT_TABLE_STRUCT)


# 获取价格信息
class GetPriceData(BaseDataTool):
    def __init__(self, data_base):
        super(GetPriceData, self).__init__(data_base=data_base)

    # 获取历史数据
    def get_history_query(self, api_name: str, code: str, ):
        # ----------------判断存在---------------#
        table_name = code + '_' + api_name
        if table_name in self.SqlObj.show_tables():
            print('ok')
            return
        # ----------------获取股价数据----------------#
        # 循环获取时间序列
        self.df_api = pd.DataFrame()
        date_list = [x.strftime('%Y%m%d') for x in pd.date_range('20000101', '20221231', freq='5000D')]
        # 从API获取数据(每次返回5000行)
        for date in date_list:
            # 前复权
            df_date = self.TuShare.query(api_name=api_name, ts_code=code, start_date=date, adj=None)
            # 返回值不为空
            if df_date is not None:
                # 重构索引
                df_date.set_index(['trade_date'], inplace=True)
                # 两个dataframe合并
                self.df_api = pd.concat([self.df_api, df_date])

        # 检查去重
        self.df_api = self.df_api.drop_duplicates()
        self.df_api.reset_index(inplace=True)
        # 入库
        self.OUTPUT_TABLE_STRUCT = {'trade_date': 'DATE', 'ts_code': 'VARCHAR(20)',
                                    'total_mv': 'FLOAT', 'circ_mv': 'FLOAT',
                                    'PK': 'trade_date'}
        self.SqlObj.insert_table(table_name, self.df_api, self.OUTPUT_TABLE_STRUCT)

    # 下载K线数据
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

        if self.OUTPUT_TABLE is not None:
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


# 用于打标签
class GetLabelData(BaseDataTool):
    #
    def __init__(self, data_base, input_table: str, input_column: list, lag_period: list,
                 natural_table='natural_trade_date', ):

        super().__init__(data_base=data_base)

        # 自然日期表
        self.natural_table = pd.DataFrame()

        # 用于参考的日期映射表
        self.DATE_TABLE = self.SqlObj.select_table(natural_table, ['date', 'map_tradedate'])

        # 要获得价格的表
        self.INPUT_TABLE = self.SqlObj.select_table(input_table, input_column, )
        # {'LIMIT': '100'}
        self.CODE_TABLE = pd.DataFrame()

        # 命名规范
        self.CODE_COLUMN = 'CODE'
        self.DATE_COLUMN = 'DATE_N'
        self.DATE_COLUMN_T = 'DATE_T'

        # 关键列的位置
        self.INPUT_TABLE.rename(columns={input_column[0]: self.CODE_COLUMN,
                                         input_column[1]: self.DATE_COLUMN},
                                inplace=True)

        self.DATE_TABLE.rename(columns={'date': self.DATE_COLUMN}, inplace=True)

        # 滞后的范围
        self.LAG_PERIOD = lag_period

    # 创建一个自然日期表
    def get_naturaldate(self, start_date, end_date):
        self.natural_table = pd.DataFrame(pd.date_range(start=start_date, end=end_date))

    # 清洗数据
    def clean_data(self):
        df = self.INPUT_TABLE
        # -----------------------------数据清洗和筛选----------------------------#
        df = df[df['report_type'] != 21]  # 排除非个股报告

        df = df[df['title'].str.contains('：')]  # 选择有冒号的数据
        df = df[~df['title'].str.contains('_')]  # 去掉_的数据

        # 字符处理
        df['title'] = df['title'].apply(lambda x: "".join(str(x).split("：")[1:]))  # 去掉：
        df['title'] = df['title'].apply(lambda x: str(x).replace('。', '.'))  # 去掉。

        # 主观筛选不包含观点的数据
        #  '年报点评', '分析报告',
        exclude_list = ['股东大会', '纪要', '点评', '分析', '简评', '短评', '报告', '快报', '快评']
        exclude_rule = "|".join(exclude_list)
        df = df[~df['title'].str.contains(exclude_rule)]

        # -----------------------------长度缩尾处理----------------------------#
        df['title_len'] = df['title'].apply(lambda x: len(str(x)))
        lt = df['title_len'].quantile(q=self.LEFT_TAIL)
        rt = df['title_len'].quantile(q=self.RIGHT_TAIL)
        df = df[(lt <= df['title_len']) & (df['title_len'] <= rt)]  # 缩尾处理
        print(lt, rt)

        # -----------------------------保存----------------------------#
        self.INPUT_TABLE = df

    # 从df映射交易日期date
    def get_tradedate(self):
        # 通过左连接映射traget_column
        self.OUTPUT_TABLE = pd.merge(self.INPUT_TABLE, self.DATE_TABLE,
                                     how='left', on=[self.DATE_COLUMN])
        self.OUTPUT_TABLE.rename(columns={'map_tradedate': self.DATE_COLUMN_T}, inplace=True)
        self.OUTPUT_TABLE_STRUCT.update({self.DATE_COLUMN_T: 'DATE'})

    # 从指定日期获得价格
    # 输入为一个df,包含了自然日期的列,期望增加滞前滞后的价格列
    def get_tag(self, ):
        # ------------------ 初始化 ------------------------#
        self.OUTPUT_TABLE = pd.DataFrame()

        # ------------------数据筛选-----------------------  #
        self.clean_data()

        # ------------------ 增加交易日期列 ------------------------#
        self.get_tradedate()

        # ------------------ 在股票代码中循环 ------------------------#

        for code in tqdm(self.OUTPUT_TABLE[self.CODE_COLUMN].unique().tolist()):
            self.CODE_TABLE = self.OUTPUT_TABLE.loc[self.OUTPUT_TABLE[self.CODE_COLUMN] == code, :].copy()

            # ------------------ 根据交易日期查找价格 ------------------------#
            # 获取历史记录
            df_code_history = BaseDataTool(data_base='tushare_daily').SqlObj.select_table(code,
                                                                                          ['trade_date', 'close'])
            df_code_history.rename(columns={'trade_date': self.DATE_COLUMN_T}, inplace=True)

            # 循环滞后
            for i in self.LAG_PERIOD:
                lag_column = 'close_l{}'.format(i)
                df_code_history[lag_column] = df_code_history['close'].shift(-i)

            # 连接
            self.CODE_TABLE = pd.merge(self.CODE_TABLE, df_code_history,
                                       how='left', on=[self.DATE_COLUMN_T])

            # ------------------ 分片插入 ------------------------#
            self.CODE_TABLE.dropna(inplace=True)
            if self.CODE_TABLE.empty:
                continue
            # ------------------ 打标签 ------------------------#
            # 不同组合的标签打法
            for start_i in self.LAG_PERIOD:
                for end_i in [i for i in self.LAG_PERIOD if i > start_i]:
                    # 命名
                    rtn_column = 'RETURN_{}_{}'.format(start_i, end_i)
                    tag_column = 'TAG_{}_{}'.format(start_i, end_i)
                    close_end = 'close_l{}'.format(end_i)
                    close_start = 'close_l{}'.format(start_i)

                    # 收益算法
                    self.CODE_TABLE[rtn_column] = self.CODE_TABLE[[close_start, close_end]].apply(
                        lambda x: (math.log(x[close_end] / x[close_start]) * 100), axis=1
                    )

                    # 打标签的算法
                    def return_tag(df_x):
                        if df_x > 0:
                            return 0
                        elif df_x < 0:
                            return 1
                        else:
                            return 2

                    self.CODE_TABLE[tag_column] = self.CODE_TABLE[[rtn_column]].apply(
                        lambda x: return_tag(x[rtn_column]), axis=1)

            # ------------------入库-----------------------  #
            # 生成合成title
            self.CODE_TABLE['TITLE_ALL'] = self.CODE_TABLE[['report_year', 'organ_name', 'author', 'title']].apply(
                lambda x: str(x['report_year']) + ',' +
                          str(x['organ_name']) + ',' +
                          str(x['author']) + ',' +
                          str(x['title']), axis=1)

            # 转换ID
            self.CODE_TABLE['ID_MD5'] = self.CODE_TABLE['TITLE_ALL'].apply(
                lambda x: hashlib.md5((str(x).strip()).encode('UTF-8')).hexdigest())
            # self.CODE_TABLE['ID_MD5'] = self.CODE_TABLE['ID_MD5'].apply(
            #     lambda x: hashlib.md5(x.encode('UTF-8')).hexdigest())

            # 入库结构
            self.OUTPUT_TABLE_STRUCT = {i: 'FLOAT' for i in self.CODE_TABLE.columns}
            self.OUTPUT_TABLE_STRUCT.update(
                {'CODE': 'VARCHAR(20)', 'title': 'VARCHAR(30)', 'TITLE_ALL': 'VARCHAR(50)',
                 'DATE_N': 'DATE', 'DATE_T': 'DATE', 'ID_MD5': 'VARCHAR(150)', 'PK': 'ID_MD5'
                 })

            self.SqlObj.insert_table('rpt_price', self.CODE_TABLE, self.OUTPUT_TABLE_STRUCT, )

            # ------------------ 拼接所有的code子表 ------------------------#
            # price_table = pd.concat([price_table, df_code], ignore_index=True, axis=0)

        # ------------------入库-----------------------  #
        # self.OUTPUT_TABLE = price_table

    # 输出用于训练的数据
    def get_csv(self):

        # -----------------------读取数据-----------------------#
        attr_columns = [i for i in self.SqlObj.select_columns('rpt_price') if 'TAG' in i]
        attr_columns += ['TITLE_ALL', 'title']
        self.OUTPUT_TABLE = self.SqlObj.select_table('rpt_price', attr_columns)

        # -----------------------删除不需要的标签-----------------------#

        # index_drop = self.OUTPUT_TABLE[self.OUTPUT_TABLE['TAG_-1_1'] == 0].index
        # self.OUTPUT_TABLE.drop(index=index_drop, inplace=True)

        # self.OUTPUT_TABLE['TAG_-1_1'] = self.OUTPUT_TABLE['TAG_-1_1'].apply(
        #     lambda x: 0 if x == -1 else 1)

        # -----------------------训练集参数设置-----------------------#
        output_filename = 'rpt_report_price'
        output_path = 'C:/Users/Administrator/Desktop/' + output_filename + '/'
        train_per = 0.9
        dev_per = 0.05
        df_len = self.OUTPUT_TABLE.shape[0]

        # -----------------------随机排序---------------------#
        self.OUTPUT_TABLE.take(np.random.permutation(df_len), axis=0)
        self.OUTPUT_TABLE.reset_index(inplace=True)

        # -----------------------切片---------------------#
        df_train = self.OUTPUT_TABLE.loc[:int(df_len * train_per), :]
        df_dev = self.OUTPUT_TABLE.loc[int(df_len * train_per):int(df_len * train_per + df_len * dev_per), :]
        df_test = self.OUTPUT_TABLE.loc[int(df_len * train_per + df_len * dev_per):, :]

        # -----------------------文件保存----------------------#
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        df_train.to_csv(output_path + 'train.csv', index=False)
        df_dev.to_csv(output_path + 'dev.csv', index=False)
        df_test.to_csv(output_path + 'test.csv', index=False)

        # -----------------------文件压缩----------------------#
        import zipfile
        zf = zipfile.ZipFile('G:/我的云端硬盘/DataSets/{}.zip'.format(output_filename), "w", zipfile.ZIP_DEFLATED)
        for path, dirnames, filenames in os.walk(output_path):
            # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
            fpath = path.replace(output_path, '')
            for filename in filenames:
                zf.write(os.path.join(path, filename), os.path.join(fpath, filename))
        zf.close()
