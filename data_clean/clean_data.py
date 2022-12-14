import os.path

import pandas as pd

from data_clean.settings import *
from tools.mysql_tool import *
import re
from datetime import datetime as dt
import tushare as ts
from tqdm.auto import tqdm
import math


# 基础类
class GenBaseData:
    def __init__(self, data_base):
        # 本地数据库连接工具
        self.SqlObj = MysqlDao(dataBase=data_base)
        self.df_sql = pd.DataFrame()

        # 在线API工具
        self.TuShare = ts.pro_api(TUSHARE_AK)
        self.df_api = pd.DataFrame()

        # 输入和输出数据
        self.INPUT_TABLE = pd.DataFrame()
        self.OUTPUT_TABLE = pd.DataFrame()


# 交易日期处理类
class GenDateData:
    def __init__(self):
        self.SqlObj = MysqlDao()
        self.TuShare = ts.pro_api(TUSHARE_AK)

        # 要用到的表
        self.TradeTable = None
        self.NaturalTable = None
        self.MergeTable = None

        # 建表

    def get_trade_table(self):
        # ---------------------生成交易日期表------------------------ #
        self.TradeTable = self.TuShare.query(api_name='index_daily', ts_code=DATE_SHARE,
                                             start_date=DATE_START, end_date=DATE_END, fields='trade_date')
        self.TradeTable['trade_date'] = self.TradeTable['trade_date'].apply(
            lambda x: pd.to_datetime(x, format="%Y%m%d").date())
        self.TradeTable['date'] = self.TradeTable['trade_date']

        # ---------------------生成自然日期表------------------------ #
        self.NaturalTable = pd.DataFrame(pd.date_range(start=DATE_START, end=DATE_END))
        self.NaturalTable.rename(columns={0: 'date'}, inplace=True)

        # ---------------------合并------------------------ #
        self.TradeTable['date'] = self.TradeTable['date'].astype('datetime64[ns]')
        self.MergeTable = pd.merge(self.NaturalTable, self.TradeTable,
                                   how='left', on=['date'])

        # ---------------------映射------------------------ #
        self.MergeTable['map_tradedate'] = self.MergeTable['trade_date'].fillna(method='bfill')
        # ---------------------滞后------------------------ #
        dict_type = {"date": 'DATE', "PK": "date"}
        for i in DATE_LAGLIST:
            self.MergeTable['map_tradedate_l{}'.format(i)] = self.MergeTable['map_tradedate'].shift(-i)
            dict_type.update({'map_tradedate_l{}'.format(i): "DATE"})

        # ---------------------入库----------------------- #
        # self.MergeTable = self.MergeTable.where(self.MergeTable.notnull(), None)
        self.SqlObj.insert_table('natural_trade_date', self.MergeTable, dict_type)


# 语言处理类
class GenPriceData:
    # 初始化
    def __init__(self):
        # 本地数据库表
        self.SqlObj = MysqlDao()
        self.df_date_db = self.SqlObj.select_table(DATE_TABLE, ['*'])
        self.df_report_db = pd.DataFrame  # 从数据库加载的表

        # 实例化Tushare对象
        self.TuShare = ts.pro_api(TUSHARE_AK)
        self.df_kline_tu = pd.DataFrame

    # 获取公告后的价格用于打标签
    def get_report_price(self):
        # ---------------------从数据库读数据-------------------#
        # 增加新的计算列到数据库
        self.df_report_db = self.SqlObj.select_table(table_name=TABLE_NULLPRICE,
                                                     select_column=MYSQL_COLUMN,
                                                     filter_dict={'null_flag': 'NULL', "LIMIT": MYSQL_LIMIT})

        # ------------------------对每行执行查询---------------------#
        def query_price(df, lag_t: int):
            # 处理传入参数
            ann_date = df['ann_date']
            code = df['stockcode']

            # 要返回的值
            price = None

            # 先映射到交易日期
            df_tradedate = self.df_date_db[self.df_date_db['date'] == ann_date]

            # 如果需要滞后
            if lag_t != 0:
                df_tradedate = df_tradedate['map_tradedate_l{}'.format(lag_t)]
            else:
                df_tradedate = df_tradedate['map_tradedate']

            # 保留df中的第一列
            trade_date = df_tradedate.iloc[0]
            # 时间转换为tu查询的格式
            trade_date = dt.strftime(trade_date, "%Y%m%d")

            # 网络可能出错
            try:
                # 在接口中查询
                self.df_kline_tu = self.TuShare.query(api_name='daily',
                                                      ts_code=code,
                                                      trade_date=trade_date)
                price = self.df_kline_tu['close']

            except Exception as e:
                print(e, code, trade_date, price, )

            return price

        # ------------------------对每行执行查询---------------------#

        for t in DATE_LAGLIST:
            tqdm.pandas(desc="DATE_LAGLIST L{}".format(t), ncols=85)

            # 定义列名
            price_start = 'price_l0'
            price_end = 'price_l{}'.format(t)
            price_return = 'return_l{}'.format(t)

            # 在API获取
            self.df_report_db[price_end] = self.df_report_db[
                ['stockcode', 'ann_date', 'report_id', ]].progress_apply(
                lambda x: query_price(x, lag_t=t),
                axis=1)
            MYSQL_STRUCT.update({price_end: "FLOAT"})

            # 计算收益
            if t != 0:
                self.df_report_db[price_return] = self.df_report_db[[price_start, price_end, ]].apply(
                    lambda x: math.log(x[price_end] / x[price_start]), axis=1)
                MYSQL_STRUCT.update({price_return: "FLOAT"})

        # ------------------------入库---------------------#
        # self.df_report_db.dropna(inplace=True)
        self.SqlObj.insert_table(MYSQL_INSERT_TABLE, self.df_report_db, MYSQL_STRUCT)

    # 循环获取所有历史价格
    def get_all_price(self):
        while True:
            try:
                self.get_report_price()
                self.get_tag_base()
                # print("完成的行：{}".format(self.SqlObj.cur.rowcount))
            except Exception as e:
                print(e)

            if self.df_report_db.empty:
                break

    #
    # 筛选有效数据
    def filter_data(self):
        # 排除非个股报告
        self.df_report_db = self.df_report_db[self.df_report_db['report_type'] != 21]
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

    # 基准的打标签算法
    def get_tag_base(self):
        self.df_report_db = self.SqlObj.select_table(VIEW_RETURN, ['*'], )
        df = self.df_report_db

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

        # 选择指定长度的数据

        # -----------------------------缩尾处理----------------------------#
        df['title_len'] = df['title'].apply(lambda x: len(str(x)))
        lt = df['title_len'].quantile(q=LEFT_TAIL)
        rt = df['title_len'].quantile(q=RIGHT_TAIL)
        df = df[(lt <= df['title_len']) & (df['title_len'] <= rt)]  # 缩尾处理
        print(lt, rt)

        # -----------------------------打标签----------------------------#
        dict_struct = {'report_id': 'INT', 'title': 'VARCHAR(25)', 'PK': 'report_id'}
        for t in [1, 5]:
            tag_input = 'return_l{}'.format(t)
            tag_output = 'TAG_L{}'.format(t)
            df[tag_output] = df[tag_input].apply(lambda x: 1 if x > 0 else 0)
            dict_struct.update({tag_output: 'int'})

        # -----------------------------筛选用于训练的记录----------------------------#
        tag_column = [i for i in df.columns if 'TAG_L' in str(i)]
        use_column = ['report_id', 'title'] + tag_column
        self.df_report_db = df[use_column]

        # -----------------------------入库----------------------------#
        self.SqlObj.insert_table(TABLE_TAG_BASE, self.df_report_db, dict_struct)

        # -----------------------------输出CSV用于训练----------------------------#
        self.get_csv_data()

    #     用于训练的标准格式
    def get_csv_data(self):
        self.df_report_db = self.SqlObj.select_table(TABLE_TAG_BASE, ['*'])

        # -----------------------参数设置-----------------------#
        output_path = 'C:/Users/Administrator/Desktop/rpt_report_price/'
        train_per = 0.7
        dev_per = 0.2
        test_per = 1 - train_per - dev_per

        # -----------------------切片-----------------------#
        df_len = self.df_report_db.shape[0]
        # 随机排序
        self.df_report_db.take(np.random.permutation(df_len), axis=0)
        # 切片
        df_train = self.df_report_db.loc[:int(df_len * train_per), :]
        df_dev = self.df_report_db.loc[:int(df_len * dev_per), :]
        df_test = self.df_report_db.loc[:int(df_len * test_per), :]

        # -----------------------文件保存----------------------#
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        df_train.to_csv(output_path + 'train.csv')
        df_dev.to_csv(output_path + 'dev.csv')
        df_test.to_csv(output_path + 'test.csv')


# 打标签的类
class GenTag(GenBaseData):
    def __init__(self, data_base, input_table, target_column):
        """
        :param data_base:
        :param input_table:
        :param target_column:要打标签的目标列
        """
        super().__init__(data_base)

        # 要打标签的数据 df
        self.INPUT_TABLE = self.SqlObj.select_table(input_table, target_column)

    # 传入要赋予标签的列
    def get_tag(self):
        print(self.INPUT_TABLE)
        self.OUTPUT_TABLE = None

# -----------------------数据清洗-----------------------#
# data = GenData()
# data.get_report_price()
# data.filter_data()
# GenDateData().get_trade_table()
# GenPriceData().get_tag_base()
GenTag('zyyx','')

