import time

import numpy as np
import pandas as pd
from tqdm import tqdm


class CalDiv:
    def __init__(self):
        # ----------------获取用于计算的数据----------------#
        self.MV_TABLE = pd.read_parquet('mv.parquet')
        self.DIV_TABLE = pd.read_parquet('AShareDividend.parquet')
        # self.DIV_TABLE = self.DIV_TABLE.iloc[:10000, :]
        # self.MV_TABLE = self.MV_TABLE.iloc[-10000:, :]
        # 生成的中间表
        self.DIV_YEAR_TABLE = pd.DataFrame()
        # 输出的静态股息表
        self.DIV_RATE_TABLE = pd.DataFrame()
        # 输出的预期股息表
        self.MERGE_TABLE = pd.DataFrame()

        # ----------------筛选计算列----------------#
        self.DIV_TABLE = self.DIV_TABLE[self.DIV_TABLE['s_div_progress'] == '3']  # 只保留3
        # self.DIV_TABLE['ex_dt'].dropna(inplace=True)  # 去掉无除息除权的
        self.DIV_TABLE = self.DIV_TABLE[['stockcode', 'report_period',
                                         'ann_date',
                                         # 's_div_prelandate', 's_div_preanndt',
                                         # 's_div_smtgdate', 'dvd_ann_dt', 'ex_dt',

                                         'cash_dvd_per_sh_pre_tax', 's_div_baseshare']]
        self.MV_TABLE = self.MV_TABLE[['stockcode', 'ann_date', 's_val_mv']]

        # ----------------参数区----------------#
        self.LAG_PERIOD = [1, 2, 3, 4]  # 滞后期
        self.MERGE_COLUMN = ['report_year', 'dvd_pre_tax_sum', 'ann_date_max']  # 需要合并的列名
        self.RENAME_COLUMN = {i: i + '_l{}' for i in self.MERGE_COLUMN}

    # 生成年度股息表
    def get_div_by_year(self):
        # ----------------提取report_period中的年份----------------#
        self.DIV_TABLE['report_year'] = self.DIV_TABLE['report_period'].astype('str').str[:-4].astype('int')

        # ----------------计算总股息----------------#
        self.DIV_TABLE['dvd_pre_tax'] = self.DIV_TABLE['cash_dvd_per_sh_pre_tax'] * self.DIV_TABLE[
            's_div_baseshare'] * 10000

        # ----------------获取股票名称----------------#
        # 并行计算 #
        for code in tqdm(self.DIV_TABLE['stockcode'].unique()):
            # 选取单个股票
            df_code = self.DIV_TABLE[self.DIV_TABLE['stockcode'] == code]

            # 按照年份聚合:税前股息累加,税前股息计数,每年最后的公告日期
            df_date = df_code.groupby(['report_year'], sort='report_year').agg(
                {'dvd_pre_tax': [('dvd_pre_tax_sum', 'sum'), ('dvd_pre_tax_count', 'count')],
                 'ann_date': [('ann_date_max', 'max')]
                 }
            )
            # 处理agg后的列名
            df_date.columns = df_date.columns.droplevel(0)
            df_date.reset_index(inplace=True)
            # 增加stockcode名
            df_date['stockcode'] = code

            # ----------------获取历史股息信息----------------#
            for lag_t in self.LAG_PERIOD:  # 最近2次年报的股息
                lag = pd.merge(df_date[self.MERGE_COLUMN],
                               pd.DataFrame(df_date['report_year'] - lag_t),
                               how='right', on=['report_year'], )
                lag.rename(columns={k: str(v).format(lag_t) for k, v in self.RENAME_COLUMN.items()}, inplace=True)
                df_date = pd.concat([df_date, lag], axis=1)

            # df_date_lag['va'] = df_date[df_date['report_year'] == df_date_lag['report_year']]['dvd_pre_tax_sum']

            # time.sleep(1111)

            # #近期算法
            # df_date['report_year_l{}'.format(lag_t)] = df_date['report_year'].shift(lag_t)
            # df_date['dvd_pre_tax_sum_l{}'.format(lag_t)] = df_date['dvd_pre_tax_sum'].shift(lag_t)
            # df_date['dvd_pre_tax_count_l{}'.format(lag_t)] = df_date['dvd_pre_tax_count'].shift(lag_t)
            # df_date['ann_date_max_l{}'.format(lag_t)] = df_date['ann_date_max'].shift(lag_t)
            # 拼接
            self.DIV_YEAR_TABLE = pd.concat([self.DIV_YEAR_TABLE, df_date])

        # ----------------获取滞后年报信息----------------#
        # for lag_t in self.LAG_PERIOD:  # 最近2次年报的股息
        #     获取上一年的报告期
        #     self.DIV_YEAR_TABLE['report_year_l{}'.format(lag_t)] = self.DIV_YEAR_TABLE['report_year'] - lag_t
        # 获取上一年的股息和
        # df_m =
        # print(df_m)

        # ----------------保存----------------)
        self.DIV_YEAR_TABLE.to_csv('div_by_year.csv', index=False)

    # 计算股息率

    def get_div_rate(self):
        # ----------------生成id用于匹配历史股息----------------#
        tqdm.pandas(desc='Start get_id')

        # 根据ann_date的前4位的年份生成id
        self.MV_TABLE['id'] = self.MV_TABLE[['ann_date', 'stockcode']].progress_apply(
            lambda x: x['stockcode'] + str(int(str(x['ann_date'])[:-4]) - 1), axis=1)

        # ----------------合并市值表和股息表 计算股息率----------------#
        self.DIV_RATE_TABLE = pd.merge(self.MV_TABLE, self.DIV_YEAR_TABLE, how='left', on=['id'])

        # 最近3次年报的股息
        tqdm.pandas(desc='Start get_div_rate')
        history_div = ['div_rate_l{}'.format(i) for i in self.LAG_PERIOD]

        self.DIV_RATE_TABLE[history_div] = self.DIV_RATE_TABLE.progress_apply(
            lambda x: [10000 * x['dvd_pre_tax_l{}'.format(i)] / x['s_val_mv'] for i in self.LAG_PERIOD],
            axis=1,
            result_type='expand')

        self.DIV_RATE_TABLE.to_csv('divrate.csv')
        # self.DIV_RATE_TABLE.to_parquet('divrate.parquet')

    def get_exp_dr(self):
        self.DIV_RATE_TABLE = pd.read_csv('603630.csv')

        self.DIV_RATE_TABLE = self.DIV_RATE_TABLE[
            ['stockcode', 'ann_date', 'report_year', 'dvd_pre_tax',
             'report_year_l1', 'dvd_pre_tax_l1', ]]

        # ----------------获取股票名称----------------#
        self.DIV_RATE_TABLE = self.DIV_RATE_TABLE[self.DIV_RATE_TABLE['stockcode'] == '603630.SH']

        # 按照时间降序
        self.DIV_RATE_TABLE.sort_values(by='ann_date', ascending=False, inplace=True)

        print(self.DIV_RATE_TABLE)
        # self.DIV_RATE_TABLE.to_csv('603630.csv')

    # 重新算分红
    def get_merge_table(self):
        # 股息表
        self.DIV_YEAR_TABLE = pd.read_csv('div_by_year.csv')
        # 给成prelanddate
        # 用上一年的匹配
        self.MV_TABLE['report_year'] = (self.MV_TABLE['ann_date'].astype('str').str[:-4].astype('int')) - 1
        self.DIV_RATE_TABLE = pd.merge(self.MV_TABLE, self.DIV_YEAR_TABLE, how='left', on=['stockcode', 'report_year'])
        # 增加静态计算逻辑列
        # self.DIV_RATE_TABLE.to_csv('merge.csv', index=False)
        self.DIV_RATE_TABLE.to_parquet('merge.parquet', index=False)

    # 外推法计算预期
    def get_exp_div(self):
        #
        self.MERGE_TABLE = pd.read_parquet('merge.parquet')
        self.MERGE_TABLE = self.MERGE_TABLE.iloc[:1000, :]
        # ----------------在截面数据中计算----------------#
        # 静态股利
        self.DIV_RATE_TABLE['last_div'] = self.DIV_RATE_TABLE.apply(lambda x: x['dvd_pre_taxsum'] if x[''] > 0 else 0)
        print(self.MERGE_TABLE)

    def get_no_history(self):
        pd.options.mode.chained_assignment = None

        # ----------------计算总股息----------------#
        self.DIV_TABLE['dvd_pre_tax'] = self.DIV_TABLE['cash_dvd_per_sh_pre_tax'] * self.DIV_TABLE[
            's_div_baseshare']
        self.DIV_TABLE['report_year'] = self.DIV_TABLE['report_period'].astype('str').str[:-4]
        # wind里面有已经实施但是ex_dt字段为空的值
        # 不用ex_dt作为参照
        self.DIV_TABLE['ann_date'] = self.DIV_TABLE['ann_date'].astype('int')
        self.MV_TABLE['ann_date'] = self.MV_TABLE['ann_date'].astype('int')

        # -------------------按照个股迭代------------- #
        # 筛选出MV表中历史信息 ex_dt
        for code in tqdm(self.MV_TABLE['stockcode'].unique()):
            # 选取单个股票
            df_div = self.DIV_TABLE[self.DIV_TABLE['stockcode'] == code]
            df_code = self.MV_TABLE[self.MV_TABLE['stockcode'] == code]

            # -------------------按照日期逐步迭代------------- #
            # 按照当前日期搜索历史信息
            def search_history(x):
                # 按照ann_date和ex_dt筛选历史股息信息
                div_info = df_div[df_div['ann_date'] < x['ann_date']]

                # 按照年份计算筛选后历史的股息
                div_info_year = div_info.groupby(['report_year']).agg({'dvd_pre_tax': 'sum', 'stockcode': 'count'})
                div_info_year.reset_index(inplace=True)
                # if not div_info_year.empty:
                #     print(div_info_year)
                # 滞后

                div_info_year = div_info_year[div_info_year['report_year'] == str(int(str(x['ann_date'])[:-4]) - 1)]
                # if not div_info_year.empty:
                #     print(div_info_year)

                return div_info_year['dvd_pre_tax'].values * 10000, div_info_year['stockcode'].values
                # if div_info_year.empty:
                #     return None, None
                # else:
                #     print(div_info_year['dvd_pre_tax'].loc[0], div_info_year['stockcode'].loc[0])
                #     return div_info_year['dvd_pre_tax'].loc[0], div_info_year['stockcode'].loc[0]

            #
            df_code[['dvd_l1', 'dvd_count_l1']] = df_code.apply(lambda x: search_history(x), axis=1,
                                                                result_type='expand')
            self.DIV_RATE_TABLE = pd.concat([self.DIV_RATE_TABLE, df_code])

        self.DIV_RATE_TABLE.to_csv('history.csv')
        # print(self.DIV_TABLE['report_year'])

        # 匹配
        # self.DIV_RATE_TABLE = pd.merge(self.MV_TABLE, self.DIV_YEAR_TABLE, how='left', on=['code', ''])
        # 去除匹配后时间错误的列


if __name__ == '__main__':
    app = CalDiv()
    # 计算年度分红
    # app.get_div_by_year()

    # 合并年度分红到每天
    # app.get_merge_table()

    # 用合并后的表计算预期
    app.get_exp_div()
