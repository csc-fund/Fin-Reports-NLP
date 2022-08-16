import time

import numpy as np
import pandas as pd
from tqdm import tqdm

# ----------------参数和命名----------------#
LAG_PERIOD = 4  # 滞后期 当前时期为T,该参数表示使用了[T-2,T-3...,T-LAG_PERIOD]来预测T-1
REFER_DATE = 'ann_date'  # ann_date,s_div_prelandate
MERGE_COLUMN = ['report_year', 'dvd_pre_tax_sum', REFER_DATE + '_max']  # 计算出的列的命名

# 线性回归使用的参数
VAR_X = np.var(range(LAG_PERIOD - 1), ddof=1)  # 计算X的方差 (样本 自由度-1)
AVG_X = np.average(range(LAG_PERIOD - 1))  # 计算X的均值
AVG_COLUMN = 'AVG_DIV_{}'.format(LAG_PERIOD - 1)  # 分红列平均值
VAR_COLUMN = 'VAR_DIV_{}'.format(LAG_PERIOD - 1)  # 分红列的方差
PRE_COLUMN = ['DIV_L{}'.format(i) for i in reversed(range(LAG_PERIOD)) if i != 0]  # 用于预测的滞后年
PRODUCT_COLUMN = ['PRODUCT_{}'.format(i) for i in reversed(range(LAG_PERIOD)) if i != 0]  # 积距
PRED_COLUMN = 'PRED_DIV_{}'.format(LAG_PERIOD)  # 预测出的分红

# ----------------读取原始数据----------------#
MV_TABLE = pd.read_parquet('mv.parquet')
DIV_TABLE = pd.read_parquet('AShareDividend.parquet')

# ----------------筛选计算列----------------#
DIV_TABLE = DIV_TABLE[DIV_TABLE['s_div_progress'] == '3']  # 只保留3
DIV_TABLE = DIV_TABLE[['stockcode', 'report_period', REFER_DATE, 'cash_dvd_per_sh_pre_tax', 's_div_baseshare']]
MV_TABLE = MV_TABLE[['stockcode', 'ann_date', ]]


# 生成年度股息表
def get_div_by_year():
    # ----------------提取report_period中的年份----------------#
    DIV_TABLE['report_year'] = DIV_TABLE['report_period'].astype('str').str[:-4].astype('int')
    DIV_TABLE['dvd_pre_tax'] = DIV_TABLE['cash_dvd_per_sh_pre_tax'] * DIV_TABLE['s_div_baseshare'] * 10000  # 计算总股息

    # ----------------在股票名称中循环----------------#
    DIV_YEAR_TABLE = pd.DataFrame()  # 输出的年度分红表
    RENAME_COLUMN = {i: i + '_l{}' for i in MERGE_COLUMN}
    for code in tqdm(DIV_TABLE['stockcode'].unique()):
        # 选取单个股票
        df_code = DIV_TABLE[DIV_TABLE['stockcode'] == code]
        # 按照年份聚合:税前股息累加,税前股息计数,每年最后的日期
        df_date = df_code.groupby(['report_year'], sort='report_year').agg(
            {'dvd_pre_tax': [('dvd_pre_tax_sum', 'sum')],
             REFER_DATE: [('{}_max'.format(REFER_DATE), 'max')]
             })
        # agg后的列名处理
        df_date.columns = df_date.columns.droplevel(0)
        df_date.reset_index(inplace=True)
        df_date['stockcode'] = code  # 增加stockcode名

        # ----------------合并历史股息信息----------------#
        for lag_t in range(LAG_PERIOD):
            # 根据report_year在df_date中获得历史数据
            lag = pd.merge(df_date[MERGE_COLUMN], pd.DataFrame(df_date['report_year'] - lag_t - 1),
                           how='right', on=['report_year'], )
            lag.rename(columns={k: str(v).format(lag_t + 1) for k, v in RENAME_COLUMN.items()}, inplace=True)  # 列名处理
            # 合并历史数据
            df_date = pd.concat([df_date, lag], axis=1)

        # 拼接每只股票
        DIV_YEAR_TABLE = pd.concat([DIV_YEAR_TABLE, df_date])

    # ----------------合并年度分红表与市值表----------------#
    # 用上一年的匹配
    MV_TABLE['report_year'] = (MV_TABLE['ann_date'].astype('str').str[:-4].astype('int')) - 1
    MERGE_TABLE = pd.merge(MV_TABLE, DIV_YEAR_TABLE, how='left', on=['stockcode', 'report_year'])
    # 保存合并后的 代码-日期-分红表
    MERGE_TABLE.rename(columns={i: i + '_l0' for i in RENAME_COLUMN}, inplace=True)  # 重命名列便于区分
    MERGE_TABLE.to_parquet('merge.parquet', index=False)


# 计算预期股息
def get_exp_div():
    st_time = time.time()
    # 用合并后的表计算
    MERGE_TABLE = pd.read_parquet('merge.parquet')
    # MERGE_TABLE = MERGE_TABLE.iloc[-10000:, :]  # 取部分样本核对

    # ----------------计算能够使用的历史分红信息----------------#
    for i in range(LAG_PERIOD):
        # 使用当前日期与历史发布日期比较大小
        MERGE_TABLE['INFO_L{}'.format(i)] = np.where(
            MERGE_TABLE['ann_date'].astype('float') > MERGE_TABLE[REFER_DATE + '_max_l{}'.format(i)].astype('float'), 1,
            0)
    # ----------------把历史信息矩阵映射到分红的值----------------#
    for i in range(LAG_PERIOD):
        MERGE_TABLE['DIV_L{}'.format(i)] = np.where(
            MERGE_TABLE['INFO_L{}'.format(i)] == 0, 0, MERGE_TABLE['dvd_pre_tax_sum_l{}'.format(i)])

    # ----------------当前没有T-1期分红信息的处理1:用T-2期分红信息替代----------------#
    MERGE_TABLE['INFO_L01'] = np.where(
        MERGE_TABLE['INFO_L0'] == 0, MERGE_TABLE['ann_date'] - MERGE_TABLE[REFER_DATE + '_max_l1'].astype('float'), 0)
    MERGE_TABLE['INFO_L01'] = np.where(
        (20000 > MERGE_TABLE['INFO_L01']) & (MERGE_TABLE['INFO_L01'] > 0), 1, 0)  # 只用2年内的分红信息
    # MERGE_TABLE['INFO_L01'].fillna(0, inplace=True)
    # 在原有的T-1期为0的数据上填充
    MERGE_TABLE['INFO_L01'] = np.where(
        (MERGE_TABLE['INFO_L0'] == 0) & (MERGE_TABLE['INFO_L01'] == 1), 1, 0)
    # 映射到T-2期分红的值
    MERGE_TABLE['DIV_L01'] = np.where(
        MERGE_TABLE['INFO_L01'] == 1, MERGE_TABLE['dvd_pre_tax_sum_l1'], MERGE_TABLE['DIV_L0'])

    # ----------------当前没有T-1期分红信息的处理2:OLS线性预测----------------#
    # 计算Y的均值
    MERGE_TABLE[AVG_COLUMN] = np.average(MERGE_TABLE[PRE_COLUMN].astype('float'), axis=1)
    # 计算Y的方差 (样本 自由度-1)
    MERGE_TABLE[VAR_COLUMN] = np.var(MERGE_TABLE[PRE_COLUMN].astype('float'), axis=1, ddof=1)

    # ----------------计算XY的协方差----------------#
    # 逐个计算出积距: (Yi-Y)*(Xi-X)
    for i in range(LAG_PERIOD - 1):
        MERGE_TABLE[PRODUCT_COLUMN[i]] = (MERGE_TABLE[PRE_COLUMN[i]] - MERGE_TABLE[AVG_COLUMN]) * (i - AVG_X)

    # 协方差(样本): SUM(Yi-Y)*(Xi-X)/N-1
    MERGE_TABLE['COV_XY'] = (np.sum(MERGE_TABLE[PRODUCT_COLUMN], axis=1)) / (LAG_PERIOD - 2)

    # ----------------计算Beta_hat----------------#
    # 斜率: Beta_hat = COV(XY)/COV(XX)
    MERGE_TABLE['BETA_HAT'] = MERGE_TABLE['COV_XY'] / VAR_X

    # ----------------计算Alpha_hat---------------#
    # 截距: Alpha_hat=AVG(Y)-Beta_hat*AVG(X)
    MERGE_TABLE['ALPHA_HAT'] = MERGE_TABLE[AVG_COLUMN] - MERGE_TABLE['BETA_HAT'] * AVG_X

    # ----------------预测Y----------------#
    # 预测:
    MERGE_TABLE[PRED_COLUMN] = MERGE_TABLE['ALPHA_HAT'] + MERGE_TABLE['BETA_HAT'] * (LAG_PERIOD - 1)

    # ----------------去除预测的负值----------------#
    MERGE_TABLE[PRED_COLUMN] = np.where(MERGE_TABLE[PRED_COLUMN] < 0, 0, MERGE_TABLE[PRED_COLUMN])

    ed_time = time.time()
    print('计算用时: {} 秒'.format(ed_time - st_time))

    # ----------------保存结果----------------#
    # 输出真实值线性外推预测值
    # DIV_L0是T-1期年报真实值
    # PRED_COLUMN是使用参数LAG_PERIOD计算出的OLS线性预测值
    # MERGE_TABLE = MERGE_TABLE[['stockcode', 'ann_date', 'DIV_L0', 'DIV_L01'] + [PRED_COLUMN] + PRE_COLUMN]
    # MERGE_TABLE.to_csv('cal_div.csv', index=False)
    # print('保存用时: {} 秒'.format(time.time() - ed_time))


if __name__ == '__main__':
    # 生成年度股息表
    # get_div_by_year()
    # 计算预期股息
    get_exp_div()
