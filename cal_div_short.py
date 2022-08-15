import numpy as np
import pandas as pd
from tqdm import tqdm

# ----------------参数区----------------#
LAG_PERIOD = 5  # 滞后期
MERGE_COLUMN = ['report_year', 'dvd_pre_tax_sum', 'ann_date_max']  # 需要合并的列名
REFER_DATE = 's_div_prelandate'  # ann_date,s_div_prelandate

# ----------------读取原始数据----------------#
MV_TABLE = pd.read_parquet('mv.parquet')
DIV_TABLE = pd.read_parquet('AShareDividend.parquet')

# ----------------筛选计算列----------------#
DIV_TABLE = DIV_TABLE[DIV_TABLE['s_div_progress'] == '3']  # 只保留3
DIV_TABLE = DIV_TABLE[
    ['stockcode', 'report_period', 'ann_date', 's_div_prelandate', 'cash_dvd_per_sh_pre_tax', 's_div_baseshare']]
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
             's_div_prelandate': [('{}_max'.format(REFER_DATE), 'max')]
             })

        # agg后的列名处理
        df_date.columns = df_date.columns.droplevel(0)
        df_date.reset_index(inplace=True)
        df_date['stockcode'] = code  # 增加stockcode名

        # ----------------合并历史股息信息----------------#
        for lag_t in range(LAG_PERIOD):
            # 根据report_year在df_date中获得历史数据
            lag = pd.merge(df_date[MERGE_COLUMN], pd.DataFrame(df_date['report_year'] - lag_t + 1),
                           how='right', on=['report_year'], )
            lag.rename(columns={k: str(v).format(lag_t) for k, v in RENAME_COLUMN.items()}, inplace=True)  # 列名处理
            # 合并历史数据
            df_date = pd.concat([df_date, lag], axis=1)

        # 拼接每只股票
        DIV_YEAR_TABLE = pd.concat([DIV_YEAR_TABLE, df_date])

    # ----------------保存----------------#
    DIV_YEAR_TABLE.to_parquet('div_by_year.parquet', index=False)


# 计算预期股息
def get_exp_div():
    # 年度股息表
    DIV_YEAR_TABLE = pd.read_parquet('div_by_year.parquet')
    # 用上一年的匹配
    MV_TABLE['report_year'] = (MV_TABLE['ann_date'].astype('str').str[:-4].astype('int')) - 1
    DIV_RATE_TABLE = pd.merge(MV_TABLE, DIV_YEAR_TABLE, how='left', on=['stockcode', 'report_year'])
    # 增加静态计算逻辑列
    # DIV_RATE_TABLE.to_csv('merge.csv', index=False)
    DIV_RATE_TABLE.to_parquet('merge.parquet', index=False)
    pass


if __name__ == '__main__':
    get_div_by_year()  # 生成年度股息表
