import numpy as np
import pandas as pd
from tqdm import tqdm

# ----------------参数区----------------#
LAG_PERIOD = [1, 2, 3, 4, 5]  # 滞后期
MERGE_COLUMN = ['report_year', 'dvd_pre_tax_sum', 'ann_date_max']  # 需要合并的列名
RENAME_COLUMN = {i: i + '_l{}' for i in MERGE_COLUMN}

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
    DIV_TABLE['dvd_pre_tax'] = DIV_TABLE['cash_dvd_per_sh_pre_tax'] * DIV_TABLE[
        's_div_baseshare'] * 10000  # 计算总股息

    DIV_YEAR_TABLE = pd.DataFrame()  # 生成的年度分红表
    # ----------------在股票名称中循环----------------#
    for code in tqdm(DIV_TABLE['stockcode'].unique()):
        # 选取单个股票
        df_code = DIV_TABLE[DIV_TABLE['stockcode'] == code]

        # 按照年份聚合:税前股息累加,税前股息计数,每年最后的公告日期
        df_date = df_code.groupby(['report_year'], sort='report_year').agg(
            {'dvd_pre_tax': [('dvd_pre_tax_sum', 'sum'), ('dvd_pre_tax_count', 'count')],
             'ann_date': [('ann_date_max', 'max')]
             }
        )

        # agg后的列名处理
        df_date.columns = df_date.columns.droplevel(0)
        df_date.reset_index(inplace=True)
        df_date['stockcode'] = code  # 增加stockcode名

        # ----------------获取历史股息信息----------------#
        for lag_t in LAG_PERIOD:  # 最近2次年报的股息
            lag = pd.merge(df_date[MERGE_COLUMN],
                           pd.DataFrame(df_date['report_year'] - lag_t),
                           how='right', on=['report_year'], )
            lag.rename(columns={k: str(v).format(lag_t) for k, v in RENAME_COLUMN.items()}, inplace=True)
            df_date = pd.concat([df_date, lag], axis=1)

        DIV_YEAR_TABLE = pd.concat([DIV_YEAR_TABLE, df_date])  # 拼接每只股票

    # ----------------保存----------------#
    DIV_YEAR_TABLE.to_parquet('div_by_year.parquet', index=False)


if __name__ == '__main__':
    get_div_by_year()  # 生成年度股息表
