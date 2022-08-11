import pandas as pd
from tqdm import tqdm


class CalDiv:
    def __init__(self):
        # ----------------获取用于计算的数据----------------#
        self.MV_TABLE = pd.read_parquet('mv.parquet')
        self.DIV_TABLE = pd.read_parquet('AShareDividend.parquet')
        # self.DIV_TABLE = self.DIV_TABLE.iloc[:10000, :]
        # self.MV_TABLE = self.MV_TABLE.iloc[:100000, :]
        # 生成的中间表
        self.DIV_YEAR_TABLE = pd.DataFrame()
        # 输出的表
        self.DIV_RATE_TABLE = pd.DataFrame()

        # ----------------筛选计算列----------------#
        self.DIV_TABLE = self.DIV_TABLE[self.DIV_TABLE['s_div_progress'] == '3']  # 只保留3
        self.DIV_TABLE = self.DIV_TABLE[['stockcode', 'report_period', 'ex_dt',
                                         'cash_dvd_per_sh_pre_tax', 's_div_baseshare']]
        self.MV_TABLE = self.MV_TABLE[['stockcode', 'ann_date', 's_val_mv']]

    # 生成年度股息表
    def get_div_by_year(self):
        # ----------------提取report_period中的年份----------------#
        self.DIV_TABLE['report_year'] = self.DIV_TABLE['report_period'].apply(
            lambda x: pd.to_datetime(x, format='%Y%m%d'))
        self.DIV_TABLE.set_index('report_year', inplace=True)

        # ----------------计算总股息----------------#
        self.DIV_TABLE['dvd_pre_tax'] = self.DIV_TABLE['cash_dvd_per_sh_pre_tax'] * self.DIV_TABLE[
            's_div_baseshare']

        # ----------------获取股票名称----------------#)
        for code in tqdm(self.DIV_TABLE['stockcode'].unique()):
            # 选取单个股票
            df_code = self.DIV_TABLE[self.DIV_TABLE['stockcode'] == code]

            # 按照年份聚合,累加税前股息
            df_date = df_code.groupby([df_code.index.year]).agg({'dvd_pre_tax': 'sum', 'stockcode': 'count'})
            df_date.rename(columns={'stockcode': 'dvd_count'}, inplace=True)  # 统计在该年份求和了多少次股息
            df_date.reset_index(inplace=True)

            # 滞后获取所有过去年份的股息
            df_date.sort_values(by='report_year', inplace=True, ascending=True)  # 按照年份升序
            for lag_t in [1, 2, 3]:  # 最近3次年报的股息
                df_date['report_year_l{}'.format(lag_t)] = df_date['report_year'].shift(lag_t)
                df_date['dvd_pre_tax_l{}'.format(lag_t)] = df_date['dvd_pre_tax'].shift(lag_t)
                df_date['dvd_count_l{}'.format(lag_t)] = df_date['dvd_count'].shift(lag_t)

            # 增加一列id用于匹配
            df_date['id'] = code + df_date['report_year'].astype('str')
            # 拼接
            self.DIV_YEAR_TABLE = pd.concat([self.DIV_YEAR_TABLE, df_date])

        # ----------------保存----------------)
        self.DIV_YEAR_TABLE.to_csv('div_by_year.csv')

    # 计算股息率
    def get_div_rate(self):
        # ----------------生成id用于匹配历史股息----------------#
        tqdm.pandas(desc='Start get_id')

        # 根据ann_date的前4位的年份生成id
        self.MV_TABLE['id'] = self.MV_TABLE[['ann_date', 'stockcode']].progress_apply(
            lambda x: x['stockcode'] + str(x['ann_date'])[:-4], axis=1)

        # ----------------合并市值表和股息表 计算股息率----------------#
        self.DIV_RATE_TABLE = pd.merge(self.MV_TABLE, self.DIV_YEAR_TABLE, how='left', on=['id'])

        # 最近3次年报的股息
        tqdm.pandas(desc='Start get_div_rate')
        history_div = ['div_rate_l{}'.format(i) for i in [1, 2, 3]]

        self.DIV_RATE_TABLE[history_div] = self.DIV_RATE_TABLE.progress_apply(
            lambda x: [10000 * x['dvd_pre_tax_l{}'.format(i)] / x['s_val_mv'] for i in [1, 2, 3]],
            axis=1,
            result_type='expand')

        self.DIV_RATE_TABLE.to_csv('divrate.csv')


if __name__ == '__main__':
    app = CalDiv()
    app.get_div_by_year()
    app.get_div_rate()
