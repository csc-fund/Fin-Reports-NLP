import re

from tools.mysql_tool import MysqlDao

df_all = MysqlDao(dataBase='zyyx').select_table('rpt_earnings_adjust', ['stockcode', 'ann_date'])
# df['trade_date'] = df['trade_date'].astype('str')
# df.index = df['trade_date'].astype('str')
# df = df[df['trade_date'] == '2022-08-01']
# df = df.loc['2001-06-11', :]
# df_code = df[df['stockcode'] == '603915.SH']
df_index = df_all[(df_all['stockcode'] == '603915.SH')].index.values[0]
# print(df_index.values[0])
print(df_all.loc[df_index+1, ['stockcode']].values)
