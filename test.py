import re

s = '$trade_dt<20220201'
s2 = '$trade_dt>20220201'
s3 = 'trade_dt>=20220201'
s4 = 'trade_dt!=20220201'

# r3 = re.compile(r'\$trade_dt[0-9a-zA-Z]+ ')
# r4 = re.compile(r'trade_dt[.,a-zA-Z,]')

# r4 = re.compile(r'trade_dt.isin')
s5 = '$trade_dt>20220201>s@'
s = re.findall(r"\d+\.?\d*", s5)
print(s)
