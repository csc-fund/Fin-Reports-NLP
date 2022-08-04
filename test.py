import re

str1 = '$tradedate<20220201.mix.sa<=adsadsa='
r1 = re.compile(r'\$tradedate<')
r2 = re.compile(r'(?![0-9]+){8}[0-9a-zA-Z_,>,<,=,!,.]+')

s1 = re.sub(r1, '', str1)
s2 = re.sub(r2, '', s1)
print(s2)
