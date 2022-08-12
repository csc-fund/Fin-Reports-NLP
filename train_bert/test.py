#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :test.py
# @Time      :2022/8/12 10:34
# @Author    :Colin
# @Note      :None


import tushare as ts
pro = ts.pro_api('56a12424870cd0953907cde2c660b498c8fe774145b7f17afdc746dd')
df = pro.index_daily(ts_code='399300.SZ')
# print(df)