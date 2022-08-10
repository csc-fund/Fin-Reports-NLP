# 使用 read_parquet 加载parquet文件
import pandas as pd
from pandas import read_parquet
from tools.mysql_tool import MysqlDao

ms = MysqlDao(data_base='zyyx')
df = read_parquet("AShareDividend.parquet")
ms.insert_table('AShareDividend', df)
