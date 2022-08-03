import pandas as pd

# 数据库部分
MYSQL_HOST = '127.0.0.1'
MYSQL_NAME = 'root'
MYSQL_PASSWORD = ''
MYSQL_DATABASE = 'zyyx'
MYSQL_TABLENAME = 'rpt_earnings_adjust'
MYSQL_COLUMN = ["title", "report_id", "report_type",
                "stockcode", "stock_name",
                "organ_id", "organ_name",
                "author",
                "ann_date", "current_create_date", "previous_create_date",
                ]

# 必要参数设置
TUSHARE_VIPAPI = ['income_vip', 'balancesheet_vip', 'cashflow_vip', 'forecast_vip', 'express_vip', 'fina_mainbz_vip',
                  ]
TUSHARE_AK = '56a12424870cd0953907cde2c660b498c8fe774145b7f17afdc746dd'

# 日期设置
DATE_SHARE='399300.SZ'
DATE_START = "20050101"
DATE_END = "20221231"
DATE_LAGLIST = [1, 5, 10, 15, 20, 25, 30]
# 爬取的日期


# 初始链接

# 要爬取的栏目
