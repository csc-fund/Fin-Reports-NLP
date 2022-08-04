# TUSHARE参数设置
TUSHARE_AK = '56a12424870cd0953907cde2c660b498c8fe774145b7f17afdc746dd'

# 数据库部分
MYSQL_TABLENAME = 'rpt_e_a'
TABLE_NULLPRICE = 'find_null_price'
VIEW_RETURN = 'view_rpt_price_0_5'
MYSQL_COLUMN = ["title", "report_id", "report_type",
                "stockcode", "stock_name",
                "organ_id", "organ_name",
                "author",
                "ann_date", "current_create_date", "previous_create_date",
                ]
MYSQL_INSERT_TABLE_ALL = 'rpt_price'
MYSQL_INSERT_TABLE = 'rpt_price_l'
MYSQL_STRUCT = {"title": "VARCHAR(150)", "report_id": "INT", "report_type": "INT",
                "stockcode": "VARCHAR(20)", "stock_name": "VARCHAR(50)",
                "organ_id": "INT", "organ_name": "VARCHAR(20)",
                "author": "VARCHAR(100)",
                "current_create_date": "DATE", "previous_create_date": "DATE",
                "ann_date": "DATE",
                "PK": "report_id"}
MYSQL_LIMIT = 500

# 要读取的打好标签的数据
TABLE_TAG_BASE = 'TAG_BASE_REPORT'

# 日期设置
DATE_SHARE = '399300.SZ'
DATE_START = "20050101"
DATE_END = "20221231"
DATE_LAGLIST_ALL = [0, 1, 5, 10, 15, 20, 25, 30]
DATE_LAGLIST = [0, 1, 5]
DATE_TABLE = 'natural_trade_date'

# 数据处理
LEFT_TAIL = 0.05
RIGHT_TAIL = 0.95
