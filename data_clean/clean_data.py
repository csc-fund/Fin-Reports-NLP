import re
from data_clean.mysql_tool import *


# 语言处理类
class GenData:
    def __init__(self):
        self.SqlObj = MysqlDao()
        self.df = self.SqlObj.select_table(table_name=MYSQL_TABLENAME,
                                           select_column=MYSQL_COLUMN,
                                           filter_dict={"LIMIT": 1000})

    # 获取公告后的价格用于打标签
    def get_report_price(self):
        pass


    # 筛选有效数据
    def filter_data(self):
        # 排除非个股报告
        self.df = self.df[self.df['report_type'] != 21]
        # 选择有冒号的数据
        # self.df = self.df[self.df['report_type'].str.contains('：')]
        # 选择指定长度的数据
        # self.df = self.df[self.df['title'].str.contains('：')]

    # 去除无效数据
    def clean_noise(self):
        def delete_tag(s):
            r1 = re.compile(r'\{IMG:.?.?.?\}')  # 图片
            s = re.sub(r1, '', s)
            r2 = re.compile(r'[a-zA-Z]+://[^\u4e00-\u9fa5|\?]+')  # 网址
            s = re.sub(r2, '', s)
            r3 = re.compile(r'<.*?>')  # 网页标签
            s = re.sub(r3, '', s)
            r4 = re.compile(r'&[a-zA-Z0-9]{1,4}')  # &nbsp  &gt  &type &rdqu   ....
            s = re.sub(r4, '', s)
            r5 = re.compile(r'[0-9a-zA-Z]+@[0-9a-zA-Z]+')  # 邮箱
            s = re.sub(r5, '', s)
            r6 = re.compile(r'[#]')  # #号
            s = re.sub(r6, '', s)
            return s

        self.df['title'] = self.df[['title']].apply(lambda x: delete_tag(x) if str(x) != 'nan' else x)


# -----------------------数据清洗-----------------------#


# -----------------------数据清洗-----------------------#
data = GenData()
data.filter_data()
print(data.df)
